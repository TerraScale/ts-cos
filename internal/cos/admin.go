package cos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// userBuckets are the stores that keep per-user keys ("u.<ULID>.<key>"). Every new per-user store must be
// listed here, or deleting its user leaves that user's data behind.
var userBuckets = []string{chatBucket, agentBucket, contextBucket, memoryBucket, inboxBucket, pushBucket}

// isAdmin reports whether the signed-in user may open /admin; the top bar links to it only then.
func isAdmin(ctx context.Context) bool {
	u, err := UserFrom(ctx)
	return err == nil && u.Role == roleAdmin
}

// adminRow is one account in the user list.
type adminRow struct {
	User     User
	Created  time.Time
	Disabled time.Time // zero when the account is enabled
	Pending  bool      // no password yet: the invite link is the way in
}

// active is an account that can sign in, the only kind that keeps the last admin in place.
func (r adminRow) active() bool { return r.Disabled.IsZero() && !r.Pending }

func (r adminRow) State() string {
	switch {
	case !r.Disabled.IsZero():
		return "disabled"
	case r.Pending:
		return "invite pending"
	}
	return "active"
}

// accountByID reads account id's record through the stream leader; found is false when there is none.
func (a *Auth) accountByID(ctx context.Context, id string) (authUser, uint64, bool, error) {
	if !ulidPattern.MatchString(id) {
		return authUser{}, 0, false, nil
	}
	value, revision, found, err := leaderGet(ctx, a.nc, authBucket, "uid."+id)
	if err != nil || !found {
		return authUser{}, 0, false, err
	}
	var rec authUser
	err = json.Unmarshal(value, &rec)
	return rec, revision, err == nil, err
}

// adminRows lists every account, oldest first. The key list is local; each record comes from the stream leader.
func (a *Auth) adminRows(ctx context.Context) ([]adminRow, error) {
	users, _, _ := a.kv()
	if users == nil {
		return nil, errors.New("COS is starting")
	}
	lister, err := users.ListKeysFiltered(ctx, "uid.*")
	if err != nil {
		return nil, err
	}
	var ids []string
	for key := range lister.Keys() {
		ids = append(ids, strings.TrimPrefix(key, "uid."))
	}
	rows := make([]adminRow, 0, len(ids))
	for _, id := range ids {
		rec, _, found, err := a.accountByID(ctx, id)
		if err != nil {
			return nil, err
		}
		if found {
			rows = append(rows, adminRow{User: User{ID: id, Name: rec.Name, Role: rec.Role}, Created: rec.Created, Disabled: rec.DisabledAt, Pending: rec.Bcrypt == ""})
		}
	}
	slices.SortFunc(rows, func(x, y adminRow) int { return x.Created.Compare(y.Created) })
	return rows, nil
}

// findRow returns the listed account with ID id.
func findRow(rows []adminRow, id string) (adminRow, bool) {
	i := slices.IndexFunc(rows, func(r adminRow) bool { return r.User.ID == id })
	if i < 0 {
		return adminRow{}, false
	}
	return rows[i], true
}

// guardRemoval refuses to disable or delete the signed-in admin and the last active admin. It returns the
// message to show, or "" when the change may go ahead.
// Accepted residual: two admins disabling each other at the same instant can leave no active admin, because these
// reads are not atomic with the write.
func guardRemoval(actor, target User, rows []adminRow) string {
	if target.ID == actor.ID {
		return "You cannot disable or delete your own account."
	}
	if target.Role == roleAdmin && !slices.ContainsFunc(rows, func(r adminRow) bool {
		return r.User.Role == roleAdmin && r.active() && r.User.ID != target.ID
	}) {
		return "The last active admin cannot be disabled or deleted."
	}
	return ""
}

// setAccountDisabled stamps or clears DisabledAt with a compare-and-set. Disabling also signs out every session
// of the account, on every node.
// Accepted residual: a login that verified just before the stamp can still create one session after the sweep.
func (a *Auth) setAccountDisabled(ctx context.Context, id string, disabled bool) error {
	users, sessions, _ := a.kv()
	if users == nil || sessions == nil {
		return errors.New("COS is starting")
	}
	rec, revision, found, err := a.accountByID(ctx, id)
	if err != nil {
		return err
	}
	if !found {
		return errors.New("account not found")
	}
	if disabled == rec.DisabledAt.IsZero() {
		rec.DisabledAt = time.Time{}
		if disabled {
			rec.DisabledAt = time.Now().UTC()
		}
		value, _ := json.Marshal(rec)
		if _, err := users.Update(ctx, "uid."+id, value, revision); err != nil {
			return err
		}
	}
	if disabled && !revokeSessions(ctx, sessions, id) {
		return errors.New("sessions could not be revoked")
	}
	return nil
}

// brainThread is the COS thread that user id's brain has open, if any.
func brainThread(ctx context.Context, nc *nats.Conn, id string) (AgentState, bool, error) {
	value, _, found, err := leaderGet(ctx, nc, agentBucket, User{ID: id}.Key("state"))
	if err != nil || !found {
		return AgentState{}, false, err
	}
	var st AgentState
	if err := json.Unmarshal(value, &st); err != nil {
		return AgentState{}, false, err
	}
	return st, st.Host != "" && st.ThreadID != "", nil
}

// settleBrainThread closes the thread before any data goes. A thread the host still holds open (an active run
// skips the close) is an error, so no data is deleted under a live thread.
func settleBrainThread(ctx context.Context, nc *nats.Conn, threads threadCloser, id string) error {
	st, open, err := brainThread(ctx, nc, id)
	if err != nil || !open {
		return err
	}
	var res TreeResult
	if err := threads.Call(ctx, st.Host, "close", []string{st.ThreadID}, &res); err != nil {
		return err
	}
	if len(res.Skipped) > 0 {
		return fmt.Errorf("thread %s is still open", st.ThreadID)
	}
	return nil
}

// bucketKeys is how many live keys one per-user bucket holds for one user.
type bucketKeys struct {
	Bucket string
	Keys   int
}

// userDataCounts counts id's live keys in each of userBuckets, for the delete confirmation.
func userDataCounts(ctx context.Context, js jetstream.JetStream, id string) ([]bucketKeys, error) {
	var out []bucketKeys
	for _, bucket := range userBuckets {
		kv, err := js.KeyValue(ctx, bucket)
		if errors.Is(err, jetstream.ErrBucketNotFound) {
			out = append(out, bucketKeys{bucket, 0})
			continue
		}
		if err != nil {
			return nil, err
		}
		lister, err := kv.ListKeysFiltered(ctx, User{ID: id}.Key(">"))
		if err != nil {
			return nil, err
		}
		n := 0
		for range lister.Keys() {
			n++
		}
		out = append(out, bucketKeys{bucket, n})
	}
	return out, nil
}

// purgeUserData drops every record of user id from userBuckets. Each key gets a KV purge first, so watchers on
// other nodes (the Chat mirror, the inbox mirror) see a PURGE and drop the entry. The stream purge by subject then
// removes the history and those markers, so no old revision survives.
func purgeUserData(ctx context.Context, js jetstream.JetStream, id string) error {
	for _, bucket := range userBuckets {
		kv, err := js.KeyValue(ctx, bucket)
		if errors.Is(err, jetstream.ErrBucketNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		lister, err := kv.ListKeysFiltered(ctx, User{ID: id}.Key(">"))
		if err != nil {
			return err
		}
		var keys []string
		for key := range lister.Keys() {
			keys = append(keys, key)
		}
		for _, key := range keys {
			if err := kv.Purge(ctx, key); err != nil {
				return err
			}
		}
		stream, err := js.Stream(ctx, "KV_"+bucket)
		if err != nil {
			return err
		}
		if err := stream.Purge(ctx, jetstream.WithPurgeSubject("$KV."+bucket+"."+User{ID: id}.Key(">"))); err != nil {
			return err
		}
	}
	return nil
}

// sweepDeletedUsers purges the data of every owner whose account is gone. A crash or a failed step in remove can
// leave data behind, and a brain step can write after the account is deleted. It runs on the leader only. An owner
// is purged only when its account read comes back not found with no error; any read error skips that owner.
func sweepDeletedUsers(ctx context.Context, js jetstream.JetStream, nc *nats.Conn) (int, error) {
	owners := map[string]bool{}
	for _, bucket := range userBuckets {
		kv, err := js.KeyValue(ctx, bucket)
		if errors.Is(err, jetstream.ErrBucketNotFound) {
			continue
		}
		if err != nil {
			return 0, err
		}
		lister, err := kv.ListKeys(ctx)
		if err != nil {
			return 0, err
		}
		for key := range lister.Keys() {
			if owner, _, ok := splitUserKey(key); ok && owner != probeUserID {
				owners[owner] = true
			}
		}
	}
	var errs []error
	purged := 0
	for owner := range owners {
		_, _, found, err := leaderGet(ctx, nc, authBucket, "uid."+owner)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if found {
			continue
		}
		if err := purgeUserData(ctx, js, owner); err != nil {
			errs = append(errs, err)
			continue
		}
		purged++
	}
	return purged, errors.Join(errs...)
}

// requireAdmin answers 404 to everyone else, so /admin does not reveal that it exists.
func requireAdmin(next http.HandlerFunc) http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		if !isAdmin(r.Context()) {
			http.NotFound(rw, r)
			return
		}
		next(rw, r)
	}
}
