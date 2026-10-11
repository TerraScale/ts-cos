package cos

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// User is a signed-in COS account. Every per-user store key starts with User.Key's "u.<ULID>." prefix.
type User struct {
	ID   string `json:"id"`   // ULID
	Name string `json:"name"` // login name, authUserPattern
	Role string `json:"role"` // roleAdmin or roleMember
}

const roleAdmin, roleMember = "admin", "member"

// probeUserID is the password-less deploy health probe ("cos-probe"); newULID never returns it (timestamp 0).
const probeUserID = "00000000000000000000000000"

var probeUser = User{ID: probeUserID, Name: "cos-probe", Role: roleMember}

var ErrNoUser = errors.New("no COS user in context")

var ulidPattern = regexp.MustCompile(`^[0-9A-HJKMNP-TV-Z]{26}$`)

type userKey struct{}

func WithUser(ctx context.Context, u User) context.Context {
	return context.WithValue(ctx, userKey{}, u)
}

// UserFrom fails closed: ErrNoUser unless ctx holds a User whose ID matches ulidPattern.
func UserFrom(ctx context.Context) (User, error) {
	u, ok := ctx.Value(userKey{}).(User)
	if !ok || !ulidPattern.MatchString(u.ID) {
		return User{}, ErrNoUser
	}
	return u, nil
}

// UserKey is UserFrom(ctx) then u.Key(key); stores call it so a missing user never falls back to a shared key.
func UserKey(ctx context.Context, key string) (string, error) {
	u, err := UserFrom(ctx)
	if err != nil {
		return "", err
	}
	return u.Key(key), nil
}

func (u User) Key(key string) string { return "u." + u.ID + "." + key }

// splitUserKey reads a stored "u.<ULID>.<key>" key; ok is false for legacy keys, which wait for migrateToUser.
func splitUserKey(key string) (owner, id string, ok bool) {
	rest, ok := strings.CutPrefix(key, "u.")
	if !ok {
		return "", "", false
	}
	owner, id, ok = strings.Cut(rest, ".")
	return owner, id, ok && ulidPattern.MatchString(owner) && id != ""
}

// UserKV is kv seen as the user in ctx: every key is stored as "u.<ULID>.<key>" and returned without the prefix.
// It fails closed with ErrNoUser, so a store without a user never touches shared keys.
func UserKV(ctx context.Context, kv jetstream.KeyValue) (jetstream.KeyValue, error) {
	u, err := UserFrom(ctx)
	if err != nil {
		return nil, err
	}
	return userKV{kv: kv, prefix: u.Key("")}, nil
}

// userKV implements every jetstream.KeyValue method explicitly, without embedding the interface, so a
// method nats.go adds later fails to compile here instead of passing through unscoped.
type userKV struct {
	kv     jetstream.KeyValue
	prefix string // "u.<ULID>."
}

var _ jetstream.KeyValue = userKV{}

var errPerUserView = errors.New("not allowed on a per-user view")

// scoped returns the stored key for key; an empty key is invalid, as in the base store.
func (u userKV) scoped(key string) (string, error) {
	if key == "" {
		return "", jetstream.ErrInvalidKey
	}
	return u.prefix + key, nil
}

// scopedFilters prefixes each filter; an empty list would watch the whole bucket, so it is refused.
func (u userKV) scopedFilters(keys []string) ([]string, error) {
	if len(keys) == 0 {
		return nil, jetstream.ErrInvalidKey
	}
	filters := make([]string, len(keys))
	for i, key := range keys {
		filter, err := u.scoped(key)
		if err != nil {
			return nil, err
		}
		filters[i] = filter
	}
	return filters, nil
}

// entry strips the prefix from a returned entry's key.
func (u userKV) entry(entry jetstream.KeyValueEntry, err error) (jetstream.KeyValueEntry, error) {
	if err != nil {
		return nil, err
	}
	return userEntry{entry, strings.TrimPrefix(entry.Key(), u.prefix)}, nil
}

func (u userKV) Get(ctx context.Context, key string) (jetstream.KeyValueEntry, error) {
	k, err := u.scoped(key)
	if err != nil {
		return nil, err
	}
	return u.entry(u.kv.Get(ctx, k))
}

func (u userKV) GetRevision(ctx context.Context, key string, revision uint64) (jetstream.KeyValueEntry, error) {
	k, err := u.scoped(key)
	if err != nil {
		return nil, err
	}
	return u.entry(u.kv.GetRevision(ctx, k, revision))
}

func (u userKV) Put(ctx context.Context, key string, value []byte) (uint64, error) {
	k, err := u.scoped(key)
	if err != nil {
		return 0, err
	}
	return u.kv.Put(ctx, k, value)
}

func (u userKV) PutString(ctx context.Context, key string, value string) (uint64, error) {
	k, err := u.scoped(key)
	if err != nil {
		return 0, err
	}
	return u.kv.PutString(ctx, k, value)
}

func (u userKV) Create(ctx context.Context, key string, value []byte, opts ...jetstream.KVCreateOpt) (uint64, error) {
	k, err := u.scoped(key)
	if err != nil {
		return 0, err
	}
	return u.kv.Create(ctx, k, value, opts...)
}

func (u userKV) Update(ctx context.Context, key string, value []byte, revision uint64) (uint64, error) {
	k, err := u.scoped(key)
	if err != nil {
		return 0, err
	}
	return u.kv.Update(ctx, k, value, revision)
}

func (u userKV) Delete(ctx context.Context, key string, opts ...jetstream.KVDeleteOpt) error {
	k, err := u.scoped(key)
	if err != nil {
		return err
	}
	return u.kv.Delete(ctx, k, opts...)
}

func (u userKV) Purge(ctx context.Context, key string, opts ...jetstream.KVDeleteOpt) error {
	k, err := u.scoped(key)
	if err != nil {
		return err
	}
	return u.kv.Purge(ctx, k, opts...)
}

func (u userKV) Watch(ctx context.Context, keys string, opts ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
	k, err := u.scoped(keys)
	if err != nil {
		return nil, err
	}
	w, err := u.kv.Watch(ctx, k, opts...)
	if err != nil {
		return nil, err
	}
	return newUserWatcher(w, u.prefix), nil
}

func (u userKV) WatchAll(ctx context.Context, opts ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
	return u.Watch(ctx, ">", opts...)
}

func (u userKV) WatchFiltered(ctx context.Context, keys []string, opts ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
	filters, err := u.scopedFilters(keys)
	if err != nil {
		return nil, err
	}
	w, err := u.kv.WatchFiltered(ctx, filters, opts...)
	if err != nil {
		return nil, err
	}
	return newUserWatcher(w, u.prefix), nil
}

// Keys is ListKeys collected and sorted, with the base store's ErrNoKeysFound for an empty result.
func (u userKV) Keys(ctx context.Context, opts ...jetstream.WatchOpt) ([]string, error) {
	lister, err := u.ListKeys(ctx, opts...)
	if err != nil {
		return nil, err
	}
	defer lister.Stop()
	var keys []string
	for key := range lister.Keys() {
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		return nil, jetstream.ErrNoKeysFound
	}
	slices.Sort(keys)
	return slices.Compact(keys), nil
}

func (u userKV) ListKeys(ctx context.Context, opts ...jetstream.WatchOpt) (jetstream.KeyLister, error) {
	opts = append(opts, jetstream.IgnoreDeletes(), jetstream.MetaOnly())
	w, err := u.WatchAll(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return newUserLister(w), nil
}

func (u userKV) ListKeysFiltered(ctx context.Context, filters ...string) (jetstream.KeyLister, error) {
	w, err := u.WatchFiltered(ctx, filters, jetstream.IgnoreDeletes(), jetstream.MetaOnly())
	if err != nil {
		return nil, err
	}
	return newUserLister(w), nil
}

func (u userKV) History(ctx context.Context, key string, opts ...jetstream.WatchOpt) ([]jetstream.KeyValueEntry, error) {
	k, err := u.scoped(key)
	if err != nil {
		return nil, err
	}
	entries, err := u.kv.History(ctx, k, opts...)
	if err != nil {
		return nil, err
	}
	for i, entry := range entries {
		entries[i] = userEntry{entry, strings.TrimPrefix(entry.Key(), u.prefix)}
	}
	return entries, nil
}

func (u userKV) Bucket() string { return u.kv.Bucket() }

// PurgeDeletes would remove other users' delete markers too, so it is refused on a user's view.
func (u userKV) PurgeDeletes(context.Context, ...jetstream.KVPurgeOpt) error {
	return fmt.Errorf("PurgeDeletes: %w", errPerUserView)
}

func (u userKV) Status(ctx context.Context) (jetstream.KeyValueStatus, error) {
	return u.kv.Status(ctx)
}

// userEntry is a returned entry whose key is the bare key the user wrote.
type userEntry struct {
	jetstream.KeyValueEntry
	key string
}

func (e userEntry) Key() string { return e.key }

// userWatcher maps a bucket watcher's entries to bare keys; the nil "initial values done" marker passes through.
type userWatcher struct {
	src  jetstream.KeyWatcher
	out  chan jetstream.KeyValueEntry
	done chan struct{}
	once sync.Once
}

func newUserWatcher(src jetstream.KeyWatcher, prefix string) jetstream.KeyWatcher {
	w := &userWatcher{src: src, out: make(chan jetstream.KeyValueEntry), done: make(chan struct{})}
	go func() {
		defer close(w.out)
		for entry := range src.Updates() {
			if entry != nil {
				entry = userEntry{entry, strings.TrimPrefix(entry.Key(), prefix)}
			}
			select {
			case w.out <- entry:
			case <-w.done:
				return
			}
		}
	}()
	return w
}

func (w *userWatcher) Updates() <-chan jetstream.KeyValueEntry { return w.out }

func (w *userWatcher) Stop() error {
	w.once.Do(func() { close(w.done) })
	return w.src.Stop()
}

// userLister streams the bare keys of a watcher until its nil "initial values done" marker.
type userLister struct {
	w    jetstream.KeyWatcher
	keys chan string
	done chan struct{}
	once sync.Once
}

func newUserLister(w jetstream.KeyWatcher) jetstream.KeyLister {
	l := &userLister{w: w, keys: make(chan string), done: make(chan struct{})}
	go func() {
		defer close(l.keys)
		defer w.Stop()
		for entry := range w.Updates() {
			if entry == nil {
				return
			}
			select {
			case l.keys <- entry.Key():
			case <-l.done:
				return
			}
		}
	}()
	return l
}

func (l *userLister) Keys() <-chan string { return l.keys }

func (l *userLister) Stop() error {
	l.once.Do(func() { close(l.done) })
	return l.w.Stop()
}

// migrateToUser moves every key of kv that is not already per-user ("u.*") under id's prefix: Create the new key,
// then delete the old key at the revision it copied. When the new key already exists the newer of the two values
// wins. A key that changed meanwhile is skipped and the next run moves it. It returns how many keys moved.
func migrateToUser(ctx context.Context, kv jetstream.KeyValue, id string) (int, error) {
	if !ulidPattern.MatchString(id) {
		return 0, errors.New("refusing to migrate to a user without a ULID")
	}
	lister, err := kv.ListKeys(ctx)
	if err != nil {
		return 0, err
	}
	var keys []string
	for key := range lister.Keys() {
		if !strings.HasPrefix(key, "u.") {
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)
	keys = slices.Compact(keys)
	prefix := "u." + id + "."
	moved := 0
	for _, key := range keys {
		legacy, err := kv.Get(ctx, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) { // deleted since the listing
			continue
		}
		if err != nil {
			return moved, err
		}
		newKey := prefix + key
		if _, err = kv.Create(ctx, newKey, legacy.Value()); errors.Is(err, jetstream.ErrKeyExists) {
			// The newer write wins: a legacy value written after the new key replaces it.
			current, err := kv.Get(ctx, newKey)
			if errors.Is(err, jetstream.ErrKeyNotFound) { // deleted since the Create; the next run copies it
				continue
			}
			if err != nil {
				return moved, err
			}
			if legacy.Created().After(current.Created()) {
				if _, err := kv.Update(ctx, newKey, legacy.Value(), current.Revision()); errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
					continue // changed since the read; the next run retries
				} else if err != nil {
					return moved, err
				}
			}
		} else if err != nil {
			return moved, err
		}
		// Deleting at the revision that was copied means a write that lands meanwhile is never lost.
		if err := kv.Delete(ctx, key, jetstream.LastRevision(legacy.Revision())); errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
			continue // rewritten since the read; the next run moves it
		} else if err != nil {
			return moved, err
		}
		moved++
	}
	return moved, nil
}

const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// newULID is a 48-bit Unix millisecond timestamp and 80 random bits, encoded as 26 Crockford base32 characters.
func newULID() string {
	var b [16]byte
	binary.BigEndian.PutUint64(b[:8], uint64(time.Now().UnixMilli())<<16)
	rand.Read(b[6:])
	n := new(big.Int).SetBytes(b[:])
	var out [26]byte
	var digit big.Int
	for i := len(out) - 1; i >= 0; i-- {
		n.DivMod(n, big.NewInt(32), &digit)
		out[i] = crockford[digit.Int64()]
	}
	return string(out[:])
}

// createUser adds a login name and its account record; the name index keeps login names unique.
func createUser(ctx context.Context, users jetstream.KeyValue, name, role, hash string) (User, error) {
	if !authUserPattern.MatchString(name) || (role != roleAdmin && role != roleMember) {
		return User{}, errors.New("invalid COS user name or role")
	}
	id := newULID()
	if _, err := users.Create(ctx, "name."+name, []byte(id)); err != nil {
		if errors.Is(err, jetstream.ErrKeyExists) {
			return User{}, fmt.Errorf("user %s exists: %w", name, jetstream.ErrKeyExists)
		}
		return User{}, err
	}
	value, _ := json.Marshal(authUser{Name: name, Role: role, Bcrypt: hash, Created: time.Now().UTC()})
	if _, err := users.Create(ctx, "uid."+id, value); err != nil {
		_ = users.Delete(ctx, "name."+name)
		return User{}, err
	}
	return User{ID: id, Name: name, Role: role}, nil
}

// deleteUser removes u's login name, account and sessions, in that order, so no login name is left
// pointing at a missing account. The admin delete purges u's data and invite before calling it.
func deleteUser(ctx context.Context, users, sessions, lockouts jetstream.KeyValue, u User) error {
	// Checked first: an empty or malformed ID would make the session filter ".*" and match every session.
	if !ulidPattern.MatchString(u.ID) {
		return errors.New("refusing to delete a user without a ULID")
	}
	// The revision makes the delete a compare-and-set, so a name re-taken since the read is kept.
	if entry, err := users.Get(ctx, "name."+u.Name); err == nil && string(entry.Value()) == u.ID {
		if err := users.Delete(ctx, "name."+u.Name, jetstream.LastRevision(entry.Revision())); err != nil {
			return err
		}
	}
	if err := users.Delete(ctx, "uid."+u.ID); err != nil {
		return err
	}
	if !revokeSessions(ctx, sessions, u.ID) {
		return errors.New("COS sessions could not be revoked")
	}
	_ = lockouts.Delete(ctx, "acct."+u.Name)
	return nil
}
