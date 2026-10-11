package cos

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/crypto/bcrypt"
)

// inviteTTL is how long an invite link works. A new invite for the account replaces it at once.
const inviteTTL = 7 * 24 * time.Hour

// inviteGoneNotice is the one notice for every refused link, so a link's state is not revealed.
const inviteGoneNotice = "This invite link is invalid or expired."

var errInviteSpent = errors.New("invite already used")

// inviteRecord is the "invite.<sha256 of the token>" value: the account the link opens and when it stops working.
type inviteRecord struct {
	UserID  string    `json:"userId"`
	Expires time.Time `json:"expires"`
}

// pendingInvite is a link's account, with the revisions the one-time accept compares.
type pendingInvite struct {
	user            User
	account         authUser
	accountRevision uint64
	key             string // "invite.<hash>"
	revision        uint64
}

// newInviteToken is 32 random bytes, base64url without padding; only its hash is stored.
func newInviteToken() string {
	raw := make([]byte, 32)
	rand.Read(raw)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func tokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func inviteKey(token string) string { return "invite." + tokenHash(token) }

// issueInvite gives account id a new link and returns its token. The old link stops working first, so one
// account never has two live links.
func (a *Auth) issueInvite(ctx context.Context, id string) (string, error) {
	users, _, _ := a.kv()
	if err := a.revokeInvite(ctx, users, id); err != nil {
		return "", err
	}
	token := newInviteToken()
	hash := tokenHash(token)
	// invitefor is written first: if the record write fails, a stale pointer is harmless.
	if _, err := users.Put(ctx, "invitefor."+id, []byte(hash)); err != nil {
		return "", err
	}
	value, _ := json.Marshal(inviteRecord{UserID: id, Expires: time.Now().Add(inviteTTL).UTC()})
	if _, err := users.Create(ctx, "invite."+hash, value); err != nil {
		return "", err
	}
	return token, nil
}

// revokeInvite deletes account id's outstanding invite record, if it has one. It reads through the stream leader,
// so a link issued a moment ago is always found.
func (a *Auth) revokeInvite(ctx context.Context, users jetstream.KeyValue, id string) error {
	hash, _, found, err := leaderGet(ctx, a.nc, authBucket, "invitefor."+id)
	if err != nil || !found {
		return err
	}
	if err := users.Delete(ctx, "invite."+string(hash)); err != nil {
		return err
	}
	return users.Delete(ctx, "invitefor."+id)
}

// lookupInvite finds the pending account a token opens. ok is false for an unknown, expired or spent token,
// and for an account that already has a password or is disabled.
func (a *Auth) lookupInvite(ctx context.Context, token string) (pendingInvite, bool, error) {
	key := inviteKey(token)
	value, revision, found, err := leaderGet(ctx, a.nc, authBucket, key)
	if err != nil || !found {
		return pendingInvite{}, false, err
	}
	var rec inviteRecord
	if json.Unmarshal(value, &rec) != nil || !ulidPattern.MatchString(rec.UserID) || !time.Now().Before(rec.Expires) {
		return pendingInvite{}, false, nil
	}
	value, accountRevision, found, err := leaderGet(ctx, a.nc, authBucket, "uid."+rec.UserID)
	if err != nil || !found {
		return pendingInvite{}, false, err
	}
	var acct authUser
	if err := json.Unmarshal(value, &acct); err != nil {
		return pendingInvite{}, false, err
	}
	if acct.Bcrypt != "" || !acct.DisabledAt.IsZero() {
		return pendingInvite{}, false, nil
	}
	inv := pendingInvite{
		user:            User{ID: rec.UserID, Name: acct.Name, Role: acct.Role},
		account:         acct,
		accountRevision: accountRevision,
		key:             key,
		revision:        revision,
	}
	return inv, true, nil
}

// setInvitePassword sets the password of the pending account and spends the link. The compare-and-set on the
// account makes a second use fail; the invite record goes last, and a record left behind is inert.
func (a *Auth) setInvitePassword(ctx context.Context, users jetstream.KeyValue, inv pendingInvite, password string) error {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcryptCost)
	if err != nil {
		return err
	}
	acct := inv.account
	acct.Bcrypt = string(hash)
	value, _ := json.Marshal(acct)
	if _, err := users.Update(ctx, "uid."+inv.user.ID, value, inv.accountRevision); errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
		return errInviteSpent
	} else if err != nil {
		return err
	}
	// The LastRevision check keeps a newer invite that was issued meanwhile.
	if err := users.Delete(ctx, inv.key, jetstream.LastRevision(inv.revision)); err != nil {
		logErr(ctx, slog.LevelWarn, "COS invite record not removed", err)
	}
	return nil
}

// inviteOpen admits the link in the path. Every refusal is the same page, and each failed lookup counts against the
// client address, so the tokens cannot be guessed.
func (a *Auth) inviteOpen(rw http.ResponseWriter, r *http.Request) (pendingInvite, bool) {
	rw.Header().Set("Referrer-Policy", "no-referrer") // the token is in the path
	_, _, lockouts := a.kv()
	if lockouts == nil {
		renderAuth(rw, r, http.StatusServiceUnavailable, inviteUnavailablePage("COS is starting. Try again in a minute."))
		return pendingInvite{}, false
	}
	ipKey := lockoutIPKey(clientIP(r))
	failures, err := a.failures(r.Context(), ipKey)
	if err != nil {
		logErr(r.Context(), slog.LevelError, "read invite lockout", err)
		renderAuth(rw, r, http.StatusServiceUnavailable, inviteUnavailablePage("COS cannot read its invites right now. Try again."))
		return pendingInvite{}, false
	}
	if failures >= lockAfter {
		renderAuth(rw, r, http.StatusTooManyRequests, inviteUnavailablePage("Too many failed attempts. Try again in 15 minutes."))
		return pendingInvite{}, false
	}
	inv, ok, err := a.lookupInvite(r.Context(), r.PathValue("token"))
	if err != nil {
		logErr(r.Context(), slog.LevelError, "read invite", err)
		renderAuth(rw, r, http.StatusServiceUnavailable, inviteUnavailablePage("COS cannot read its invites right now. Try again."))
		return pendingInvite{}, false
	}
	if !ok {
		a.fail(r.Context(), lockouts, ipKey)
		renderAuth(rw, r, http.StatusNotFound, inviteUnavailablePage(inviteGoneNotice))
		return pendingInvite{}, false
	}
	return inv, true
}

// inviteForm shows the set-password page for a live link.
func (a *Auth) inviteForm(rw http.ResponseWriter, r *http.Request) {
	if _, ok := a.inviteOpen(rw, r); ok {
		renderAuth(rw, r, http.StatusOK, invitePage(r.PathValue("token"), ""))
	}
}

// inviteAccept sets the password, signs the account in and spends the link.
func (a *Auth) inviteAccept(rw http.ResponseWriter, r *http.Request) {
	inv, ok := a.inviteOpen(rw, r)
	if !ok {
		return
	}
	token := r.PathValue("token")
	next := r.PostFormValue("new")
	if next != r.PostFormValue("confirm") {
		renderAuth(rw, r, http.StatusBadRequest, invitePage(token, "The new passwords do not match."))
		return
	}
	if len(next) < passwordMin || len(next) > passwordMax {
		renderAuth(rw, r, http.StatusBadRequest, invitePage(token, "Use 12 to 72 characters."))
		return
	}
	users, sessions, _ := a.kv()
	if err := a.setInvitePassword(r.Context(), users, inv, next); err != nil {
		if errors.Is(err, errInviteSpent) {
			renderAuth(rw, r, http.StatusNotFound, inviteUnavailablePage(inviteGoneNotice))
			return
		}
		logErr(r.Context(), slog.LevelError, "set invited password", err)
		renderAuth(rw, r, http.StatusServiceUnavailable, invitePage(token, "Could not set the password. Try again."))
		return
	}
	if err := a.startSession(rw, r, sessions, inv.user); err != nil {
		renderAuth(rw, r, http.StatusOK, loginPage("Password set. Sign in to continue."))
		return
	}
	http.Redirect(rw, r, "/", http.StatusSeeOther)
}

// inviteLink is the link an admin sends, built from the request the way secureRequest reads its scheme.
func inviteLink(r *http.Request, token string) string {
	scheme := "http"
	if secureRequest(r) {
		scheme = "https"
	}
	return scheme + "://" + r.Host + "/invite/" + token
}
