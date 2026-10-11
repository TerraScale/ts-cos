package cos

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
)

// threadCloser is the bus call that closes a COS thread on its host; *Bus satisfies it.
type threadCloser interface {
	Call(ctx context.Context, host, command string, args []string, output any) error
}

// adminPages serves /admin. threads closes a deleted user's COS thread before any of its data is deleted.
type adminPages struct {
	auth    *Auth
	threads threadCloser
	head    func(context.Context) chatHeader // the shared top bar's counts; nil shows an empty bar
}

// topbar is the shared top bar's header for the admin pages.
func (p adminPages) topbar(ctx context.Context) chatHeader {
	if p.head == nil {
		return chatHeader{}
	}
	return p.head(ctx)
}

// adminView is the user list. Invite is set only by the response that issues a link, and is shown once.
type adminView struct {
	Me     string // the signed-in admin's ID, so their own row has no disable or delete
	Rows   []adminRow
	Notice string
	Tone   string // "ok" or "danger"
	Name   string
	Invite string
	Head   chatHeader
}

// deleteView is the delete confirmation for one user.
type deleteView struct {
	Row    adminRow
	Data   []bucketKeys
	Host   string // the host of the thread that closes first, if the user has one
	Notice string
	Head   chatHeader
}

// adminRoutes are the admin pages, each behind requireAdmin as well as the admin scope.
func adminRoutes(p adminPages) []webRoute {
	return []webRoute{
		{"GET /admin", scopeAdmin, requireAdmin(p.list)},
		{"POST /admin/invite", scopeAdmin, requireAdmin(p.invite)},
		{"POST /admin/users/{id}/invite", scopeAdmin, requireAdmin(p.reinvite)},
		{"POST /admin/users/{id}/disable", scopeAdmin, requireAdmin(p.disable)},
		{"POST /admin/users/{id}/enable", scopeAdmin, requireAdmin(p.enable)},
		{"GET /admin/users/{id}/delete", scopeAdmin, requireAdmin(p.confirmDelete)},
		{"POST /admin/users/{id}/delete", scopeAdmin, requireAdmin(p.remove)},
	}
}

func (p adminPages) list(rw http.ResponseWriter, r *http.Request) {
	p.renderList(rw, r, http.StatusOK, adminView{})
}

// renderList shows the user list with v's notice, and the new invite link when v has one.
func (p adminPages) renderList(rw http.ResponseWriter, r *http.Request, status int, v adminView) {
	rows, err := p.auth.adminRows(r.Context())
	if err != nil {
		logErr(r.Context(), slog.LevelError, "read COS users", err)
		http.Error(rw, "COS cannot read its users right now. Try again.", http.StatusServiceUnavailable)
		return
	}
	v.Me = currentUser(r).ID
	v.Rows = rows
	v.Head = p.topbar(r.Context())
	renderAuth(rw, r, status, adminPage(v))
}

// invite creates a login name with no password and issues its first link.
func (p adminPages) invite(rw http.ResponseWriter, r *http.Request) {
	name := strings.TrimSpace(r.PostFormValue("name"))
	if !authUserPattern.MatchString(name) {
		p.renderList(rw, r, http.StatusBadRequest, adminView{Notice: "Login names use 1 to 64 lowercase letters, digits, _ or -.", Tone: "danger"})
		return
	}
	users, _, _ := p.auth.kv()
	if users == nil {
		p.renderList(rw, r, http.StatusServiceUnavailable, adminView{Notice: "COS is starting. Try again in a minute.", Tone: "danger"})
		return
	}
	u, err := createUser(r.Context(), users, name, roleMember, "")
	if errors.Is(err, jetstream.ErrKeyExists) {
		p.renderList(rw, r, http.StatusConflict, adminView{Notice: "The login name " + name + " is taken.", Tone: "danger"})
		return
	}
	if err != nil {
		logErr(r.Context(), slog.LevelError, "create COS user", err)
		p.renderList(rw, r, http.StatusServiceUnavailable, adminView{Notice: "Could not create the user. Try again.", Tone: "danger"})
		return
	}
	p.issued(rw, r, u.Name, u.ID, "Invite ready for "+name+".")
}

// reinvite gives a pending account a new link; the old link stops working at once.
func (p adminPages) reinvite(rw http.ResponseWriter, r *http.Request) {
	rec, _, found, err := p.auth.accountByID(r.Context(), r.PathValue("id"))
	if err != nil {
		logErr(r.Context(), slog.LevelError, "read COS user", err)
		p.renderList(rw, r, http.StatusServiceUnavailable, adminView{Notice: "COS cannot read its users right now. Try again.", Tone: "danger"})
		return
	}
	if !found {
		http.NotFound(rw, r)
		return
	}
	if rec.Bcrypt != "" {
		p.renderList(rw, r, http.StatusConflict, adminView{Notice: rec.Name + " already has a password, so it needs no invite.", Tone: "danger"})
		return
	}
	p.issued(rw, r, rec.Name, r.PathValue("id"), "New invite ready for "+rec.Name+". The old link no longer works.")
}

// issued issues a link for account id and renders the list with it, shown once.
func (p adminPages) issued(rw http.ResponseWriter, r *http.Request, name, id, notice string) {
	token, err := p.auth.issueInvite(r.Context(), id)
	if err != nil {
		logErr(r.Context(), slog.LevelError, "issue COS invite", err)
		p.renderList(rw, r, http.StatusServiceUnavailable, adminView{Notice: "The invite was not issued. Use Invite again on " + name + ".", Tone: "danger"})
		return
	}
	p.renderList(rw, r, http.StatusOK, adminView{Notice: notice + " Copy the link now; it is shown once.", Tone: "ok", Name: name, Invite: inviteLink(r, token)})
}

func (p adminPages) disable(rw http.ResponseWriter, r *http.Request) { p.setDisabled(rw, r, true) }
func (p adminPages) enable(rw http.ResponseWriter, r *http.Request)  { p.setDisabled(rw, r, false) }

// setDisabled disables or enables the account in the path, refusing the signed-in admin and the last active admin.
func (p adminPages) setDisabled(rw http.ResponseWriter, r *http.Request, disabled bool) {
	ctx := r.Context()
	rows, err := p.auth.adminRows(ctx)
	if err != nil {
		logErr(ctx, slog.LevelError, "read COS users", err)
		p.renderList(rw, r, http.StatusServiceUnavailable, adminView{Notice: "COS cannot read its users right now. Try again.", Tone: "danger"})
		return
	}
	target, ok := findRow(rows, r.PathValue("id"))
	if !ok {
		http.NotFound(rw, r)
		return
	}
	if disabled {
		if refusal := guardRemoval(currentUser(r), target.User, rows); refusal != "" {
			p.renderList(rw, r, http.StatusConflict, adminView{Notice: refusal, Tone: "danger"})
			return
		}
	}
	if err := p.auth.setAccountDisabled(ctx, target.User.ID, disabled); err != nil {
		logErr(ctx, slog.LevelError, "set COS user disabled", err, "user", target.User.Name)
		p.renderList(rw, r, http.StatusServiceUnavailable, adminView{Notice: "Could not update " + target.User.Name + ". Try again.", Tone: "danger"})
		return
	}
	http.Redirect(rw, r, "/admin", http.StatusSeeOther)
}

// confirmDelete lists what deleting the user removes, and asks for the login name.
func (p adminPages) confirmDelete(rw http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	rows, err := p.auth.adminRows(ctx)
	if err != nil {
		logErr(ctx, slog.LevelError, "read COS users", err)
		http.Error(rw, "COS cannot read its users right now. Try again.", http.StatusServiceUnavailable)
		return
	}
	target, ok := findRow(rows, r.PathValue("id"))
	if !ok {
		http.NotFound(rw, r)
		return
	}
	p.renderDelete(rw, r, http.StatusOK, target, "")
}

// renderDelete shows the delete confirmation for target, with the data counts and the thread that closes first.
func (p adminPages) renderDelete(rw http.ResponseWriter, r *http.Request, status int, target adminRow, notice string) {
	ctx := r.Context()
	data, err := userDataCounts(ctx, p.auth.js, target.User.ID)
	if err != nil {
		logErr(ctx, slog.LevelError, "count COS user data", err)
		http.Error(rw, "COS cannot read its data right now. Try again.", http.StatusServiceUnavailable)
		return
	}
	v := deleteView{Row: target, Data: data, Notice: notice, Head: p.topbar(r.Context())}
	if st, open, err := brainThread(ctx, p.auth.nc, target.User.ID); err != nil {
		logErr(ctx, slog.LevelError, "read COS user thread", err)
	} else if open {
		v.Host = st.Host
	}
	renderAuth(rw, r, status, deletePage(v))
}

// remove removes a user in this order: disable (no new writes), close the brain's thread (no data goes under a live
// thread), purge the per-user data, drop the invite, then the login name and account. Each step can be retried.
func (p adminPages) remove(rw http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	rows, err := p.auth.adminRows(ctx)
	if err != nil {
		logErr(ctx, slog.LevelError, "read COS users", err)
		http.Error(rw, "COS cannot read its users right now. Try again.", http.StatusServiceUnavailable)
		return
	}
	target, ok := findRow(rows, r.PathValue("id"))
	if !ok {
		http.NotFound(rw, r)
		return
	}
	if r.PostFormValue("confirm") != target.User.Name {
		p.renderDelete(rw, r, http.StatusBadRequest, target, "The typed login name does not match. Nothing was deleted.")
		return
	}
	if refusal := guardRemoval(currentUser(r), target.User, rows); refusal != "" {
		p.renderDelete(rw, r, http.StatusConflict, target, refusal)
		return
	}
	id := target.User.ID
	users, sessions, lockouts := p.auth.kv()
	if err := p.auth.setAccountDisabled(ctx, id, true); err != nil {
		logErr(ctx, slog.LevelError, "disable COS user for delete", err, "user", target.User.Name)
		p.renderDelete(rw, r, http.StatusServiceUnavailable, target, "Could not disable the account. Nothing was deleted.")
		return
	}
	if err := settleBrainThread(ctx, p.auth.nc, p.threads, id); err != nil {
		logErr(ctx, slog.LevelError, "close COS user thread", err, "user", target.User.Name)
		p.renderDelete(rw, r, http.StatusConflict, target, "The COS thread could not be closed, so no data was deleted. The account stays disabled; try again.")
		return
	}
	if err := purgeUserData(ctx, p.auth.js, id); err != nil {
		logErr(ctx, slog.LevelError, "purge COS user data", err, "user", target.User.Name)
		p.renderDelete(rw, r, http.StatusServiceUnavailable, target, "Some data could not be deleted. Try again to finish.")
		return
	}
	if err := p.auth.revokeInvite(ctx, users, id); err != nil {
		logErr(ctx, slog.LevelError, "revoke COS user invite", err, "user", target.User.Name)
		p.renderDelete(rw, r, http.StatusServiceUnavailable, target, "The invite could not be removed. Try again to finish.")
		return
	}
	if err := deleteUser(ctx, users, sessions, lockouts, target.User); err != nil {
		logErr(ctx, slog.LevelError, "delete COS user", err, "user", target.User.Name)
		p.renderDelete(rw, r, http.StatusServiceUnavailable, target, "The account could not be removed. Try again to finish.")
		return
	}
	http.Redirect(rw, r, "/admin", http.StatusSeeOther)
}

// currentUser is the signed-in user; the admin routes only run behind require, so it is always set.
func currentUser(r *http.Request) User {
	u, _ := UserFrom(r.Context())
	return u
}
