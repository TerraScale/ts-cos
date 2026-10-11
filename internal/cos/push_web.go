package cos

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"slices"

	"git.terrascale.tech/TerraScale/ts-cos/internal/webpush"
	"github.com/a-h/templ"
)

// pushRoutes serve the device subscription and the settings panel behind sign-in.
func (w *Web) pushRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /push/key", func(rw http.ResponseWriter, r *http.Request) {
		p := w.pushReady(rw)
		if p == nil {
			return
		}
		rw.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprint(rw, p.vapid.PublicKey())
	})
	mux.HandleFunc("POST /push/subscribe", func(rw http.ResponseWriter, r *http.Request) {
		p := w.pushReady(rw)
		if p == nil {
			return
		}
		if mt, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type")); mt != "application/json" {
			http.Error(rw, "Push subscriptions must be JSON", http.StatusUnsupportedMediaType)
			return
		}
		var sub webpush.Subscription
		if !pushDecode(rw, r, &sub) {
			return
		}
		u, err := url.Parse(sub.Endpoint)
		if err != nil || !pushHostAllowed(u) || sub.Validate() != nil {
			http.Error(rw, "Unsupported push endpoint", http.StatusBadRequest)
			return
		}
		if err := p.Subscribe(r.Context(), pushUser(r), r.UserAgent(), sub); err != nil {
			slog.Error("save push subscription", "error", err)
			http.Error(rw, "Push subscription failed; try again", http.StatusInternalServerError)
			return
		}
		rw.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /push/unsubscribe", func(rw http.ResponseWriter, r *http.Request) {
		p := w.pushReady(rw)
		if p == nil {
			return
		}
		var body struct {
			Endpoint string `json:"endpoint"`
		}
		if !pushDecode(rw, r, &body) {
			return
		}
		if err := p.Unsubscribe(r.Context(), pushUser(r), body.Endpoint); err != nil {
			slog.Error("delete push subscription", "error", err)
			http.Error(rw, "Push update failed; try again", http.StatusInternalServerError)
			return
		}
		rw.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /push/test", func(rw http.ResponseWriter, r *http.Request) {
		p := w.pushReady(rw)
		if p == nil {
			return
		}
		sent, err := p.Send(r.Context(), Notice{Category: "test", Title: "COS", Body: "Test notification from " + p.host, URL: "/settings", Tag: "test", User: pushUser(r)})
		msg := fmt.Sprintf("Sent to %d devices", sent)
		switch {
		case err != nil:
			msg = "Test notification failed; try again"
		case sent == 0:
			msg = "No devices are subscribed yet"
		}
		if r.Header.Get("Datastar-Request") != "true" {
			http.Redirect(rw, r, "/settings", http.StatusSeeOther)
			return
		}
		pushPatch(r.Context(), rw, pushNotice(msg))
	})
	mux.HandleFunc("POST /push/prefs/{category}/{state}", func(rw http.ResponseWriter, r *http.Request) {
		p := w.pushReady(rw)
		if p == nil {
			return
		}
		category, state := r.PathValue("category"), r.PathValue("state")
		if !pushKnown(category) || state != "on" && state != "off" {
			http.NotFound(rw, r)
			return
		}
		if err := p.SetPref(r.Context(), pushUser(r), category, state == "on"); err != nil {
			slog.Error("save push preference", "error", err)
			http.Error(rw, "Preference update failed; try again", http.StatusInternalServerError)
			return
		}
		if r.Header.Get("Datastar-Request") != "true" {
			http.Redirect(rw, r, "/settings", http.StatusSeeOther)
			return
		}
		pushPatch(r.Context(), rw, pushPanel(w.pushState(r.Context(), pushUser(r))))
	})
}

// pushReady answers 503 until the push store is open; callers stop when it returns nil.
func (w *Web) pushReady(rw http.ResponseWriter) *Push {
	p := w.push.Load()
	if p == nil {
		http.Error(rw, "Push is starting; retry in a few seconds", http.StatusServiceUnavailable)
	}
	return p
}

// pushState is what the settings panel shows user; an unready store shows the starting notice.
func (w *Web) pushState(ctx context.Context, user string) PushState {
	p := w.push.Load()
	if p == nil {
		return PushState{}
	}
	devices, err := p.Devices(ctx, user)
	if err != nil {
		slog.Error("read push devices", "error", err)
		return PushState{}
	}
	off, err := p.off(ctx, user)
	if err != nil {
		slog.Error("read push preferences", "error", err)
		return PushState{}
	}
	return PushState{Ready: true, Devices: devices, Off: off}
}

// pushPatch sends one Datastar element patch.
func pushPatch(ctx context.Context, rw http.ResponseWriter, c templ.Component) {
	rw.Header().Set("Content-Type", "text/event-stream")
	if err := patchElements(ctx, rw, c); err != nil {
		slog.Error("patch push panel", "error", err)
	}
}

// pushDecode reads one JSON body of at most 8 KiB and answers 400 when it is not valid.
func pushDecode(rw http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(rw, r.Body, 8<<10)
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		http.Error(rw, "Invalid JSON body", http.StatusBadRequest)
		return false
	}
	return true
}

// pushUser is the signed-in user; Protect always sets it, so "" only appears in unauthenticated tests.
func pushUser(r *http.Request) string {
	user, _ := r.Context().Value(authUserKey{}).(string)
	return user
}

func pushKnown(id string) bool {
	return slices.ContainsFunc(pushCategories, func(c pushCategory) bool { return c.ID == id })
}
