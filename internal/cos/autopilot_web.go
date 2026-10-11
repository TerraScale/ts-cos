package cos

import (
	"fmt"
	"html"
	"net/http"
	"strconv"
)

func (w *Web) autopilotRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /autopilot/interval/{minutes}", func(rw http.ResponseWriter, r *http.Request) {
		if !w.autopilotRequest(rw, r) {
			return
		}
		minutes, _ := strconv.Atoi(r.PathValue("minutes"))
		if err := w.Autopilot.SetInterval(r.Context(), minutes); err != nil {
			autopilotNotice(rw, publicError(err))
			return
		}
		autopilotNotice(rw, fmt.Sprintf("Heartbeat set to every %d minutes on every node.", minutes))
	})
	mux.HandleFunc("POST /autopilot/run", func(rw http.ResponseWriter, r *http.Request) {
		if !w.autopilotRequest(rw, r) {
			return
		}
		host, err := w.Autopilot.RunNow(r.Context())
		if err != nil {
			autopilotNotice(rw, err.Error())
			return
		}
		autopilotNotice(rw, "Heartbeat started on the leader, "+host+".")
	})
}

// autopilotRequest rejects cross-site form posts: browsers cannot send the
// Datastar header cross-origin without a CORS preflight this server never allows.
func (w *Web) autopilotRequest(rw http.ResponseWriter, r *http.Request) bool {
	if r.Header.Get("Datastar-Request") != "true" {
		http.Error(rw, "Dashboard actions require a Datastar request", http.StatusForbidden)
		return false
	}
	if w.Autopilot == nil {
		http.Error(rw, "Autopilot is starting", http.StatusServiceUnavailable)
		return false
	}
	return true
}

func autopilotNotice(rw http.ResponseWriter, text string) {
	rw.Header().Set("Content-Type", "text/event-stream")
	fmt.Fprintf(rw, "event: datastar-patch-elements\ndata: elements <p id=\"autopilot-notice\" role=\"status\">%s</p>\n\n", html.EscapeString(text))
}

func publicError(err error) string {
	if e, ok := err.(interface{ PublicMessage() string }); ok {
		return e.PublicMessage()
	}
	return "The change could not be saved; try again."
}

func boolText(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
