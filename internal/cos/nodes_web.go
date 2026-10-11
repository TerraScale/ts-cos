package cos

import (
	"log/slog"
	"net/http"
	"time"
)

// nodesRoutes serves the overview and its live patches.
func (w *Web) nodesRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /nodes", w.nodesPage)
	mux.HandleFunc("GET /nodes/events", w.nodesEvents)
}

func (w *Web) nodesPage(rw http.ResponseWriter, r *http.Request) {
	cards, err := NodeCards(r.Context(), w.Bus)
	if err != nil {
		logErr(r.Context(), slog.LevelError, "read node cards", err)
		http.Error(rw, "Nodes are unavailable; retry in a few seconds", http.StatusServiceUnavailable)
		return
	}
	rw.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := nodesPage(w.header(r.Context()), cards).Render(r.Context(), rw); err != nil {
		logErr(r.Context(), slog.LevelError, "render nodes", err)
	}
}

// nodesEvents repatches the overview every 10 s until the browser disconnects.
func (w *Web) nodesEvents(rw http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	controller := http.NewResponseController(rw)
	rw.Header().Set("Content-Type", "text/event-stream")
	rw.Header().Set("X-Accel-Buffering", "no")
	patch := func() error {
		cards, err := NodeCards(ctx, w.Bus)
		if err != nil {
			logErr(ctx, slog.LevelError, "read node cards", err)
			return err // end the stream so the browser reconnects instead of freezing
		}
		_ = controller.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if err := patchElements(ctx, rw, nodesGrid(cards)); err != nil {
			return err
		}
		return controller.Flush()
	}
	if patch() != nil {
		return
	}
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if patch() != nil {
				return
			}
		}
	}
}
