package cos

import (
	"io"
	"log/slog"
	"net/http"
	"time"
)

// metricsPage renders the Metrics page: KPI tiles, the Nodes and Work summaries.
func (w *Web) metricsPage(rw http.ResponseWriter, r *http.Request) {
	h := w.header(r.Context())
	b, ok := w.homeBoard(r.Context())
	nodes, nodesOK := w.homeNodes(r.Context())
	rw.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := metricsPage(h, w.kpis(h), nodes, nodesOK, b, ok).Render(r.Context(), rw); err != nil {
		logErr(r.Context(), slog.LevelError, "render metrics", err)
	}
}

// metricsEvents repaints the Metrics page on every state change, KPI probe and 10 s node tick.
func (w *Web) metricsEvents(rw http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	controller := http.NewResponseController(rw)
	kpiWake, stopKPI := w.KPI.Subscribe()
	defer stopKPI()
	notify := make(chan struct{}, 1)
	w.mu.Lock()
	w.clients[notify] = struct{}{}
	w.mu.Unlock()
	defer func() { w.mu.Lock(); delete(w.clients, notify); w.mu.Unlock() }()
	rw.Header().Set("Content-Type", "text/event-stream")
	rw.Header().Set("X-Accel-Buffering", "no")
	var last chatHeader
	// patchNodes repatches the Nodes summary; node samples are not events, so the ticker calls it too.
	patchNodes := func() error {
		nodes, ok := w.homeNodes(ctx)
		return patchElements(ctx, rw, nodesSummary(nodes, ok))
	}
	// home patches the header, KPI tiles, Nodes and Work summaries from one state read.
	home := func(v Dashboard) error {
		h := headerOf(v)
		last = h
		if err := patchElements(ctx, rw, chatHead(h, "metrics")); err != nil {
			return err
		}
		if err := patchElements(ctx, rw, kpiTiles(w.kpis(h))); err != nil {
			return err
		}
		if err := patchNodes(); err != nil {
			return err
		}
		b, ok := w.homeBoard(ctx)
		return patchElements(ctx, rw, workPane(b, ok))
	}
	flush := func(err error) bool {
		if err == nil {
			err = controller.Flush()
		}
		return err == nil
	}
	_ = controller.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if !flush(home(w.state(ctx))) {
		return
	}
	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()
	nodesTick := time.NewTicker(10 * time.Second)
	defer nodesTick.Stop()
	for {
		var err error
		select {
		case <-ctx.Done():
			return
		case <-notify:
			_ = controller.SetWriteDeadline(time.Now().Add(10 * time.Second))
			err = home(w.state(ctx))
		case <-kpiWake:
			_ = controller.SetWriteDeadline(time.Now().Add(10 * time.Second))
			err = patchElements(ctx, rw, kpiTiles(w.kpis(last)))
		case <-nodesTick.C:
			_ = controller.SetWriteDeadline(time.Now().Add(10 * time.Second))
			err = patchNodes()
		case <-keepalive.C:
			_ = controller.SetWriteDeadline(time.Now().Add(10 * time.Second))
			_, err = io.WriteString(rw, ": keepalive\n\n")
		}
		if !flush(err) {
			return
		}
	}
}
