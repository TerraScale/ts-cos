package cos

import (
	"encoding/json"
	"log/slog"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"
)

// modelsPanelData is the Model policy section on Settings: the policy as it runs now, and the form that changes it.
type modelsPanelData struct {
	Summary, Hash, Leader, Priority string // Priority is the comma-separated form value
	Alert, Tone                     string
	Priorities                      []string
	Pairs                           []modelsPairRow
}

type modelsPairRow struct {
	Key, Route, Advisor, Effort, HighStakes, Worker string
}

func modelsPanelFor(p ModelPolicy, alert string) modelsPanelData {
	d := modelsPanelData{Summary: modelsSummary(p), Hash: modelsHash(p), Leader: p.Leader, Priority: strings.Join(p.Priority, ", "), Priorities: p.Priority}
	if alert != "" {
		d.Alert, d.Tone = alert, "danger"
	}
	for _, k := range slices.Sorted(maps.Keys(p.Pairs)) {
		pair := p.Pairs[k]
		row := modelsPairRow{Key: k, Route: p.Route[k], Advisor: pair.Advisor.Model, HighStakes: pair.HighStakesEffort, Worker: pair.Worker.Model}
		for _, o := range pair.Advisor.Options {
			if slices.Contains(effortOptions, o.ID) {
				row.Effort = o.Value
			}
		}
		d.Pairs = append(d.Pairs, row)
	}
	return d
}

// modelsFormPatch turns the Settings form into a merge patch of only the fields that differ from cur.
func modelsFormPatch(cur ModelPolicy, form url.Values) map[string]any {
	patch := map[string]any{}
	routes, pairs := map[string]any{}, map[string]any{}
	for _, k := range slices.Sorted(maps.Keys(cur.Pairs)) {
		pair := cur.Pairs[k]
		if v := strings.TrimSpace(form.Get("route_" + k)); v != cur.Route[k] {
			routes[k] = v
		}
		change := map[string]any{}
		if v := strings.TrimSpace(form.Get("advisor_" + k)); v != pair.Advisor.Model {
			change["advisor"] = map[string]any{"model": v}
		}
		if v := strings.TrimSpace(form.Get("worker_" + k)); v != pair.Worker.Model {
			change["worker"] = map[string]any{"model": v}
		}
		if len(change) > 0 {
			pairs[k] = change
		}
	}
	if len(routes) > 0 {
		patch["route"] = routes
	}
	if len(pairs) > 0 {
		patch["pairs"] = pairs
	}
	if v := strings.TrimSpace(form.Get("leader")); v != cur.Leader {
		patch["leader"] = v
	}
	var priority []string
	for _, key := range strings.Split(form.Get("priority"), ",") {
		if key = strings.TrimSpace(key); key != "" {
			priority = append(priority, key)
		}
	}
	if !slices.Equal(priority, cur.Priority) {
		patch["priority"] = priority
	}
	return patch
}

// modelsSave queues the Model policy form as a self-change; the policy goes live when the change does.
func (w *Web) modelsSave(rw http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(rw, r.Body, 16<<10)
	if err := r.ParseForm(); err != nil {
		http.Error(rw, "Model form is too large or unreadable", http.StatusBadRequest)
		return
	}
	cur := CurrentBehavior().Models
	if w.Bus == nil {
		w.modelsRejected(rw, r, "Model policy unavailable until COS NATS is ready", http.StatusServiceUnavailable)
		return
	}
	store, err := openSelfStore(r.Context(), w.Bus, false)
	if err != nil {
		logErr(r.Context(), slog.LevelError, "open self-change store", err)
		w.modelsRejected(rw, r, "Model policy unavailable until COS NATS is ready", http.StatusServiceUnavailable)
		return
	}
	raw, _ := json.Marshal(modelsFormPatch(cur, r.PostForm))
	if _, err := modelsChange(r.Context(), store, cur, raw); err != nil {
		w.modelsRejected(rw, r, publicError(err), http.StatusBadRequest)
		return
	}
	http.Redirect(rw, r, "/settings#models", http.StatusSeeOther)
}

// modelsRejected re-renders Settings with why the Model policy change did not happen.
func (w *Web) modelsRejected(rw http.ResponseWriter, r *http.Request, alert string, status int) {
	w.renderSettings(rw, r, status, w.budgetPanel(r.Context(), time.Now()), modelsPanelFor(CurrentBehavior().Models, alert))
}
