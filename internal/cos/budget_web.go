package cos

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// budgetMetersMax is how many threads the Settings meters list, most used first.
const budgetMetersMax = 5

const budgetUnavailable = "Budgets unavailable until COS NATS is ready"

// budgetPanelData is the Token budgets section: the base limits as the form takes them, and today's meters.
type budgetPanelData struct {
	Thread, Daily         string
	ThreadHint, DailyHint string
	Alert, Tone           string
	Meters                []budgetMeter
}

// budgetHint says where a base limit comes from: the history default, or a saved setting.
func budgetHint(n int64, fromHistory bool) string {
	if fromHistory {
		return "Default " + budgetHuman(n) + " from the last 7 days · e.g. 15M or 500k"
	}
	return "Saved setting · e.g. 15M or 500k"
}

// budgetMeter is one usage row; the string fields are what the <meter> attributes take.
type budgetMeter struct {
	Label, Text         string
	Value, Max, Low     string
	Raised, NeverPaused bool
}

// budgetKV is the settings bucket the Autopilot opened; nil until the COS JetStream is ready.
func (w *Web) budgetKV() jetstream.KeyValue {
	if w.Autopilot == nil {
		return nil
	}
	w.Autopilot.mu.Lock()
	defer w.Autopilot.mu.Unlock()
	return w.Autopilot.settings
}

// budgetPanel reads the base limits from KV and today's usage from SQLite. Without KV the
// panel shows the default limits and a warning, so the page still loads.
func (w *Web) budgetPanel(ctx context.Context, now time.Time) budgetPanelData {
	st, scopes, err := budgetUsage(ctx, w.Store, now)
	if err != nil {
		logErr(ctx, slog.LevelError, "read budget usage", err)
		return budgetPanelData{Alert: "Cannot read today's token usage", Tone: "warn"}
	}
	d := budgetPanelData{Alert: budgetUnavailable, Tone: "warn"}
	if kv := w.budgetKV(); kv != nil {
		limits, err := budgetLoadLimits(ctx, kv, st.Day, scopes, st.Limits)
		if err != nil {
			logErr(ctx, slog.LevelError, "read budget limits", err)
		} else {
			st.Limits, d.Alert = limits, ""
		}
	}
	d.Thread, d.Daily = budgetInput(st.Limits.Thread), budgetInput(st.Limits.Daily)
	d.ThreadHint, d.DailyHint = budgetHint(st.Limits.Thread, st.Limits.ThreadDefault), budgetHint(st.Limits.Daily, st.Limits.DailyDefault)
	rep := st.report()
	d.Meters = []budgetMeter{budgetMeterRow("All threads", rep.DailyUsed, rep.DailyLimit)}
	d.Meters[0].Raised = rep.Raises[budgetDaily] > st.Limits.Daily
	for _, row := range rep.Threads[:min(len(rep.Threads), budgetMetersMax)] {
		m := budgetMeterRow(row.Label, row.Used, row.Limit)
		m.Raised = rep.Raises[budgetScope(row.Key)] > st.Limits.Thread
		m.NeverPaused = row.Label == budgetCOSLabel
		d.Meters = append(d.Meters, m)
	}
	return d
}

func budgetMeterRow(label string, used, limit int64) budgetMeter {
	return budgetMeter{Label: label, Text: budgetHuman(used) + " / " + budgetHuman(limit),
		Value: strconv.FormatInt(used, 10), Max: strconv.FormatInt(limit, 10), Low: strconv.FormatInt(limit*8/10, 10)}
}

// budgetInput writes a limit the way the form reads it back: 10M, or the exact count when
// the short form would not parse back to it, such as 1500000 (1.5M does not parse).
func budgetInput(n int64) string {
	if got, err := parseTokens(budgetHuman(n)); err == nil && got == n {
		return budgetHuman(n)
	}
	return strconv.FormatInt(n, 10)
}

// budgetSave stores both base limits from the Token budgets form. Both fields are checked
// before either is written, so a rejected form changes nothing.
func (w *Web) budgetSave(rw http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(rw, r.Body, 4<<10)
	if err := r.ParseForm(); err != nil {
		http.Error(rw, "Budget form is too large or unreadable", http.StatusBadRequest)
		return
	}
	in := budgetPanelData{Thread: strings.TrimSpace(r.PostFormValue("thread")), Daily: strings.TrimSpace(r.PostFormValue("daily"))}
	thread, err := parseTokens(in.Thread)
	if err != nil {
		w.budgetRejected(rw, r, in, "Per thread, per day: "+publicError(err), http.StatusBadRequest)
		return
	}
	daily, err := parseTokens(in.Daily)
	if err != nil {
		w.budgetRejected(rw, r, in, "All threads, per day: "+publicError(err), http.StatusBadRequest)
		return
	}
	kv := w.budgetKV()
	if kv == nil {
		w.budgetRejected(rw, r, in, budgetUnavailable, http.StatusServiceUnavailable)
		return
	}
	if err := errors.Join(budgetSetLimit(r.Context(), kv, budgetSettingsThread, thread),
		budgetSetLimit(r.Context(), kv, budgetSettingsDaily, daily)); err != nil {
		logErr(r.Context(), slog.LevelError, "save token budgets", err)
		w.budgetRejected(rw, r, in, "Budgets were not saved; try again.", http.StatusServiceUnavailable)
		return
	}
	http.Redirect(rw, r, "/settings#budgets", http.StatusSeeOther)
}

// budgetRejected re-renders Settings with the typed values and why the save did not happen.
func (w *Web) budgetRejected(rw http.ResponseWriter, r *http.Request, in budgetPanelData, alert string, status int) {
	d := w.budgetPanel(r.Context(), time.Now())
	d.Thread, d.Daily, d.Alert, d.Tone = in.Thread, in.Daily, alert, "danger"
	w.renderSettings(rw, r, status, d)
}
