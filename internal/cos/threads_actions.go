package cos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"time"
)

// threadOps are the Threads panel row buttons; the route answers 404 for anything else.
var threadOps = []string{"settle", "merge", "delete", "all"}

// threadWorktree is the bus's unpushed answer for a thread's worktree; the bus adds more fields the panel ignores.
type threadWorktree struct {
	Dirty   bool `json:"dirty"`
	Ahead   int  `json:"ahead"`
	Missing bool `json:"missing"`
}

// threadStep is one thing a row button does: verb names it in refusals, and run returns its past-tense outcome.
type threadStep struct {
	verb string
	run  func(context.Context) (string, error)
}

// threadAction runs one Threads row button on a tab and answers with its outcome as the notice.
func (w *Web) threadAction(rw http.ResponseWriter, r *http.Request) {
	op := r.PathValue("op")
	if !slices.Contains(threadOps, op) {
		http.NotFound(rw, r)
		return
	}
	tab := mainTab
	if id := r.PathValue("tab"); id != chatMainTab {
		t, ok := w.Chat.Tab(r.Context(), id)
		if !ok || t.Closed {
			tabRedirect(rw, r, "", "This tab is closed")
			return
		}
		tab = t
	}
	host, thread := r.FormValue("host"), r.FormValue("thread")
	outcome, ok := "This thread is not in this tab's panel", false
	if row, found := w.tabRow(r.Context(), tab, host, thread); found {
		outcome, ok = w.runThreadOp(r.Context(), tab, row, op)
	}
	w.recordThreadAction(r.Context(), tab, host, thread, op, outcome, ok)
	tabRedirect(rw, r, tab.ID, outcome)
}

// tabRow finds the panel row for host and thread; the tab's COS thread and other tabs' threads are not rows.
func (w *Web) tabRow(ctx context.Context, tab ChatTab, host, thread string) (panelThread, bool) {
	for _, row := range tabThreads(w.state(ctx).Live, w.Chat.Tags(ctx, tab.ID)) {
		if row.Host == host && row.ThreadID == thread {
			return row, true
		}
	}
	return panelThread{}, false
}

// runThreadOp does one row button; the outcome is the notice the reader sees, and ok says everything it meant to do happened.
func (w *Web) runThreadOp(ctx context.Context, tab ChatTab, row panelThread, op string) (string, bool) {
	switch op {
	case "settle":
		return runSteps(ctx, []threadStep{w.settleStep(row)})
	case "merge":
		return runSteps(ctx, []threadStep{w.mergeStep(tab, row)})
	case "delete":
		if problem := w.unpushedProblem(ctx, row, row.PR != nil && row.PR.State == "merged"); problem != "" {
			return "Not deleted: " + problem, false
		}
		return runSteps(ctx, []threadStep{w.deleteStep(row)})
	}
	if !row.openPR() {
		return "Not done: no open PR", false
	}
	if !row.PR.Mergeable {
		return "Not done: the PR is not mergeable", false
	}
	if problem := w.unpushedProblem(ctx, row, false); problem != "" {
		return "Not done: " + problem, false
	}
	return runSteps(ctx, []threadStep{w.mergeStep(tab, row), w.settleStep(row), w.deleteStep(row)})
}

// unpushedProblem says why a thread's worktree must not be deleted; "" means the delete is safe. allowAhead lets commits go, since a merged PR holds them.
func (w *Web) unpushedProblem(ctx context.Context, row panelThread, allowAhead bool) string {
	var work threadWorktree
	if err := w.threadCall(ctx, row.Host, "unpushed", row.ThreadID, &work); err != nil {
		return fmt.Sprintf("could not check '%s' for unpushed work: %s", row.Title, publicText(err))
	}
	switch {
	case work.Missing:
		return "its worktree is gone; check its branch by hand"
	case work.Dirty:
		return fmt.Sprintf("'%s' has uncommitted changes", row.Title)
	case work.Ahead > 0 && !allowAhead:
		return fmt.Sprintf("'%s' has %d commits no remote has", row.Title, work.Ahead)
	}
	return ""
}

func (w *Web) settleStep(row panelThread) threadStep {
	return threadStep{verb: "settled", run: func(ctx context.Context) (string, error) {
		var out TreeResult
		if err := w.threadCall(ctx, row.Host, "close", row.ThreadID, &out); err != nil {
			return "", err
		}
		if len(out.Skipped) > 0 {
			return "", fmt.Errorf("'%s' is still running", row.Title)
		}
		return fmt.Sprintf("settled '%s'", row.Title), nil
	}}
}

func (w *Web) deleteStep(row panelThread) threadStep {
	return threadStep{verb: "deleted", run: func(ctx context.Context) (string, error) {
		var out TreeResult
		if err := w.threadCall(ctx, row.Host, "delete", row.ThreadID, &out); err != nil {
			return "", err
		}
		if len(out.Skipped) > 0 {
			return "", fmt.Errorf("'%s' is still running", row.Title)
		}
		return fmt.Sprintf("deleted '%s'", row.Title), nil
	}}
}

// mergeStep force-merges the row's PR as a recorded full-send order; a failed record is reported, not a failed merge.
func (w *Web) mergeStep(tab ChatTab, row panelThread) threadStep {
	return threadStep{verb: "merged", run: func(ctx context.Context) (string, error) {
		if !row.openPR() {
			return "", errors.New("no open PR")
		}
		merge := w.merge
		if merge == nil {
			merge = Forgejo{Config: w.Cfg}.Merge
		}
		order := MergeOrder{FullSend: true, Reason: EventText(fmt.Sprintf("Force merge from the %s tab's thread panel", tab.Name), 200)}
		v, err := merge(ctx, row.PR.Repo, row.PR.Number, order)
		if err != nil {
			return "", err
		}
		result, _ := v.(map[string]any)
		sha, _ := result["sha"].(string)
		done := "merged " + row.PR.label()
		if err := recordMergeOrder(ctx, w.Cfg, row.PR.Repo, row.PR.Number, sha, order); err != nil {
			done += fmt.Sprintf(" (merge order not recorded: %s)", publicText(err))
		}
		return done, nil
	}}
}

// runSteps runs steps in order and stops at the first failure; the outcome says how far it got.
func runSteps(ctx context.Context, steps []threadStep) (string, bool) {
	var done []string
	for _, s := range steps {
		text, err := s.run(ctx)
		if err != nil {
			if len(done) == 0 {
				return fmt.Sprintf("Not %s: %s", s.verb, publicText(err)), false
			}
			return fmt.Sprintf("%s; not %s: %s", sentence(strings.Join(done, ", ")), s.verb, publicText(err)), false
		}
		done = append(done, text)
	}
	return sentence(strings.Join(done, ", ")), true
}

// sentence capitalizes the first letter of s; every step's text starts with an ASCII verb.
func sentence(s string) string { return strings.ToUpper(s[:1]) + s[1:] }

// threadCall runs one bus verb on a thread, through the test seam when one is set.
func (w *Web) threadCall(ctx context.Context, host, verb, thread string, out any) error {
	if w.call != nil {
		return w.call(ctx, host, verb, []string{thread}, out)
	}
	return w.Bus.Call(ctx, host, verb, []string{thread}, out)
}

// recordThreadAction leaves one outcome in the tab's chat and the event stream; a failed write is logged and never fails the request.
func (w *Web) recordThreadAction(ctx context.Context, tab ChatTab, host, thread, op, outcome string, ok bool) {
	now := time.Now().UTC()
	id := chatID(now)
	if err := w.publishThreadAction(ctx, id, now, tab, host, thread, op, outcome, ok); err != nil {
		logErr(ctx, slog.LevelError, "publish thread action", err)
	}
	if _, err := w.Chat.Put(ctx, ChatEntry{ID: id, At: now, Role: "system", Tab: tab.ID, Action: true, Text: outcome}); err != nil {
		logErr(ctx, slog.LevelError, "store thread action", err)
	}
}

func (w *Web) publishThreadAction(ctx context.Context, id string, now time.Time, tab ChatTab, host, thread, op, outcome string, ok bool) error {
	data, err := json.Marshal(map[string]any{"tab": tab.ID, "host": host, "threadId": thread, "op": op, "ok": ok, "result": outcome})
	if err != nil {
		return err
	}
	js, err := w.Bus.JetStream()
	if err != nil {
		return err
	}
	sev := SeverityInfo
	if !ok {
		sev = SeverityWarn
	}
	_, err = PublishEvent(ctx, js, Event{ID: NewEventID(SourceUser, "thread-action", id), Source: SourceUser, Kind: KindThreadAction, Time: now, Severity: sev,
		Summary: EventText(fmt.Sprintf("%s (tab %s)", outcome, tab.Name), eventSummaryMax), Host: host, Thread: thread, Data: data})
	return err
}

// recentActions is the tab's last five action outcomes, newest first; entries are in ID (time) order.
func recentActions(entries []ChatEntry) []ChatEntry {
	var out []ChatEntry
	for i := len(entries) - 1; i >= 0 && len(out) < 5; i-- {
		if entries[i].Action {
			out = append(out, entries[i])
		}
	}
	return out
}

// jsLiteral is s as a JavaScript string literal, for Datastar attributes; JSON encoding escapes it safely.
func jsLiteral(s string) string {
	b, _ := json.Marshal(s) // a string always marshals
	return string(b)
}

// confirmAction sets the shared confirm dialog's signals for one row button.
func confirmAction(op string, t panelThread) string {
	title, text := confirmCopy(op, t)
	return fmt.Sprintf("$actOp=%s;$actHost=%s;$actThread=%s;$actTitle=%s;$actText=%s",
		jsLiteral(op), jsLiteral(t.Host), jsLiteral(t.ThreadID), jsLiteral(title), jsLiteral(text))
}

// confirmCopy names a row button and says exactly what it does, for the confirm dialog.
func confirmCopy(op string, t panelThread) (title, text string) {
	switch op {
	case "merge":
		return "Force merge " + t.PR.label(), fmt.Sprintf("Force-merge %s (full send, recorded). This cannot be undone.", t.PR.label())
	case "delete":
		return "Delete '" + t.Title + "'", fmt.Sprintf("Delete '%s' and its subtree on %s. Refused if it has unpushed work. This cannot be undone.", t.Title, t.Host)
	case "all":
		return "All three for '" + t.Title + "'", fmt.Sprintf("Force-merge %s (full send, recorded), then settle and delete '%s'. This cannot be undone.", t.PR.label(), t.Title)
	}
	return "Settle '" + t.Title + "'", fmt.Sprintf("Settle '%s' and its subtree on %s. Running threads are skipped.", t.Title, t.Host)
}
