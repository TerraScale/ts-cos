package cos

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

// codexLine is the part of one Codex rollout line that usage needs.
type codexLine struct {
	Timestamp time.Time `json:"timestamp"`
	Type      string    `json:"type"`
	Payload   struct {
		Type      string `json:"type"`
		SessionID string `json:"session_id"`
		CWD       string `json:"cwd"`
		Git       struct {
			Branch string `json:"branch"`
		} `json:"git"`
		Model  string `json:"model"`
		Effort string `json:"effort"`
		Info   *struct {
			Total codexTokens `json:"total_token_usage"`
		} `json:"info"`
		CallID    string          `json:"call_id"`
		Input     string          `json:"input"`     // custom_tool_call: the JS source
		Arguments string          `json:"arguments"` // function_call: a JSON string
		Output    json.RawMessage `json:"output"`    // a string, or a list of input_text blocks
	} `json:"payload"`
}

// codexTokens is a session's running total. Input includes the cached input, and output includes the reasoning output.
type codexTokens struct {
	Input      int64 `json:"input_tokens"`
	Cached     int64 `json:"cached_input_tokens"`
	CacheWrite int64 `json:"cache_write_input_tokens"`
	Output     int64 `json:"output_tokens"`
}

// codexDelta is what one token_count adds to the running total. A zero delta is a duplicate event and reports false.
// A total that fell means the counter reset, so the new total is the whole delta.
func codexDelta(total, prev codexTokens) (codexTokens, bool) {
	d := codexTokens{total.Input - prev.Input, total.Cached - prev.Cached, total.CacheWrite - prev.CacheWrite, total.Output - prev.Output}
	if d.Input < 0 || d.Cached < 0 || d.CacheWrite < 0 || d.Output < 0 {
		d = total
	}
	return d, d != codexTokens{}
}

// codexProject encodes a cwd the way Claude names its project dirs, so usageProject reads both alike.
func codexProject(cwd string) string {
	return strings.NewReplacer("/", "-", ".", "-").Replace(cwd)
}

// scanCodexSession sums one rollout file. Each token_count carries the session's running total, so usage is the
// difference from the event before it, and the model and effort come from the latest turn_context.
func scanCodexSession(path, host string, since time.Time) ([]UsageRow, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var session, project, branch, model, effort string
	var prev codexTokens
	var pending string // kind of the latest rework output not yet answered
	rebases := map[string]bool{}
	merged := map[usageGroup]UsageRow{}
	r := bufio.NewReader(f)
	for {
		line, err := r.ReadBytes('\n')
		if err == io.EOF {
			break // a final line without '\n' is still being written
		}
		if err != nil {
			return nil, err
		}
		if !bytes.Contains(line, []byte(`"session_meta"`)) && !bytes.Contains(line, []byte(`"turn_context"`)) && !bytes.Contains(line, []byte(`"token_count"`)) && !bytes.Contains(line, []byte(`"call_id"`)) {
			continue
		}
		var e codexLine
		if json.Unmarshal(line, &e) != nil {
			continue
		}
		switch e.Type {
		case "session_meta":
			session, branch, project = e.Payload.SessionID, e.Payload.Git.Branch, codexProject(e.Payload.CWD)
		case "turn_context":
			model, effort = e.Payload.Model, e.Payload.Effort
		case "response_item":
			switch e.Payload.Type {
			case "custom_tool_call", "function_call":
				if strings.Contains(e.Payload.Input, "git rebase") || strings.Contains(e.Payload.Arguments, "git rebase") {
					rebases[e.Payload.CallID] = true
				}
			case "custom_tool_call_output", "function_call_output":
				text := usageResultText(e.Payload.Output)
				switch {
				case slices.ContainsFunc(usageGateFails, func(s string) bool { return strings.Contains(text, s) }):
					pending = "gate"
				case rebases[e.Payload.CallID]:
					pending = "rebase"
				}
			}
		case "event_msg":
			if e.Payload.Type != "token_count" || e.Payload.Info == nil {
				continue
			}
			// prev moves on every event, even before since, so the first event in the window adds only its own delta.
			d, ok := codexDelta(e.Payload.Info.Total, prev)
			prev = e.Payload.Info.Total
			ts := e.Timestamp.UTC()
			if !ok || ts.Before(since) {
				continue
			}
			kind, worktree := usageProject(project)
			row := UsageRow{Day: ts.Format(time.DateOnly), Host: host, Project: project, Kind: kind, Worktree: worktree,
				Branch: branch, Session: session, Model: usageModel(model), Effort: effort,
				Input: d.Input - d.Cached, CacheRead: d.Cached, CacheWrite5m: d.CacheWrite, Output: d.Output,
				Messages: 1, First: ts, Last: ts}
			if pending != "" {
				// The delta after the rework output is the turn that reacts to it.
				// shortcut: codex API retries are not counted, upgrade when rollouts mark retried requests.
				row.Waste = map[string]int64{pending: usageTokens(UsageTotal{Input: row.Input, Output: row.Output, CacheWrite: row.CacheWrite5m})}
				pending = ""
			}
			usageAdd(merged, row)
		}
	}
	return usageRows(merged), nil
}

// codexScanner caches each rollout file's rows until its size or mtime changes, the way usageScanner does for transcripts.
type codexScanner struct {
	files map[string]usageFile
}

// Scan returns this host's rows for every rollout file under root. A missing root has no rows and no error.
func (s *codexScanner) Scan(root, host string, now time.Time) ([]UsageRow, error) {
	since := now.UTC().Truncate(24*time.Hour).AddDate(0, 0, -usageKeepDays)
	sinceDay := since.Format(time.DateOnly)
	cutoff := now.AddDate(0, 0, -usageKeepDays)
	files := map[string]usageFile{}
	merged := map[usageGroup]UsageRow{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || filepath.Ext(path) != ".jsonl" {
			return nil
		}
		info, err := d.Info()
		if err != nil || info.ModTime().Before(cutoff) {
			return nil
		}
		file, ok := s.files[path]
		if !ok || !file.modTime.Equal(info.ModTime()) || file.size != info.Size() {
			rows, err := scanCodexSession(path, host, since)
			if err != nil {
				slog.Warn("scan codex session", "path", path, "error", err)
				return nil
			}
			file = usageFile{info.ModTime(), info.Size(), rows}
		}
		files[path] = file
		for _, r := range file.rows {
			if r.Day >= sinceDay {
				usageAdd(merged, r)
			}
		}
		return nil
	})
	s.files = files
	return usageRows(merged), err
}

// todayCodexUsage scans the rollout files modified since the UTC day started, and keeps only today's rows.
func todayCodexUsage(root, host string, now time.Time) ([]UsageRow, error) {
	dayStart := now.UTC().Truncate(24 * time.Hour)
	day := dayStart.Format(time.DateOnly)
	merged := map[usageGroup]UsageRow{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || filepath.Ext(path) != ".jsonl" {
			return nil
		}
		info, err := d.Info()
		if err != nil || info.ModTime().Before(dayStart) {
			return nil
		}
		rows, err := scanCodexSession(path, host, dayStart)
		if err != nil {
			slog.Warn("scan codex session", "path", path, "error", err)
			return nil
		}
		for _, r := range rows {
			if r.Day == day {
				usageAdd(merged, r)
			}
		}
		return nil
	})
	return usageRows(merged), err
}

// codexTotals counts one UTC day straight from the rollout files under root. Like transcriptTotals it is a second
// parser on purpose, so it shares none of scanCodexSession's code.
func codexTotals(root, day string) (usageCount, error) {
	start, err := time.Parse(time.DateOnly, day)
	if err != nil {
		return usageCount{}, err
	}
	var total usageCount
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || filepath.Ext(path) != ".jsonl" {
			return nil
		}
		info, err := d.Info()
		if err != nil || info.ModTime().Before(start) {
			return nil
		}
		return sumCodexDay(path, day, &total)
	})
	return total, err
}

// sumCodexDay adds one file's token_count deltas for day to total. A reset or duplicate is handled as in codexDelta.
func sumCodexDay(path, day string, total *usageCount) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	var prev [4]int64 // input, cached input, cache writes, output
	r := bufio.NewReader(f)
	for {
		line, err := r.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		var e map[string]any
		if json.Unmarshal(line, &e) != nil || e["type"] != "event_msg" {
			continue
		}
		p, _ := e["payload"].(map[string]any)
		info, _ := p["info"].(map[string]any)
		cum, _ := info["total_token_usage"].(map[string]any)
		if p["type"] != "token_count" || cum == nil {
			continue
		}
		now := [4]int64{jsonInt(cum, "input_tokens"), jsonInt(cum, "cached_input_tokens"), jsonInt(cum, "cache_write_input_tokens"), jsonInt(cum, "output_tokens")}
		var delta [4]int64
		reset := false
		for i := range now {
			delta[i] = now[i] - prev[i]
			reset = reset || delta[i] < 0
		}
		prev = now
		if reset {
			delta = now
		}
		stamp, _ := e["timestamp"].(string)
		ts, err := time.Parse(time.RFC3339Nano, stamp)
		if err != nil || ts.UTC().Format(time.DateOnly) != day || delta == [4]int64{} {
			continue
		}
		total.Messages++
		total.Tokens += delta[0] - delta[1] + delta[2] + delta[3]
		total.Output += delta[3]
		total.CacheRead += delta[1]
	}
}
