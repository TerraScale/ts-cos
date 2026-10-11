package cos

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := OpenStore(filepath.Join(t.TempDir(), "cos.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

func storeStory(t *testing.T, s *Store) Story {
	t.Helper()
	story, err := s.Story(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return story
}

func storeRevision(t *testing.T, s *Store) int64 {
	t.Helper()
	r, err := s.Revision(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestSetTaskPatchPreservesOtherAnnotations(t *testing.T) {
	original := Task{Ticket: "TS-769", ModelWhy: "Go changes", Why: "replace manual state", Next: "run tests"}
	tests := []struct {
		name  string
		patch map[string]any
		want  Task
	}{
		{
			name:  "partial update",
			patch: map[string]any{"ticket": "ts-769", "next": "review PR"},
			want:  Task{Ticket: "TS-769", ModelWhy: "Go changes", Why: "replace manual state", Next: "review PR"},
		},
		{
			name:  "explicit empty field clears only that field",
			patch: map[string]any{"ticket": "TS-769", "why": ""},
			want:  Task{Ticket: "TS-769", ModelWhy: "Go changes", Next: "run tests"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := openTestStore(t)
			got, err := s.SetTask(t.Context(), map[string]any{
				"ticket": " ts-769 ", "modelWhy": "Go changes",
				"why": "replace manual state", "next": "run tests",
			})
			if err != nil {
				t.Fatal(err)
			}
			if got != original {
				t.Fatalf("initial annotations = %#v, want %#v", got, original)
			}
			got, err = s.SetTask(t.Context(), tt.patch)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Errorf("SetTask() = %#v, want %#v", got, tt.want)
			}
			if got := storeStory(t, s).Tasks; !reflect.DeepEqual(got, []Task{tt.want}) {
				t.Errorf("stored tasks = %#v, want %#v", got, []Task{tt.want})
			}
			patches, err := s.TaskPatches(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			wantPatch := map[string]any{"ticket": tt.want.Ticket, "modelWhy": tt.want.ModelWhy, "why": tt.want.Why, "next": tt.want.Next}
			if !reflect.DeepEqual(patches, map[string]map[string]any{"TS-769": wantPatch}) {
				t.Errorf("stored annotation patch = %#v, want %#v", patches, wantPatch)
			}
		})
	}
}

func TestNotesReplaceDirectionAndRemoveOrClearByKind(t *testing.T) {
	s := openTestStore(t)
	for _, text := range []string{"initial direction", "current direction"} {
		if _, err := s.AddNote(t.Context(), "direction", text); err != nil {
			t.Fatal(err)
		}
	}
	var notes []Note
	for _, kind := range []string{"need", "need", "decide", "missing", "suggest"} {
		n, err := s.AddNote(t.Context(), kind, kind+" note")
		if err != nil {
			t.Fatal(err)
		}
		notes = append(notes, n)
	}
	story := storeStory(t, s)
	if story.Direction != "current direction" || !reflect.DeepEqual(story.Notes, notes) {
		t.Fatalf("direction replacement lost other notes: %#v", story)
	}
	var directions int
	if err := s.DB.QueryRow(`SELECT count(*) FROM notes WHERE kind='direction'`).Scan(&directions); err != nil {
		t.Fatal(err)
	}
	if directions != 1 {
		t.Errorf("stored directions = %d, want 1", directions)
	}
	if err := s.RemoveNote(t.Context(), "decide", notes[0].ID); err != nil {
		t.Fatal(err)
	}
	if got := storeStory(t, s).Notes; !reflect.DeepEqual(got, notes) {
		t.Fatalf("wrong kind removed another kind's note: %#v", got)
	}
	if err := s.RemoveNote(t.Context(), "need", notes[0].ID); err != nil {
		t.Fatal(err)
	}
	if got := storeStory(t, s).Notes; !reflect.DeepEqual(got, notes[1:]) {
		t.Fatalf("RemoveNote() left %#v, want %#v", got, notes[1:])
	}
	if err := s.ClearNotes(t.Context(), "need"); err != nil {
		t.Fatal(err)
	}
	if got := storeStory(t, s); got.Direction != "current direction" || !reflect.DeepEqual(got.Notes, notes[2:]) {
		t.Fatalf("ClearNotes(need) changed unrelated notes: %#v", got)
	}
	if err := s.ClearNotes(t.Context(), "direction"); err != nil {
		t.Fatal(err)
	}
	if got := storeStory(t, s); got.Direction != "" || !reflect.DeepEqual(got.Notes, notes[2:]) {
		t.Errorf("ClearNotes(direction) = %#v", got)
	}
}

func TestRemoveTaskPreservesOtherAnnotations(t *testing.T) {
	s := openTestStore(t)
	for _, ticket := range []string{"TS-769", "TS-770"} {
		if _, err := s.SetTask(t.Context(), map[string]any{"ticket": ticket, "next": "keep annotation"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.RemoveTask(t.Context(), "ts-769"); err != nil {
		t.Fatal(err)
	}
	if got := storeStory(t, s).Tasks; !reflect.DeepEqual(got, []Task{{Ticket: "TS-770", Next: "keep annotation"}}) {
		t.Errorf("RemoveTask() left %#v", got)
	}
	patches, err := s.TaskPatches(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := patches["TS-769"]; exists {
		t.Error("removed task annotation remains in the patches")
	}
}

func TestStoreValidationDoesNotChangeStateOrRevision(t *testing.T) {
	s := openTestStore(t)
	if _, err := s.SetTask(t.Context(), map[string]any{"ticket": "TS-769", "why": "keep reason", "next": "keep this"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AddNote(t.Context(), "direction", "keep this direction"); err != nil {
		t.Fatal(err)
	}
	want, revision := storeStory(t, s), storeRevision(t, s)
	tests := []struct {
		name  string
		apply func() error
	}{
		{"unknown task field", func() error {
			_, err := s.SetTask(t.Context(), map[string]any{"ticket": "TS-769", "unknown": "value"})
			return err
		}},
		{"wrong task field type", func() error {
			_, err := s.SetTask(t.Context(), map[string]any{"ticket": "TS-769", "next": 123})
			return err
		}},
		{"missing ticket", func() error {
			_, err := s.SetTask(t.Context(), map[string]any{"ticket": "  ", "next": "value"})
			return err
		}},
		{"empty direction", func() error { _, err := s.AddNote(t.Context(), "direction", " \n "); return err }},
		{"invalid note kind", func() error { _, err := s.AddNote(t.Context(), "invalid", "value"); return err }},
		{"invalid remove kind", func() error { return s.RemoveNote(t.Context(), "invalid", 1) }},
		{"invalid clear kind", func() error { return s.ClearNotes(t.Context(), "invalid") }},
	}
	for _, field := range []struct{ name, value string }{
		{"priority", "FULL SEND"}, {"repo", "ts-cos"}, {"outcome", "ship slice 1"},
		{"status", "running"}, {"host", "local"}, {"model", "codex"}, {"pr", "https://example.test/pulls/1"},
	} {
		tests = append(tests, struct {
			name  string
			apply func() error
		}{
			name: "reject derived field " + field.name,
			apply: func() error {
				_, err := s.SetTask(t.Context(), map[string]any{"ticket": "TS-769", field.name: field.value, "next": "lose this"})
				return err
			},
		})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.apply(); err == nil {
				t.Fatal("invalid input succeeded")
			}
			if got := storeStory(t, s); !reflect.DeepEqual(got, want) {
				t.Errorf("failed validation changed story: %#v, want %#v", got, want)
			}
			if got := storeRevision(t, s); got != revision {
				t.Errorf("failed validation changed revision: %d, want %d", got, revision)
			}
		})
	}
}

func TestDirectionReplacementRollsBackAfterSQLiteValidationFailure(t *testing.T) {
	s := openTestStore(t)
	if _, err := s.AddNote(t.Context(), "direction", "existing direction"); err != nil {
		t.Fatal(err)
	}
	want, revision := storeStory(t, s), storeRevision(t, s)
	// Reject the INSERT after replacement has already deleted the old direction.
	if _, err := s.DB.Exec(`CREATE TRIGGER reject_direction BEFORE INSERT ON notes
		WHEN NEW.text = 'rejected direction' BEGIN SELECT RAISE(ABORT, 'invalid direction'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AddNote(t.Context(), "direction", "rejected direction"); err == nil {
		t.Fatal("SQLite validation failure was ignored")
	}
	if got := storeStory(t, s); !reflect.DeepEqual(got, want) {
		t.Errorf("failed replacement lost the original direction: %#v", got)
	}
	if got := storeRevision(t, s); got != revision {
		t.Errorf("rolled-back mutation changed revision to %d, want %d", got, revision)
	}
}

func TestStoreRestartPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cos.db")
	s, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if s != nil {
			_ = s.Close()
		}
	}()
	if _, err := s.SetTask(t.Context(), map[string]any{"ticket": "TS-769", "why": "ship slice 1", "modelWhy": "Go changes", "next": "review"}); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"direction", "need"} {
		if _, err := s.AddNote(t.Context(), kind, kind+" text"); err != nil {
			t.Fatal(err)
		}
	}
	live := Live{At: "2026-10-09T12:00:00Z", Fleet: liveWithThreads("TS-769"), Errors: map[string]string{"host": "offline"}}
	if err := s.SaveLive(t.Context(), live); err != nil {
		t.Fatal(err)
	}
	wantStory, revision := storeStory(t, s), storeRevision(t, s)
	wantPatches, err := s.TaskPatches(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = nil
	s, err = OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := storeStory(t, s); !reflect.DeepEqual(got, wantStory) {
		t.Errorf("reopened story = %#v, want %#v", got, wantStory)
	}
	if got := storeRevision(t, s); got != revision {
		t.Errorf("reopen changed revision: %d, want %d", got, revision)
	}
	gotPatches, err := s.TaskPatches(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotPatches, wantPatches) {
		t.Errorf("reopened patches = %#v, want %#v", gotPatches, wantPatches)
	}
	gotLive, err := s.Live(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotLive, live) {
		t.Errorf("reopened live = %#v, want %#v", gotLive, live)
	}
}

func liveWithThreads(tickets ...string) []HostFleet {
	threads := []FleetThread{}
	for _, ticket := range tickets {
		threads = append(threads, FleetThread{Title: ticket + " work"})
	}
	return []HostFleet{{Host: "beta", Threads: threads}}
}

func TestSaveLivePrunesOnlyDisappearedTicketsAfterSuccessfulPoll(t *testing.T) {
	tests := []struct {
		name        string
		polls       []Live
		wantRemoved bool
	}{
		{"successful poll removes disappeared override", []Live{{Fleet: liveWithThreads("TS-770")}}, true},
		{"successful poll keeps still live ticket", []Live{{Fleet: liveWithThreads("TS-769", "TS-770")}}, false},
		{"failed fleet poll preserves overrides", []Live{{Errors: map[string]string{"fleet": "unavailable"}}}, false},
		{"failed PR poll preserves overrides", []Live{{Fleet: liveWithThreads("TS-770"), Errors: map[string]string{"prs": "unavailable"}}}, false},
		{"other source error does not block cleanup", []Live{{Fleet: liveWithThreads("TS-770"), Errors: map[string]string{"forgejo": "unavailable"}}}, true},
		// RefreshLive retains the last successful fleet when the fleet poll is unavailable.
		{"successful recovery after failed fleet poll still removes disappeared override", []Live{
			{Fleet: liveWithThreads("TS-769", "TS-770"), Errors: map[string]string{"fleet": "unavailable"}},
			{Fleet: liveWithThreads("TS-770")},
		}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := openTestStore(t)
			if err := s.SaveLive(t.Context(), Live{Fleet: liveWithThreads("TS-769", "TS-770")}); err != nil {
				t.Fatal(err)
			}
			for _, ticket := range []string{"TS-769", "TS-770", "LOCAL-1"} {
				if _, err := s.SetTask(t.Context(), map[string]any{"ticket": ticket, "next": "keep override"}); err != nil {
					t.Fatal(err)
				}
			}
			for _, poll := range tt.polls {
				if err := s.SaveLive(t.Context(), poll); err != nil {
					t.Fatal(err)
				}
			}
			patches, err := s.TaskPatches(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			_, exists := patches["TS-769"]
			if exists == tt.wantRemoved {
				t.Errorf("TS-769 override exists = %t, want %t", exists, !tt.wantRemoved)
			}
			for _, ticket := range []string{"TS-770", "LOCAL-1"} {
				if patches[ticket]["next"] != "keep override" {
					t.Errorf("unrelated %s override was lost: %#v", ticket, patches[ticket])
				}
			}
		})
	}
}

func TestStoreWatchSeparateProcessWALCommit(t *testing.T) {
	const writerPathEnv = "COS_TEST_WATCH_WRITER_PATH"
	if path := os.Getenv(writerPathEnv); path != "" {
		s, err := OpenStore(path)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		var mode string
		if err := s.DB.QueryRow(`PRAGMA journal_mode`).Scan(&mode); err != nil || mode != "wal" {
			t.Fatalf("writer journal mode = %q, error = %v", mode, err)
		}
		input := bufio.NewScanner(os.Stdin)
		fmt.Fprintln(os.Stdout, "ready")
		if !input.Scan() || input.Text() != "commit" {
			t.Fatal("writer did not receive commit instruction")
		}
		if _, err := s.SetTask(t.Context(), map[string]any{"ticket": "TS-769", "next": "written by another process"}); err != nil {
			t.Fatal(err)
		}
		fmt.Fprintln(os.Stdout, "committed")
		if !input.Scan() || input.Text() != "finish" {
			t.Fatal("writer did not receive finish instruction")
		}
		return
	}

	s := openTestStore(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestStoreWatchSeparateProcessWALCommit$")
	cmd.Env = append(os.Environ(), writerPathEnv+"="+s.Path)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	defer func() {
		stdin.Close()
		if !waited {
			cancel()
			_ = cmd.Wait()
		}
	}()
	reader := bufio.NewReader(stdout)
	if line, err := reader.ReadString('\n'); err != nil || strings.TrimSpace(line) != "ready" {
		t.Fatalf("writer readiness = %q, error = %v", line, err)
	}
	// Start watching after the second handle opens, so opening it cannot satisfy the assertion.
	changes, errs, err := s.Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintln(stdin, "commit"); err != nil {
		t.Fatal(err)
	}
	if line, err := reader.ReadString('\n'); err != nil || strings.TrimSpace(line) != "committed" {
		t.Fatalf("writer commit = %q, error = %v", line, err)
	}
	select {
	case _, ok := <-changes:
		if !ok {
			t.Fatal("watcher closed before detecting the commit")
		}
	case err := <-errs:
		t.Fatalf("watcher error: %v", err)
	case <-ctx.Done():
		t.Fatal("watcher missed the separate process WAL commit")
	}
	if got := storeStory(t, s).Tasks; !reflect.DeepEqual(got, []Task{{Ticket: "TS-769", Next: "written by another process"}}) {
		t.Errorf("watch event preceded visible committed task: %#v", got)
	}
	if got := storeRevision(t, s); got != 1 {
		t.Errorf("external commit revision = %d, want 1", got)
	}
	if _, err := fmt.Fprintln(stdin, "finish"); err != nil {
		t.Fatal(err)
	}
	err = cmd.Wait()
	waited = true
	if err != nil {
		t.Fatalf("writer process failed: %v, stderr = %s", err, stderr.String())
	}
}
