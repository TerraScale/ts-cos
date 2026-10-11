package cos

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// journalAt formats a time the way `journalctl -o short-unix` prints it.
func journalAt(at time.Time, text string) string {
	return fmt.Sprintf("%d.000000 delta %s\n", at.Unix(), text)
}

func restartSummary(restarts []NodeRestart) []string {
	var out []string
	for _, r := range restarts {
		out = append(out, strings.TrimSpace(r.Kind+" "+r.Detail))
	}
	return out
}

func TestClassifyRestarts(t *testing.T) {
	t0 := time.Date(2026, 10, 10, 9, 0, 0, 0, time.UTC)
	const (
		started   = "systemd[1]: Started TerraScale COS."
		stopped   = "systemd[1]: Stopped TerraScale COS."
		scheduled = "systemd[1]: cos.service: Scheduled restart job, restart counter is at 1."
		deployLog = "cos[812]: 2026/10/10 10:00:00 INFO restarting into another build action=update revision=abc123def456"
		e2eKill   = "cos-e2e[4321]: check=cluster.restart action=restart"
	)
	tests := []struct {
		name string
		unit string // cos.service journal
		e2e  string // cos-e2e tag journal, read separately
		want []string
	}{
		{
			name: "deploy restart",
			unit: journalAt(t0, started) +
				journalAt(t0.Add(time.Hour), deployLog) +
				journalAt(t0.Add(time.Hour+time.Second), scheduled) +
				journalAt(t0.Add(time.Hour+2*time.Second), started),
			want: []string{"manual", "deploy abc123def456"},
		},
		{
			name: "e2e kill restarts within three minutes",
			unit: journalAt(t0, started) +
				journalAt(t0.Add(2*time.Hour+time.Second), scheduled) +
				journalAt(t0.Add(2*time.Hour+5*time.Second), started),
			e2e:  journalAt(t0.Add(2*time.Hour), e2eKill),
			want: []string{"manual", "e2e check=cluster.restart action=restart"},
		},
		{
			name: "e2e kill older than three minutes is a crash",
			unit: journalAt(t0, started) +
				journalAt(t0.Add(3*time.Hour+4*time.Minute), scheduled) +
				journalAt(t0.Add(3*time.Hour+4*time.Minute+5*time.Second), started),
			e2e:  journalAt(t0.Add(3*time.Hour), e2eKill),
			want: []string{"manual", "unexpected cos.service: Scheduled restart job, restart counter is at 1."},
		},
		{
			name: "crash with scheduled restart job",
			unit: journalAt(t0, started) +
				journalAt(t0.Add(5*time.Hour), scheduled) +
				journalAt(t0.Add(5*time.Hour+5*time.Second), started),
			want: []string{"manual", "unexpected cos.service: Scheduled restart job, restart counter is at 1."},
		},
		{
			name: "manual systemctl restart",
			unit: journalAt(t0, started) +
				journalAt(t0.Add(6*time.Hour), stopped) +
				journalAt(t0.Add(6*time.Hour+time.Second), started),
			want: []string{"manual", "manual"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// The e2e source is listed first on purpose: classifyRestarts must sort by time across both sources.
			lines := append(parseJournal(tt.e2e), parseJournal(tt.unit)...)
			if got := restartSummary(classifyRestarts(lines)); strings.Join(got, "|") != strings.Join(tt.want, "|") {
				t.Fatalf("restarts = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestClassifyRestartsKeepsNewest(t *testing.T) {
	t0 := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	var lines []journalLine
	for i := range 60 {
		lines = append(lines, journalLine{Time: t0.Add(time.Duration(i) * time.Hour), Text: "systemd[1]: Started TerraScale COS."})
	}
	got := classifyRestarts(lines)
	if len(got) != restartKeep || !got[len(got)-1].Time.Equal(t0.Add(59*time.Hour)) {
		t.Fatalf("kept %d restarts ending %v, want the newest %d", len(got), got[len(got)-1].Time, restartKeep)
	}
}

func TestParseJournalSkipsNoEntries(t *testing.T) {
	lines := parseJournal("-- No entries --\n\n1760090400.500000 delta systemd[1]: Started TerraScale COS.\n")
	if len(lines) != 1 || lines[0].Text != "delta systemd[1]: Started TerraScale COS." || lines[0].Time.Unix() != 1760090400 {
		t.Fatalf("parsed %+v", lines)
	}
}

func TestNodeDetailRestartsRender(t *testing.T) {
	at := time.Date(2026, 10, 10, 9, 0, 0, 0, time.UTC)
	card := nodeFixtureCards(at)[0]
	card.Restarts = []NodeRestart{
		{Time: at, Kind: "deploy", Detail: "abc123def456"},
		{Time: at.Add(time.Hour), Kind: "e2e", Detail: "check=cluster.restart action=restart"},
		{Time: at.Add(2 * time.Hour), Kind: "unexpected", Detail: "crash"},
		{Time: at.Add(3 * time.Hour), Kind: "manual"},
	}
	var buf bytes.Buffer
	if err := nodeDetailPage(chatHeader{}, card, "24h", nil).Render(context.Background(), &buf); err != nil {
		t.Fatal(err)
	}
	html := buf.String()
	for _, want := range []string{"Restarts (7 d)", "Oct 10 09:00", `data-stage="done">deploy</span>`, `data-stage="review">e2e</span>`, `data-stage="failed">unexpected</span>`, `data-stage="queued">manual</span>`} {
		if !strings.Contains(html, want) {
			t.Errorf("detail page missing %s", want)
		}
	}
	assertCleanMarkup(t, html)

	card.Restarts = nil
	buf.Reset()
	if err := nodeDetailPage(chatHeader{}, card, "24h", nil).Render(context.Background(), &buf); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "No restarts in 7 days") {
		t.Error("empty history needs the muted empty line")
	}
}

func TestNodesPageRestartTile(t *testing.T) {
	now := time.Now()
	cards := nodeFixtureCards(now)
	cards[0].Restarts = []NodeRestart{{Time: now, Kind: "deploy"}, {Time: now, Kind: "e2e"}, {Time: now, Kind: "unexpected"}}
	cards[1].Restarts = []NodeRestart{{Time: now, Kind: "unexpected"}}
	cards[1].restartsErr = true
	var buf bytes.Buffer
	if err := nodesPage(chatHeader{}, ClusterView{}, cards).Render(context.Background(), &buf); err != nil {
		t.Fatal(err)
	}
	html := buf.String()
	if !strings.Contains(html, `data-stage="failed">1 unexpected</span>`) {
		t.Error("overview tile missing the unexpected-restart flag")
	}
	if strings.Count(html, "unexpected</span>") != 1 {
		t.Error("a host whose restart history failed must not flag unexpected restarts")
	}
	assertCleanMarkup(t, html)
}

func TestNodeRestartsMissingBucketIsEmpty(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	js := busTestJetStream(t, ctx)
	busTestReady(t, "JetStream", func() bool {
		ready, stop := context.WithTimeout(ctx, time.Second)
		defer stop()
		_, err := js[0].AccountInfo(ready)
		return err == nil
	})
	restarts, err := NodeRestarts(ctx, js[0], "delta")
	if err != nil || restarts != nil {
		t.Fatalf("NodeRestarts = %v, %v; want nil, nil before any node wrote a record", restarts, err)
	}
}
