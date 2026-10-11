package cos

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

// publishEscalation writes one rules-engine escalation to the log, as Autopilot.publish does.
func (e *agentTestEnv) publishEscalation(entry AutopilotEntry) {
	e.t.Helper()
	data, err := json.Marshal(entry)
	if err != nil {
		e.t.Fatal(err)
	}
	if _, err := e.js.Publish(e.ctx, agentDigestSubject, data); err != nil {
		e.t.Fatal(err)
	}
}

// window starts the digest window now: only escalations published after this call are new.
func (e *agentTestEnv) window() time.Time {
	e.t.Helper()
	since := time.Now().UTC()
	st := e.state()
	st.DigestSince = since
	e.setState(st)
	return since
}

// savingCounter is today's counter for one saving kind.
func (e *agentTestEnv) savingCounter(kind string) SavingCounter {
	e.t.Helper()
	days, err := SavingsReport(e.ctx, e.savingsKV, 1, time.Now())
	if err != nil {
		e.t.Fatal(err)
	}
	return days[0].Kinds[kind]
}

// digestTestWord gives each i its own three-letter word, so flood summaries differ in shape and do not fold.
func digestTestWord(i int) string {
	return string([]byte{'a' + byte(i/676%26), 'a' + byte(i/26%26), 'a' + byte(i%26)})
}

func TestAgentDigest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	e := newAgentTestEnv(t, ctx)

	t.Run("first start replays nothing", func(t *testing.T) {
		e.reset(t)
		e.publishEscalation(AutopilotEntry{Kind: ClassEscalate, Event: "old", Action: "orphan", Reason: "no live thread owns it"})
		e.step()
		e.expectLaunches(0)
		if e.state().DigestSince.IsZero() {
			t.Fatal("first step did not start the digest window")
		}
	})

	t.Run("one wake per window with the escalations folded", func(t *testing.T) {
		e.reset(t)
		before := e.savingCounter(SavingDedup)
		since := e.window()
		for i := range 3 {
			e.publishEscalation(AutopilotEntry{Kind: ClassEscalate, Event: fmt.Sprintf("question-%d", i), Host: "delta", Target: "delta thread-1",
				Action: "worker-question", Reason: "a worker needs a decision: pick the rebase base"})
		}
		for i := range 2 {
			e.publishEscalation(AutopilotEntry{Kind: ClassEscalate, Event: fmt.Sprintf("orphan-%d", i), Host: "delta",
				Action: "orphan", Reason: "no live thread owns it"})
		}

		// The worker question is urgent, so the digest is held for agentUrgentDelay and the window does not move.
		e.step()
		e.expectLaunches(0)
		if !e.state().DigestSince.Equal(since) {
			t.Fatalf("held digest moved the window to %s", e.state().DigestSince)
		}
		st := e.state()
		text, err := e.a.digestWake(e.ctx, &st, time.Now().UTC().Add(agentUrgentDelay))
		if err != nil {
			t.Fatal(err)
		}
		for _, want := range []string{"Escalations since " + since.Format("15:04") + " (5 events, 0 merged, 0 left out):",
			"[urgent] worker-question", "(x3)", "[warn] orphan", "(x2)"} {
			if !strings.Contains(text, want) {
				t.Fatalf("digest missing %q:\n%s", want, text)
			}
		}
		if strings.Count(text, "[urgent] worker-question") != 1 {
			t.Fatalf("worker question not folded to one line:\n%s", text)
		}
		if !st.DigestSince.After(since) {
			t.Fatalf("digest window did not advance past %s", since)
		}
		again, err := e.a.escalations(e.ctx, st.DigestSince)
		if err != nil {
			t.Fatal(err)
		}
		if len(again) != 0 {
			t.Fatalf("%d escalations re-read after the digest, the last one included", len(again))
		}
		if got := e.savingCounter(SavingDedup); got.Events <= before.Events || got.Tokens <= before.Tokens {
			t.Fatalf("dedup saving not recorded: before %+v, after %+v", before, got)
		}
	})

	t.Run("flood is capped at the digest size", func(t *testing.T) {
		e.reset(t)
		before := e.savingCounter(SavingDigestCap)
		e.window()
		for i := range 300 {
			e.publishEscalation(AutopilotEntry{Kind: ClassEscalate, Event: fmt.Sprintf("flood-%d", i), Host: "delta",
				Target: fmt.Sprintf("delta thread-%d", i), Action: "worker-question",
				Reason: digestTestWord(i) + " " + strings.Repeat("the worker is waiting on a decision ", 8)})
		}
		st := e.state()
		text, err := e.a.digestWake(e.ctx, &st, time.Now().UTC().Add(agentUrgentDelay))
		if err != nil {
			t.Fatal(err)
		}
		header, body, ok := strings.Cut(text, "\n")
		if !ok || !strings.HasPrefix(header, "Escalations since") {
			t.Fatalf("digest header missing:\n%s", text)
		}
		if len(body) > digestMaxBytes {
			t.Fatalf("digest body is %d bytes, cap is %d", len(body), digestMaxBytes)
		}
		if !strings.Contains(body, "more events omitted") {
			t.Fatalf("capped digest has no omitted footer:\n%s", body)
		}
		t.Logf("flood: body %d bytes, cap %d, last line %q", len(body), digestMaxBytes, body[strings.LastIndex(strings.TrimSpace(body), "\n")+1:])
		if got := e.savingCounter(SavingDigestCap); got.Tokens <= before.Tokens {
			t.Fatalf("digest-cap saving not recorded: before %+v, after %+v", before, got)
		}
	})
}
