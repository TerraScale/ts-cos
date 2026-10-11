package cos

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/nats-io/nats.go"
)

func TestSelfCheckPathRejectsPercent(t *testing.T) {
	bad := selfCheck{Summary: "s", Path: "/%2e%2e/x", Contains: []string{"x"}}
	if err := bad.validate(); err == nil {
		t.Fatal("percent-encoded path passed validation")
	}
	good := selfCheck{Summary: "s", Path: "/inbox", Contains: []string{"x"}}
	if err := good.validate(); err != nil {
		t.Fatal(err)
	}
}

func TestSelfCheckReplyIsClipped(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	buses, configs := authTestCluster(t, ctx)
	var wg sync.WaitGroup
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})
	// delta and mariogk-dev run the real probe; beta is a fake that lies about its host.
	for _, i := range []int{0, 2} {
		wg.Go(func() { runSelfProbe(ctx, configs[i], buses[i]) })
	}
	fake, err := buses[0].conn.Subscribe(selfCheckSubject("beta"), func(m *nats.Msg) {
		_ = m.Respond([]byte(`{"host":"evil","pass":true,"evidence":"` + strings.Repeat("a", 300) + `"}`))
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fake.Unsubscribe() })
	busTestReady(t, "three hosts", func() bool {
		hosts, err := buses[1].Hosts(ctx)
		return err == nil && len(hosts) == 3
	})

	results, _, err := runSelfCheck(ctx, buses[1], selfCheck{Summary: "s", Template: "updated", Contains: []string{"Updated:"}})
	if err != nil {
		t.Fatal(err)
	}
	var beta *selfCheckResult
	for i := range results {
		if results[i].Host == "beta" {
			beta = &results[i]
		}
	}
	if beta == nil {
		t.Fatalf("no beta result in %+v", results)
	}
	if len(beta.Evidence) > 203 {
		t.Fatalf("evidence is %d bytes, want at most 203", len(beta.Evidence))
	}
}
