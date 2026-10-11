package cos

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

// TestE2EChatProbe runs the chat.non-leader-message probe on a three-node test cluster: each node serves its own chat stream.
func TestE2EChatProbe(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	buses, _ := chatTestCluster(t, ctx)
	webs := make([]*Web, len(buses))
	for i, bus := range buses {
		store, err := OpenStore(filepath.Join(t.TempDir(), "cos.db"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { store.Close() })
		webs[i] = NewWeb(store, nil)
		webs[i].Chat.recheck = 100 * time.Millisecond
		// Start every node first: only the JetStream leader creates the chat bucket.
		go webs[i].Chat.Run(ctx, bus)
	}
	bases := make([]string, len(buses))
	for i, web := range webs {
		chatTestAwait(t, web.Chat, "chat ready", web.Chat.Ready)
		server := httptest.NewServer(web.routes())
		t.Cleanup(server.Close)
		bases[i] = server.URL
	}
	evidence, err := chatProbeAcross(ctx, buses[2], bases, make([]string, len(bases)))
	if err != nil {
		t.Fatal(err)
	}
	t.Log(evidence)
}
