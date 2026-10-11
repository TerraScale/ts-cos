package cos

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func hookTestSign(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

// TestHooksStoreSignedDeliveriesOnce posts through the signed-out dashboard handler
// of one node of a real three-node cluster, as Forgejo does via cos.terrascale.tech.
func TestHooksStoreSignedDeliveriesOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	buses, configs := authTestCluster(t, ctx)
	hooks := NewHooks(buses[1])
	started := make(chan struct{})
	go func() { hooks.Start(ctx); close(started) }()
	select {
	case <-started:
	case <-time.After(40 * time.Second):
		t.Fatal("ingest stream was not created")
	}
	js, _ := buses[1].JetStream()
	auth := NewAuth(configs[1].Home, js)
	auth.Start(ctx)
	server := httptest.NewServer(auth.Protect(http.NotFoundHandler(), hooks))
	defer server.Close()
	secret := buses[0].HookSecret("forgejo")
	if len(secret) != 64 || secret != buses[2].HookSecret("forgejo") || buses[0].HookSecret("github") != "" {
		t.Fatal("hook secrets must be equal on every node and only for known sources")
	}
	post := func(path, body string, header ...string) int {
		req, _ := http.NewRequest("POST", server.URL+path, strings.NewReader(body))
		for i := 0; i+1 < len(header); i += 2 {
			req.Header.Set(header[i], header[i+1])
		}
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		return response.StatusCode
	}
	stream, err := js.Stream(ctx, IngestStream)
	if err != nil {
		t.Fatal(err)
	}
	stored := func() uint64 {
		info, err := stream.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return info.State.Msgs
	}

	body := `{"action":"opened","number":17}`
	forgejo := []string{"X-Forgejo-Event", "pull_request", "X-Forgejo-Delivery", "6f1c2c1e-1111-4a5b-8c9d-123456789abc", "X-Forgejo-Signature", hookTestSign(secret, []byte(body))}
	if code := post("/hooks/forgejo", body, forgejo...); code != http.StatusAccepted {
		t.Fatalf("signed Forgejo delivery = %d", code)
	}
	if code := post("/hooks/forgejo", body, forgejo...); code != http.StatusAccepted || stored() != 1 {
		t.Fatalf("redelivery = %d, stored %d", code, stored())
	}
	msg, err := stream.GetLastMsgForSubject(ctx, IngestForgejo)
	if err != nil || string(msg.Data) != body || msg.Header.Get("X-Forgejo-Event") != "pull_request" || msg.Header.Get(jetstream.MsgIDHeader) != "forgejo:6f1c2c1e-1111-4a5b-8c9d-123456789abc" {
		t.Fatalf("stored delivery %+v, %v", msg, err)
	}

	for name, test := range map[string]struct {
		path, body string
		header     []string
		want       int
	}{
		"unsigned":                  {"/hooks/forgejo", body, nil, http.StatusUnauthorized},
		"another key":               {"/hooks/forgejo", body, []string{"X-Forgejo-Signature", hookTestSign("guess", []byte(body))}, http.StatusUnauthorized},
		"altered body":              {"/hooks/forgejo", body + " ", forgejo[4:], http.StatusUnauthorized},
		"unknown source":            {"/hooks/github", body, nil, http.StatusNotFound},
		"retired Ku source":         {"/hooks/ku", body, forgejo[4:], http.StatusNotFound},
		"over the size cap":         {"/hooks/forgejo", strings.Repeat("x", ingestMaxMsg+1), nil, http.StatusRequestEntityTooLarge},
		"dashboard stays signed in": {"/autopilot/run", "", nil, http.StatusUnauthorized},
	} {
		if code := post(test.path, test.body, test.header...); code != test.want {
			t.Errorf("%s = %d, want %d", name, code, test.want)
		}
	}
	if stored() != 1 {
		t.Fatalf("rejected deliveries were stored: %d", stored())
	}

	var last int
	for range hookPerMinute + 1 {
		last = post("/hooks/forgejo", "{}", "Cf-Connecting-Ip", "203.0.113.7")
	}
	if last != http.StatusTooManyRequests {
		t.Fatalf("request %d from one address = %d", hookPerMinute+1, last)
	}
}
