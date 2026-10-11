package cos

import (
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"git.terrascale.tech/TerraScale/ts-cos/internal/webpush"
	"github.com/nats-io/nats.go/jetstream"
)

// newPushSubscription makes what a browser hands over: a P-256 key pair and 16 auth bytes. It returns the
// private key and auth so a receiver can decrypt what Send posts.
func newPushSubscription(endpoint string) (webpush.Subscription, *ecdh.PrivateKey, []byte, error) {
	priv, err := ecdh.P256().GenerateKey(rand.Reader)
	if err != nil {
		return webpush.Subscription{}, nil, nil, err
	}
	auth := make([]byte, 16)
	if _, err := rand.Read(auth); err != nil {
		return webpush.Subscription{}, nil, nil, err
	}
	var sub webpush.Subscription
	sub.Endpoint = endpoint
	sub.Keys.P256dh = base64.RawURLEncoding.EncodeToString(priv.PublicKey().Bytes())
	sub.Keys.Auth = base64.RawURLEncoding.EncodeToString(auth)
	return sub, priv, auth, nil
}

// pushTestStores opens the push store on every bus. Only the JetStream leader may create the bucket,
// so each node must keep trying until the leader has done so.
func pushTestStores(t *testing.T, ctx context.Context, buses []*Bus) []*Push {
	t.Helper()
	stores := make([]*Push, len(buses))
	chatTestWait(t, "push stores", func() bool {
		ready := true
		for i, bus := range buses {
			if stores[i] != nil {
				continue
			}
			var err error
			if stores[i], err = OpenPush(ctx, bus); err != nil {
				stores[i], ready = nil, false
			}
		}
		return ready
	})
	return stores
}

func TestPushStoreReplicatesKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	buses, _ := chatTestCluster(t, ctx)
	stores := pushTestStores(t, ctx, buses)
	if stores[0].vapid.PublicKey() != stores[2].vapid.PublicKey() {
		t.Fatal("two nodes opened different VAPID keys")
	}
}

func TestPushSubscribeRoutes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	buses, configs := chatTestCluster(t, ctx)
	authTestAdmin(t, configs[0].Home, "tester", "first-password-1", 0600)
	js, err := buses[0].JetStream()
	if err != nil {
		t.Fatal(err)
	}
	auth := NewAuth(configs[0].Home, js)
	go auth.Start(ctx)
	w := &Web{auth: auth, Hooks: http.NotFoundHandler(), Chat: NewChat()}
	stores := pushTestStores(t, ctx, buses)
	p := stores[0]
	w.push.Store(p)
	server := httptest.NewServer(w.Handler())
	t.Cleanup(server.Close)

	c := authTestClient{t, &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	chatTestWait(t, "auth store", func() bool { users, _, _ := auth.kv(); return users != nil })
	// One attempt only: failed sign-ins count toward the lockout.
	response, _ := c.do(server, "POST", "/login", url.Values{"user": {"tester"}, "password": {"first-password-1"}}, nil)
	cookie := sessionOf(response)
	if cookie == nil {
		t.Fatalf("sign-in: HTTP %d", response.StatusCode)
	}
	post := func(contentType string, body any) int {
		t.Helper()
		data, _ := json.Marshal(body)
		req, _ := http.NewRequest("POST", server.URL+"/push/subscribe", strings.NewReader(string(data)))
		req.Header.Set("Content-Type", contentType)
		req.AddCookie(cookie)
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		return response.StatusCode
	}

	good, _, _, err := newPushSubscription("https://fcm.googleapis.com/fcm/send/test-good")
	if err != nil {
		t.Fatal(err)
	}
	if got := post("application/json", good); got != http.StatusNoContent {
		t.Fatalf("valid subscription: HTTP %d, want 204", got)
	}
	entry, err := p.kv.Get(ctx, pushSubKey(good.Endpoint))
	if err != nil {
		t.Fatal(err)
	}
	var stored pushSub
	if err := json.Unmarshal(entry.Value(), &stored); err != nil || stored.User != "tester" {
		t.Fatalf("stored subscription %+v, %v", stored, err)
	}
	for _, endpoint := range []string{"http://fcm.googleapis.com/fcm/send/x", "https://10.0.0.1/send/x", "https://fcm.googleapis.com:8443/send/x", "https://evil.example/send/x"} {
		bad, _, _, err := newPushSubscription(endpoint)
		if err != nil {
			t.Fatal(err)
		}
		if got := post("application/json", bad); got != http.StatusBadRequest {
			t.Fatalf("%s: HTTP %d, want 400", endpoint, got)
		}
	}
	if got := post("text/plain", good); got != http.StatusUnsupportedMediaType {
		t.Fatalf("text/plain: HTTP %d, want 415", got)
	}
}

func TestPushSendDelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	buses, _ := chatTestCluster(t, ctx)
	p := pushTestStores(t, ctx, buses)[0]

	// The receiver stands in for a push service; /gone answers like an expired subscription.
	type delivery struct {
		payload pushPayload
		err     error
	}
	deliveries := make(chan delivery, 8)
	ok, priv, auth, err := newPushSubscription("")
	if err != nil {
		t.Fatal(err)
	}
	receiver := httptest.NewTLSServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/gone" {
			rw.WriteHeader(http.StatusGone)
			return
		}
		body, _ := io.ReadAll(r.Body)
		plain, err := webpush.Decrypt(priv, auth, body)
		var d delivery
		d.err = err
		_ = json.Unmarshal(plain, &d.payload)
		deliveries <- d
		rw.WriteHeader(http.StatusCreated)
	}))
	t.Cleanup(receiver.Close)
	p.client = pushClient()
	p.client.Transport = receiver.Client().Transport

	ok.Endpoint = receiver.URL + "/ok"
	gone, _, _, err := newPushSubscription(receiver.URL + "/gone")
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Subscribe(ctx, "tester", "test", ok); err != nil {
		t.Fatal(err)
	}
	if err := p.Subscribe(ctx, "tester", "test", gone); err != nil {
		t.Fatal(err)
	}
	receive := func() pushPayload {
		t.Helper()
		select {
		case d := <-deliveries:
			if d.err != nil {
				t.Fatal(d.err)
			}
			return d.payload
		default:
			t.Fatal("no delivery reached the receiver")
		}
		return pushPayload{}
	}
	nothing := func() {
		t.Helper()
		select {
		case d := <-deliveries:
			t.Fatalf("unexpected delivery: %+v", d.payload)
		default:
		}
	}

	sent, err := p.Send(ctx, Notice{Category: "chat", Title: "COS", Body: "hello", URL: "/", Tag: "chat"})
	if err != nil || sent != 1 {
		t.Fatalf("send: %d, %v; want 1 device (the expired one is deleted)", sent, err)
	}
	if got := receive(); got.Title != "COS" || got.Category != "chat" || got.Body != "hello" {
		t.Fatalf("payload %+v", got)
	}
	if _, err := p.kv.Get(ctx, pushSubKey(gone.Endpoint)); err != jetstream.ErrKeyNotFound {
		t.Fatalf("expired subscription still stored: %v", err)
	}

	if err := p.SetPref(ctx, "tester", "chat", false); err != nil {
		t.Fatal(err)
	}
	if sent, err := p.Send(ctx, Notice{Category: "chat", Title: "COS", Body: "muted", Tag: "chat"}); err != nil || sent != 0 {
		t.Fatalf("muted category: %d, %v; want 0 devices", sent, err)
	}
	nothing()
	if sent, err := p.Send(ctx, Notice{Category: "test", Title: "COS", Body: "test", Tag: "test", User: "tester"}); err != nil || sent != 1 {
		t.Fatalf("test notice: %d, %v; want 1 (test is never filtered)", sent, err)
	}
	if got := receive(); got.Category != "test" {
		t.Fatalf("test payload %+v", got)
	}

	if err := p.Unsubscribe(ctx, "mallory", ok.Endpoint); err != nil {
		t.Fatal(err)
	}
	if _, err := p.kv.Get(ctx, pushSubKey(ok.Endpoint)); err != nil {
		t.Fatalf("another user removed the subscription: %v", err)
	}
	if err := p.Unsubscribe(ctx, "tester", ok.Endpoint); err != nil {
		t.Fatal(err)
	}
	if _, err := p.kv.Get(ctx, pushSubKey(ok.Endpoint)); err != jetstream.ErrKeyNotFound {
		t.Fatalf("owner unsubscribe left the subscription: %v", err)
	}
}
