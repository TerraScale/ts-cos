package cos

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"time"

	"git.terrascale.tech/TerraScale/ts-cos/internal/webpush"
	"github.com/nats-io/nats.go/jetstream"
)

// Push checks: the installer assets are served by every node, a subscription written through one node
// is replicated and removed through another, and a push service can verify the cluster's VAPID key.

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

// pushCheck gives a check the bus for KV reads and the node URLs; sessions come from the run's shared user.
func pushCheck(run func(context.Context, *E2EEnv, *Bus, []string) (string, error)) func(context.Context, *E2EEnv) (string, error) {
	return func(ctx context.Context, e *E2EEnv) (string, error) {
		bus, err := e.bus(ctx)
		if err != nil {
			return "", err
		}
		defer bus.Close()
		urls, err := e.nodeURLs(ctx)
		if err != nil {
			return "", err
		}
		return run(ctx, e, bus, urls)
	}
}

// sessionUser names the run's shared user; call it after session, which opens that user.
func (e *E2EEnv) sessionUser() string {
	e.sessMu.Lock()
	defer e.sessMu.Unlock()
	return e.sess.user.name
}

// pushGet fetches target with an optional session cookie and reads at most 64 KiB of the body.
func pushGet(ctx context.Context, target, cookie string) (*http.Response, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, nil, err
	}
	if cookie != "" {
		req.AddCookie(&http.Cookie{Name: sessionCookie, Value: cookie})
	}
	res, err := e2eHTTP.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("GET %s: %w", target, errors.Unwrap(err))
	}
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, 64<<10))
	return res, body, err
}

// pushPost sends a JSON body with the session cookie and returns the HTTP status.
func pushPost(ctx context.Context, target, cookie string, body any) (int, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(data))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(&http.Cookie{Name: sessionCookie, Value: cookie})
	res, err := e2eHTTP.Do(req)
	if err != nil {
		return 0, fmt.Errorf("POST %s: %w", target, errors.Unwrap(err))
	}
	defer res.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 4<<10))
	return res.StatusCode, nil
}

// pushKV opens the replicated push bucket through the bus.
func pushKV(ctx context.Context, bus *Bus) (jetstream.KeyValue, error) {
	js, err := bus.JetStream()
	if err != nil {
		return nil, err
	}
	return js.KeyValue(ctx, pushBucket)
}

// pushRead reads key through the bus. A replica may trail the write it acknowledged, so it retries until
// the key exists (want) or is gone (!want).
func pushRead(ctx context.Context, kv jetstream.KeyValue, key string, want bool) ([]byte, error) {
	var value []byte
	var err error
	for range 20 {
		var entry jetstream.KeyValueEntry
		if entry, err = kv.Get(ctx, key); err == nil {
			value = entry.Value()
		}
		if (err == nil) == want {
			return value, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
	return value, err
}

// checkPushServiceWorker needs no session: browsers fetch these before anyone signs in.
func checkPushServiceWorker(ctx context.Context, e *E2EEnv) (string, error) {
	urls, err := e.nodeURLs(ctx)
	if err != nil {
		return "", err
	}
	for _, base := range urls {
		res, _, err := pushGet(ctx, base+"/sw.js", "")
		if err != nil {
			return "", err
		}
		if res.StatusCode != http.StatusOK || res.Header.Get("Content-Type") == "" || res.Header.Get("Service-Worker-Allowed") != "/" || res.Header.Get("Cache-Control") != "no-cache" {
			return "", fmt.Errorf("GET %s/sw.js: HTTP %d, headers %v", base, res.StatusCode, res.Header)
		}
		res, body, err := pushGet(ctx, base+"/manifest.webmanifest", "")
		if err != nil {
			return "", err
		}
		var manifest struct {
			StartURL string `json:"start_url"`
			Scope    string `json:"scope"`
		}
		if res.StatusCode != http.StatusOK || res.Header.Get("Content-Type") != "application/manifest+json" || json.Unmarshal(body, &manifest) != nil || manifest.Scope != "/" || manifest.StartURL != "/" {
			return "", fmt.Errorf("GET %s/manifest.webmanifest: HTTP %d, want application/manifest+json with scope and start_url /", base, res.StatusCode)
		}
	}
	return fmt.Sprintf("sw.js and the manifest are served on %v with the installer headers", e2eHosts), nil
}

// checkPushSubscribe writes a subscription through node A, reads it back through the bus and removes it through node B.
func checkPushSubscribe(ctx context.Context, e *E2EEnv, bus *Bus, urls []string) (string, error) {
	cookie, err := e.session(ctx, urls[0])
	if err != nil {
		return "", err
	}
	user := e.sessionUser()
	raw := make([]byte, 8)
	_, _ = rand.Read(raw)
	sub, _, _, err := newPushSubscription("https://fcm.googleapis.com/fcm/send/cos-e2e-" + hex.EncodeToString(raw))
	if err != nil {
		return "", err
	}
	kv, err := pushKV(ctx, bus)
	if err != nil {
		return "", err
	}
	key := pushSubKey(sub.Endpoint)
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = kv.Delete(cleanup, key)
	}()
	if status, err := pushPost(ctx, urls[0]+"/push/subscribe", cookie, sub); err != nil || status != http.StatusNoContent {
		return "", fmt.Errorf("subscribe on %s: HTTP %d %v", urls[0], status, err)
	}
	value, err := pushRead(ctx, kv, key, true)
	if err != nil {
		return "", fmt.Errorf("subscription not in %s on the bus: %w", pushBucket, err)
	}
	var stored pushSub
	if err := json.Unmarshal(value, &stored); err != nil || stored.User != user {
		return "", fmt.Errorf("stored subscription belongs to %q, want %q: %v", stored.User, user, err)
	}
	if status, err := pushPost(ctx, urls[1]+"/push/unsubscribe", cookie, map[string]string{"endpoint": sub.Endpoint}); err != nil || status != http.StatusNoContent {
		return "", fmt.Errorf("unsubscribe on %s: HTTP %d %v", urls[1], status, err)
	}
	if _, err := pushRead(ctx, kv, key, false); !errors.Is(err, jetstream.ErrKeyNotFound) {
		return "", fmt.Errorf("subscription still stored after unsubscribe on %s: %v", urls[1], err)
	}
	return fmt.Sprintf("subscription written on %s, read back through the bus, removed through %s", e2eHosts[0], e2eHosts[1]), nil
}

// checkPushDeliver signs a real push with the cluster's VAPID key and checks a receiver accepts it and decrypts it.
func checkPushDeliver(ctx context.Context, e *E2EEnv, bus *Bus, urls []string) (string, error) {
	cookie, err := e.session(ctx, urls[1])
	if err != nil {
		return "", err
	}
	res, served, err := pushGet(ctx, urls[1]+"/push/key", cookie)
	if err != nil {
		return "", err
	}
	if res.StatusCode != http.StatusOK {
		return "", fmt.Errorf("GET %s/push/key: HTTP %d", urls[1], res.StatusCode)
	}
	kv, err := pushKV(ctx, bus)
	if err != nil {
		return "", err
	}
	raw, err := pushRead(ctx, kv, pushVAPIDKey, true)
	if err != nil {
		return "", fmt.Errorf("VAPID key not in %s on the bus: %w", pushBucket, err)
	}
	vapid, err := webpush.ParseVAPID(raw, pushSubject)
	if err != nil {
		return "", err
	}
	if vapid.PublicKey() != string(served) {
		return "", fmt.Errorf("the cluster's VAPID key does not match the key %s serves", urls[1])
	}

	sub, priv, auth, err := newPushSubscription("")
	if err != nil {
		return "", err
	}
	type delivery struct {
		key   string
		plain []byte
		err   error
	}
	got := make(chan delivery, 1)
	receiver := httptest.NewTLSServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var d delivery
		// The audience is the origin the browser would compute for this endpoint.
		d.key, d.err = webpush.VerifyAuthorization(r.Header.Get("Authorization"), "https://"+r.Host, time.Now())
		if d.err == nil {
			d.plain, d.err = webpush.Decrypt(priv, auth, body)
		}
		got <- d
		if d.err != nil {
			rw.WriteHeader(http.StatusForbidden)
			return
		}
		rw.WriteHeader(http.StatusCreated)
	}))
	defer receiver.Close()
	sub.Endpoint = receiver.URL + "/send/cos-e2e"
	client := pushClient()
	client.Transport = receiver.Client().Transport
	payload, err := json.Marshal(pushPayload{Title: "COS e2e", Body: "Delivery check", URL: "/settings", Tag: "e2e", Category: "test"})
	if err != nil {
		return "", err
	}
	if err := vapid.Send(ctx, client, sub, payload, webpush.Message{TTL: pushTTL["test"], Urgency: "normal"}); err != nil {
		// The receiver queues its reason before answering 403, so the real cause is waiting here.
		var d delivery
		select {
		case d = <-got:
		default:
		}
		return "", fmt.Errorf("push to the test receiver: %w; receiver: %v", err, d.err)
	}
	var d delivery
	select {
	case d = <-got:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	if d.err != nil {
		return "", fmt.Errorf("receiver rejected the push: %w", d.err)
	}
	if d.key != string(served) {
		return "", fmt.Errorf("receiver verified key %q, want the key %s serves", d.key, urls[1])
	}
	var decoded pushPayload
	if err := json.Unmarshal(d.plain, &decoded); err != nil || decoded.Title != "COS e2e" {
		return "", fmt.Errorf("payload %q does not decrypt to the e2e notice: %v", d.plain, err)
	}
	return fmt.Sprintf("delivered %d bytes, VAPID key matches cluster", len(d.plain)), nil
}
