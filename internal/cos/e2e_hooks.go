package cos

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// TS-814 live checks for the Forgejo webhook ingest. Every probe is a signed pull_request opened by
// a foreign author on a foreign repo: normalizeForgejo drops it, so it never reaches COS_EVENTS or
// the rules. Pushes are never sent, because a push on main triggers real work. Assertions read
// COS_INGEST's last sequence, since a work queue deletes each message once it is consumed.

const (
	e2eProbeAuthor  = "e2e-foreign"
	e2eProbeRepo    = "e2e-foreign/e2e-probe"
	e2eHookWindow   = 10 * time.Second
	e2eEventsWindow = 5 * time.Second
)

// forgejoProbe is one signed delivery with a fresh delivery ID, so no earlier request can dedupe it.
type forgejoProbe struct {
	delivery string
	body     []byte
}

func newForgejoProbe(now time.Time) forgejoProbe {
	return forgejoProbe{delivery: "e2e-" + probeHex(16), body: forgejoProbeBody(probeHex(20), now)}
}

// forgejoProbeBody is a pull_request opened by a foreign author on the probe repo. Marshal cannot fail for these types.
func forgejoProbeBody(sha string, now time.Time) []byte {
	body, _ := json.Marshal(map[string]any{
		"action":       "opened",
		"number":       1,
		"sender":       map[string]string{"login": e2eProbeAuthor},
		"repository":   map[string]string{"full_name": e2eProbeRepo},
		"pull_request": map[string]any{"number": 1, "title": "[e2e] webhook probe", "updated_at": now, "user": map[string]string{"login": e2eProbeAuthor}, "head": map[string]string{"sha": sha}},
	})
	return body
}

// signHookBody computes X-Forgejo-Signature the way Forgejo does: hex HMAC-SHA256 of the body.
func signHookBody(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

// ingestAdded is how many messages COS_INGEST gained between two last-sequence reads.
func ingestAdded(before, after uint64) int64 {
	return int64(after) - int64(before)
}

func probeHex(n int) string {
	raw := make([]byte, n)
	rand.Read(raw)
	return hex.EncodeToString(raw)
}

// hookTarget is what one check needs from the cluster.
type hookTarget struct {
	js     jetstream.JetStream
	urls   []string
	secret string // the key hooks.go verifies with
}

// connectHooks opens the bus for one check and loads the webhook secret the way StartBus does.
func connectHooks(ctx context.Context, e *E2EEnv) (*Bus, hookTarget, error) {
	var t hookTarget
	bus, err := e.bus(ctx)
	if err != nil {
		return nil, t, err
	}
	t.js, err = bus.JetStream()
	if err != nil {
		bus.Close()
		return nil, t, err
	}
	t.urls, err = e.nodeURLs(ctx)
	if err != nil {
		bus.Close()
		return nil, t, err
	}
	t.secret, err = e2eHookSecret(ctx)
	if err != nil {
		bus.Close()
		return nil, t, err
	}
	return bus, t, nil
}

// e2eHookSecret derives the Forgejo webhook secret from the Vault master, as StartBus does on each node.
// The client bus does not carry hook secrets, so the check derives its own copy and never prints it.
func e2eHookSecret(ctx context.Context) (string, error) {
	api, err := newOCIVault()
	if err != nil {
		return "", err
	}
	master, err := vaultMaster(ctx, api)
	if err != nil {
		return "", err
	}
	defer clear(master)
	return derive(master, "hook forgejo"), nil
}

// postForgejoHook sends the probe body to one node's /hooks/forgejo and returns the status.
func postForgejoHook(ctx context.Context, node string, p forgejoProbe, signature string) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, node+"/hooks/forgejo", bytes.NewReader(p.body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Forgejo-Event", "pull_request")
	req.Header.Set("X-Forgejo-Delivery", p.delivery)
	req.Header.Set("X-Forgejo-Signature", signature)
	res, err := e2eHTTP.Do(req)
	if err != nil {
		return 0, fmt.Errorf("POST /hooks/forgejo: %w", errors.Unwrap(err))
	}
	defer res.Body.Close()
	io.Copy(io.Discard, io.LimitReader(res.Body, 64<<10))
	return res.StatusCode, nil
}

// ingestSeq is COS_INGEST's last stream sequence, the only durable record that a delivery was stored.
func ingestSeq(ctx context.Context, js jetstream.JetStream) (uint64, error) {
	stream, err := js.Stream(ctx, IngestStream)
	if err != nil {
		return 0, err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return 0, err
	}
	return info.State.LastSeq, nil
}

// awaitIngestAck waits until the ingest consumer's ack floor passes seq, so the delivery was consumed.
func awaitIngestAck(ctx context.Context, js jetstream.JetStream, seq uint64) error {
	deadline := time.Now().Add(e2eHookWindow)
	for {
		cons, err := js.Consumer(ctx, IngestStream, forgejoIngestConsumer)
		if err != nil {
			return err
		}
		info, err := cons.Info(ctx)
		if err != nil {
			return err
		}
		if info.AckFloor.Stream >= seq {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("ingest consumer acked up to seq %d, not %d, within 10 s", info.AckFloor.Stream, seq)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// forgejoEventsMentioning returns the subjects of cos.events.forgejo.> published since from that
// contain any needle. It fails rather than report a partial scan.
func forgejoEventsMentioning(ctx context.Context, js jetstream.JetStream, from time.Time, needles ...string) ([]string, error) {
	cons, err := js.OrderedConsumer(ctx, EventsStream, jetstream.OrderedConsumerConfig{FilterSubjects: []string{"cos.events.forgejo.>"},
		DeliverPolicy: jetstream.DeliverByStartTimePolicy, OptStartTime: &from})
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, e2eEventsWindow)
	defer cancel()
	var hits []string
	for {
		batch, err := cons.Fetch(100, jetstream.FetchMaxWait(time.Second))
		if err != nil {
			return nil, fmt.Errorf("COS_EVENTS scan incomplete: %w", err)
		}
		for msg := range batch.Messages() {
			for _, n := range needles {
				if bytes.Contains(msg.Data(), []byte(n)) {
					hits = append(hits, msg.Subject())
					break
				}
			}
		}
		if err := batch.Error(); err != nil {
			return nil, fmt.Errorf("COS_EVENTS scan incomplete: %w", err)
		}
		if cons.CachedInfo().NumPending == 0 {
			return hits, nil
		}
	}
}

func checkForgejoValid(ctx context.Context, e *E2EEnv) (string, error) {
	bus, t, err := connectHooks(ctx, e)
	if err != nil {
		return "", err
	}
	defer bus.Close()
	p := newForgejoProbe(time.Now())
	before, err := ingestSeq(ctx, t.js)
	if err != nil {
		return "", err
	}
	status, err := postForgejoHook(ctx, t.urls[0], p, signHookBody(t.secret, p.body))
	if err != nil {
		return "", err
	}
	if status != http.StatusAccepted {
		return "", fmt.Errorf("hooks.go answered %d for a valid delivery, want 202", status)
	}
	after, err := ingestSeq(ctx, t.js)
	if err != nil {
		return "", err
	}
	if added := ingestAdded(before, after); added != 1 {
		return "", fmt.Errorf("COS_INGEST gained %d messages for one delivery, want 1", added)
	}
	return fmt.Sprintf("%s: 202, stored at COS_INGEST seq %d", p.delivery, after), nil
}

func checkForgejoBadSignature(ctx context.Context, e *E2EEnv) (string, error) {
	bus, t, err := connectHooks(ctx, e)
	if err != nil {
		return "", err
	}
	defer bus.Close()
	p := newForgejoProbe(time.Now())
	before, err := ingestSeq(ctx, t.js)
	if err != nil {
		return "", err
	}
	// Signed with a different key, so the HMAC cannot match the one hooks.go holds.
	status, err := postForgejoHook(ctx, t.urls[0], p, signHookBody("not-"+t.secret, p.body))
	if err != nil {
		return "", err
	}
	if status != http.StatusUnauthorized {
		return "", fmt.Errorf("hooks.go answered %d for a bad signature, want 401", status)
	}
	after, err := ingestSeq(ctx, t.js)
	if err != nil {
		return "", err
	}
	if added := ingestAdded(before, after); added != 0 {
		return "", fmt.Errorf("COS_INGEST gained %d messages for a rejected delivery, want 0", added)
	}
	return fmt.Sprintf("%s: 401, nothing stored", p.delivery), nil
}

func checkForgejoReplay(ctx context.Context, e *E2EEnv) (string, error) {
	bus, t, err := connectHooks(ctx, e)
	if err != nil {
		return "", err
	}
	defer bus.Close()
	if len(t.urls) < 2 {
		return "", fmt.Errorf("replay needs two nodes, have %d", len(t.urls))
	}
	p := newForgejoProbe(time.Now())
	sig := signHookBody(t.secret, p.body)
	before, err := ingestSeq(ctx, t.js)
	if err != nil {
		return "", err
	}
	status, err := postForgejoHook(ctx, t.urls[0], p, sig)
	if err != nil {
		return "", err
	}
	if status != http.StatusAccepted {
		return "", fmt.Errorf("hooks.go answered %d for the first delivery, want 202", status)
	}
	mid, err := ingestSeq(ctx, t.js)
	if err != nil {
		return "", err
	}
	if added := ingestAdded(before, mid); added != 1 {
		return "", fmt.Errorf("first delivery stored %d times, want 1", added)
	}
	// Same delivery ID, body and signature, sent to another node.
	status, err = postForgejoHook(ctx, t.urls[1], p, sig)
	if err != nil {
		return "", err
	}
	if status != http.StatusAccepted {
		return "", fmt.Errorf("hooks.go answered %d for the replay, want 202", status)
	}
	after, err := ingestSeq(ctx, t.js)
	if err != nil {
		return "", err
	}
	if added := ingestAdded(mid, after); added != 0 {
		return "", fmt.Errorf("replay stored %d more messages, want 0", added)
	}
	return fmt.Sprintf("%s: 202 on two nodes, stored once", p.delivery), nil
}

func checkForgejoForeignAuthor(ctx context.Context, e *E2EEnv) (string, error) {
	bus, t, err := connectHooks(ctx, e)
	if err != nil {
		return "", err
	}
	defer bus.Close()
	// One second of slack before the POST covers clock skew between this host and the stream.
	from := time.Now().Add(-time.Second)
	p := newForgejoProbe(time.Now())
	before, err := ingestSeq(ctx, t.js)
	if err != nil {
		return "", err
	}
	status, err := postForgejoHook(ctx, t.urls[0], p, signHookBody(t.secret, p.body))
	if err != nil {
		return "", err
	}
	if status != http.StatusAccepted {
		return "", fmt.Errorf("hooks.go answered %d for a valid delivery, want 202", status)
	}
	after, err := ingestSeq(ctx, t.js)
	if err != nil {
		return "", err
	}
	if added := ingestAdded(before, after); added != 1 {
		return "", fmt.Errorf("COS_INGEST gained %d messages for one delivery, want 1", added)
	}
	if err := awaitIngestAck(ctx, t.js, after); err != nil {
		return "", fmt.Errorf("probe was not consumed: %w", err)
	}
	hits, err := forgejoEventsMentioning(ctx, t.js, from, p.delivery, e2eProbeRepo)
	if err != nil {
		return "", err
	}
	if len(hits) > 0 {
		return "", fmt.Errorf("COS_EVENTS has %d forgejo events for a foreign-author probe: %v", len(hits), hits)
	}
	return fmt.Sprintf("%s consumed at seq %d; no cos.events.forgejo.* event names it or %s", p.delivery, after, e2eProbeRepo), nil
}
