package cos

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Forgejo POSTs webhooks to /hooks/<source>, outside the login. A body with a valid
// HMAC is stored unchanged on cos.ingest.<source> for TS-814 to normalize; the sender
// gets 202 only once JetStream has stored it on a quorum.
const hookPerMinute = 300 // requests per client address on one node

var hookSources = map[string]string{"forgejo": IngestForgejo}

type Hooks struct {
	bus   *Bus
	limit limiter
}

func NewHooks(bus *Bus) *Hooks { return &Hooks{bus: bus} }

// Start creates the ingest stream once the cluster has quorum.
func (h *Hooks) Start(ctx context.Context) {
	js, _ := h.bus.JetStream()
	for {
		call, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := EnsureEventStreams(call, js)
		cancel()
		if err == nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

// HookSecret is the HMAC secret Forgejo signs webhooks with; every node
// derives the same value from the Vault master. TS-814 registers it with the senders.
func (b *Bus) HookSecret(source string) string {
	return b.hookSecrets[source]
}

func (h *Hooks) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	subject, ok := hookSources[r.PathValue("source")]
	if !ok {
		http.NotFound(rw, r)
		return
	}
	if !h.limit.allow(clientIP(r), hookPerMinute) {
		http.Error(rw, "Too many webhook requests", http.StatusTooManyRequests)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(rw, r.Body, ingestMaxMsg))
	if err != nil {
		http.Error(rw, "Webhook body too large", http.StatusRequestEntityTooLarge)
		return
	}
	source := r.PathValue("source")
	if !validHookSignature(h.bus.HookSecret(source), r.Header, body) {
		http.Error(rw, "Invalid webhook signature", http.StatusUnauthorized)
		return
	}
	msg := nats.NewMsg(subject)
	msg.Data = body
	for key, values := range r.Header {
		if key != "Cookie" && key != "Authorization" {
			msg.Header[key] = values
		}
	}
	js, _ := h.bus.JetStream()
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if _, err := js.PublishMsg(ctx, msg, jetstream.WithMsgID(hookDeliveryID(source, r.Header, body)), jetstream.WithExpectStream(IngestStream)); err != nil {
		http.Error(rw, "COS could not store the webhook; retry later", http.StatusServiceUnavailable)
		return
	}
	rw.WriteHeader(http.StatusAccepted)
}

// validHookSignature checks Forgejo's X-Forgejo-Signature (hex HMAC-SHA256).
func validHookSignature(secret string, header http.Header, body []byte) bool {
	got, err := hex.DecodeString(header.Get("X-Forgejo-Signature"))
	if secret == "" || err != nil {
		return false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return hmac.Equal(got, mac.Sum(nil))
}

// hookDeliveryID deduplicates redeliveries: Forgejo's delivery UUID, else the body hash.
func hookDeliveryID(source string, header http.Header, body []byte) string {
	if id := header.Get("X-Forgejo-Delivery"); source == "forgejo" && id != "" {
		return source + ":" + id
	}
	sum := sha256.Sum256(body)
	return source + ":" + hex.EncodeToString(sum[:])
}

// limiter counts requests per key in fixed one-minute windows on this node.
type limiter struct {
	mu     sync.Mutex
	window time.Time
	seen   map[string]int
}

func (l *limiter) allow(key string, max int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.seen == nil || time.Since(l.window) >= time.Minute {
		l.window, l.seen = time.Now(), map[string]int{}
	}
	l.seen[key]++
	return l.seen[key] <= max
}
