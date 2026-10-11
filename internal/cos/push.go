package cos

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/TerraScale/ts-cos/internal/webpush"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	pushBucket     = "cos_push"
	pushSubject    = "https://cos.terrascale.tech"
	pushVAPIDKey   = "vapid"
	pushSubPrefix  = "sub."
	pushPrefPrefix = "prefs."
	pushTitleMax   = 80
	pushBodyMax    = 300
	pushAgentMax   = 120
)

type pushCategory struct{ ID, Label string }

// pushCategories are the user-facing kinds, in the order the settings page lists them.
var pushCategories = []pushCategory{
	{"inbox", "Inbox items"},
	{"chat", "COS replies"},
	{"done", "Finished and deployed work"},
	{"node", "Node alerts"},
}

// pushTTL is how long a push waits for its device; "test" is not a user category, so it is never filtered.
var pushTTL = map[string]time.Duration{"inbox": 24 * time.Hour, "chat": time.Hour, "done": 12 * time.Hour, "node": time.Hour, "test": 5 * time.Minute}

// pushServices are the only hosts a subscription may name: subscribers can only make nodes POST to real push services.
var pushServices = []string{"fcm.googleapis.com", "push.apple.com", "push.services.mozilla.com", "notify.windows.com"}

var pushTopicPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,32}$`)

// Push stores subscriptions and the VAPID key in the replicated cos_push bucket and signs every send.
type Push struct {
	kv     jetstream.KeyValue
	js     jetstream.JetStream
	host   string
	vapid  *webpush.VAPID
	client *http.Client
	chat   *Chat
}

// pushSub is one device's subscription, stored under pushSubKey(endpoint).
type pushSub struct {
	Sub     webpush.Subscription `json:"sub"`
	User    string               `json:"user"`
	Agent   string               `json:"agent,omitempty"`
	Created time.Time            `json:"created"`
}

// pushPrefs lists the categories a user turned off; a category not listed is on.
type pushPrefs struct {
	Off []string `json:"off"`
}

// Notice is one notification. An empty User sends to every subscribed user.
type Notice struct {
	Category, Title, Body, URL, Tag string
	Urgent                          bool
	Close                           []string
	User                            string
}

// PushState is what the settings panel shows for one user; Ready is false until the store opens.
type PushState struct {
	Ready   bool
	Devices int
	Off     map[string]bool
}

// pushPayload is the JSON the service worker reads.
type pushPayload struct {
	Title    string   `json:"title"`
	Body     string   `json:"body"`
	URL      string   `json:"url"`
	Tag      string   `json:"tag"`
	Category string   `json:"category"`
	Urgent   bool     `json:"urgent"`
	Close    []string `json:"close"`
}

// OpenPush loads the cluster's VAPID key, creating it once for every node.
func OpenPush(ctx context.Context, bus *Bus) (*Push, error) {
	kv, err := openKV(ctx, bus, pushBucket)
	if err != nil {
		return nil, err
	}
	js, err := bus.JetStream()
	if err != nil {
		return nil, err
	}
	raw, err := loadVAPID(ctx, kv)
	if err != nil {
		return nil, err
	}
	vapid, err := webpush.ParseVAPID(raw, pushSubject)
	if err != nil {
		return nil, err
	}
	return &Push{kv: kv, js: js, host: bus.host, vapid: vapid, client: pushClient()}, nil
}

// loadVAPID returns the stored key. Two nodes racing to create it both end with the winner's key.
func loadVAPID(ctx context.Context, kv jetstream.KeyValue) ([]byte, error) {
	entry, err := kv.Get(ctx, pushVAPIDKey)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		raw, genErr := webpush.GenerateVAPID()
		if genErr != nil {
			return nil, genErr
		}
		_, err = kv.Create(ctx, pushVAPIDKey, raw)
		if err == nil {
			return raw, nil
		}
		if !errors.Is(err, jetstream.ErrKeyExists) {
			return nil, err
		}
		entry, err = kv.Get(ctx, pushVAPIDKey)
	}
	if err != nil {
		return nil, err
	}
	return entry.Value(), nil
}

// pushClient never follows redirects: a subscription must not steer a node to another host.
func pushClient() *http.Client {
	return &http.Client{Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

// RunPush opens the push store, retrying until JetStream has quorum, then runs the notifier while this node leads.
func RunPush(ctx context.Context, bus *Bus, chat *Chat, ready func(*Push)) {
	retry := time.NewTicker(2 * time.Second)
	defer retry.Stop()
	var p *Push
	for p == nil {
		attempt, stop := context.WithTimeout(ctx, 5*time.Second)
		var err error
		p, err = OpenPush(attempt, bus)
		stop()
		if err != nil {
			logErr(ctx, slog.LevelWarn, "COS push not ready", err)
			select {
			case <-ctx.Done():
				return
			case <-retry.C:
			}
		}
	}
	slog.Info("COS push ready")
	p.chat = chat
	ready(p)
	bus.Lead(ctx, p.notify)
}

// pushSubKey names a subscription by a hash of its endpoint: endpoints are long and hold characters KV keys reject.
func pushSubKey(endpoint string) string {
	sum := sha256.Sum256([]byte(endpoint))
	return pushSubPrefix + hex.EncodeToString(sum[:16])
}

// pushHostAllowed accepts https URLs without a port on a push service or one of its subdomains.
func pushHostAllowed(u *url.URL) bool {
	if u.Scheme != "https" || u.Port() != "" {
		return false
	}
	for _, service := range pushServices {
		if u.Hostname() == service || strings.HasSuffix(u.Hostname(), "."+service) {
			return true
		}
	}
	return false
}

// pushHost is the only part of an endpoint that may reach the logs.
func pushHost(endpoint string) string {
	u, err := url.Parse(endpoint)
	if err != nil {
		return ""
	}
	return u.Hostname()
}

// Subscribe stores or refreshes one device's subscription for user.
func (p *Push) Subscribe(ctx context.Context, user, agent string, sub webpush.Subscription) error {
	data, err := json.Marshal(pushSub{Sub: sub, User: user, Agent: clipRunes(agent, pushAgentMax), Created: time.Now().UTC()})
	if err != nil {
		return err
	}
	_, err = p.kv.Put(ctx, pushSubKey(sub.Endpoint), data)
	return err
}

// Unsubscribe deletes a subscription only for the user who owns it; anyone else's request does nothing.
func (p *Push) Unsubscribe(ctx context.Context, user, endpoint string) error {
	key := pushSubKey(endpoint)
	entry, err := p.kv.Get(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	var s pushSub
	if err := json.Unmarshal(entry.Value(), &s); err != nil {
		return err
	}
	if s.User != user {
		return nil
	}
	return p.kv.Delete(ctx, key)
}

// Devices counts user's subscriptions.
func (p *Push) Devices(ctx context.Context, user string) (int, error) {
	subs, err := p.subs(ctx)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, s := range subs {
		if s.User == user {
			n++
		}
	}
	return n, nil
}

// Send pushes n to every matching device and returns how many accepted it. Dead subscriptions are deleted.
// The first error is returned only when nothing was sent.
func (p *Push) Send(ctx context.Context, n Notice) (int, error) {
	subs, err := p.subs(ctx)
	if err != nil {
		return 0, err
	}
	payload, err := json.Marshal(pushPayload{Title: clipRunes(n.Title, pushTitleMax), Body: clipRunes(n.Body, pushBodyMax), URL: n.URL, Tag: n.Tag, Category: n.Category, Urgent: n.Urgent, Close: n.Close})
	if err != nil {
		return 0, err
	}
	msg := webpush.Message{TTL: pushTTL[n.Category], Urgency: "normal"}
	if n.Urgent {
		msg.Urgency = "high"
	}
	if pushTopicPattern.MatchString(n.Tag) {
		msg.Topic = n.Tag
	}
	sent, first := 0, error(nil)
	for key, s := range subs {
		if n.User != "" && s.User != n.User {
			continue
		}
		off, err := p.off(ctx, s.User)
		if err != nil {
			first = cmp.Or(first, err)
			continue
		}
		if n.Category != "test" && off[n.Category] {
			continue
		}
		switch err := p.vapid.Send(ctx, p.client, s.Sub, payload, msg); {
		case err == nil:
			sent++
		case errors.Is(err, webpush.ErrGone):
			_ = p.kv.Delete(ctx, key)
		default:
			var uerr *url.Error // its message carries the endpoint, which is a credential
			if errors.As(err, &uerr) {
				err = uerr.Err
			}
			slog.Warn("COS push send failed", "host", pushHost(s.Sub.Endpoint), "error", err)
			first = cmp.Or(first, err)
		}
	}
	if sent > 0 {
		return sent, nil
	}
	return 0, first
}

// subs reads every subscription by KV key; an entry that fails to decode is skipped so one bad device cannot stop the rest.
func (p *Push) subs(ctx context.Context) (map[string]pushSub, error) {
	lister, err := p.kv.ListKeysFiltered(ctx, pushSubPrefix+"*")
	if err != nil {
		return nil, err
	}
	out := map[string]pushSub{}
	for key := range lister.Keys() {
		entry, err := p.kv.Get(ctx, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		var s pushSub
		if json.Unmarshal(entry.Value(), &s) == nil {
			out[key] = s
		}
	}
	return out, nil
}

// off returns the categories user turned off; no prefs yet means everything is on.
func (p *Push) off(ctx context.Context, user string) (map[string]bool, error) {
	off := map[string]bool{}
	entry, err := p.kv.Get(ctx, pushPrefPrefix+user)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return off, nil
	}
	if err != nil {
		return nil, err
	}
	var prefs pushPrefs
	if err := json.Unmarshal(entry.Value(), &prefs); err != nil {
		return nil, err
	}
	for _, c := range prefs.Off {
		off[c] = true
	}
	return off, nil
}

// SetPref turns one category on or off for user. The write is compare-and-set, so concurrent toggles never drop each other.
func (p *Push) SetPref(ctx context.Context, user, category string, on bool) error {
	key := pushPrefPrefix + user
	var err error
	for range 5 {
		var prefs pushPrefs
		var rev uint64
		if entry, getErr := p.kv.Get(ctx, key); getErr == nil {
			rev = entry.Revision()
			if err := json.Unmarshal(entry.Value(), &prefs); err != nil {
				return err
			}
		} else if !errors.Is(getErr, jetstream.ErrKeyNotFound) {
			return getErr
		}
		prefs.Off = pushToggle(prefs.Off, category, on)
		data, _ := json.Marshal(prefs)
		if rev == 0 {
			_, err = p.kv.Create(ctx, key, data)
		} else {
			_, err = p.kv.Update(ctx, key, data, rev)
		}
		if err == nil {
			return nil
		}
		if !errors.Is(err, jetstream.ErrKeyRevisionMismatch) && !errors.Is(err, jetstream.ErrKeyExists) {
			return err
		}
	}
	return err
}

// pushToggle returns off with category removed (on) or added (off), sorted so the stored JSON is stable.
func pushToggle(off []string, category string, on bool) []string {
	out := []string{}
	for _, c := range off {
		if c != category {
			out = append(out, c)
		}
	}
	if !on {
		out = append(out, category)
	}
	slices.Sort(out)
	return out
}

// clipRunes keeps the first n runes; notification text is user-visible, so no byte-level cuts.
func clipRunes(s string, n int) string {
	if r := []rune(s); len(r) > n {
		return string(r[:n])
	}
	return s
}
