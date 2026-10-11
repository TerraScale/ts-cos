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
	"sync"
	"time"

	"github.com/TerraScale/ts-cos/internal/webpush"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	pushBucket     = "cos_push"
	pushSubject    = "https://cos.terrascale.tech"
	pushVAPIDKey   = "vapid"
	pushSubPrefix  = "sub."
	pushPrefPrefix = "prefs." // pre-TS-831 prefs, keyed by login name
	pushPrefsKey   = "prefs"  // a user's prefs, under their own key prefix
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

// pushSub is one device's subscription, stored under the owner's pushSubKey(endpoint).
type pushSub struct {
	Sub     webpush.Subscription `json:"sub"`
	Agent   string               `json:"agent,omitempty"`
	Created time.Time            `json:"created"`
}

// pushDevice is one stored subscription: owner is the ULID in its key, key the name under it.
type pushDevice struct {
	owner, key string
	sub        pushSub
}

// pushPrefs lists the categories a user turned off; a category not listed is on.
type pushPrefs struct {
	Off []string `json:"off"`
}

// Notice is one notification. User is the ULID it goes to; an empty User is only for the cluster categories
// "done" and "node", which go to every user.
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
	bus.Lead(ctx, func(ctx context.Context) {
		var migrating sync.WaitGroup
		defer migrating.Wait()
		migrating.Go(func() {
			migrateEvery(ctx, "push migration to the first admin", func(ctx context.Context) (int, error) { return p.migrate(ctx, bus) })
		})
		p.notify(ctx)
	})
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

// Subscribe stores or refreshes the caller's subscription for one device. A device belongs to the last user who
// subscribed it, so other users' copies of this endpoint are deleted.
func (p *Push) Subscribe(ctx context.Context, agent string, sub webpush.Subscription) error {
	mine, err := UserKey(ctx, pushSubKey(sub.Endpoint))
	if err != nil {
		return err
	}
	data, err := json.Marshal(pushSub{Sub: sub, Agent: clipRunes(agent, pushAgentMax), Created: time.Now().UTC()})
	if err != nil {
		return err
	}
	if _, err := p.kv.Put(ctx, mine, data); err != nil {
		return err
	}
	others, err := p.kv.ListKeysFiltered(ctx, "u.*."+pushSubKey(sub.Endpoint))
	if err != nil {
		return err
	}
	for key := range others.Keys() {
		if key != mine {
			if err := p.kv.Delete(ctx, key); err != nil {
				return err
			}
		}
	}
	return nil
}

// Unsubscribe deletes the caller's subscription for endpoint; another user's copy is not visible to the caller.
func (p *Push) Unsubscribe(ctx context.Context, endpoint string) error {
	kv, err := UserKV(ctx, p.kv)
	if err != nil {
		return err
	}
	return kv.Delete(ctx, pushSubKey(endpoint))
}

// Devices counts the caller's subscriptions.
func (p *Push) Devices(ctx context.Context) (int, error) {
	devices, err := p.devices(ctx)
	return len(devices), err
}

// Send pushes n to every matching device and returns how many accepted it. Dead subscriptions are deleted.
// The first error is returned only when nothing was sent.
func (p *Push) Send(ctx context.Context, n Notice) (int, error) {
	if err := n.audience(); err != nil {
		return 0, err
	}
	var devices []pushDevice
	var err error
	if n.User != "" {
		devices, err = p.devices(WithUser(ctx, User{ID: n.User}))
	} else {
		devices, err = p.allDevices(ctx)
	}
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
	for _, d := range devices {
		owned := WithUser(ctx, User{ID: d.owner}) // each device's prefs and deletes are its owner's
		off, err := p.off(owned)
		if err != nil {
			first = cmp.Or(first, err)
			continue
		}
		if n.Category != "test" && off[n.Category] {
			continue
		}
		switch err := p.vapid.Send(ctx, p.client, d.sub.Sub, payload, msg); {
		case err == nil:
			sent++
		case errors.Is(err, webpush.ErrGone):
			_ = p.forget(owned, d)
		default:
			var uerr *url.Error // its message carries the endpoint, which is a credential
			if errors.As(err, &uerr) {
				err = uerr.Err
			}
			slog.Warn("COS push send failed", "host", pushHost(d.sub.Sub.Endpoint), "error", err)
			first = cmp.Or(first, err)
		}
	}
	if sent > 0 {
		return sent, nil
	}
	return 0, first
}

// audience says who may receive n: the user named in it, or for the cluster categories every user's devices.
// Anything else without a user is refused, so a notice never reaches an account it was not meant for.
func (n Notice) audience() error {
	switch {
	case n.User != "":
		if !ulidPattern.MatchString(n.User) {
			return errors.New("push notice names an invalid user")
		}
	case n.Category == "done" || n.Category == "node":
	default:
		return errors.New("push " + n.Category + " notice has no user")
	}
	return nil
}

// devices lists the caller's subscriptions; the owner is the caller, never a field of the stored value.
func (p *Push) devices(ctx context.Context) ([]pushDevice, error) {
	u, err := UserFrom(ctx)
	if err != nil {
		return nil, err
	}
	kv, err := UserKV(ctx, p.kv)
	if err != nil {
		return nil, err
	}
	lister, err := kv.ListKeysFiltered(ctx, pushSubPrefix+"*")
	if err != nil {
		return nil, err
	}
	out := []pushDevice{}
	for key := range lister.Keys() {
		entry, err := kv.Get(ctx, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		var s pushSub
		if json.Unmarshal(entry.Value(), &s) == nil {
			out = append(out, pushDevice{owner: u.ID, key: key, sub: s})
		}
	}
	return out, nil
}

// allDevices lists every user's subscriptions. Only Send uses it, to reach the audience of a cluster notice.
// An entry that fails to decode is skipped so one bad device cannot stop the rest.
func (p *Push) allDevices(ctx context.Context) ([]pushDevice, error) {
	lister, err := p.kv.ListKeysFiltered(ctx, "u.*."+pushSubPrefix+"*")
	if err != nil {
		return nil, err
	}
	out := []pushDevice{}
	for key := range lister.Keys() {
		owner, bare, ok := splitUserKey(key)
		if !ok {
			continue
		}
		entry, err := p.kv.Get(ctx, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		var s pushSub
		if json.Unmarshal(entry.Value(), &s) == nil {
			out = append(out, pushDevice{owner: owner, key: bare, sub: s})
		}
	}
	return out, nil
}

// forget deletes one of the owner's subscriptions, for a device the push service reports gone.
func (p *Push) forget(ctx context.Context, d pushDevice) error {
	kv, err := UserKV(ctx, p.kv)
	if err != nil {
		return err
	}
	return kv.Delete(ctx, d.key)
}

// off returns the categories the caller turned off; no prefs yet means everything is on.
func (p *Push) off(ctx context.Context) (map[string]bool, error) {
	off := map[string]bool{}
	kv, err := UserKV(ctx, p.kv)
	if err != nil {
		return nil, err
	}
	entry, err := kv.Get(ctx, pushPrefsKey)
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

// SetPref turns one category on or off for the caller. The write is compare-and-set, so concurrent toggles never drop each other.
func (p *Push) SetPref(ctx context.Context, category string, on bool) error {
	kv, err := UserKV(ctx, p.kv)
	if err != nil {
		return err
	}
	for range 5 {
		var prefs pushPrefs
		var rev uint64
		if entry, getErr := kv.Get(ctx, pushPrefsKey); getErr == nil {
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
			_, err = kv.Create(ctx, pushPrefsKey, data)
		} else {
			_, err = kv.Update(ctx, pushPrefsKey, data, rev)
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

// migrate moves the pre-TS-831 subscriptions and the first admin's prefs under the first admin, then deletes other
// users' legacy prefs, which no account owns. vapid and last.inbox are shared and never touched.
func (p *Push) migrate(ctx context.Context, bus *Bus) (int, error) {
	admin, err := nodeUser(ctx, bus)
	if err != nil {
		return 0, err
	}
	name, err := firstAdminName(ctx, bus.conn, admin.ID)
	if err != nil {
		return 0, err
	}
	moved, err := moveKeys(ctx, p.kv, func(key string) (string, bool) {
		switch {
		case strings.HasPrefix(key, pushSubPrefix):
			return admin.Key(key), true
		case key == pushPrefPrefix+name:
			return admin.Key(pushPrefsKey), true
		}
		return "", false
	})
	if err != nil {
		return moved, err
	}
	// What is left under prefs. is another user's legacy copy. The admin's own copy stays while a conflict keeps it.
	stale, err := p.kv.ListKeysFiltered(ctx, pushPrefPrefix+"*")
	if err != nil {
		return moved, err
	}
	var others []string
	for key := range stale.Keys() {
		if key != pushPrefPrefix+name {
			others = append(others, key)
		}
	}
	for _, key := range others {
		if err := p.kv.Delete(ctx, key); err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
			return moved, err
		}
	}
	return moved, nil
}

// firstAdminName is the first admin's login name, which keyed the legacy prefs.
func firstAdminName(ctx context.Context, nc *nats.Conn, id string) (string, error) {
	value, _, found, err := leaderGet(ctx, nc, authBucket, "uid."+id)
	if err != nil {
		return "", err
	}
	if !found {
		return "", errors.New("first admin account is missing")
	}
	var account authUser
	if err := json.Unmarshal(value, &account); err != nil {
		return "", err
	}
	return account.Name, nil
}

// clipRunes keeps the first n runes; notification text is user-visible, so no byte-level cuts.
func clipRunes(s string, n int) string {
	if r := []rune(s); len(r) > n {
		return string(r[:n])
	}
	return s
}
