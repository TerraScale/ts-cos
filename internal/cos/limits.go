package cos

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// CLIProxyAPI routes every model. The leader reads its credentials every limitsPoll and
// replicates the result, so each node knows which models have headroom before it sends.
const (
	cliproxyURL    = "https://cliproxy.mariogk.com"
	cliproxySecret = "cos-cliproxy"
	limitsBucket   = "COS_LIMITS"
	limitsKey      = "state"
	limitsPoll     = 30 * time.Second
	limitsStale    = 3 * time.Minute // older (or future, clock skew) state counts as unknown, and unknown counts as limited

	limitsTimeout   = 15 * time.Second
	limitsSettle    = time.Minute // until values this close are one reading; remaining_seconds is whole seconds and jitters
	limitsMaxBody   = 8 << 20
	limitsDownAfter = 2 // consecutive failed polls before the Inbox hears about it
)

func init() {
	cliCommands["limits"] = cliCommand{Usage: "limits", Run: limitsCommand}
	serveWorkers = append(serveWorkers, runLimits)
}

// LimitsState is the leader's last read of CLIProxyAPI's credentials, replicated in KV COS_LIMITS/state.
// Times are on the reading leader's clock: cooldown ends come from remaining_seconds, never retry_at, so CLIProxyAPI's clock does not matter.
type LimitsState struct {
	At          time.Time         `json:"at"`
	Reachable   bool              `json:"reachable"`
	Error       string            `json:"error,omitempty"` // why the read failed; never a secret or a response body
	Credentials []LimitCredential `json:"credentials"`
}

type LimitCredential struct {
	Provider      string        `json:"provider"`          // claude | codex
	Account       string        `json:"account"`           // label or email, never a token
	State         string        `json:"state"`             // active | cooling | quota | disabled | error
	Until         time.Time     `json:"until,omitzero"`    // end of a credential-wide cooldown
	Reason        string        `json:"reason,omitempty"`  // CLIProxyAPI cooldown reason or status_message
	Models        []LimitModel  `json:"models,omitempty"`  // per-model cooldowns
	Windows       []LimitWindow `json:"windows,omitempty"` // Anthropic 5h / 7d windows (Claude only)
	Success       int64         `json:"success"`           // lifetime requests
	Failed        int64         `json:"failed"`
	RecentSuccess int64         `json:"recentSuccess"` // sum of CLIProxyAPI's recent_requests buckets
	RecentFailed  int64         `json:"recentFailed"`
}

type LimitModel struct {
	Model  string    `json:"model"`
	State  string    `json:"state"` // cooling | quota
	Until  time.Time `json:"until"`
	Reason string    `json:"reason,omitempty"`
}

type LimitWindow struct {
	Name        string    `json:"name"`        // 5h | 7d
	Status      string    `json:"status"`      // allowed | allowed_warning | rejected
	Utilization float64   `json:"utilization"` // 0..1
	Reset       time.Time `json:"reset,omitzero"`
}

// Headroom reports whether a thread on model may take a message now. Unknown (unreachable, stale or future state) counts as limited.
// until is the earliest time a limited model may recover (zero when unknown).
func (s LimitsState) Headroom(model string, now time.Time) (ok bool, until time.Time, why string) {
	age := now.Sub(s.At)
	switch {
	case !s.Reachable:
		return false, time.Time{}, "CLIProxyAPI state unknown: " + cmp.Or(s.Error, "unreachable")
	case age > limitsStale:
		return false, time.Time{}, "CLIProxyAPI state unknown: last read is stale"
	case age < -limitsStale:
		return false, time.Time{}, "CLIProxyAPI state unknown: read time is ahead of the clock"
	}
	provider := limitsProvider(model)
	if provider == "" {
		return true, time.Time{}, "" // CLIProxyAPI does not route it
	}
	count, unknown := 0, false
	var earliest time.Time
	for _, c := range s.Credentials {
		if c.Provider != provider {
			continue
		}
		count++
		free, recover := c.gate(model, now)
		if free {
			return true, time.Time{}, ""
		}
		if recover.IsZero() {
			unknown = true
		} else if earliest.IsZero() || recover.Before(earliest) {
			earliest = recover
		}
	}
	switch {
	case count == 0:
		return false, time.Time{}, "no " + provider + " credentials in CLIProxyAPI"
	case unknown:
		return false, time.Time{}, fmt.Sprintf("all %d %s credentials for %s are limited", count, provider, model)
	}
	return false, earliest, fmt.Sprintf("all %d %s credentials for %s are limited until %s", count, provider, model, earliest.UTC().Format(time.RFC3339))
}

// gate reports whether c can serve model now; otherwise recover is when it can, zero when unknown.
func (c LimitCredential) gate(model string, now time.Time) (free bool, recover time.Time) {
	blocked, known := false, true
	hold := func(at time.Time) {
		blocked = true
		if at.IsZero() {
			known = false
		} else if at.After(recover) {
			recover = at
		}
	}
	switch c.State {
	case "disabled", "error":
		hold(time.Time{})
	case "cooling", "quota":
		if c.Until.After(now) {
			hold(c.Until)
		}
	}
	for _, w := range c.Windows {
		if w.Status == "rejected" && (w.Reset.IsZero() || w.Reset.After(now)) {
			hold(w.Reset)
		}
	}
	for _, m := range c.Models {
		if limitsModelMatches(m.Model, model) && m.Until.After(now) {
			hold(m.Until)
		}
	}
	switch {
	case !blocked:
		return true, time.Time{}
	case !known:
		return false, time.Time{}
	}
	return false, recover
}

// limitsModelMatches reports whether a cooldown key covers model: the same name, or model plus a -YYYYMMDD date.
func limitsModelMatches(key, model string) bool {
	if key == model {
		return true
	}
	date, ok := strings.CutPrefix(key, model+"-")
	return ok && len(date) == 8 && strings.Trim(date, "0123456789") == ""
}

// limitsProvider names the CLIProxyAPI provider that serves model, or "" when CLIProxyAPI does not route it.
func limitsProvider(model string) string {
	switch {
	case strings.HasPrefix(model, "claude-"):
		return "claude"
	case strings.HasPrefix(model, "gpt-"), strings.HasPrefix(model, "codex"):
		return "codex"
	case len(model) > 1 && model[0] == 'o' && model[1] >= '0' && model[1] <= '9':
		return "codex"
	}
	return ""
}

// readLimits returns the replicated state; a missing key is an unknown (zero) state, which Headroom treats as limited.
func readLimits(ctx context.Context, kv jetstream.KeyValue) (LimitsState, error) {
	var s LimitsState
	_, err := kvJSON(ctx, kv, limitsKey, &s)
	return s, err
}

// openLimits binds COS_LIMITS; every node can read it, the leader creates it.
func openLimits(ctx context.Context, bus *Bus) (jetstream.KeyValue, error) {
	return openKVConfig(ctx, bus, jetstream.KeyValueConfig{Bucket: limitsBucket, History: 1})
}

// cliproxyAuthFile is the part of CLIProxyAPI's auth-files entry that COS reads; everything else stays out of state.
type cliproxyAuthFile struct {
	Provider      string `json:"provider"`
	Label         string `json:"label"`
	Account       string `json:"account"`
	Email         string `json:"email"`
	Status        string `json:"status"`
	StatusMessage string `json:"status_message"`
	Cooldowns     []struct {
		Scope            string `json:"scope"`
		Reason           string `json:"reason"`
		ModelKey         string `json:"model_key"`
		RemainingSeconds int64  `json:"remaining_seconds"`
	} `json:"cooldowns"`
	Quota *struct {
		Signals map[string]json.RawMessage `json:"signals"`
	} `json:"quota"`
	Success        int64 `json:"success"`
	Failed         int64 `json:"failed"`
	RecentRequests []struct {
		Success int64 `json:"success"`
		Failed  int64 `json:"failed"`
	} `json:"recent_requests"`
}

// limitCredentials maps CLIProxyAPI's credentials to state; now turns remaining_seconds into absolute times.
func limitCredentials(files []cliproxyAuthFile, now time.Time) []LimitCredential {
	out := make([]LimitCredential, 0, len(files))
	for _, f := range files {
		c := LimitCredential{
			Provider: f.Provider, Account: cmp.Or(f.Label, f.Account, f.Email), State: "active",
			Success: f.Success, Failed: f.Failed,
		}
		for _, r := range f.RecentRequests {
			c.RecentSuccess += r.Success
			c.RecentFailed += r.Failed
		}
		for _, cd := range f.Cooldowns {
			if cd.RemainingSeconds <= 0 {
				continue
			}
			until := now.Add(time.Duration(cd.RemainingSeconds) * time.Second)
			state := "cooling"
			if cd.Reason == "credential_quota" {
				state = "quota"
			}
			if cd.Scope == "model" {
				c.Models = append(c.Models, LimitModel{Model: cd.ModelKey, State: state, Until: until, Reason: cd.Reason})
			} else if until.After(c.Until) {
				c.Until, c.State, c.Reason = until, state, cd.Reason
			}
		}
		switch {
		case f.Status == "disabled":
			c.State, c.Reason = "disabled", f.StatusMessage
		case c.State == "active" && f.Status == "error":
			c.State, c.Reason = "error", f.StatusMessage
		}
		slices.SortFunc(c.Models, func(a, b LimitModel) int { return cmp.Compare(a.Model, b.Model) })
		if f.Provider == "claude" && f.Quota != nil {
			c.Windows = limitWindows(f.Quota.Signals)
		}
		out = append(out, c)
	}
	slices.SortFunc(out, func(a, b LimitCredential) int {
		return cmp.Or(cmp.Compare(a.Provider, b.Provider), cmp.Compare(a.Account, b.Account))
	})
	return out
}

// limitWindows reads the Anthropic 5h and 7d signals; a window with none of its signals is absent.
func limitWindows(signals map[string]json.RawMessage) []LimitWindow {
	var out []LimitWindow
	for _, name := range []string{"5h", "7d"} {
		prefix := "Anthropic-Ratelimit-Unified-" + name + "-"
		status, hasStatus := signals[prefix+"Status"]
		util, hasUtil := signals[prefix+"Utilization"]
		reset, hasReset := signals[prefix+"Reset"]
		if !hasStatus && !hasUtil && !hasReset {
			continue
		}
		w := LimitWindow{Name: name, Status: signalText(status)}
		if u, err := strconv.ParseFloat(signalText(util), 64); err == nil {
			w.Utilization = u
		}
		if sec, err := strconv.ParseInt(signalText(reset), 10, 64); err == nil {
			w.Reset = time.Unix(sec, 0).UTC()
		}
		out = append(out, w)
	}
	return out
}

// signalText reads a signal whether CLIProxyAPI sent it as a JSON string or a number.
func signalText(raw json.RawMessage) string {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	return string(raw)
}

// limitsPoller reads CLIProxyAPI and publishes what changed. Dependencies are fields so tests run without Vault, NATS or the network.
type limitsPoller struct {
	client  *http.Client
	baseURL string
	key     func(context.Context) (string, error)
	store   func(context.Context, LimitsState) error
	publish func(context.Context, Event) error
	file    func(context.Context, InboxInput) error
	now     func() time.Time
	timeout time.Duration

	token    string      // management key, read once and dropped on 401
	last     LimitsState // last published state, for change detection
	seen     bool
	failures int  // consecutive failed polls
	filed    bool // the unreachable Inbox item is open
}

// runLimits polls CLIProxyAPI on the leader only, so each change is published once per cluster.
func runLimits(ctx context.Context, cfg Config, bus *Bus) {
	p := &limitsPoller{
		client: &http.Client{}, baseURL: cliproxyURL, key: cliproxyKey, now: time.Now, timeout: limitsTimeout,
		store: func(ctx context.Context, s LimitsState) error {
			kv, err := openLimits(ctx, bus)
			if err != nil {
				return err
			}
			return kvPutJSON(ctx, kv, limitsKey, s)
		},
		publish: func(ctx context.Context, e Event) error {
			js, err := bus.JetStream()
			if err != nil {
				return err
			}
			_, err = PublishEvent(ctx, js, e)
			return err
		},
		file: func(ctx context.Context, in InboxInput) error {
			inbox, err := OpenInbox(ctx, bus, cfg.LocalHost, false)
			if err != nil {
				return err
			}
			_, err = inbox.Add(ctx, in)
			return err
		},
	}
	bus.Lead(ctx, p.run)
}

// cliproxyKey reads the management key from OCI Vault; the value is never logged.
func cliproxyKey(ctx context.Context) (string, error) {
	api, err := newOCIVault()
	if err != nil {
		return "", err
	}
	key, err := api.read(ctx, cliproxySecret)
	if err != nil {
		return "", err
	}
	defer clear(key)
	return strings.TrimSpace(string(key)), nil
}

func (p *limitsPoller) run(ctx context.Context) {
	ticker := time.NewTicker(limitsPoll)
	defer ticker.Stop()
	for {
		p.poll(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// poll reads once, replicates the result, publishes a change and files the Inbox item after limitsDownAfter failures.
func (p *limitsPoller) poll(ctx context.Context) {
	now := p.now()
	state, err := p.fetch(ctx, now)
	if err != nil {
		state = LimitsState{At: now, Error: err.Error()}
		p.failures++
	} else {
		p.failures, p.filed = 0, false
	}
	if err := p.store(ctx, state); err != nil {
		logErr(ctx, slog.LevelError, "COS limits store", err)
	}
	if !p.seen || limitsChanged(p.last, state) {
		if e, err := limitsEvent(state); err != nil {
			logErr(ctx, slog.LevelError, "COS limits event", err)
		} else if err := p.publish(ctx, e); err != nil {
			logErr(ctx, slog.LevelError, "COS limits publish", err, "event", e.ID)
		} else {
			p.last, p.seen = state, true
		}
	}
	if p.failures >= limitsDownAfter && !p.filed {
		err := p.file(ctx, InboxInput{
			Kind: "fyi", Title: "CLIProxyAPI unreachable: COS holds messages to every thread until it answers",
			Context: state.Error, Source: "cos", Key: "limits-unreachable",
		})
		if err != nil {
			logErr(ctx, slog.LevelError, "COS limits inbox", err)
		} else {
			p.filed = true
		}
	}
}

// fetch returns the state for one successful read, or the public reason it failed.
func (p *limitsPoller) fetch(ctx context.Context, now time.Time) (LimitsState, error) {
	if p.token == "" {
		keyCtx, cancel := context.WithTimeout(ctx, p.timeout)
		key, err := p.key(keyCtx)
		cancel()
		if err != nil {
			logErr(ctx, slog.LevelError, "COS CLIProxyAPI key unavailable", err)
			return LimitsState{}, errors.New("CLIProxyAPI: management key unavailable")
		}
		p.token = key
	}
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.baseURL+"/v0/management/auth-files", nil)
	if err != nil {
		return LimitsState{}, errors.New("CLIProxyAPI: connection failed")
	}
	req.Header.Set("Authorization", "Bearer "+p.token)
	resp, err := p.client.Do(req)
	if err != nil {
		return LimitsState{}, errors.New("CLIProxyAPI: connection failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusUnauthorized {
			p.token = ""
		}
		return LimitsState{}, fmt.Errorf("CLIProxyAPI: HTTP %d", resp.StatusCode)
	}
	var body struct {
		Files []cliproxyAuthFile `json:"files"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, limitsMaxBody)).Decode(&body); err != nil {
		return LimitsState{}, errors.New("CLIProxyAPI: bad response")
	}
	return LimitsState{At: now, Reachable: true, Credentials: limitCredentials(body.Files, now)}, nil
}

// limitsChanged compares states, ignoring At, lifetime and recent counters, and Window utilization.
// Until values within limitsSettle count as the same.
func limitsChanged(a, b LimitsState) bool {
	if a.Reachable != b.Reachable || a.Error != b.Error || len(a.Credentials) != len(b.Credentials) {
		return true
	}
	for i, x := range a.Credentials {
		y := b.Credentials[i]
		if x.Provider != y.Provider || x.Account != y.Account || x.State != y.State || !limitsNear(x.Until, y.Until) || len(x.Models) != len(y.Models) {
			return true
		}
		for j, m := range x.Models {
			n := y.Models[j]
			if m.Model != n.Model || m.State != n.State || !limitsNear(m.Until, n.Until) {
				return true
			}
		}
		if !slices.EqualFunc(x.Windows, y.Windows, func(m, n LimitWindow) bool { return m.Name == n.Name && m.Status == n.Status }) {
			return true
		}
	}
	return false
}

func limitsNear(a, b time.Time) bool {
	d := a.Sub(b)
	return d >= -limitsSettle && d <= limitsSettle
}

// limitsEventData is the event payload; models go first when it is too big, then trailing credentials.
type limitsEventData struct {
	Reachable   bool                    `json:"reachable"`
	Error       string                  `json:"error,omitempty"`
	Credentials []limitsEventCredential `json:"credentials"`
	Omitted     int                     `json:"omitted,omitempty"`
}

type limitsEventCredential struct {
	Provider string              `json:"provider"`
	Account  string              `json:"account"`
	State    string              `json:"state"`
	Until    time.Time           `json:"until,omitzero"`
	Models   []limitsEventModel  `json:"models,omitempty"`
	Windows  []limitsEventWindow `json:"windows,omitempty"`
}

type limitsEventWindow struct {
	Name   string `json:"name"`
	Status string `json:"status"`
}

type limitsEventModel struct {
	Model string    `json:"model"`
	State string    `json:"state"`
	Until time.Time `json:"until,omitzero"`
}

// limitsEvent builds the limits.changed event for s.
func limitsEvent(s LimitsState) (Event, error) {
	d := limitsEventData{Reachable: s.Reachable, Error: s.Error, Credentials: make([]limitsEventCredential, 0, len(s.Credentials))}
	for _, c := range s.Credentials {
		ec := limitsEventCredential{Provider: c.Provider, Account: c.Account, State: c.State, Until: c.Until}
		for _, m := range c.Models {
			ec.Models = append(ec.Models, limitsEventModel{Model: m.Model, State: m.State, Until: m.Until})
		}
		for _, w := range c.Windows {
			ec.Windows = append(ec.Windows, limitsEventWindow{Name: w.Name, Status: w.Status})
		}
		d.Credentials = append(d.Credentials, ec)
	}
	data, err := json.Marshal(d)
	if err != nil {
		return Event{}, err
	}
	if len(data) > eventDataMax {
		for i := range d.Credentials {
			d.Credentials[i].Models, d.Credentials[i].Windows = nil, nil
		}
		if data, err = json.Marshal(d); err != nil {
			return Event{}, err
		}
	}
	for len(data) > eventDataMax && len(d.Credentials) > 0 {
		d.Credentials = d.Credentials[:len(d.Credentials)-1]
		d.Omitted++
		if data, err = json.Marshal(d); err != nil {
			return Event{}, err
		}
	}
	sum := sha256.Sum256(data)
	severity := SeverityInfo
	if !s.Reachable {
		severity = SeverityWarn
	}
	return Event{
		ID:       NewEventID(SourceLimits, s.At.UTC().Format(time.RFC3339), hex.EncodeToString(sum[:8])),
		Source:   SourceLimits,
		Kind:     KindLimitsChanged,
		Time:     s.At,
		Severity: severity,
		Summary:  limitsSummary(s),
		Data:     data,
	}, nil
}

// limitsSummary names how many credentials of each provider are cooling.
func limitsSummary(s LimitsState) string {
	if !s.Reachable {
		return "CLIProxyAPI unreachable"
	}
	var parts []string
	for _, provider := range []string{"claude", "codex"} {
		total, cooling := 0, 0
		for _, c := range s.Credentials {
			if c.Provider != provider {
				continue
			}
			total++
			if c.State == "cooling" || c.State == "quota" {
				cooling++
			}
		}
		if total > 0 {
			parts = append(parts, fmt.Sprintf("%d of %d %s credentials cooling", cooling, total, provider))
		}
	}
	if len(parts) == 0 {
		return "CLIProxyAPI has no credentials"
	}
	return EventText(strings.Join(parts, "; "), eventSummaryMax)
}

// limitsCommand prints the replicated state; before the first poll it prints the zero state.
func limitsCommand(ctx context.Context, cfg Config, args []string, w io.Writer) error {
	if len(args) != 0 {
		return errors.New("usage: cos limits")
	}
	bus, err := ConnectBus(ctx, cfg)
	if err != nil {
		return err
	}
	defer bus.Close()
	kv, err := openLimits(ctx, bus)
	if err != nil {
		return err
	}
	s, err := readLimits(ctx, kv)
	if err != nil {
		return err
	}
	return output(w, s)
}

// limitsJSON serves the replicated state to the dashboard.
func (w *Web) limitsJSON(rw http.ResponseWriter, r *http.Request) {
	kv, err := openLimits(r.Context(), w.Bus)
	if err != nil {
		http.Error(rw, "Cannot read COS limits", http.StatusInternalServerError)
		return
	}
	s, err := readLimits(r.Context(), kv)
	if err != nil {
		http.Error(rw, "Cannot read COS limits", http.StatusInternalServerError)
		return
	}
	rw.Header().Set("Content-Type", "application/json")
	rw.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(rw).Encode(s)
}
