package cos

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

var limitsNow = time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)

// limitsFixture maps the fabricated auth-files response (5 claude, 2 codex) at limitsNow.
func limitsFixture(t *testing.T) []LimitCredential {
	t.Helper()
	data, err := os.ReadFile("testdata/cliproxy/auth-files.json")
	if err != nil {
		t.Fatal(err)
	}
	var body struct {
		Files []cliproxyAuthFile `json:"files"`
	}
	if err := json.Unmarshal(data, &body); err != nil {
		t.Fatal(err)
	}
	return limitCredentials(body.Files, limitsNow)
}

func limitsCred(t *testing.T, creds []LimitCredential, account string) LimitCredential {
	t.Helper()
	for _, c := range creds {
		if c.Account == account {
			return c
		}
	}
	t.Fatalf("no credential %s", account)
	return LimitCredential{}
}

func TestLimitsUnitMapping(t *testing.T) {
	creds := limitsFixture(t)
	if len(creds) != 7 {
		t.Fatalf("got %d credentials, want 7", len(creds))
	}
	// Sorted by provider then account.
	if creds[0].Account != "a@example.com" || creds[5].Provider != "codex" {
		t.Fatalf("not sorted: %+v", creds)
	}
	a := limitsCred(t, creds, "a@example.com")
	if a.State != "active" || a.RecentSuccess != 5 || a.RecentFailed != 1 || a.Success != 25469 {
		t.Fatalf("active credential = %+v", a)
	}
	if len(a.Windows) != 2 || a.Windows[0].Name != "5h" || a.Windows[0].Status != "allowed" || a.Windows[0].Utilization != 0.13 {
		t.Fatalf("windows = %+v", a.Windows)
	}
	if !a.Windows[0].Reset.Equal(time.Unix(1791741000, 0)) {
		t.Fatalf("5h reset = %v", a.Windows[0].Reset)
	}
	b := limitsCred(t, creds, "b@example.com")
	if len(b.Models) != 1 || b.Models[0].Model != "claude-opus-5-5" || b.Models[0].State != "cooling" || !b.Models[0].Until.Equal(limitsNow.Add(600*time.Second)) {
		t.Fatalf("model cooldown = %+v", b.Models)
	}
	c := limitsCred(t, creds, "c@example.com")
	// retry_at (2030) disagrees with remaining_seconds; only remaining_seconds counts.
	if c.State != "quota" || c.Reason != "credential_quota" || !c.Until.Equal(limitsNow.Add(356400*time.Second)) {
		t.Fatalf("credential quota = %+v", c)
	}
	d := limitsCred(t, creds, "d@example.com")
	if len(d.Windows) != 2 || d.Windows[1].Status != "rejected" || d.Windows[1].Utilization != 1 {
		t.Fatalf("rejected window = %+v", d.Windows)
	}
	if e := limitsCred(t, creds, "e@example.com"); e.State != "error" || e.Reason != "token refresh failed" {
		t.Fatalf("error status = %+v", e)
	}
	if x := limitsCred(t, creds, "codex1@example.com"); x.State != "disabled" || x.Windows != nil {
		t.Fatalf("disabled codex = %+v", x)
	}
	if x := limitsCred(t, creds, "codex2@example.com"); x.State != "active" || x.Windows != nil || x.Success != 26246 {
		t.Fatalf("active codex = %+v", x)
	}
	out, err := json.Marshal(creds)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(out), "fake-access-token") {
		t.Fatal("state copied a token")
	}
}

func TestLimitsUnitHeadroom(t *testing.T) {
	fixture := limitsFixture(t)
	at := func(creds ...LimitCredential) LimitsState {
		return LimitsState{At: limitsNow, Reachable: true, Credentials: creds}
	}
	// Two claude credentials, each with an Opus cooldown ending at a different time.
	opusCooling := at(
		LimitCredential{Provider: "claude", Account: "x", State: "active", Models: []LimitModel{{Model: "claude-opus-5-5", State: "quota", Until: limitsNow.Add(10 * time.Minute)}}},
		LimitCredential{Provider: "claude", Account: "y", State: "active", Models: []LimitModel{{Model: "claude-opus-5-5", State: "cooling", Until: limitsNow.Add(20 * time.Minute)}}},
	)
	cases := []struct {
		name   string
		state  LimitsState
		model  string
		wantOK bool
		until  time.Time
	}{
		{"active credential", at(fixture...), "claude-opus-5-5", true, time.Time{}},
		{"every opus credential cooling", opusCooling, "claude-opus-5-5", false, limitsNow.Add(10 * time.Minute)},
		{"sonnet still free", opusCooling, "claude-sonnet-5-5", true, time.Time{}},
		{"credential quota blocks every model", at(LimitCredential{Provider: "claude", Account: "q", State: "quota", Until: limitsNow.Add(2 * time.Hour)}), "claude-sonnet-5-5", false, limitsNow.Add(2 * time.Hour)},
		{"elapsed cooldown is free", at(LimitCredential{Provider: "claude", Account: "q", State: "quota", Until: limitsNow.Add(-time.Minute)}), "claude-sonnet-5-5", true, time.Time{}},
		{"rejected 7d window blocks", at(LimitCredential{Provider: "claude", Account: "w", State: "active", Windows: []LimitWindow{{Name: "7d", Status: "rejected", Reset: limitsNow.Add(72 * time.Hour)}}}), "claude-opus-5-5", false, limitsNow.Add(72 * time.Hour)},
		{"opus cooldown does not block claude-opus-5", at(LimitCredential{Provider: "claude", Account: "m", State: "active", Models: []LimitModel{{Model: "claude-opus-5-5", State: "quota", Until: limitsNow.Add(time.Hour)}}}), "claude-opus-5", true, time.Time{}},
		{"dated cooldown blocks its base model", at(LimitCredential{Provider: "claude", Account: "m", State: "active", Models: []LimitModel{{Model: "claude-haiku-4-5-20251001", State: "quota", Until: limitsNow.Add(time.Hour)}}}), "claude-haiku-4-5", false, limitsNow.Add(time.Hour)},
		{"disabled codex alone is limited", at(LimitCredential{Provider: "codex", Account: "d", State: "disabled"}), "gpt-6.1-sol", false, time.Time{}},
		{"active codex serves gpt", at(limitsCred(t, fixture, "codex2@example.com")), "gpt-6.1-sol", true, time.Time{}},
		{"o-digit model maps to codex", at(LimitCredential{Provider: "codex", Account: "d", State: "disabled"}), "o3", false, time.Time{}},
		{"no codex credentials", at(limitsCred(t, fixture, "a@example.com")), "gpt-6.1-sol", false, time.Time{}},
		{"auto is not routed", at(), "auto", true, time.Time{}},
		{"empty model is not routed", at(), "", true, time.Time{}},
		{"unreachable is unknown", LimitsState{At: limitsNow, Error: "CLIProxyAPI: HTTP 401"}, "claude-opus-5-5", false, time.Time{}},
		{"stale read is unknown", LimitsState{At: limitsNow.Add(-4 * time.Minute), Reachable: true, Credentials: fixture}, "claude-opus-5-5", false, time.Time{}},
		{"future read is unknown", LimitsState{At: limitsNow.Add(4 * time.Minute), Reachable: true, Credentials: fixture}, "claude-opus-5-5", false, time.Time{}},
		{"no read yet is unknown", LimitsState{}, "claude-opus-5-5", false, time.Time{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ok, until, why := tc.state.Headroom(tc.model, limitsNow)
			if ok != tc.wantOK || !until.Equal(tc.until) {
				t.Fatalf("Headroom(%q) = %v, %v, %q; want %v, %v", tc.model, ok, until, why, tc.wantOK, tc.until)
			}
			if !ok && why == "" {
				t.Fatal("limited without a reason")
			}
		})
	}
}

// limitsRig runs the poller against a fake CLIProxyAPI with recorded side effects.
type limitsRig struct {
	p      *limitsPoller
	body   []byte
	status int
	keys   int
	states []LimitsState
	events []Event
	files  []InboxInput
	clock  time.Time
}

func newLimitsRig(t *testing.T) *limitsRig {
	t.Helper()
	body, err := os.ReadFile("testdata/cliproxy/auth-files.json")
	if err != nil {
		t.Fatal(err)
	}
	r := &limitsRig{body: body, status: http.StatusOK, clock: limitsNow}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Header.Get("Authorization") != "Bearer fake-key" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if r.status != http.StatusOK {
			w.WriteHeader(r.status)
			w.Write([]byte("upstream secret-body"))
			return
		}
		w.Write(r.body)
	}))
	t.Cleanup(srv.Close)
	r.p = &limitsPoller{
		client: srv.Client(), baseURL: srv.URL,
		key: func(context.Context) (string, error) {
			r.keys++
			return "fake-key", nil
		},
		store: func(_ context.Context, s LimitsState) error {
			r.states = append(r.states, s)
			return nil
		},
		publish: func(_ context.Context, e Event) error {
			r.events = append(r.events, e)
			return nil
		},
		file: func(_ context.Context, in InboxInput) error {
			r.files = append(r.files, in)
			return nil
		},
		now: func() time.Time { return r.clock },
	}
	return r
}

func TestLimitsUnitPollPublishesChanges(t *testing.T) {
	r := newLimitsRig(t)
	ctx := context.Background()
	r.p.poll(ctx)
	if len(r.states) != 1 || len(r.events) != 1 {
		t.Fatalf("first poll stored %d states, published %d events", len(r.states), len(r.events))
	}
	if e := r.events[0]; e.Validate() != nil || e.Kind != KindLimitsChanged || e.Subject() != "cos.events.limits.limits.changed" {
		t.Fatalf("first event = %+v, validate %v", e, e.Validate())
	}
	// Same state 30 s later: the cooldown counts down as CLIProxyAPI reports it, so nothing changed.
	r.clock = r.clock.Add(limitsPoll)
	r.p.poll(ctx)
	if len(r.events) != 1 || len(r.states) != 2 {
		t.Fatalf("unchanged poll published %d events, stored %d states", len(r.events), len(r.states))
	}
	// A longer model cooldown is a change.
	r.body = []byte(strings.Replace(string(r.body), `"remaining_seconds": 600`, `"remaining_seconds": 3600`, 1))
	r.p.poll(ctx)
	if len(r.events) != 2 {
		t.Fatalf("changed cooldown published %d events, want 2", len(r.events))
	}
	if !strings.Contains(string(r.events[1].Data), "claude-opus-5-5") {
		t.Fatalf("event data = %s", r.events[1].Data)
	}
}

func TestLimitsUnitPollUnauthorized(t *testing.T) {
	r := newLimitsRig(t)
	ctx := context.Background()
	r.status = http.StatusUnauthorized
	r.p.poll(ctx)
	s := r.states[0]
	if s.Reachable || s.Error != "CLIProxyAPI: HTTP 401" || s.Credentials != nil {
		t.Fatalf("state = %+v", s)
	}
	if strings.Contains(s.Error, "fake-key") || strings.Contains(s.Error, "secret-body") {
		t.Fatalf("error leaks the key or body: %q", s.Error)
	}
	// The dropped key is read again on the next poll.
	r.status = http.StatusOK
	r.p.poll(ctx)
	if r.keys != 2 || !r.states[1].Reachable {
		t.Fatalf("keys read %d times, state after recovery = %+v", r.keys, r.states[1])
	}
}

func TestLimitsUnitPollInboxOncePerOutage(t *testing.T) {
	r := newLimitsRig(t)
	ctx := context.Background()
	r.status = http.StatusServiceUnavailable
	r.p.poll(ctx)
	if len(r.files) != 0 {
		t.Fatal("one failed poll filed an Inbox item")
	}
	r.p.poll(ctx)
	r.p.poll(ctx)
	if len(r.files) != 1 || r.files[0].Key != "limits-unreachable" || r.files[0].Context != "CLIProxyAPI: HTTP 503" {
		t.Fatalf("files = %+v", r.files)
	}
	r.status = http.StatusOK
	r.p.poll(ctx)
	r.status = http.StatusServiceUnavailable
	r.p.poll(ctx)
	r.p.poll(ctx)
	if len(r.files) != 2 {
		t.Fatalf("second outage filed %d items in total, want 2", len(r.files))
	}
}

func TestLimitsUnitEventFitsWithManyCredentials(t *testing.T) {
	var creds []LimitCredential
	for i := range 50 {
		c := LimitCredential{Provider: "claude", Account: strings.Repeat("account", 6) + string(rune('a'+i%26)) + "@example.com", State: "quota", Until: limitsNow.Add(time.Hour)}
		for range 5 {
			c.Models = append(c.Models, LimitModel{Model: "claude-opus-5-5-with-a-long-name", State: "quota", Until: limitsNow.Add(time.Hour)})
		}
		creds = append(creds, c)
	}
	e, err := limitsEvent(LimitsState{At: limitsNow, Reachable: true, Credentials: creds})
	if err != nil {
		t.Fatal(err)
	}
	if len(e.Data) > eventDataMax || !json.Valid(e.Data) || e.Validate() != nil {
		t.Fatalf("event data is %d bytes, valid %v, validate %v", len(e.Data), json.Valid(e.Data), e.Validate())
	}
	var d limitsEventData
	if err := json.Unmarshal(e.Data, &d); err != nil {
		t.Fatal(err)
	}
	if d.Omitted == 0 || d.Omitted+len(d.Credentials) != 50 {
		t.Fatalf("omitted %d of 50 with %d kept", d.Omitted, len(d.Credentials))
	}
}

func TestLimitsUnitStoreRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	bus, _ := releaseTestBus(t)
	kv, err := openLimits(ctx, bus)
	if err != nil {
		t.Fatal(err)
	}
	if s, err := readLimits(ctx, kv); err != nil || s.Reachable || len(s.Credentials) != 0 {
		t.Fatalf("missing state = %+v, %v", s, err)
	}
	want := LimitsState{At: limitsNow, Reachable: true, Credentials: limitsFixture(t)}
	if err := kvPutJSON(ctx, kv, limitsKey, want); err != nil {
		t.Fatal(err)
	}
	got, err := readLimits(ctx, kv)
	if err != nil {
		t.Fatal(err)
	}
	if !got.At.Equal(want.At) || len(got.Credentials) != 7 || got.Credentials[0].Account != "a@example.com" {
		t.Fatalf("read back %+v", got)
	}
	w := &Web{Bus: bus}
	rec := httptest.NewRecorder()
	w.limitsJSON(rec, httptest.NewRequest(http.MethodGet, "/limits", nil))
	if rec.Code != http.StatusOK || rec.Header().Get("Cache-Control") != "no-store" || !strings.Contains(rec.Body.String(), `"reachable":true`) {
		t.Fatalf("GET /limits = %d %s", rec.Code, rec.Body.String())
	}
}
