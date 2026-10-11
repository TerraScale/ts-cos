package cos

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"git.terrascale.tech/TerraScale/ts-cos/checks"
)

// TestRepoAcceptanceChecks is the CI half of the self-change gate: every committed
// acceptance check must pass against this build.
func TestRepoAcceptanceChecks(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "cos.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	h := NewWeb(store, nil).routes()
	files, _ := fs.Glob(checks.Files, "*.json")
	if len(files) == 0 {
		t.Fatal("no acceptance checks embedded")
	}
	for _, name := range files {
		data, _ := checks.Files.ReadFile(name)
		var c selfCheck
		dec := json.NewDecoder(strings.NewReader(string(data)))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&c); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if err := c.validate(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if c.Template != "" {
			out, err := embeddedBehavior().Render(c.Template, behaviorTemplates[c.Template])
			if err == nil {
				err = c.judge(out)
			}
			if err != nil {
				t.Errorf("%s: %v", name, err)
			}
			continue
		}
		r := httptest.NewRecorder()
		req := httptest.NewRequest("GET", c.Path, nil)
		req.RemoteAddr = "127.0.0.1:1234"
		h.ServeHTTP(r, req)
		if r.Code != http.StatusOK {
			t.Errorf("%s: GET %s = %d", name, c.Path, r.Code)
		} else if err := c.judge(r.Body.String()); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestSelfCheckValidate(t *testing.T) {
	ok := selfCheck{Summary: "Header reads Fleet", Path: "/", Contains: []string{"Fleet"}}
	if err := ok.validate(); err != nil {
		t.Fatal(err)
	}
	bad := []selfCheck{
		{Path: "/", Contains: []string{"x"}},
		{Summary: "s", Contains: []string{"x"}},
		{Summary: "s", Path: "/", Template: "updated", Contains: []string{"x"}},
		{Summary: "s", Path: "http://evil/", Contains: []string{"x"}},
		{Summary: "s", Path: "//evil/", Contains: []string{"x"}},
		{Summary: "s", Path: "/../etc", Contains: []string{"x"}},
		{Summary: "s", Template: "Updated!", Contains: []string{"x"}},
		{Summary: "s", Path: "/"},
		{Summary: "s", Path: "/", Contains: []string{" "}},
	}
	for i, c := range bad {
		if c.validate() == nil {
			t.Errorf("case %d accepted: %+v", i, c)
		}
	}
	if err := ok.judge("<h1>Fleet</h1>"); err != nil {
		t.Fatal(err)
	}
	if err := (selfCheck{Absent: []string{"Old"}}).judge("Old header"); err == nil {
		t.Fatal("absent text not detected")
	}
}

func TestSelfRequestAndGuard(t *testing.T) {
	r, err := selfRequest(`{"request":"rename the header","summary":"Header reads Fleet","check":{"summary":"h","path":"/","contains":["Fleet"]}}`)
	if err != nil || r.State != selfQueued {
		t.Fatal(r, err)
	}
	if selfModel["model"] != "claude-opus-5-5" {
		t.Fatal("self-change worker is not Opus")
	}
	for _, raw := range []string{`{}`, `{"request":"x","summary":"a\nb","check":{"summary":"h","path":"/","contains":["F"]}}`, `{"request":"x","summary":"s","check":{}}`, `{"request":"x","summary":"s","check":{"summary":"h","path":"/","contains":["F"]},"skipGate":true}`} {
		if _, err := selfRequest(raw); err == nil {
			t.Errorf("accepted %s", raw)
		}
	}
	for _, files := range [][]string{{"internal/cos/dashboard_auth.go"}, {".forgejo/workflows/ci.yml"}, {"internal/cos/selfchange.go"}, {"checks/TS-1.json"}, {"internal/cos/e2e.go"}, {"internal/cos/login.templ"}} {
		if selfGuard(files, "TS-9") == "" {
			t.Errorf("guard allowed %v", files)
		}
	}
	if g := selfGuard([]string{"internal/cos/dashboard.templ", "checks/TS-9.json", "behavior/templates/updated.tmpl"}, "TS-9"); g != "" {
		t.Fatal(g)
	}
}

func TestSelfRequestValidation(t *testing.T) {
	const check = `"check":{"summary":"h","path":"/","contains":["F"]}`
	req := func(request, summary string) string {
		return `{"request":"` + request + `","summary":"` + summary + `",` + check + `}`
	}
	for i, raw := range []string{
		req("x", "s") + " {}",
		req("x", `s\r`),
		req(`x\u0007`, "s"),
		req("x", strings.Repeat("é", 121)),
	} {
		if _, err := selfRequest(raw); err == nil {
			t.Errorf("case %d accepted: %s", i, raw)
		}
	}
	for i, raw := range []string{
		req(`line one\nline two`, "s"),
		req("x", strings.Repeat("é", 120)),
	} {
		if _, err := selfRequest(raw); err != nil {
			t.Errorf("case %d rejected: %v", i, err)
		}
	}
}

func TestSelfUndoOncePerChange(t *testing.T) {
	orig := selfRecord{ID: "00000000000000a1", Summary: "Header reads Fleet", Ticket: "SELF-1", State: selfDone}
	u, err := selfUndo([]selfRecord{orig}, "")
	if err != nil || u.ID != orig.ID+"-undo" || u.Reverts != orig.ID {
		t.Fatal(u, err)
	}
	// The revert finished but the original lost its RevertedBy: still one undo.
	done := selfRecord{ID: u.ID, Reverts: orig.ID, Ticket: "SELF-1", State: selfDone}
	if _, err := selfUndo([]selfRecord{orig, done}, ""); err == nil {
		t.Fatal("a reverted change can be undone twice")
	}
}

// selfFake is a Forgejo for one ts-cos self-change PR (number 7) and its revert (8).
type selfFake struct {
	mu        sync.Mutex
	files     map[string]map[string]string // ref -> path -> content
	prOpen    map[int]bool
	merged    map[int]bool
	prFiles   map[int][]string
	ci        string
	reviewer  string
	review    string
	mergeBody map[int]map[string]any
}

func (f *selfFake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	repo := "/repos/TerraScale/ts-cos"
	p := strings.TrimPrefix(r.URL.Path, repo)
	var body map[string]any
	if data, _ := io.ReadAll(r.Body); len(data) > 0 {
		json.Unmarshal(data, &body)
	}
	pr := func(n int, branch string) map[string]any {
		return map[string]any{"number": n, "html_url": "https://git/pulls/" + itoa(n), "state": map[bool]string{true: "open", false: "closed"}[f.prOpen[n]],
			"merged": f.merged[n], "merge_commit_sha": map[bool]string{true: "merge" + itoa(n) + "0000000"}[f.merged[n]], "mergeable": true,
			"user": map[string]string{"login": "mariogk"}, "head": map[string]string{"ref": branch, "sha": "head" + itoa(n)}}
	}
	switch {
	case r.Method == "POST" && p == "/branches":
		f.files[body["new_branch_name"].(string)] = map[string]string{}
	case r.Method == "GET" && strings.HasPrefix(p, "/contents/"):
		ref := r.URL.Query().Get("ref")
		content, ok := f.files[ref][strings.TrimPrefix(p, "/contents/")]
		if !ok {
			http.NotFound(w, r)
			return
		}
		forgejoTestJSON(w, map[string]string{"content": base64.StdEncoding.EncodeToString([]byte(content)), "sha": "blob:" + content})
	case r.Method == "POST" && strings.HasPrefix(p, "/contents/"):
		data, _ := base64.StdEncoding.DecodeString(body["content"].(string))
		f.files[body["branch"].(string)][strings.TrimPrefix(p, "/contents/")] = string(data)
		w.WriteHeader(201)
	case r.Method == "POST" && p == "/contents":
		f.files[body["new_branch"].(string)] = map[string]string{}
		w.WriteHeader(201)
	case r.Method == "GET" && p == "/pulls":
		list := []any{}
		if _, ok := f.prOpen[7]; ok {
			list = append(list, pr(7, "cos/ts-901"))
		}
		forgejoTestJSON(w, list)
	case r.Method == "POST" && p == "/pulls":
		f.prOpen[8], f.prFiles[8] = true, []string{"internal/cos/dashboard.templ", "checks/TS-901.json"}
		forgejoTestJSON(w, pr(8, body["head"].(string)))
	case r.Method == "GET" && (p == "/pulls/7" || p == "/pulls/8"):
		n := int(p[len(p)-1] - '0')
		forgejoTestJSON(w, pr(n, "x"))
	case r.Method == "GET" && strings.HasSuffix(p, "/files"):
		n := int(p[len("/pulls/")] - '0')
		out := []map[string]string{}
		for _, name := range f.prFiles[n] {
			out = append(out, map[string]string{"filename": name, "status": "modified"})
		}
		forgejoTestJSON(w, out)
	case r.Method == "GET" && strings.HasPrefix(p, "/commits/") && strings.HasSuffix(p, "/status"):
		forgejoTestJSON(w, map[string]any{"statuses": []map[string]string{{"context": "ts-cos CI / test (pull_request)", "status": f.ci}, {"context": "ts-reviewer", "status": f.reviewer}}, "total_count": 2})
	case r.Method == "GET" && strings.HasSuffix(p, "/comments"):
		forgejoTestJSON(w, []map[string]string{{"body": f.review}})
	case r.Method == "POST" && strings.HasSuffix(p, "/merge"):
		n := int(p[len("/pulls/")] - '0')
		f.merged[n], f.prOpen[n], f.mergeBody[n] = true, false, body
		if n == 7 {
			f.files["main"]["internal/cos/dashboard.templ"] = "new"
		}
	case r.Method == "GET" && strings.HasPrefix(p, "/git/commits/"):
		forgejoTestJSON(w, map[string]any{"parents": []map[string]string{{"sha": "parent7"}}, "files": []map[string]string{{"filename": "internal/cos/dashboard.templ", "status": "modified"}}})
	default:
		t := "unexpected " + r.Method + " " + r.URL.String()
		http.Error(w, t, 500)
	}
}

func itoa(n int) string { return string(rune('0' + n)) }

type selfTestRig struct {
	d      *selfDriver
	fake   *selfFake
	said   []string
	pass   bool // what every node's check returns
	live   bool // rollout finished
	closed []string
	// Full e2e suite: what it reports and which own check it skipped per run.
	e2eFail string
	e2eRuns []string
}

func newSelfTestRig(t *testing.T, ctx context.Context) *selfTestRig {
	t.Helper()
	buses, configs := authTestCluster(t, ctx)
	bus, cfg := buses[0], configs[0]
	var store selfStore
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		var err error
		if store, err = openSelfStore(ctx, bus, true); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
	}
	fake := &selfFake{files: map[string]map[string]string{"main": {"internal/cos/dashboard.templ": "old"}, "merge70000000": {"internal/cos/dashboard.templ": "new"}, "parent7": {"internal/cos/dashboard.templ": "old"}},
		prOpen: map[int]bool{}, merged: map[int]bool{}, prFiles: map[int][]string{7: {"internal/cos/dashboard.templ", "checks/TS-901.json"}}, mergeBody: map[int]map[string]any{}, ci: "pending", reviewer: "pending"}
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)
	dir := filepath.Join(cfg.Home, ".local/share/forgejo-cli")
	os.MkdirAll(dir, 0700)
	os.WriteFile(filepath.Join(dir, "keys.json"), []byte(`{"hosts":{"git.terrascale.tech":{"token":"t"}}}`), 0600)
	cfg.ForgejoURL, cfg.HTTP = server.URL, server.Client()
	rig := &selfTestRig{fake: fake}
	d := newSelfDriver(cfg, bus, store)
	d.ticket = func(context.Context, selfRecord) (string, error) { return "TS-901", nil }
	d.launch = func(_ context.Context, r selfRecord, brief string) (string, string, error) {
		if !strings.Contains(brief, "checks/TS-901.json") || !strings.Contains(brief, r.Branch) {
			t.Errorf("brief misses the check or branch:\n%s", brief)
		}
		fake.mu.Lock()
		fake.prOpen[7] = true
		fake.files["head7"] = fake.files[r.Branch]
		fake.mu.Unlock()
		return "delta", "thread-1", nil
	}
	d.check = func(context.Context, selfCheck) ([]selfCheckResult, bool, error) {
		res := []selfCheckResult{{"beta", rig.pass, "e"}, {"delta", rig.pass, "e"}, {"mariogk-dev", rig.pass, "e"}}
		return res, rig.pass, nil
	}
	d.rollout = func(context.Context, selfRecord) (bool, string, error) { return rig.live, "", nil }
	d.say = func(_ context.Context, line string) { rig.said = append(rig.said, line) }
	d.e2e = func(_ context.Context, skip string) string {
		rig.e2eRuns = append(rig.e2eRuns, skip)
		return rig.e2eFail
	}
	d.finish = func(ctx context.Context, r selfRecord) {
		rig.closed = append(rig.closed, r.ID+":"+r.State)
		if r.State == selfDone && r.Reverts != "" {
			orig, rev, _ := d.store.get(ctx, r.Reverts)
			orig.RevertedBy = r.ID
			d.store.save(ctx, orig, rev)
		}
	}
	rig.d = d
	return rig
}

func (rig *selfTestRig) state(t *testing.T, ctx context.Context, id string) selfRecord {
	t.Helper()
	r, _, err := rig.d.store.get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func (rig *selfTestRig) set(fn func(f *selfFake)) {
	rig.fake.mu.Lock()
	defer rig.fake.mu.Unlock()
	fn(rig.fake)
}

func TestSelfChangeLoopAndUndo(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	rig := newSelfTestRig(t, ctx)
	r, err := selfRequest(`{"request":"the header should say Fleet","summary":"Header reads Fleet","check":{"summary":"header","path":"/","contains":["Fleet"]}}`)
	if err != nil {
		t.Fatal(err)
	}
	if err := rig.d.store.create(ctx, r); err != nil {
		t.Fatal(err)
	}
	steps := func(n int) {
		for range n {
			rig.d.step(ctx)
		}
	}
	steps(4) // ticket, failing check on branch, worker, PR found
	got := rig.state(t, ctx, r.ID)
	if got.State != selfGating || got.Ticket != "TS-901" || got.Thread != "thread-1" || got.PR != 7 {
		t.Fatalf("after 4 steps: %+v", got)
	}
	if rig.fake.files["cos/ts-901"]["checks/TS-901.json"] != selfCheckJSON(r.Check) {
		t.Fatal("acceptance check not committed first on the branch")
	}
	steps(1)
	if got = rig.state(t, ctx, r.ID); got.State != selfGating || !strings.Contains(got.Note, "CI running") {
		t.Fatalf("merged before CI: %+v", got)
	}
	// Green CI, but ts-reviewer reports a security finding: wait for the worker's fix.
	rig.set(func(f *selfFake) {
		f.ci, f.reviewer = "success", "failure"
		f.review = "<!-- ts-reviewer:head7 -->\n### Findings (1)\n\n#### HIGH · Session cookie leaks\n\nThe probe logs the credential."
	})
	steps(1)
	if got = rig.state(t, ctx, r.ID); got.State != selfGating || !strings.Contains(got.Note, "security") {
		t.Fatalf("security finding did not hold the merge: %+v", got)
	}
	// Advisory findings do not block; the full live e2e suite must pass next.
	rig.set(func(f *selfFake) {
		f.review = "<!-- ts-reviewer:head7 -->\n### Findings (1)\n\n#### LOW · Name could be clearer\n\nRename x."
	})
	rig.e2eFail = "access.login.public.delta: HTTP 502"
	steps(2)
	if got = rig.state(t, ctx, r.ID); got.State != selfGating || !strings.Contains(got.Note, "e2e failing") || len(rig.e2eRuns) != 1 || rig.e2eRuns[0] != "accept.ts-901.live" {
		t.Fatalf("merged past a failing e2e suite or reran it at once: %+v %v", got, rig.e2eRuns)
	}
	rig.e2eFail = ""
	clock := time.Now().Add(11 * time.Minute)
	rig.d.now = func() time.Time { return clock }
	steps(1)
	if got = rig.state(t, ctx, r.ID); got.State != selfGating || got.Note != "e2e green" || len(rig.e2eRuns) != 2 {
		t.Fatalf("e2e not rerun after 10 minutes: %+v", got)
	}
	steps(1)
	got = rig.state(t, ctx, r.ID)
	if got.State != selfMerged || got.SHA != "merge70000000" {
		t.Fatalf("not merged: %+v", got)
	}
	if msg := rig.fake.mergeBody[7]["MergeMessageField"]; msg != "Update-Note: Header reads Fleet\n" || rig.fake.mergeBody[7]["head_commit_id"] != "head7" {
		t.Fatalf("merge body: %v", rig.fake.mergeBody[7])
	}
	steps(1)
	if got = rig.state(t, ctx, r.ID); got.State != selfMerged {
		t.Fatalf("live before rollout: %+v", got)
	}
	rig.live = true
	steps(2) // live; check still failing on the nodes
	if got = rig.state(t, ctx, r.ID); got.State != selfLive {
		t.Fatalf("verified a failing check: %+v", got)
	}
	rig.pass = true
	steps(1)
	got = rig.state(t, ctx, r.ID)
	if got.State != selfDone || got.VerifiedAt.IsZero() {
		t.Fatalf("not done: %+v", got)
	}
	if last := rig.said[len(rig.said)-1]; last != "Updated: Header reads Fleet (merge70)" {
		t.Fatalf("chat said %q (all: %q)", last, rig.said)
	}
	for _, line := range rig.said {
		if strings.Contains(line, "\n") || len(line) > behaviorLineMax {
			t.Errorf("status line is a wall of text: %q", line)
		}
	}

	// "undo that" reverts the newest live change through the same gate.
	records, _ := rig.d.store.list(ctx)
	u, err := selfUndo(records, "")
	if err != nil || u.Reverts != r.ID {
		t.Fatal(u, err)
	}
	rig.d.store.create(ctx, u)
	rig.live, rig.pass = false, true
	rig.set(func(f *selfFake) { f.review = "<!-- ts-reviewer:head8 -->\n### Findings (0)\n" })
	steps(3) // revert PR opened, e2e green, merged
	if got = rig.state(t, ctx, u.ID); got.State != selfMerged || got.PR != 8 {
		t.Fatalf("revert not merged: %+v", got)
	}
	rig.live = true
	steps(2)
	if got = rig.state(t, ctx, u.ID); got.State != selfLive {
		t.Fatalf("revert verified while the change is still live: %+v", got)
	}
	rig.pass = false
	steps(1)
	if got = rig.state(t, ctx, u.ID); got.State != selfDone {
		t.Fatalf("revert not done: %+v", got)
	}
	if last := rig.said[len(rig.said)-1]; last != "Reverted: Header reads Fleet (merge80)" {
		t.Fatalf("chat said %q", last)
	}
	if orig := rig.state(t, ctx, r.ID); orig.RevertedBy != u.ID {
		t.Fatal("original not marked reverted")
	}
	if _, err := selfUndo(func() []selfRecord { l, _ := rig.d.store.list(ctx); return l }(), ""); err == nil {
		t.Fatal("a reverted change can be undone twice")
	}
	log := selfLog(func() []selfRecord { l, _ := rig.d.store.list(ctx); return l }(), true, time.Now())
	if text := log["text"].(string); !strings.Contains(text, "TS-901") || !strings.Contains(text, "(reverted)") {
		t.Fatalf("changelog text: %q", text)
	}
}

func TestSelfUndoRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	rig := newSelfTestRig(t, ctx)
	failed := selfRecord{ID: "x-undo", Reverts: "x", Ticket: "TS-901", State: selfFailed}
	if err := rig.d.store.create(ctx, failed); err != nil {
		t.Fatal(err)
	}
	// A failed undo is replaced by a fresh queued one with the same ID.
	fresh := selfRecord{ID: "x-undo", Reverts: "x", Ticket: "TS-901", State: selfQueued}
	if err := selfQueueUndo(ctx, rig.d.store, fresh); err != nil {
		t.Fatal(err)
	}
	if got := rig.state(t, ctx, "x-undo"); got.State != selfQueued {
		t.Fatalf("retry not queued: %+v", got)
	}
	// A queued (running) undo is kept: a second attempt is refused.
	if err := selfQueueUndo(ctx, rig.d.store, fresh); err == nil || !strings.Contains(err.Error(), "already being reverted") {
		t.Fatalf("running undo replaced: %v", err)
	}
}

func TestSelfChangeMergedPRIsNotFailed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	rig := newSelfTestRig(t, ctx)
	r, err := selfRequest(`{"request":"the header should say Fleet","summary":"Header reads Fleet","check":{"summary":"header","path":"/","contains":["Fleet"]}}`)
	if err != nil {
		t.Fatal(err)
	}
	r.State, r.Ticket, r.Branch, r.PR = selfGating, "TS-901", "cos/ts-901", 7
	if err := rig.d.store.create(ctx, r); err != nil {
		t.Fatal(err)
	}
	// Merged by someone else (or after a lost save): the record follows the merge.
	rig.set(func(f *selfFake) { f.merged[7], f.prOpen[7] = true, false })
	rig.d.step(ctx)
	if got := rig.state(t, ctx, r.ID); got.State != selfMerged || got.SHA != "merge70000000" {
		t.Fatalf("merged PR not followed: %+v", got)
	}
}

func TestSelfChangeGuardsAndQueue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	rig := newSelfTestRig(t, ctx)
	js, err := rig.d.bus.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	info, err := js.Stream(ctx, "KV_"+selfBucket)
	if err != nil {
		t.Fatal(err)
	}
	if info.CachedInfo().Config.AllowDirect {
		t.Fatal("COS_SELF allows direct gets; a lagging replica could serve a stale record")
	}
	rig.set(func(f *selfFake) { f.ci, f.reviewer = "success", "success" })

	// A check that already passes means there is nothing to change.
	rig.pass = true
	r, _ := selfRequest(`{"request":"x","summary":"Already true","check":{"summary":"h","path":"/","contains":["COS"]}}`)
	rig.d.store.create(ctx, r)
	rig.d.step(ctx)
	rig.d.step(ctx)
	if got := rig.state(t, ctx, r.ID); got.State != selfBlocked {
		t.Fatalf("passing check not blocked: %+v", got)
	}

	// Touching login is refused before merge.
	rig.pass = false
	rig.set(func(f *selfFake) { f.prFiles[7] = []string{"internal/cos/dashboard_auth.go", "checks/TS-901.json"} })
	r2, _ := selfRequest(`{"request":"y","summary":"Login skips the password","check":{"summary":"h","path":"/","contains":["X"]}}`)
	r2.ID = "z" + r2.ID
	rig.d.store.create(ctx, r2)
	for range 5 {
		rig.d.step(ctx)
	}
	got := rig.state(t, ctx, r2.ID)
	if got.State != selfBlocked || !strings.Contains(got.Note, "protected") || rig.fake.merged[7] {
		t.Fatalf("protected change not blocked: %+v", got)
	}
	if !strings.HasPrefix(rig.said[len(rig.said)-1], "TS-901 · needs you: touches protected") {
		t.Fatalf("chat said %q", rig.said[len(rig.said)-1])
	}

	// One rollout at a time: a green PR waits while another change is rolling out.
	rolling := selfRecord{ID: "a0", Ticket: "TS-800", State: selfMerged, MergedAt: time.Now()}
	rig.d.store.create(ctx, rolling)
	waiting := selfRecord{ID: "a1", Ticket: "TS-901", Branch: "cos/ts-901", PR: 7, State: selfGating, Check: r.Check, CreatedAt: time.Now()}
	rig.set(func(f *selfFake) {
		f.prOpen[7], f.prFiles[7] = true, []string{"internal/cos/dashboard.templ", "checks/TS-901.json"}
		f.files["head7"] = map[string]string{"checks/TS-901.json": selfCheckJSON(r.Check)}
	})
	rig.d.store.create(ctx, waiting)
	rig.d.step(ctx)
	if got := rig.state(t, ctx, "a1"); got.State != selfGating || !strings.Contains(got.Note, "queued behind TS-800") {
		t.Fatalf("second rollout not queued: %+v", got)
	}

	// A PR that edits its own acceptance check is refused.
	rig.set(func(f *selfFake) { f.files["head7"]["checks/TS-901.json"] = `{"summary":"weaker"}` })
	rolling.State = selfDone
	_, rev, _ := rig.d.store.get(ctx, "a0")
	rig.d.store.save(ctx, rolling, rev)
	rig.d.step(ctx)
	if got := rig.state(t, ctx, "a1"); got.State != selfBlocked || !strings.Contains(got.Note, "changed its acceptance check") {
		t.Fatalf("weakened check not blocked: %+v", got)
	}
}

func TestSelfCheckOnEveryNode(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	buses, configs := authTestCluster(t, ctx)
	live := selfCheck{Summary: "updated line", Template: "updated", Contains: []string{"Updated:"}}
	// Before the probe is deployed the e2e check skips and names the prerequisite.
	var results []selfCheckResult
	busTestReady(t, "every node reports the probe missing", func() bool {
		results, _, _ = runSelfCheck(ctx, buses[1], live)
		return len(results) == 3 && results[0].Evidence == selfNotDeployed && results[2].Evidence == selfNotDeployed
	})
	if _, err := selfE2EVerdict(live, results, false); !errors.As(err, new(e2eSkip)) || !strings.Contains(err.Error(), "TS-817 build not rolled out") {
		t.Fatalf("undeployed probe: %v", err)
	}
	if failedEverywhere(results) {
		t.Fatal("nodes without the probe count as a failing check")
	}
	var wg sync.WaitGroup
	defer wg.Wait()
	defer cancel()
	for i, bus := range buses {
		wg.Go(func() { runSelfProbe(ctx, configs[i], bus) })
	}
	busTestReady(t, "all nodes answer", func() bool {
		var passed bool
		results, passed, _ = runSelfCheck(ctx, buses[1], live)
		return passed
	})
	missing := selfCheck{Summary: "new label", Template: "updated", Contains: []string{"Shipped:"}}
	results, passed, err := runSelfCheck(ctx, buses[1], missing)
	if err != nil || passed || !failedEverywhere(results) || !strings.Contains(results[0].Evidence, `missing "Shipped:"`) {
		t.Fatalf("failing check: %v %v %+v", passed, err, results)
	}
	if _, err := selfE2EVerdict(missing, results, passed); err == nil || errors.As(err, new(e2eSkip)) {
		t.Fatalf("deployed probe with a failing check must fail, got %v", err)
	}
	bad, _ := json.Marshal(map[string]any{"summary": "x", "path": "http://evil/", "contains": []string{"x"}})
	m, err := buses[0].conn.RequestWithContext(ctx, selfCheckSubject("beta"), bad)
	if err != nil || !strings.Contains(string(m.Data), "plain dashboard path") {
		t.Fatalf("node ran an invalid check: %v %s", err, m.Data)
	}
}

func TestSelfKeyAllocation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	rig := newSelfTestRig(t, ctx)
	for want := 1; want <= 3; want++ {
		key, err := rig.d.store.nextKey(ctx)
		if err != nil || key != fmt.Sprintf("SELF-%d", want) {
			t.Fatalf("key %d = %q, %v", want, key, err)
		}
	}
	if records, err := rig.d.store.list(ctx); err != nil || len(records) != 0 {
		t.Fatalf("counter leaked into the changelog: %v %v", records, err)
	}
	if b := selfBranch("SELF-3"); b != "cos/self-3" {
		t.Fatal(b)
	}
}
