package cos

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func behaviorTestFiles() map[string]string { return maps.Clone(embeddedBehavior().files) }

func TestBehaviorEmbeddedValid(t *testing.T) {
	b := embeddedBehavior()
	got, err := b.Render("updated", SelfChange{Summary: "Header label reads Fleet", SHA: "abcdef0123"})
	if err != nil || got != "Updated: Header label reads Fleet (abcdef0)" {
		t.Fatalf("updated = %q, %v", got, err)
	}
	if brief := b.AgentBrief(); !strings.HasPrefix(brief, "# COS") || !strings.Contains(brief, "## Rules that always apply") {
		t.Fatalf("agent brief misses brief or rules:\n%s", brief)
	}
	if b.Wake.MaxWakesPerHour == 0 || CurrentBehavior() != b {
		t.Fatalf("embedded behavior not active: %+v", b.Wake)
	}
}

func TestBehaviorRejectsInvalid(t *testing.T) {
	cases := map[string]func(map[string]string){
		"missing brief": func(f map[string]string) { delete(f, "brief.md") },
		"brief header":  func(f map[string]string) { f["brief.md"] = "You are COS" },
		"empty rules":   func(f map[string]string) { f["rules.md"] = " \n" },
		"wake syntax":   func(f map[string]string) { f["wake.json"] = "{" },
		"wake unknown field": func(f map[string]string) {
			f["wake.json"] = `{"maxWakesPerHour":6,"digestSeconds":600,"immediate":["chat.message"],"enabled":true}`
		},
		"wake range": func(f map[string]string) {
			f["wake.json"] = `{"maxWakesPerHour":0,"digestSeconds":600,"immediate":["chat.message"]}`
		},
		"wake without chat": func(f map[string]string) { f["wake.json"] = `{"maxWakesPerHour":6,"digestSeconds":600,"immediate":[]}` },
		"wake bad kind": func(f map[string]string) {
			f["wake.json"] = `{"maxWakesPerHour":6,"digestSeconds":600,"immediate":["chat.message","Bad Kind"]}`
		},
		"template syntax":           func(f map[string]string) { f["templates/updated.tmpl"] = "Updated: {{.Summary" },
		"template field":            func(f map[string]string) { f["templates/updated.tmpl"] = "Updated: {{.Title}}" },
		"template missing":          func(f map[string]string) { delete(f, "templates/reverted.tmpl") },
		"status line wall":          func(f map[string]string) { f["templates/progress.tmpl"] = "{{.Ticket}}\n{{.Step}}" },
		"status line empty":         func(f map[string]string) { f["templates/updated.tmpl"] = "  " },
		"template redefines":        func(f map[string]string) { f["templates/digest.tmpl"] += `{{define "updated"}}x{{end}}` },
		"template defines new":      func(f map[string]string) { f["templates/digest.tmpl"] += `{{define "other"}}x{{end}}` },
		"unknown file":              func(f map[string]string) { f["notes.txt"] = "x" },
		"oversized bundle":          func(f map[string]string) { f["rules.md"] = strings.Repeat("x", behaviorMaxBytes) },
		"template output too large": func(f map[string]string) { f["templates/digest.tmpl"] = `{{range 100000}}xxxxxxxxxx{{end}}` },
	}
	for name, mutate := range cases {
		files := behaviorTestFiles()
		mutate(files)
		if _, err := ParseBehavior("test", files); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	files := behaviorTestFiles()
	files["templates/extra.tmpl"] = "{{.}}"
	if _, err := ParseBehavior("test", files); err != nil {
		t.Fatalf("extra template rejected: %v", err)
	}
}

func TestBehaviorRenderCapped(t *testing.T) {
	files := behaviorTestFiles()
	files["templates/big.tmpl"] = `{{range 10000}}xxxxxxxxxx{{end}}`
	b, err := ParseBehavior("test", files)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Render("big", nil); !errors.Is(err, errRenderTooLarge) {
		t.Fatalf("big render = %v, want errRenderTooLarge", err)
	}
}

func TestBehaviorLoaderKeepsPrevious(t *testing.T) {
	l := &BehaviorLoader{}
	files := behaviorTestFiles()
	files["templates/updated.tmpl"] = "Live: {{.Summary}}"
	good, _ := json.Marshal(behaviorBundle{Version: "v2", Files: files})
	if _, err := l.Load(good); err != nil {
		t.Fatal(err)
	}
	files["wake.json"] = "{"
	bad, _ := json.Marshal(behaviorBundle{Version: "v3", Files: files})
	for _, raw := range [][]byte{bad, []byte("not json")} {
		if _, err := l.Load(raw); err == nil {
			t.Fatal("invalid bundle accepted")
		}
		if got, _ := l.Current().Render("updated", SelfChange{Summary: "x"}); l.Current().Version != "v2" || got != "Live: x" {
			t.Fatalf("previous behavior not kept: %s %q", l.Current().Version, got)
		}
		if l.rejected.Load() == nil {
			t.Fatal("rejection not recorded")
		}
	}
}

// behaviorTestForgejo serves main's head and its behavior/ tree from a mutable file set.
type behaviorTestForgejo struct {
	mu    sync.Mutex
	sha   string
	files map[string]string
}

func (f *behaviorTestForgejo) set(sha string, files map[string]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sha, f.files = sha, maps.Clone(files)
}

func (f *behaviorTestForgejo) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	repo := "/repos/" + behaviorRepo
	switch {
	case r.URL.Path == repo+"/branches/main":
		forgejoTestJSON(w, map[string]any{"commit": map[string]string{"id": f.sha}})
	case r.URL.Path == repo+"/git/trees/"+f.sha:
		tree := []map[string]string{{"path": "behavior", "type": "tree", "sha": "d"}, {"path": "behavior/behavior.go", "type": "blob", "sha": "go"}, {"path": "README.md", "type": "blob", "sha": "readme"}}
		for name := range f.files {
			tree = append(tree, map[string]string{"path": "behavior/" + name, "type": "blob", "sha": f.sha + ":" + name})
		}
		forgejoTestJSON(w, map[string]any{"tree": tree})
	case strings.HasPrefix(r.URL.Path, repo+"/git/blobs/"+f.sha+":"):
		name := strings.TrimPrefix(r.URL.Path, repo+"/git/blobs/"+f.sha+":")
		forgejoTestJSON(w, map[string]string{"content": base64.StdEncoding.EncodeToString([]byte(f.files[name]))})
	default:
		http.NotFound(w, r)
	}
}

func TestBehaviorHotReloadCluster(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	forgejo := &behaviorTestForgejo{}
	forgejo.set("1111111111", behaviorTestFiles())
	server := httptest.NewServer(forgejo)
	t.Cleanup(server.Close)

	ports := busTestPorts(t, 6)
	hosts := []string{"delta", "beta", "mariogk-dev"}
	nodes, loaders, configs := make([]*Bus, 3), make([]*BehaviorLoader, 3), make([]Config, 3)
	var runs sync.WaitGroup
	defer runs.Wait()
	defer cancel()
	for i, host := range hosts {
		var peers []int
		for j := range hosts {
			if i != j {
				peers = append(peers, ports[j+3])
			}
		}
		cfg, busNode := busTestConfig(t, host, ports[i], ports[i+3], peers)
		cfg.ForgejoURL, cfg.HTTP = server.URL, server.Client()
		dir := filepath.Join(cfg.Home, ".local/share/forgejo-cli")
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "keys.json"), []byte(`{"hosts":{"git.terrascale.tech":{"token":"`+forgejoTestToken+`"}}}`), 0600); err != nil {
			t.Fatal(err)
		}
		node, err := startBus(ctx, cfg, busNode, func(context.Context, string, []string) (any, error) { return nil, nil })
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(node.Close)
		nodes[i], loaders[i], configs[i] = node, &BehaviorLoader{}, cfg
		runs.Go(func() { RunBehavior(ctx, cfg, node, loaders[i]) })
	}
	allOn := func(version string) func() bool {
		return func() bool {
			for _, l := range loaders {
				if l.Current().Version != version {
					return false
				}
			}
			return true
		}
	}
	// Cluster formation and meta-leader election take several seconds under -race.
	for deadline := time.Now().Add(30 * time.Second); !allOn("1111111111")(); time.Sleep(50 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("first publish not loaded on every node")
		}
	}

	// A merged template change reaches every node without a restart.
	files := behaviorTestFiles()
	files["templates/updated.tmpl"] = "Shipped: {{.Summary}} ({{short .SHA}})"
	forgejo.set("2222222222", files)
	js, err := nodes[0].JetStream()
	if err != nil {
		t.Fatal(err)
	}
	kv, err := behaviorBucketKV(ctx, js, false)
	if err != nil {
		t.Fatal(err)
	}
	f := Forgejo{Config: configs[0]}
	if _, err := publishBehavior(ctx, f, kv, "1111111111"); err != nil {
		t.Fatal(err)
	}
	busTestReady(t, "template change on every node", allOn("2222222222"))
	for _, l := range loaders {
		if got, _ := l.Current().Render("updated", SelfChange{Summary: "x", SHA: "abcdef0"}); got != "Shipped: x (abcdef0)" {
			t.Fatalf("node renders %q", got)
		}
	}

	// Invalid behavior on main is never published and is reported.
	files["wake.json"] = `{"maxWakesPerHour":0}`
	forgejo.set("3333333333", files)
	if _, err := publishBehavior(ctx, f, kv, "2222222222"); err == nil || !strings.Contains(err.Error(), "maxWakesPerHour") {
		t.Fatalf("invalid main published: %v", err)
	}
	// A bundle this binary cannot parse is rejected on load; every node keeps the previous one.
	bad, _ := json.Marshal(behaviorBundle{Version: "4444444444", Files: files})
	if _, err := kv.Put(ctx, behaviorKey, bad); err != nil {
		t.Fatal(err)
	}
	busTestReady(t, "rejection reported by every node", func() bool {
		status, err := BehaviorStatus(ctx, nodes[1])
		if err != nil {
			return false
		}
		nodes := status["nodes"].([]BehaviorNode)
		if len(nodes) != 3 || status["rejected"] == nil {
			return false
		}
		for _, n := range nodes {
			if n.Rejected == "" || n.Version != "2222222222" {
				return false
			}
		}
		return true
	})
	if !allOn("2222222222")() {
		t.Fatal("a node dropped the previous valid behavior")
	}
}
