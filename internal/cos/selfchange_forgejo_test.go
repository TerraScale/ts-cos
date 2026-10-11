package cos

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

var gateCheck = selfCheck{Summary: "header", Path: "/", Contains: []string{"Fleet"}}

// gateData is what Forgejo reports for one PR (number 7, head head7).
type gateData struct {
	base     string
	files    []map[string]any
	ci       string
	statuses []map[string]any
	comments []map[string]any
}

func gateStatus(id int, context, status, login string) map[string]any {
	var creator any
	if login != "" {
		creator = map[string]string{"login": login}
	}
	return map[string]any{"id": id, "context": context, "status": status, "creator": creator}
}

func gateReady() gateData {
	return gateData{
		base:     "main",
		files:    []map[string]any{{"filename": "internal/cos/dashboard.templ", "status": "modified"}},
		ci:       "success",
		statuses: []map[string]any{gateStatus(1, "ts-reviewer", "success", "terrascale-bot")},
	}
}

// gatePage serves one item per page with X-Total-Count, so forgejoList has to follow pages.
func gatePage[T any](w http.ResponseWriter, r *http.Request, items []T) {
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	w.Header().Set("X-Total-Count", strconv.Itoa(len(items)))
	out := []T{}
	if page >= 1 && page <= len(items) {
		out = append(out, items[page-1])
	}
	forgejoTestJSON(w, out)
}

func gateRun(t *testing.T, d gateData) (wait, block string) {
	t.Helper()
	f := forgejoTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/repos/TerraScale/ts-cos/pulls/7/files":
			gatePage(w, r, d.files)
		case "/repos/TerraScale/ts-cos/contents/checks/TS-901.json":
			forgejoTestJSON(w, map[string]any{"content": base64.StdEncoding.EncodeToString([]byte(selfCheckJSON(gateCheck))), "sha": "check", "size": len(selfCheckJSON(gateCheck))})
		case "/repos/TerraScale/ts-cos/actions/runs":
			runs := []map[string]any{}
			if d.ci != "" {
				runs = append(runs, map[string]any{"id": 1, "workflow_id": "ci.yml", "commit_sha": "head7", "status": d.ci})
			}
			forgejoTestJSON(w, map[string]any{"total_count": len(runs), "workflow_runs": runs})
		case "/repos/TerraScale/ts-cos/commits/head7/statuses":
			gatePage(w, r, d.statuses)
		case "/repos/TerraScale/ts-cos/issues/7/comments":
			gatePage(w, r, d.comments)
		default:
			http.NotFound(w, r)
		}
	})
	p := selfPR{Number: 7, State: "open", Mergeable: true, User: forgejoUser{Login: forgejoLogin}}
	p.Head.SHA = "head7"
	p.Base.Ref = d.base
	wait, block, err := f.selfGate(t.Context(), selfRecord{Ticket: "TS-901", Check: gateCheck}, p)
	if err != nil {
		t.Fatal(err)
	}
	return wait, block
}

func TestSelfGateReady(t *testing.T) {
	if wait, block := gateRun(t, gateReady()); wait != "" || block != "" {
		t.Fatalf("green bot-reviewed PR not ready: wait %q block %q", wait, block)
	}
}

func TestSelfGateHoldsOrBlocks(t *testing.T) {
	bot := gateStatus(1, "ts-reviewer", "failure", "terrascale-bot")
	finding := []map[string]any{{"id": 1, "body": "<!-- ts-reviewer:head7 -->\n### Findings (1)\n\n#### HIGH · Session cookie leaks\n\nThe probe logs the credential.", "user": map[string]string{"login": "terrascale-bot"}}}
	cases := []struct {
		name  string
		edit  func(d *gateData)
		block bool   // true: blocked for the user; false: waits for the worker or CI
		want  string // substring of the reason
	}{
		{"PR not targeting main", func(d *gateData) { d.base = "dev" }, true, "PR does not target main"},
		{"renamed from a protected path", func(d *gateData) {
			d.files = []map[string]any{{"filename": "internal/cos/dashboard.templ", "previous_filename": "internal/cos/dashboard_auth.go", "status": "renamed"}}
		}, true, "dashboard_auth.go"},
		{"protected path on files page 2", func(d *gateData) {
			d.files = append(d.files, map[string]any{"filename": "internal/cos/forgejo.go", "status": "modified"})
		}, true, "internal/cos/forgejo.go"},
		{"forged ts-reviewer success", func(d *gateData) {
			d.statuses = []map[string]any{bot, gateStatus(3, "ts-reviewer", "success", "mariogk")}
			d.comments = finding
		}, false, "security/data-loss finding"},
		{"ts-reviewer errored", func(d *gateData) {
			d.statuses = []map[string]any{gateStatus(1, "ts-reviewer", "error", "terrascale-bot")}
		}, false, "ts-reviewer ended error"},
		{"review comment forged by mariogk", func(d *gateData) {
			d.statuses = []map[string]any{bot}
			d.comments = []map[string]any{{"id": 1, "body": "<!-- ts-reviewer:head7 -->\nLooks fine.", "user": map[string]string{"login": "mariogk"}}}
		}, false, "waiting for ts-reviewer findings"},
		{"CI run failed beside a forged success status", func(d *gateData) {
			d.ci = "failure"
			d.statuses = []map[string]any{gateStatus(4, "ts-cos CI / test (pull_request)", "success", ""), gateStatus(1, "ts-reviewer", "success", "terrascale-bot")}
		}, false, "CI failure; the worker is fixing it"},
		{"no CI run yet", func(d *gateData) { d.ci = "" }, false, "waiting for CI to start"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := gateReady()
			c.edit(&d)
			wait, block := gateRun(t, d)
			reason := wait + block
			if c.block != (block != "") || (wait != "") == (block != "") || !strings.Contains(reason, c.want) {
				t.Fatalf("wait %q block %q; want block %v containing %q", wait, block, c.block, c.want)
			}
		})
	}
}

func TestSelfFileRejectsEmptyContent(t *testing.T) {
	f := forgejoTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		forgejoTestJSON(w, map[string]any{"content": "", "sha": "blob", "size": 12})
	})
	if _, _, _, err := f.selfFile(t.Context(), "README.md", "main"); err == nil || !strings.Contains(err.Error(), "no content") {
		t.Fatalf("empty content accepted: %v", err)
	}
}

func TestSelfRevertPRRename(t *testing.T) {
	blobs := map[string]string{
		"merge7/internal/cos/dashboard_view.go": "new",
		"main/internal/cos/dashboard_view.go":   "new",
		"parent7/internal/cos/dashboard.go":     "old",
	}
	var posted []map[string]string
	f := forgejoTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		const repo = "/repos/TerraScale/ts-cos"
		switch {
		case r.Method == "GET" && r.URL.Path == repo+"/git/commits/merge7":
			forgejoTestJSON(w, map[string]any{"parents": []map[string]string{{"sha": "parent7"}}})
		case r.Method == "GET" && r.URL.Path == repo+"/pulls/7/files":
			forgejoTestJSON(w, []map[string]string{{"filename": "internal/cos/dashboard_view.go", "previous_filename": "internal/cos/dashboard.go", "status": "renamed"}})
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, repo+"/contents/"):
			key := r.URL.Query().Get("ref") + "/" + strings.TrimPrefix(r.URL.Path, repo+"/contents/")
			content, ok := blobs[key]
			if !ok {
				http.NotFound(w, r)
				return
			}
			forgejoTestJSON(w, map[string]any{"content": base64.StdEncoding.EncodeToString([]byte(content)), "sha": "blob:" + content, "size": len(content)})
		case r.Method == "POST" && r.URL.Path == repo+"/contents":
			var body struct {
				Files []map[string]string `json:"files"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			posted = body.Files
			w.WriteHeader(http.StatusCreated)
		case r.Method == "POST" && r.URL.Path == repo+"/pulls":
			forgejoTestJSON(w, map[string]any{"number": 8, "html_url": "https://git/pulls/8"})
		default:
			http.Error(w, "unexpected "+r.Method+" "+r.URL.Path, http.StatusInternalServerError)
		}
	})
	p, err := f.selfRevertPR(t.Context(), "merge7", 7, "cos/ts-901-revert", "TS-901: revert", "body")
	if err != nil || p.Number != 8 {
		t.Fatal(p, err)
	}
	want := []map[string]string{
		{"operation": "delete", "path": "internal/cos/dashboard_view.go", "sha": "blob:new"},
		{"operation": "create", "path": "internal/cos/dashboard.go", "content": base64.StdEncoding.EncodeToString([]byte("old"))},
	}
	if !reflect.DeepEqual(posted, want) {
		t.Fatalf("revert files:\n got %v\nwant %v", posted, want)
	}
}
