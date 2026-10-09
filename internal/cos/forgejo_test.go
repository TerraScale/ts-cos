package cos

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

const forgejoTestToken = "forgejo-test-secret"

func forgejoTestClient(t *testing.T, handler http.HandlerFunc) Forgejo {
	t.Helper()
	home := t.TempDir()
	dir := filepath.Join(home, ".local/share/forgejo-cli")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "keys.json"), []byte(`{"hosts":{"git.terrascale.tech":{"token":"`+forgejoTestToken+`"}}}`), 0600); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "token "+forgejoTestToken {
			t.Error("missing Forgejo token header")
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Error("missing JSON content type")
		}
		handler(w, r)
	}))
	t.Cleanup(server.Close)
	return Forgejo{Config: Config{Home: home, ForgejoURL: server.URL, HTTP: server.Client()}}
}

func forgejoTestPR(number int, author, branch string) forgejoPR {
	p := forgejoPR{Number: number, URL: "https://git.terrascale.tech/TerraScale/test/pulls/1", Title: "TS-769: connector", Body: "ts-769 TS-101", State: "open", Mergeable: true, User: forgejoUser{Login: author}}
	p.Head.Ref = branch
	p.Head.SHA = "0123456789abcdef0123456789abcdef01234567"
	p.Base.Ref = "main"
	return p
}

func forgejoTestJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

func TestForgejoMergeGates(t *testing.T) {
	green := []forgejoStatus{{ID: 1, Context: "build", Status: "success"}, {ID: 2, Context: "ts-reviewer", Status: "success"}}
	approved := []forgejoReview{{ID: 1, User: forgejoUser{Login: forgejoReviewer}, State: "APPROVED"}}
	tests := []struct {
		name, login, author, branch, state string
		statuses                           []forgejoStatus
		reviews                            []forgejoReview
		fullSend, adopted, skip, file      bool
		conflict, emptySHA, wantMerge      bool
	}{
		{name: "green approved", statuses: green, reviews: approved, wantMerge: true},
		{name: "no checks", reviews: approved},
		{name: "no approval", statuses: green},
		{name: "failure", statuses: []forgejoStatus{{Context: "build", Status: "failure"}}, reviews: approved},
		{name: "pending", statuses: []forgejoStatus{{Context: "build", Status: "pending"}}, reviews: approved},
		{name: "unknown status", statuses: []forgejoStatus{{Context: "build", Status: "warning"}}, reviews: approved},
		{name: "conflict", statuses: green, reviews: approved, conflict: true},
		{name: "full send bot rejected", fullSend: true, statuses: []forgejoStatus{{Context: "build", Status: "success"}, {Context: "ts-reviewer", Status: "failure"}}, reviews: []forgejoReview{{User: forgejoUser{Login: forgejoReviewer}, State: "REQUEST_CHANGES"}}, wantMerge: true},
		{name: "full send missing other checks", fullSend: true, statuses: []forgejoStatus{{Context: "ts-reviewer", Status: "success"}}, reviews: approved},
		{name: "full send other failure", fullSend: true, statuses: []forgejoStatus{{Context: "build", Status: "failure"}, {Context: "ts-reviewer", Status: "success"}}, reviews: approved},
		{name: "full send human block", fullSend: true, statuses: green, reviews: append(append([]forgejoReview{}, approved...), forgejoReview{User: forgejoUser{Login: "human"}, State: "REQUEST_CHANGES"})},
		{name: "full send conflict", fullSend: true, conflict: true, statuses: green, reviews: approved},
		{name: "temporary skip file", file: true, statuses: []forgejoStatus{{Context: "build", Status: "success"}, {Context: "ts-reviewer", Status: "error"}}, reviews: []forgejoReview{{User: forgejoUser{Login: forgejoReviewer}, State: "REQUEST_CHANGES"}}, wantMerge: true},
		{name: "skip no other checks", skip: true, statuses: []forgejoStatus{{Context: "ts-reviewer", Status: "success"}}},
		{name: "skip human block", skip: true, statuses: green, reviews: []forgejoReview{{User: forgejoUser{Login: "human"}, State: "REQUEST_CHANGES"}}},
		{name: "non COS", branch: "feature/work", statuses: green, reviews: approved},
		{name: "adopted", branch: "feature/work", adopted: true, statuses: green, reviews: approved, wantMerge: true},
		{name: "hold parity", branch: "cos-hold/work", statuses: green, reviews: approved, wantMerge: true},
		{name: "foreign author adopted", author: "other", adopted: true, fullSend: true, skip: true, statuses: green, reviews: approved},
		{name: "wrong authenticated login", login: "other", statuses: green, reviews: approved},
		{name: "closed", state: "closed", statuses: green, reviews: approved},
		{name: "missing head", emptySHA: true, statuses: green, reviews: approved},
		{name: "latest build successful", statuses: []forgejoStatus{{ID: 3, Context: "build", Status: "success"}, {ID: 1, Context: "build", Status: "failure"}}, reviews: approved, wantMerge: true},
		{name: "latest build failing", statuses: []forgejoStatus{{ID: 3, Context: "build", Status: "failure"}, {ID: 1, Context: "build", Status: "success"}}, reviews: approved},
		{name: "latest bot rejects", statuses: green, reviews: []forgejoReview{{ID: 2, User: forgejoUser{Login: forgejoReviewer}, State: "REQUEST_CHANGES"}, {ID: 1, User: forgejoUser{Login: forgejoReviewer}, State: "APPROVED"}}},
		{name: "latest human approves", statuses: green, reviews: append(append([]forgejoReview{}, approved...), forgejoReview{ID: 3, User: forgejoUser{Login: "human"}, State: "APPROVED"}, forgejoReview{ID: 2, User: forgejoUser{Login: "human"}, State: "REQUEST_CHANGES"}), wantMerge: true},
		{name: "stale bot approval", statuses: green, reviews: []forgejoReview{{User: forgejoUser{Login: forgejoReviewer}, State: "APPROVED", Stale: true}}},
		{name: "dismissed bot approval", statuses: green, reviews: []forgejoReview{{User: forgejoUser{Login: forgejoReviewer}, State: "APPROVED", Dismissed: true}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("COS_FULL_SEND", "0")
			t.Setenv("COS_ADOPTED", "0")
			if test.fullSend {
				t.Setenv("COS_FULL_SEND", "1")
			}
			if test.adopted {
				t.Setenv("COS_ADOPTED", "1")
			}
			pr := forgejoTestPR(1, forgejoLogin, "cos/work")
			if test.author != "" {
				pr.User.Login = test.author
			}
			if test.branch != "" {
				pr.Head.Ref = test.branch
			}
			if test.state != "" {
				pr.State = test.state
			}
			pr.Mergeable = !test.conflict
			if test.emptySHA {
				pr.Head.SHA = ""
			}
			login := forgejoLogin
			if test.login != "" {
				login = test.login
			}
			merges := 0
			f := forgejoTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.URL.Path == "/user":
					forgejoTestJSON(w, forgejoUser{Login: login})
				case r.Method == http.MethodPost && r.URL.Path == "/repos/TerraScale/test/pulls/1/merge":
					merges++
					var body map[string]any
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Error(err)
					}
					if body["head_commit_id"] != pr.Head.SHA || body["Do"] != "squash" || body["delete_branch_after_merge"] != true || body["force_merge"] != true {
						t.Errorf("incorrect merge payload: %#v", body)
					}
					w.WriteHeader(http.StatusNoContent)
				case r.Method != http.MethodGet:
					t.Errorf("unexpected mutation: %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusInternalServerError)
				case r.URL.Path == "/repos/TerraScale/test/pulls/1":
					forgejoTestJSON(w, pr)
				case strings.HasSuffix(r.URL.Path, "/status"):
					forgejoTestJSON(w, map[string]any{"statuses": test.statuses})
				case strings.HasSuffix(r.URL.Path, "/reviews"):
					forgejoTestJSON(w, test.reviews)
				default:
					t.Errorf("unexpected request: %s", r.URL)
					w.WriteHeader(http.StatusNotFound)
				}
			})
			f.Config.SkipReviewer = test.skip
			if test.file {
				path := filepath.Join(f.Config.Home, "Projects/TerraScale/.cos/skip-reviewer")
				if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			result, err := f.Merge(context.Background(), "test", 1)
			if (err == nil) != test.wantMerge || (merges == 1) != test.wantMerge {
				t.Fatalf("result=%v err=%v merges=%d wantMerge=%t", result, err, merges, test.wantMerge)
			}
			if test.wantMerge && result.(map[string]any)["merged"] != true {
				t.Errorf("missing merge result: %v", result)
			}
		})
	}
}

func TestForgejoPRsPaginationAndOwnership(t *testing.T) {
	requests := map[string]int{}
	pr := forgejoTestPR(2, forgejoLogin, "cos/ts-769-work")
	f := forgejoTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Path + " page=" + r.URL.Query().Get("page")
		requests[key]++
		if r.URL.Query().Get("limit") != "50" {
			t.Errorf("missing page size: %s", r.URL)
		}
		page := r.URL.Query().Get("page")
		switch r.URL.Path {
		case "/orgs/TerraScale/repos":
			w.Header().Set("X-Total-Count", "3")
			if page == "1" {
				forgejoTestJSON(w, []any{map[string]any{"name": "test"}, map[string]any{"name": "archive", "archived": true}})
			} else {
				forgejoTestJSON(w, []any{map[string]any{"name": "empty"}})
			}
		case "/repos/TerraScale/empty/pulls":
			forgejoTestJSON(w, []any{})
		case "/repos/TerraScale/test/pulls":
			if r.URL.Query().Get("state") != "open" {
				t.Error("must request open PRs")
			}
			if page == "1" {
				w.Header().Set("Link", `<http://fixture/pulls?page=2>; rel="next"`)
				forgejoTestJSON(w, []forgejoPR{forgejoTestPR(1, "foreign", "cos/work"), forgejoTestPR(3, forgejoLogin, "feature/work")})
			} else {
				forgejoTestJSON(w, []forgejoPR{pr})
			}
		case "/repos/TerraScale/test/commits/" + pr.Head.SHA + "/status":
			statuses := []forgejoStatus{{ID: 1, Context: "build", Status: "failure"}}
			if page == "2" {
				statuses = []forgejoStatus{{ID: 3, Context: "build", Status: "success"}, {ID: 2, Context: "ts-reviewer", Status: "success"}}
			}
			forgejoTestJSON(w, map[string]any{"total_count": 3, "statuses": statuses})
		case "/repos/TerraScale/test/pulls/2/reviews":
			w.Header().Set("X-Total-Count", "2")
			state, id := "REQUEST_CHANGES", int64(1)
			if page == "2" {
				state, id = "APPROVED", 2
			}
			forgejoTestJSON(w, []forgejoReview{{ID: id, User: forgejoUser{Login: forgejoReviewer}, State: state}})
		case "/repos/TerraScale/test/pulls/3/reviews":
			forgejoTestJSON(w, []any{})
		default:
			t.Errorf("foreign or archived data was fetched: %s", r.URL)
			w.WriteHeader(http.StatusNotFound)
		}
	})
	prs, err := f.PRs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(prs) != 2 || prs[0].Number != 2 || !prs[0].Ready || !prs[0].Cos || prs[1].Number != 3 || prs[1].Ready {
		t.Fatalf("incorrect owned PRs or ordering: %+v", prs)
	}
	if !reflect.DeepEqual(prs[0].Issues, []string{"TS-769", "TS-101"}) {
		t.Errorf("issue extraction: %v", prs[0].Issues)
	}
	for _, key := range []string{"/orgs/TerraScale/repos page=2", "/repos/TerraScale/test/pulls page=2", "/repos/TerraScale/test/pulls/2/reviews page=2", "/repos/TerraScale/test/commits/" + pr.Head.SHA + "/status page=2"} {
		if requests[key] == 0 {
			t.Errorf("page not requested: %s", key)
		}
	}
}

func TestForgejoLatestApplicableReviewsAndStatuses(t *testing.T) {
	old := time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC)
	newer := old.Add(time.Hour)
	p := forgejoState("test", forgejoTestPR(1, forgejoLogin, "cos/work"), []forgejoStatus{
		{ID: 10, Context: "build", Status: "success", CreatedAt: old},
		{ID: 2, Context: "build", Status: "failure", UpdatedAt: newer},
	}, []forgejoReview{
		{ID: 10, User: forgejoUser{Login: forgejoReviewer}, State: "REQUEST_CHANGES", SubmittedAt: old},
		{ID: 2, User: forgejoUser{Login: forgejoReviewer}, State: "APPROVED", SubmittedAt: newer},
		{ID: 20, User: forgejoUser{Login: forgejoReviewer}, State: "COMMENT", SubmittedAt: newer.Add(time.Hour)},
		{ID: 21, User: forgejoUser{Login: "human"}, State: "REQUEST_CHANGES", Stale: true},
		{ID: 22, User: forgejoUser{Login: "human"}, State: "REQUEST_CHANGES", Dismissed: true},
	}, false)
	if p.Ready || p.CI != "failure" || !p.BotApproved || p.ChangesRequested || !reflect.DeepEqual(p.Failing, []string{"build"}) {
		t.Fatalf("incorrect current state: %+v", p)
	}
}

func TestForgejoFailureOnLaterStatusPage(t *testing.T) {
	t.Setenv("COS_FULL_SEND", "1")
	pr := forgejoTestPR(1, forgejoLogin, "cos/work")
	mutations := 0
	f := forgejoTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			mutations++
		}
		switch {
		case r.URL.Path == "/user":
			forgejoTestJSON(w, forgejoUser{Login: forgejoLogin})
		case strings.HasSuffix(r.URL.Path, "/status"):
			status := "success"
			if r.URL.Query().Get("page") == "2" {
				status = "failure"
			}
			forgejoTestJSON(w, map[string]any{"total_count": 2, "statuses": []forgejoStatus{{Context: "page-" + r.URL.Query().Get("page"), Status: status}}})
		case strings.HasSuffix(r.URL.Path, "/reviews"):
			forgejoTestJSON(w, []any{})
		default:
			forgejoTestJSON(w, pr)
		}
	})
	if _, err := f.Merge(context.Background(), "test", 1); err == nil || mutations != 0 {
		t.Fatalf("later failing check did not block merge: err=%v mutations=%d", err, mutations)
	}
}

func TestForgejoErrorsDoNotExposeTokens(t *testing.T) {
	for _, code := range []int{http.StatusUnauthorized, http.StatusBadGateway} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			f := forgejoTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(code)
				_, _ = w.Write([]byte("echo " + r.Header.Get("Authorization")))
			})
			_, err := f.PRs(context.Background())
			if err == nil || strings.Contains(err.Error(), forgejoTestToken) {
				t.Fatalf("unsafe HTTP error: %v", err)
			}
		})
	}
}

func TestForgejoSkipReviewerFileRemovalRestoresGate(t *testing.T) {
	f := Forgejo{Config: Config{Home: t.TempDir()}}
	path := filepath.Join(f.Config.Home, "Projects/TerraScale/.cos/skip-reviewer")
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	state := func() PR {
		return forgejoState("test", forgejoTestPR(1, forgejoLogin, "cos/work"), []forgejoStatus{{Context: "build", Status: "success"}, {Context: "ts-reviewer", Status: "failure"}}, nil, f.skipReviewer())
	}
	if !state().Ready {
		t.Fatal("skip-reviewer file did not bypass the bot gate")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if state().Ready {
		t.Fatal("removing skip-reviewer did not restore the bot gate")
	}
}

func TestForgejoMissingReportedStatusesBlocksMerge(t *testing.T) {
	t.Setenv("COS_FULL_SEND", "1")
	pr := forgejoTestPR(1, forgejoLogin, "cos/work")
	mutations := 0
	f := forgejoTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			mutations++
		}
		switch {
		case r.URL.Path == "/user":
			forgejoTestJSON(w, forgejoUser{Login: forgejoLogin})
		case strings.HasSuffix(r.URL.Path, "/status"):
			statuses := []forgejoStatus{{Context: "build", Status: "success"}}
			if r.URL.Query().Get("page") == "2" {
				statuses = []forgejoStatus{}
			}
			forgejoTestJSON(w, map[string]any{"total_count": 2, "statuses": statuses})
		default:
			forgejoTestJSON(w, pr)
		}
	})
	if _, err := f.Merge(context.Background(), "test", 1); err == nil || mutations != 0 {
		t.Fatalf("missing reported checks did not block merge: err=%v mutations=%d", err, mutations)
	}
}
