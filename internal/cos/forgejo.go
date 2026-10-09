package cos

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const forgejoOrg = "TerraScale"
const forgejoLogin = "mariogk"
const forgejoReviewer = "terrascale-bot"
const forgejoPageSize = 50

type Forgejo struct{ Config Config }

type PR struct {
	Repo             string   `json:"repo"`
	Number           int      `json:"number"`
	URL              string   `json:"url"`
	Title            string   `json:"title"`
	Branch           string   `json:"branch"`
	Base             string   `json:"base"`
	SHA              string   `json:"sha"`
	Issues           []string `json:"issues"`
	Cos              bool     `json:"cos"`
	Hold             bool     `json:"hold"`
	Mergeable        bool     `json:"mergeable"`
	CI               string   `json:"ci"`
	Failing          []string `json:"failing"`
	Pending          []string `json:"pending"`
	BotApproved      bool     `json:"botApproved"`
	ChangesRequested bool     `json:"changesRequested"`
	MeApproved       bool     `json:"meApproved"`
	SkipReviewer     bool     `json:"skipReviewer"`
	Ready            bool     `json:"ready"`
}

type forgejoUser struct {
	Login string `json:"login"`
}

type forgejoPR struct {
	Number    int         `json:"number"`
	URL       string      `json:"html_url"`
	Title     string      `json:"title"`
	Body      string      `json:"body"`
	State     string      `json:"state"`
	Mergeable bool        `json:"mergeable"`
	User      forgejoUser `json:"user"`
	Head      struct {
		Ref string `json:"ref"`
		SHA string `json:"sha"`
	} `json:"head"`
	Base struct {
		Ref string `json:"ref"`
	} `json:"base"`
}

type forgejoStatus struct {
	ID        int64     `json:"id"`
	Context   string    `json:"context"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type forgejoReview struct {
	ID          int64       `json:"id"`
	User        forgejoUser `json:"user"`
	State       string      `json:"state"`
	Stale       bool        `json:"stale"`
	Dismissed   bool        `json:"dismissed"`
	SubmittedAt time.Time   `json:"submitted_at"`
	UpdatedAt   time.Time   `json:"updated_at"`
}

func (f Forgejo) request(ctx context.Context, method, path string, input, output any) (http.Header, error) {
	keys, err := os.ReadFile(filepath.Join(f.Config.Home, ".local/share/forgejo-cli/keys.json"))
	if err != nil {
		return nil, fmt.Errorf("read Forgejo credentials: %w", err)
	}
	var credentials struct {
		Hosts map[string]struct {
			Token string `json:"token"`
		} `json:"hosts"`
	}
	if json.Unmarshal(keys, &credentials) != nil {
		return nil, fmt.Errorf("invalid Forgejo credentials JSON")
	}
	token := strings.TrimSpace(credentials.Hosts["git.terrascale.tech"].Token)
	if token == "" {
		return nil, fmt.Errorf("missing Forgejo token for git.terrascale.tech")
	}
	var data []byte
	if input != nil {
		data, err = json.Marshal(input)
		if err != nil {
			return nil, fmt.Errorf("invalid Forgejo request")
		}
	}
	base := f.Config.ForgejoURL
	if base == "" {
		base = "https://git.terrascale.tech/api/v1"
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(base, "/")+path, bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("invalid Forgejo URL")
	}
	req.Header.Set("Authorization", "token "+token)
	req.Header.Set("Content-Type", "application/json")
	client := http.Client{Timeout: 60 * time.Second}
	if f.Config.HTTP != nil {
		client = *f.Config.HTTP
	}
	// Keep credentials and merge requests on the configured endpoint.
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	res, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("Forgejo %s %s: connection failed", method, path)
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return nil, fmt.Errorf("Forgejo %s %s: HTTP %d", method, path, res.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(res.Body, (16<<20)+1))
	if err != nil || len(body) > 16<<20 {
		return nil, fmt.Errorf("Forgejo %s: could not read response", path)
	}
	if output != nil && json.Unmarshal(body, output) != nil {
		return nil, fmt.Errorf("Forgejo %s: invalid JSON response", path)
	}
	return res.Header, nil
}

func forgejoPagePath(path string, page int) string {
	separator := "?"
	if strings.Contains(path, "?") {
		separator = "&"
	}
	return path + separator + "limit=" + strconv.Itoa(forgejoPageSize) + "&page=" + strconv.Itoa(page)
}

func forgejoMore(header http.Header, count, loaded, total int) (bool, error) {
	if n, err := strconv.Atoi(header.Get("X-Total-Count")); err == nil {
		total = n
	}
	if count == 0 {
		if loaded < total {
			return false, fmt.Errorf("Forgejo pagination ended before the reported total")
		}
		return false, nil
	}
	if total > 0 {
		return loaded < total, nil
	}
	if link := header.Get("Link"); link != "" {
		return strings.Contains(link, `rel="next"`) || strings.Contains(link, "rel=next"), nil
	}
	return count >= forgejoPageSize, nil
}

func forgejoList[T any](ctx context.Context, f Forgejo, path string) ([]T, error) {
	out := []T{}
	for page := 1; ; page++ {
		var items []T
		header, err := f.request(ctx, http.MethodGet, forgejoPagePath(path, page), nil, &items)
		if err != nil {
			return nil, err
		}
		out = append(out, items...)
		more, err := forgejoMore(header, len(items), len(out), 0)
		if err != nil {
			return nil, err
		}
		if !more {
			return out, nil
		}
	}
}

func (f Forgejo) statuses(ctx context.Context, repo, sha string) ([]forgejoStatus, error) {
	out := []forgejoStatus{}
	path := "/repos/" + forgejoOrg + "/" + url.PathEscape(repo) + "/commits/" + url.PathEscape(sha) + "/status"
	for page := 1; ; page++ {
		var result struct {
			Statuses []forgejoStatus `json:"statuses"`
			Total    int             `json:"total_count"`
		}
		header, err := f.request(ctx, http.MethodGet, forgejoPagePath(path, page), nil, &result)
		if err != nil {
			return nil, err
		}
		out = append(out, result.Statuses...)
		more, err := forgejoMore(header, len(result.Statuses), len(out), result.Total)
		if err != nil {
			return nil, err
		}
		if !more {
			return out, nil
		}
	}
}

func (f Forgejo) skipReviewer() bool {
	_, err := os.Stat(filepath.Join(f.Config.Home, "Projects/TerraScale/.cos/skip-reviewer"))
	return f.Config.SkipReviewer || err == nil
}

func forgejoNewer(id int64, created, updated time.Time, previousID int64, previousCreated, previousUpdated time.Time) bool {
	if !updated.IsZero() {
		created = updated
	}
	if !previousUpdated.IsZero() {
		previousCreated = previousUpdated
	}
	return created.After(previousCreated) || created.Equal(previousCreated) && id > previousID
}

var forgejoIssuePattern = regexp.MustCompile(`(?i)\b[a-z]{1,10}-\d+\b`)

func forgejoState(repo string, raw forgejoPR, statuses []forgejoStatus, reviews []forgejoReview, skip bool) PR {
	p := PR{Repo: repo, Number: raw.Number, URL: raw.URL, Title: raw.Title, Branch: raw.Head.Ref,
		Base: raw.Base.Ref, SHA: raw.Head.SHA, Mergeable: raw.Mergeable, SkipReviewer: skip,
		Issues: []string{}, Failing: []string{}, Pending: []string{}, CI: "pending"}
	p.Hold = strings.HasPrefix(p.Branch, "cos-hold/")
	p.Cos = p.Hold || strings.HasPrefix(p.Branch, "cos/")
	seen := map[string]bool{}
	for _, issue := range forgejoIssuePattern.FindAllString(raw.Title+" "+raw.Head.Ref+" "+raw.Body, -1) {
		issue = strings.ToUpper(issue)
		if !seen[issue] {
			p.Issues = append(p.Issues, issue)
			seen[issue] = true
		}
	}
	latestStatus := map[string]forgejoStatus{}
	for _, status := range statuses {
		previous, exists := latestStatus[status.Context]
		if !exists || forgejoNewer(status.ID, status.CreatedAt, status.UpdatedAt, previous.ID, previous.CreatedAt, previous.UpdatedAt) {
			latestStatus[status.Context] = status
		}
	}
	checks := 0
	for name, status := range latestStatus {
		if skip && name == "ts-reviewer" {
			continue
		}
		checks++
		switch status.Status {
		case "failure", "error":
			p.Failing = append(p.Failing, name)
		case "success":
			if name == "" {
				p.Pending = append(p.Pending, name)
			}
		default:
			p.Pending = append(p.Pending, name)
		}
	}
	sort.Strings(p.Failing)
	sort.Strings(p.Pending)
	if len(p.Failing) > 0 {
		p.CI = "failure"
	} else if checks > 0 && len(p.Pending) == 0 {
		p.CI = "success"
	}
	latestReview := map[string]forgejoReview{}
	for _, review := range reviews {
		if review.Stale || review.Dismissed || review.User.Login == "" || (review.State != "APPROVED" && review.State != "REQUEST_CHANGES") {
			continue
		}
		previous, exists := latestReview[review.User.Login]
		if !exists || forgejoNewer(review.ID, review.SubmittedAt, review.UpdatedAt, previous.ID, previous.SubmittedAt, previous.UpdatedAt) {
			latestReview[review.User.Login] = review
		}
	}
	for login, review := range latestReview {
		if review.State == "APPROVED" {
			p.BotApproved = p.BotApproved || login == forgejoReviewer
			p.MeApproved = p.MeApproved || login == forgejoLogin
		} else if !(skip && login == forgejoReviewer) {
			p.ChangesRequested = true
		}
	}
	p.Ready = p.Mergeable && p.CI == "success" && (p.BotApproved || skip) && !p.ChangesRequested
	return p
}

func (f Forgejo) PRs(ctx context.Context) ([]PR, error) {
	repos, err := forgejoList[struct {
		Name     string `json:"name"`
		Archived bool   `json:"archived"`
	}](ctx, f, "/orgs/"+forgejoOrg+"/repos")
	if err != nil {
		return nil, err
	}
	out := []PR{}
	skip := f.skipReviewer()
	for _, repo := range repos {
		if repo.Archived {
			continue
		}
		path := "/repos/" + forgejoOrg + "/" + url.PathEscape(repo.Name) + "/pulls"
		prs, err := forgejoList[forgejoPR](ctx, f, path+"?state=open")
		if err != nil {
			return nil, err
		}
		for _, pr := range prs {
			if pr.User.Login != forgejoLogin {
				continue
			}
			statuses, err := f.statuses(ctx, repo.Name, pr.Head.SHA)
			if err != nil {
				return nil, err
			}
			reviews, err := forgejoList[forgejoReview](ctx, f, path+"/"+strconv.Itoa(pr.Number)+"/reviews")
			if err != nil {
				return nil, err
			}
			out = append(out, forgejoState(repo.Name, pr, statuses, reviews, skip))
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Ready != out[j].Ready {
			return out[i].Ready
		}
		return out[i].Cos && !out[j].Cos
	})
	return out, nil
}

func (f Forgejo) Merge(ctx context.Context, repo string, number int) (any, error) {
	if repo == "" || repo == "." || repo == ".." || strings.ContainsAny(repo, "/\\?#") || number <= 0 {
		return nil, fmt.Errorf("invalid repository or PR number")
	}
	var me forgejoUser
	if _, err := f.request(ctx, http.MethodGet, "/user", nil, &me); err != nil {
		return nil, err
	}
	if me.Login != forgejoLogin {
		return nil, fmt.Errorf("refusing merge: authenticated Forgejo login must be mariogk")
	}
	path := "/repos/" + forgejoOrg + "/" + url.PathEscape(repo) + "/pulls/" + strconv.Itoa(number)
	var pr forgejoPR
	if _, err := f.request(ctx, http.MethodGet, path, nil, &pr); err != nil {
		return nil, err
	}
	if pr.User.Login != forgejoLogin {
		return nil, fmt.Errorf("refusing merge: PR must be authored by mariogk")
	}
	if pr.State != "open" || pr.Number != number || pr.Head.SHA == "" {
		return nil, fmt.Errorf("refusing merge: PR must be open with a valid head commit")
	}
	if !strings.HasPrefix(pr.Head.Ref, "cos/") && !strings.HasPrefix(pr.Head.Ref, "cos-hold/") && os.Getenv("COS_ADOPTED") != "1" {
		return nil, fmt.Errorf("refusing merge: non-COS branches require COS_ADOPTED=1")
	}
	statuses, err := f.statuses(ctx, repo, pr.Head.SHA)
	if err != nil {
		return nil, err
	}
	reviews, err := forgejoList[forgejoReview](ctx, f, path+"/reviews")
	if err != nil {
		return nil, err
	}
	normal := forgejoState(repo, pr, statuses, reviews, f.skipReviewer())
	fullSend := os.Getenv("COS_FULL_SEND") == "1"
	waived := forgejoState(repo, pr, statuses, reviews, true)
	ready := normal.Ready
	if fullSend {
		ready = waived.Ready
	}
	if !ready {
		return nil, fmt.Errorf("not ready: CI=%s mergeable=%t botApproved=%t changesRequested=%t", normal.CI, normal.Mergeable, normal.BotApproved, normal.ChangesRequested)
	}
	// Forgejo forbids self-approval; strict ownership means no approval POST is needed.
	_, err = f.request(ctx, http.MethodPost, path+"/merge", map[string]any{
		"Do": "squash", "delete_branch_after_merge": true, "force_merge": false, "head_commit_id": pr.Head.SHA,
	}, nil)
	if err != nil {
		return nil, err
	}
	return map[string]any{"merged": true, "url": pr.URL, "fullSend": fullSend && !normal.Ready}, nil
}
