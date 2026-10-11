package cos

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

const selfRepo = "ts-cos"

// selfProtected are paths a self-change may never touch: login, the deploy and
// rollback path, the CI/e2e gate and this loop's own guardrails. A request that
// needs them goes to the user as a normal reviewed PR instead.
var selfProtected = []string{
	".forgejo/", "deploy/", "checks/checks.go", "go.mod", "go.sum",
	"internal/cos/dashboard_auth", "internal/cos/login", "internal/cos/vault", "internal/cos/leader", "internal/cos/bus.go",
	"internal/cos/deploy", "internal/cos/rollout", "internal/cos/e2e",
	"internal/cos/selfchange", "internal/cos/selfcheck",
}

// Only security and data-loss findings from ts-reviewer block a self-change merge.
var selfBlockingFinding = regexp.MustCompile(`(?i)\b(security|secrets?|credentials?|injection|xss|csrf|unauthori[sz]ed|auth(entication)? bypass|privilege|data[ -]loss|lose (user )?data|data corruption|corrupts?)\b`)

type selfPR struct {
	Number    int    `json:"number"`
	URL       string `json:"html_url"`
	State     string `json:"state"`
	Merged    bool   `json:"merged"`
	MergeSHA  string `json:"merge_commit_sha"`
	Mergeable bool   `json:"mergeable"`
	User      forgejoUser
	Head      struct {
		Ref string `json:"ref"`
		SHA string `json:"sha"`
	} `json:"head"`
}

func selfPath(rest string) string { return "/repos/" + forgejoOrg + "/" + selfRepo + rest }

func selfEscape(p string) string { return (&url.URL{Path: p}).EscapedPath() }

func selfCheckFile(ticket string) string { return "checks/" + ticket + ".json" }

func selfCheckJSON(c selfCheck) string {
	data, _ := json.MarshalIndent(c, "", "  ")
	return string(data) + "\n"
}

// selfFile returns a file's content and blob SHA at ref; found is false on 404.
func (f Forgejo) selfFile(ctx context.Context, path, ref string) (content, sha string, found bool, err error) {
	var file struct{ Content, SHA string }
	if _, err := f.request(ctx, http.MethodGet, selfPath("/contents/"+selfEscape(path)+"?ref="+url.QueryEscape(ref)), nil, &file); err != nil {
		if strings.Contains(err.Error(), "HTTP 404") {
			return "", "", false, nil
		}
		return "", "", false, err
	}
	data, err := base64.StdEncoding.DecodeString(file.Content)
	return string(data), file.SHA, true, err
}

// selfStartBranch creates branch from main with the acceptance check as its first
// commit. It is idempotent so a leader failover can repeat it.
func (f Forgejo) selfStartBranch(ctx context.Context, branch, ticket string, c selfCheck) error {
	_, err := f.request(ctx, http.MethodPost, selfPath("/branches"), map[string]string{"new_branch_name": branch, "old_ref_name": "main"}, nil)
	if err != nil && !strings.Contains(err.Error(), "HTTP 409") {
		return err
	}
	path := selfCheckFile(ticket)
	if _, _, found, err := f.selfFile(ctx, path, branch); err != nil || found {
		return err
	}
	_, err = f.request(ctx, http.MethodPost, selfPath("/contents/"+selfEscape(path)), map[string]string{
		"branch": branch, "message": ticket + ": Acceptance check", "content": base64.StdEncoding.EncodeToString([]byte(selfCheckJSON(c))),
	}, nil)
	return err
}

// selfFindPR returns the newest PR from branch into main, or nil.
func (f Forgejo) selfFindPR(ctx context.Context, branch string) (*selfPR, error) {
	var prs []selfPR
	if _, err := f.request(ctx, http.MethodGet, selfPath("/pulls?state=all&sort=newest&limit=50"), nil, &prs); err != nil {
		return nil, err
	}
	for _, p := range prs {
		if p.Head.Ref == branch && p.User.Login == forgejoLogin {
			return &p, nil
		}
	}
	return nil, nil
}

func (f Forgejo) selfGetPR(ctx context.Context, number int) (selfPR, error) {
	var p selfPR
	_, err := f.request(ctx, http.MethodGet, selfPath("/pulls/"+strconv.Itoa(number)), nil, &p)
	return p, err
}

// selfGate decides whether a self-change PR may merge: wait (CI or review still
// running, conflict), block (guardrail or blocking finding: needs the user) or ready.
func (f Forgejo) selfGate(ctx context.Context, r selfRecord, p selfPR) (wait, block string, err error) {
	if p.User.Login != forgejoLogin || p.State != "open" || p.Head.SHA == "" {
		return "", "PR is not an open mariogk PR", nil
	}
	var files []struct{ Filename, Status string }
	if _, err := f.request(ctx, http.MethodGet, selfPath("/pulls/"+strconv.Itoa(p.Number)+"/files?limit=300"), nil, &files); err != nil {
		return "", "", err
	}
	names := make([]string, len(files))
	for i, file := range files {
		names[i] = file.Filename
	}
	if bad := selfGuard(names, r.Ticket); bad != "" {
		return "", bad, nil
	}
	if r.Reverts == "" {
		got, _, _, err := f.selfFile(ctx, selfCheckFile(r.Ticket), p.Head.SHA)
		if err != nil {
			return "", "", err
		}
		if got != selfCheckJSON(r.Check) {
			return "", "the PR changed its acceptance check " + selfCheckFile(r.Ticket), nil
		}
	}
	statuses, err := f.statuses(ctx, selfRepo, p.Head.SHA)
	if err != nil {
		return "", "", err
	}
	reviewer := ""
	for _, s := range statuses {
		switch {
		case s.Context == "ts-reviewer":
			reviewer = s.Status
		case s.Status == "pending":
			return "CI running (" + s.Context + ")", "", nil
		case s.Status != "success":
			return "CI " + s.Status + " (" + s.Context + "); the worker is fixing it", "", nil
		}
	}
	switch {
	case len(statuses) == 0:
		return "waiting for CI to start", "", nil
	case reviewer == "" || reviewer == "pending":
		return "ts-reviewer running", "", nil
	case reviewer == "failure":
		finding, found, err := f.selfBlockingReview(ctx, p)
		if err != nil {
			return "", "", err
		}
		if !found {
			return "waiting for ts-reviewer findings", "", nil
		}
		if finding != "" {
			return "ts-reviewer security/data-loss finding; the worker is fixing it: " + selfClip(finding), "", nil
		}
	}
	if !p.Mergeable {
		return "conflicts with main; the worker is rebasing", "", nil
	}
	return "", "", nil
}

// selfGuard names the first protected path a change touches; checks/ allows only the change's own check.
func selfGuard(files []string, ticket string) string {
	for _, name := range files {
		for _, p := range selfProtected {
			if strings.HasPrefix(name, p) {
				return "touches protected " + name + " (login, deploy, e2e gate or guardrails)"
			}
		}
		if strings.HasPrefix(name, "checks/") && name != selfCheckFile(ticket) {
			return "changes another acceptance check " + name
		}
	}
	return ""
}

// selfBlockingReview reads the ts-reviewer comment for the PR head; finding is the
// first security/data-loss finding, found is false until the comment exists.
func (f Forgejo) selfBlockingReview(ctx context.Context, p selfPR) (finding string, found bool, err error) {
	comments, err := forgejoList[struct{ Body string }](ctx, f, selfPath("/issues/"+strconv.Itoa(p.Number)+"/comments"))
	if err != nil {
		return "", false, err
	}
	for _, c := range comments {
		if !strings.Contains(c.Body, "<!-- ts-reviewer:"+p.Head.SHA) {
			continue
		}
		_, findings, _ := strings.Cut(c.Body, "### Findings")
		for _, block := range strings.Split(findings, "\n#### ")[1:] {
			if selfBlockingFinding.MatchString(block) {
				title, _, _ := strings.Cut(block, "\n")
				return title, true, nil
			}
		}
		return "", true, nil
	}
	return "", false, nil
}

// selfMerge squash-merges at the checked head with the Update-Note trailer TS-818's
// browser toast reads, and returns the merge commit.
func (f Forgejo) selfMerge(ctx context.Context, r selfRecord, p selfPR) (string, error) {
	_, err := f.request(ctx, http.MethodPost, selfPath("/pulls/"+strconv.Itoa(p.Number)+"/merge"), map[string]any{
		"Do": "squash", "head_commit_id": p.Head.SHA, "delete_branch_after_merge": true,
		"MergeTitleField":   fmt.Sprintf("%s: %s (#%d)", r.Ticket, r.title(), p.Number),
		"MergeMessageField": "Update-Note: " + r.note() + "\n",
	}, nil)
	if err != nil {
		return "", err
	}
	merged, err := f.selfGetPR(ctx, p.Number)
	if err != nil {
		return "", err
	}
	if !merged.Merged || merged.MergeSHA == "" {
		return "", errors.New("Forgejo did not report the merge commit")
	}
	return merged.MergeSHA, nil
}

// selfRevertPR opens a PR that restores every file of a squash commit to its parent
// version. It refuses when main changed one of those files since, which needs a worker.
func (f Forgejo) selfRevertPR(ctx context.Context, sha, branch, title, body string) (selfPR, error) {
	var commit struct {
		Parents []struct{ SHA string } `json:"parents"`
		Files   []struct {
			Filename string `json:"filename"`
			Status   string `json:"status"`
		} `json:"files"`
	}
	if _, err := f.request(ctx, http.MethodGet, selfPath("/git/commits/"+url.PathEscape(sha)+"?stat=false&verification=false&files=true"), nil, &commit); err != nil {
		return selfPR{}, err
	}
	if len(commit.Parents) != 1 || len(commit.Files) == 0 {
		return selfPR{}, errors.New("only squash commits with files can be reverted")
	}
	type change struct {
		Operation string `json:"operation"`
		Path      string `json:"path"`
		Content   string `json:"content,omitempty"`
		SHA       string `json:"sha,omitempty"`
	}
	var changes []change
	for _, file := range commit.Files {
		_, atMerge, _, err := f.selfFile(ctx, file.Filename, sha)
		if err != nil {
			return selfPR{}, err
		}
		_, onMain, _, err := f.selfFile(ctx, file.Filename, "main")
		if err != nil {
			return selfPR{}, err
		}
		if atMerge != onMain {
			return selfPR{}, fmt.Errorf("%s changed on main after %s", file.Filename, sha[:min(7, len(sha))])
		}
		before, _, existed, err := f.selfFile(ctx, file.Filename, commit.Parents[0].SHA)
		if err != nil {
			return selfPR{}, err
		}
		c := change{Path: file.Filename, SHA: onMain, Content: base64.StdEncoding.EncodeToString([]byte(before))}
		switch {
		case !existed:
			c.Operation, c.Content = "delete", ""
		case onMain == "":
			c.Operation, c.SHA = "create", ""
		default:
			c.Operation = "update"
		}
		changes = append(changes, c)
	}
	if _, err := f.request(ctx, http.MethodPost, selfPath("/contents"), map[string]any{
		"branch": "main", "new_branch": branch, "message": title, "files": changes,
	}, nil); err != nil && !strings.Contains(err.Error(), "HTTP 422") {
		return selfPR{}, err
	}
	var p selfPR
	_, err := f.request(ctx, http.MethodPost, selfPath("/pulls"), map[string]string{"base": "main", "head": branch, "title": title, "body": body}, &p)
	if err != nil && strings.Contains(err.Error(), "HTTP 409") {
		found, ferr := f.selfFindPR(ctx, branch)
		if ferr != nil || found == nil {
			return selfPR{}, err
		}
		return *found, nil
	}
	return p, err
}
