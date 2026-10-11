package cos

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
)

// panelThread is one row of a chat tab's Threads panel.
type panelThread struct {
	Host, ThreadID, Title, Model, Status, Updated string
	NeedsInput                                    bool
	PR                                            *panelPR
}

// panelPR is the open pull request a thread owns, joined with its CI state.
type panelPR struct {
	URL, Repo string
	Number    int
	State, CI string
	Mergeable bool
	Ready     bool
}

func (p panelPR) label() string {
	if p.Number == 0 {
		return p.URL
	}
	return fmt.Sprintf("%s#%d", p.Repo, p.Number)
}

// ago is the age of the thread's last activity, or a dash when T3 did not say.
func (t panelThread) ago() string { return agoText(parseUpdated(t.Updated)) }

// openPR reports whether the row's PR is still open, which the merge buttons need.
func (t panelThread) openPR() bool { return t.PR != nil && t.PR.State == "open" }

func parseUpdated(s string) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, s)
	return t
}

// tabThreads lists the live threads of one chat tab, newest first. A thread belongs when it is tagged
// for the tab or descends from a tagged thread on the same host. The tab's COS thread is never listed.
func tabThreads(live Live, tags []ChatTag) []panelThread {
	type key struct{ host, id string }
	threads := map[key]FleetThread{}
	for _, h := range live.Fleet {
		for _, t := range h.Threads {
			threads[key{h.Host, t.ThreadID}] = t
		}
	}
	prs := map[string]PR{}
	for _, p := range live.PRs {
		prs[p.URL] = p
	}
	tagged, cos := map[key]bool{}, map[key]bool{}
	for _, t := range tags {
		tagged[key{t.Host, t.ThreadID}] = true
		if t.COS {
			cos[key{t.Host, t.ThreadID}] = true
		}
	}
	belongs := func(k key) bool {
		seen := map[key]bool{}
		for !seen[k] {
			if tagged[k] {
				return true
			}
			seen[k] = true
			t, ok := threads[k]
			if !ok || t.Lineage == nil || t.Lineage.Parent == "" {
				return false
			}
			k = key{k.host, t.Lineage.Parent}
		}
		return false
	}
	out := []panelThread{}
	for k, t := range threads {
		if cos[k] || !belongs(k) {
			continue
		}
		row := panelThread{Host: k.host, ThreadID: k.id, Title: t.Title, Model: t.Model, Status: t.Status, Updated: t.Updated, NeedsInput: t.NeedsInput, PR: threadPR(t, prs)}
		out = append(out, row)
	}
	slices.SortFunc(out, func(a, b panelThread) int {
		if c := parseUpdated(b.Updated).Compare(parseUpdated(a.Updated)); c != 0 {
			return c
		}
		return strings.Compare(a.ThreadID, b.ThreadID)
	})
	return out
}

// threadPR picks the thread's open PR (or its first) and joins it with the live PR for CI and merge state.
func threadPR(t FleetThread, prs map[string]PR) *panelPR {
	var pick *ThreadPR
	for i := range t.PRs {
		if t.PRs[i].State == "open" {
			pick = &t.PRs[i]
			break
		}
	}
	if pick == nil && len(t.PRs) > 0 {
		pick = &t.PRs[0]
	}
	if pick == nil {
		return nil
	}
	out := &panelPR{URL: pick.URL, State: pick.State}
	out.Repo, out.Number = parsePRURL(pick.URL)
	if p, ok := prs[pick.URL]; ok {
		out.CI, out.Mergeable, out.Ready = p.CI, p.Mergeable, p.Ready
	}
	return out
}

// parsePRURL reads owner/repo and the number from https://host/owner/repo/pulls/<n>.
func parsePRURL(url string) (string, int) {
	parts := strings.Split(url, "/")
	n := len(parts)
	if n < 4 || parts[n-2] != "pulls" {
		return "", 0
	}
	num, err := strconv.Atoi(parts[n-1])
	if err != nil {
		return "", 0
	}
	return parts[n-4] + "/" + parts[n-3], num
}
