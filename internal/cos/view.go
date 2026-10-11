package cos

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"
)

type Dashboard struct {
	Story                   Story          `json:"story"`
	Live                    Live           `json:"live"`
	Tasks                   []Task         `json:"tasks"`
	Autopilot               AutopilotState `json:"autopilot"`
	Inbox                   []InboxItem    `json:"inbox"`
	History                 []InboxItem    `json:"history"`
	Running, Ready, Failing int
}

var ticketPattern = regexp.MustCompile(`(?i)\b[a-z]{1,10}-\d+\b`)
var tsKeyPattern = regexp.MustCompile(`(?i)\bTS-\d+\b`)

func ticketIn(text, ticket string) bool {
	for _, key := range ticketPattern.FindAllString(text, -1) {
		if strings.EqualFold(key, ticket) {
			return true
		}
	}
	return false
}

// liveTickets maps each TS-<n> key of an open thread (title or branch) or open PR to its outcome title: ts-cos's own work items.
func liveTickets(live Live) map[string]string {
	out := map[string]string{}
	for _, h := range live.Fleet {
		for _, thread := range h.Threads {
			for _, key := range tsKeyPattern.FindAllString(thread.Title+" "+thread.Branch, -1) {
				if _, ok := out[strings.ToUpper(key)]; !ok {
					out[strings.ToUpper(key)] = thread.Title
				}
			}
		}
	}
	for _, p := range live.PRs {
		for _, issue := range p.Issues {
			key := strings.ToUpper(issue)
			if _, ok := out[key]; !ok && strings.HasPrefix(key, "TS-") {
				out[key] = p.Title
			}
		}
	}
	return out
}

func View(ctx context.Context, store *Store) (Dashboard, error) {
	story, err := store.Story(ctx)
	if err != nil {
		return Dashboard{}, err
	}
	live, err := store.Live(ctx)
	if err != nil {
		return Dashboard{}, err
	}
	patches, err := store.TaskPatches(ctx)
	if err != nil {
		return Dashboard{}, err
	}
	autopilot, err := store.Autopilot(ctx)
	if err != nil {
		return Dashboard{}, err
	}
	v := Dashboard{Story: story, Live: live, Tasks: []Task{}, Autopilot: autopilot}
	if v.Inbox, v.History, err = store.Inbox(ctx); err != nil {
		return Dashboard{}, err
	}
	v.History = v.History[:min(len(v.History), inboxHistoryMax)]
	for _, h := range live.Fleet {
		if !h.Stale {
			v.Running += h.Active
		}
	}
	for _, p := range live.PRs {
		if p.Ready {
			v.Ready++
		}
		if p.CI == "failure" {
			v.Failing++
		}
	}
	for ticket, outcome := range liveTickets(live) {
		priority := "P2"
		if IsP0(ticket, outcome) {
			priority = "P0"
		}
		if fullSendTitle(outcome) {
			priority = "FULL SEND"
		}
		t := Task{Ticket: ticket, Priority: priority, Outcome: outcome, Status: "open", Next: "Choose the next step"}
		for _, h := range live.Fleet {
			for _, thread := range h.Threads {
				if ticketIn(thread.Title+" "+thread.Branch, t.Ticket) {
					if t.Host != "" {
						if h.Stale && !t.Stale {
							continue
						}
						if h.Stale == t.Stale && !active(thread.Status) {
							continue
						}
					}
					t.Host = h.Host
					t.Stale = h.Stale
					t.Repo = thread.Project
					t.Model = thread.Model
					t.Status = thread.Status
					t.Priority = priority
					if fullSendTitle(thread.Title) {
						t.Priority = "FULL SEND"
					}
					if thread.NeedsInput {
						t.Next = "Answer the pending request"
					} else if thread.LastError != "" {
						t.Next = "Resolve: " + thread.LastError
					} else {
						t.Next = "Continue the worker"
					}
				}
			}
		}
		for _, p := range live.PRs {
			if slices.Contains(p.Issues, t.Ticket) {
				t.PR = p.URL
				if t.Repo == "" {
					t.Repo = p.Repo
				}
				if p.Ready {
					t.Status = "ready"
					t.Next = "Merge the ready PR"
				} else if p.CI == "failure" {
					t.Status = "CI failing"
					t.Next = "Fix: " + strings.Join(p.Failing, ", ")
				} else if !p.Mergeable {
					t.Status = "conflict"
					t.Next = "Rebase on " + p.Base
				} else if p.ChangesRequested {
					t.Status = "changes requested"
					t.Next = "Address the reviewer findings"
				} else {
					t.Status = "in review"
					t.Next = "Wait for CI and review"
				}
				break
			}
		}
		for key, value := range patches[t.Ticket] {
			s, _ := value.(string)
			switch key {
			case "why":
				t.Why = s
			case "modelWhy":
				t.ModelWhy = s
			case "next":
				t.Next = s
			}
		}
		v.Tasks = append(v.Tasks, t)
	}
	priority := CurrentBehavior().Models.Priority
	slices.SortStableFunc(v.Tasks, func(a, b Task) int { return compareTasks(a, b, priority) })
	return v, nil
}

func Refresh(ctx context.Context, cfg Config, bus *Bus, old Live) Live {
	ctx, cancel := context.WithTimeout(ctx, 55*time.Second)
	defer cancel()
	var fleet []HostFleet
	var prs []PR
	var asks []Ask
	var fleetErr, prsErr, asksErr error
	var wg sync.WaitGroup
	wg.Go(func() { fleet, fleetErr = Fleet(ctx, bus) })
	wg.Go(func() { prs, prsErr = (Forgejo{Config: cfg}).PRs(ctx) })
	wg.Go(func() { asks, asksErr = AllAsks(ctx, bus) })
	wg.Wait()
	old.Errors = map[string]string{}
	old.At = time.Now().UTC().Format(time.RFC3339)
	if fleetErr != nil {
		old.Errors["fleet"] = fleetErr.Error()
		for i := range old.Fleet {
			old.Fleet[i].Stale = true
		}
	} else {
		old.Fleet = mergeFleet(old.Fleet, fleet)
	}
	if prsErr != nil {
		old.Errors["prs"] = prsErr.Error()
	} else {
		old.PRs = prs
	}
	if asksErr != nil {
		old.Errors["asks"] = asksErr.Error()
		for i := range old.Asks {
			old.Asks[i].Stale = true
		}
	} else {
		old.Asks = mergeAsks(old.Asks, asks)
	}
	return old
}

func Brief(v Dashboard) string {
	lines := []string{}
	add := func(format string, args ...any) {
		s := fmt.Sprintf(format, args...)
		s = strings.Join(strings.Fields(s), " ")
		r := []rune(s)
		if len(r) > 220 {
			s = string(r[:217]) + "..."
		}
		lines = append(lines, s)
	}
	add("COS %s | %d running | %d open PRs | %d ready | %d CI failing | %d open tasks", v.Live.At, v.Running, len(v.Live.PRs), v.Ready, v.Failing, len(v.Tasks))
	if v.Story.Direction != "" {
		add("DIRECTION %s", v.Story.Direction)
	}
	for _, k := range sortedKeysAny(v.Live.Errors) {
		add("ERROR %s: %s", k, v.Live.Errors[k])
	}
	for _, h := range v.Live.Fleet {
		if h.Error != "" {
			add("HOST %s ERROR %s", h.Host, h.Error)
		} else {
			add("HOST %s: %d active / %d open", h.Host, h.Active, len(h.Threads))
		}
	}
	for _, item := range v.Inbox {
		add("INBOX %s %s %s: %s | %s", item.ID, item.Kind, item.Ticket, item.Title, strings.Join(item.Options, " | "))
	}
	for _, a := range v.Live.Asks {
		if a.Kind == "marker" && a.Error == "" && !a.Stale {
			continue // filed as an Inbox item
		}
		if a.Error != "" {
			source := a.Host
			if a.ThreadID != "" {
				source += " " + a.ThreadID
			}
			add("ERROR asks %s: %s", source, a.Error)
			continue
		}
		question := a.Question
		if a.Kind == "request" {
			question = "pending " + a.RequestKind
		}
		label := "NEEDS-USER"
		if a.Stale {
			label = "STALE NEEDS-USER"
		}
		add("%s %s %s: %s | %s", label, a.Host, a.Title, question, strings.Join(a.Options, " | "))
	}
	for _, n := range v.Story.Notes {
		if n.Kind == "need" {
			add("NEEDS-USER %s", n.Text)
		}
	}
	for _, t := range v.Tasks {
		if t.Priority == "FULL SEND" || t.Status == "conflict" || t.Status == "CI failing" {
			add("TASK %s %s | %s | %s | %s", t.Priority, t.Ticket, t.Status, t.Host, t.Next)
		}
	}
	for _, p := range v.Live.PRs {
		review := "waiting"
		if p.BotApproved {
			review = "approved"
		}
		if p.ChangesRequested {
			review = "changes"
		}
		add("PR %s#%d ci=%s review=%s mergeable=%t ready=%t %s", p.Repo, p.Number, p.CI, review, p.Mergeable, p.Ready, p.URL)
	}
	for _, h := range v.Live.Fleet {
		for _, t := range h.Threads {
			if t.LastError != "" || t.Status == "failed" || t.Status == "interrupted" {
				add("THREAD %s %s | %s | %s", h.Host, t.Title, t.Status, t.LastError)
			}
		}
	}
	if len(lines) > 40 {
		omitted := len(lines) - 39
		lines = append(lines[:39], fmt.Sprintf("%d more items; use cos status / cos asks / cos fleet / cos prs", omitted))
	}
	return strings.Join(lines, "\n") + "\n"
}

func fullSendTitle(title string) bool {
	return strings.HasPrefix(strings.ToUpper(strings.TrimSpace(title)), "FULL SEND")
}

func mergeFleet(old, next []HostFleet) []HostFleet {
	previous := map[string]HostFleet{}
	for _, h := range old {
		previous[h.Host] = h
	}
	for i, h := range next {
		if h.Error != "" {
			if cached, ok := previous[h.Host]; ok {
				next[i].Threads = cached.Threads
				next[i].Active = cached.Active
				next[i].Stale = true
			}
		}
	}
	return next
}
func mergeAsks(old, next []Ask) []Ask {
	type source struct{ host, thread string }
	failed := map[source]bool{}
	for _, a := range next {
		if a.Error != "" {
			failed[source{a.Host, a.ThreadID}] = true
		}
	}
	for _, a := range old {
		if a.Error == "" && (failed[source{a.Host, ""}] || failed[source{a.Host, a.ThreadID}]) {
			a.Stale = true
			next = append(next, a)
		}
	}
	return next
}
func sortedKeysAny(m map[string]string) []string {
	keys := []string{}
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
