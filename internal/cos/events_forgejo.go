package cos

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// COS registers one webhook on the TerraScale org and one on the Tools/ku code
// repository (a separate org where the user's PRs also live), signed with the secret
// every node derives, pointing at A's ingress.
const forgejoHookURL = "https://cos.terrascale.tech/hooks/forgejo"

var (
	forgejoHookTargets = []string{"/orgs/" + forgejoOrg + "/hooks", "/repos/Tools/ku/hooks"}
	forgejoHookEvents  = []string{"push", "pull_request", "pull_request_sync",
		"pull_request_review_approved", "pull_request_review_rejected", "pull_request_review_comment",
		"action_run_failure", "action_run_recover", "action_run_success"}
)

type forgejoHook struct {
	ID     int64             `json:"id"`
	Config map[string]string `json:"config"`
}

// RegisterHooks creates COS's webhooks or updates them in place, so a rotated
// secret or a changed event list takes effect on the next leader start.
func (f Forgejo) RegisterHooks(ctx context.Context, secret string) error {
	body := map[string]any{"type": "forgejo", "active": true, "events": forgejoHookEvents, "branch_filter": "*",
		"config": map[string]string{"url": forgejoHookURL, "content_type": "json", "secret": secret}}
	for _, target := range forgejoHookTargets {
		hooks, err := forgejoList[forgejoHook](ctx, f, target)
		if err != nil {
			return err
		}
		method, path := http.MethodPost, target
		for _, hook := range hooks {
			if hook.Config["url"] == forgejoHookURL {
				method, path = http.MethodPatch, target+"/"+strconv.FormatInt(hook.ID, 10)
				break
			}
		}
		if _, err := f.request(ctx, method, path, body, nil); err != nil {
			return err
		}
	}
	return nil
}

func init() {
	serveWorkers = append(serveWorkers, RunForgejoEvents, func(ctx context.Context, cfg Config, bus *Bus) {
		bus.Lead(ctx, func(ctx context.Context) {
			retry(ctx, "COS Forgejo webhook registration", func() error {
				return Forgejo{Config: cfg}.RegisterHooks(ctx, bus.HookSecret("forgejo"))
			})
			if ctx.Err() == nil {
				slog.Info("COS Forgejo webhooks registered", "url", forgejoHookURL)
			}
		})
	})
}

// Only PRs authored by mariogk become events; pushes and CI on main always do.
const (
	forgejoAuthor         = "mariogk"
	forgejoIngestConsumer = "cos-events-forgejo"
	forgejoIngestAckWait  = 5 * time.Minute
)

// forgejoRecheckDelay lets Forgejo recompute mergeability after a push; tests shorten it.
var forgejoRecheckDelay = 15 * time.Second

type forgejoHookPR struct {
	Number    int         `json:"number"`
	Title     string      `json:"title"`
	HTMLURL   string      `json:"html_url"`
	Merged    bool        `json:"merged"`
	Mergeable bool        `json:"mergeable"`
	ClosedAt  *time.Time  `json:"closed_at"`
	UpdatedAt time.Time   `json:"updated_at"`
	User      forgejoUser `json:"user"`
	Head      struct {
		Ref string `json:"ref"`
		SHA string `json:"sha"`
	} `json:"head"`
	Base struct {
		Ref string `json:"ref"`
		SHA string `json:"sha"`
	} `json:"base"`
}

type forgejoPayload struct {
	Action      string         `json:"action"`
	PullRequest *forgejoHookPR `json:"pull_request"`
	Review      *struct {
		Type    string `json:"type"`
		Content string `json:"content"`
	} `json:"review"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
	Sender     forgejoUser `json:"sender"`
	Ref        string      `json:"ref"`
	After      string      `json:"after"`
	Pusher     forgejoUser `json:"pusher"`
	HeadCommit *struct {
		Message   string    `json:"message"`
		Timestamp time.Time `json:"timestamp"`
	} `json:"head_commit"`
	Commits []json.RawMessage `json:"commits"`
	Run     *struct {
		ID           int64  `json:"id"`
		Title        string `json:"title"`
		Status       string `json:"status"`
		HTMLURL      string `json:"html_url"`
		CommitSHA    string `json:"commit_sha"`
		TriggerEvent string `json:"trigger_event"`
		EventPayload string `json:"event_payload"`
		Repository   struct {
			FullName string `json:"full_name"`
		} `json:"repository"`
	} `json:"run"`
}

func forgejoEvent(repo string, pr *forgejoHookPR, id string, kind EventKind, severity Severity, at time.Time, summary string, data map[string]any) Event {
	e := Event{ID: NewEventID(SourceForgejo, append([]string{repo}, id)...), Source: SourceForgejo, Kind: kind, Time: at.UTC(),
		Severity: severity, Repo: repo}
	if pr != nil {
		e.PR = pr.Number
		e.Ticket = t3TicketRef.FindString(pr.Title + " " + strings.ToUpper(pr.Head.Ref))
		data["title"], data["url"], data["head"], data["branch"] = EventText(pr.Title, 200), pr.HTMLURL, pr.Head.SHA, EventText(pr.Head.Ref, 200)
		summary = repo + "#" + strconv.Itoa(pr.Number) + " " + summary + ": " + EventText(pr.Title, 120)
	} else {
		summary = repo + " " + summary
	}
	e.Summary = EventText(summary, eventSummaryMax)
	e.Data, _ = json.Marshal(data)
	return e
}

// normalizeForgejo maps one verified delivery to events; anything else is dropped.
func normalizeForgejo(event string, p forgejoPayload, now time.Time) []Event {
	repo, pr := p.Repository.FullName, p.PullRequest
	if p.Run != nil && repo == "" {
		repo = p.Run.Repository.FullName // action_run deliveries carry the repository only inside run
	}
	if repo == "" || len(repo) > 200 {
		return nil
	}
	mine := pr != nil && pr.Number > 0 && pr.User.Login == forgejoAuthor
	switch event {
	case "pull_request":
		if !mine {
			return nil
		}
		at := pr.UpdatedAt
		if at.IsZero() {
			at = now
		}
		// The payload's mergeable is stale right after a push; mergeability comes from the recheck.
		n := "#" + strconv.Itoa(pr.Number)
		switch p.Action {
		case "opened", "reopened":
			return []Event{forgejoEvent(repo, pr, n+":opened:"+pr.Head.SHA, KindPROpened, SeverityInfo, at, "opened", map[string]any{})}
		case "synchronized":
			return []Event{forgejoEvent(repo, pr, n+":sync:"+pr.Head.SHA, KindPRSynchronized, SeverityInfo, at, "updated", map[string]any{})}
		case "closed":
			if pr.Merged {
				return []Event{forgejoEvent(repo, pr, n+":merged", KindPRMerged, SeverityInfo, at, "merged", map[string]any{})}
			}
			return []Event{forgejoEvent(repo, pr, n+":closed:"+at.UTC().Format(time.RFC3339), KindPRClosed, SeverityInfo, at, "closed", map[string]any{})}
		}
		return nil
	case "pull_request_approved", "pull_request_rejected", "pull_request_comment":
		if !mine || p.Review == nil {
			return nil
		}
		state := strings.TrimPrefix(event, "pull_request_")
		severity := SeverityInfo
		if state == "rejected" {
			severity = SeverityWarn
		}
		data := map[string]any{"state": state, "reviewer": EventText(p.Sender.Login, 100), "content": EventText(p.Review.Content, 1500)}
		// Forgejo sends no review ID; the reviewer, head and body identify one submission.
		sum := sha256.Sum256([]byte(p.Sender.Login + "\x00" + p.Review.Type + "\x00" + p.Review.Content))
		id := "#" + strconv.Itoa(pr.Number) + ":review:" + pr.Head.SHA + ":" + hex.EncodeToString(sum[:8])
		return []Event{forgejoEvent(repo, pr, id, KindReview, severity, now, p.Sender.Login+" "+state, data)}
	case "push":
		if p.Ref != "refs/heads/main" || p.After == "" {
			return nil
		}
		at, message := now, ""
		if p.HeadCommit != nil {
			message = p.HeadCommit.Message
			if !p.HeadCommit.Timestamp.IsZero() {
				at = p.HeadCommit.Timestamp
			}
		}
		message, _, _ = strings.Cut(message, "\n")
		data := map[string]any{"after": p.After, "pusher": EventText(p.Pusher.Login, 100), "commits": len(p.Commits), "message": EventText(message, 200)}
		return []Event{forgejoEvent(repo, nil, "push:"+p.After, KindPushMain, SeverityInfo, at, "main updated: "+message, data)}
	case "action_run_success", "action_run_failure", "action_run_recover":
		run := p.Run
		if run == nil {
			return nil
		}
		state := "success"
		if event == "action_run_failure" {
			state = "failure"
		}
		var trigger struct {
			PullRequest *forgejoHookPR `json:"pull_request"`
			Ref         string         `json:"ref"`
		}
		_ = json.Unmarshal([]byte(run.EventPayload), &trigger)
		pr, severity := trigger.PullRequest, SeverityInfo
		switch {
		case (run.TriggerEvent == "pull_request" || run.TriggerEvent == "pull_request_target") && pr != nil && pr.User.Login == forgejoAuthor:
			if state == "failure" {
				severity = SeverityWarn
			}
		case run.TriggerEvent == "push" && trigger.Ref == "refs/heads/main":
			pr = nil
			if state == "failure" {
				severity = SeverityUrgent // main is broken
			}
		case run.TriggerEvent == "schedule":
			pr = nil // scheduled guards run on main and watch production
			if state == "failure" {
				severity = SeverityWarn
			}
		default:
			return nil
		}
		data := map[string]any{"state": state, "workflow": EventText(run.Title, 200), "run": run.ID, "runUrl": run.HTMLURL, "sha": run.CommitSHA}
		summary := "CI " + state
		if pr == nil {
			summary = "CI on main " + state + ": " + run.Title
		}
		return []Event{forgejoEvent(repo, pr, "run:"+strconv.FormatInt(run.ID, 10)+":"+state, KindCIStatus, severity, now, summary, data)}
	}
	return nil
}

// mergeability reads repo's open mariogk PRs once Forgejo has recomputed them: every
// conflict, and focus (a PR just opened or updated) when it merges cleanly. IDs carry
// the head and base, so each state is reported once.
func (f Forgejo) mergeability(ctx context.Context, repo string, focus int, now time.Time) ([]Event, error) {
	owner, name, ok := strings.Cut(repo, "/")
	if !ok {
		return nil, nil
	}
	prs, err := forgejoList[forgejoHookPR](ctx, f, "/repos/"+url.PathEscape(owner)+"/"+url.PathEscape(name)+"/pulls?state=open")
	if err != nil {
		return nil, err
	}
	var events []Event
	for i := range prs {
		pr := &prs[i]
		if pr.User.Login != forgejoAuthor || pr.Base.Ref != "main" {
			continue
		}
		id := "#" + strconv.Itoa(pr.Number) + ":%s:" + pr.Head.SHA + ":" + pr.Base.SHA
		data := map[string]any{"main": pr.Base.SHA}
		if !pr.Mergeable {
			events = append(events, forgejoEvent(repo, pr, fmt.Sprintf(id, "conflict"), KindPRConflict, SeverityWarn, now, "has conflicts", data))
		} else if pr.Number == focus {
			events = append(events, forgejoEvent(repo, pr, fmt.Sprintf(id, "mergeable"), KindPRMergeable, SeverityInfo, now, "mergeable", data))
		}
	}
	return events, nil
}

// RunForgejoEvents normalizes A's verified Forgejo deliveries from cos.ingest.forgejo.
// Every node pulls from one durable work-queue consumer, so each delivery is
// handled once and any node keeps it going while another is offline.
func RunForgejoEvents(ctx context.Context, cfg Config, bus *Bus) {
	js, err := bus.JetStream()
	if err != nil {
		slog.Error("COS Forgejo events unavailable", "error", err)
		return
	}
	var consumer jetstream.Consumer
	retry(ctx, "COS Forgejo ingest consumer", func() error {
		attempt, stop := context.WithTimeout(ctx, 10*time.Second)
		defer stop()
		if err := EnsureEventStreams(attempt, js); err != nil {
			return err
		}
		consumer, err = js.CreateOrUpdateConsumer(attempt, IngestStream, jetstream.ConsumerConfig{Durable: forgejoIngestConsumer,
			FilterSubject: IngestForgejo, AckPolicy: jetstream.AckExplicitPolicy, AckWait: forgejoIngestAckWait})
		return err
	})
	if ctx.Err() != nil {
		return
	}
	f := Forgejo{Config: cfg}
	publish := func(e Event) {
		if err := e.Validate(); err != nil {
			slog.Error("COS Forgejo event dropped", "id", e.ID, "error", err)
			return
		}
		retry(ctx, "COS Forgejo event publish", func() error { _, err := PublishEvent(ctx, js, e); return err })
	}
	consume, err := consumer.Consume(func(msg jetstream.Msg) {
		event := msg.Headers().Get("X-Forgejo-Event")
		var p forgejoPayload
		if err := json.Unmarshal(msg.Data(), &p); err != nil {
			slog.Warn("COS Forgejo delivery is not JSON; dropped", "event", event)
			_ = msg.Term()
			return
		}
		now := time.Now()
		for _, e := range normalizeForgejo(event, p, now) {
			publish(e)
		}
		focus, recheck := 0, event == "push" && p.Ref == "refs/heads/main"
		if pr := p.PullRequest; event == "pull_request" && pr != nil && pr.User.Login == forgejoAuthor &&
			(p.Action == "opened" || p.Action == "reopened" || p.Action == "synchronized") {
			focus, recheck = pr.Number, true
		}
		if recheck {
			select {
			case <-ctx.Done():
				return
			case <-time.After(forgejoRecheckDelay):
			}
			events, err := f.mergeability(ctx, p.Repository.FullName, focus, time.Now())
			if err != nil {
				slog.Warn("COS Forgejo mergeability recheck failed; the heartbeat reconciler covers it", "repo", p.Repository.FullName, "error", err)
			}
			for _, e := range events {
				publish(e)
			}
		}
		if ctx.Err() == nil {
			_ = msg.Ack()
		}
	})
	if err != nil {
		slog.Error("COS Forgejo ingest consumer failed", "error", err)
		return
	}
	<-ctx.Done()
	consume.Stop()
}
