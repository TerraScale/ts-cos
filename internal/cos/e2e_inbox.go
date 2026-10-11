package cos

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Inbox checks file probe items with source "e2e": publish skips them, so they wake nobody and push
// nothing, and every check purges what it created so the user's History stays clean.

func inboxCheck(run func(ctx context.Context, in *Inbox, bus *Bus, urls []string, u *e2eUser) (string, error)) func(context.Context, *E2EEnv) (string, error) {
	return func(ctx context.Context, e *E2EEnv) (string, error) {
		bus, err := e.bus(ctx)
		if err != nil {
			return "", err
		}
		defer bus.Close()
		urls, err := e.nodeURLs(ctx)
		if err != nil {
			return "", err
		}
		in, err := OpenInbox(ctx, bus, e.LocalHost(), false)
		if err != nil {
			return "", err
		}
		u, cleanup, err := e.tempUser(ctx, bus)
		if err != nil {
			return "", err
		}
		defer cleanup()
		return run(ctx, in, bus, urls, u)
	}
}

// e2eInboxInput is a probe item with a unique key; extra sets the fields a check varies.
func e2eInboxInput(name string, extra InboxInput) InboxInput {
	raw := make([]byte, 4)
	rand.Read(raw)
	r := hex.EncodeToString(raw)
	in := InboxInput{Kind: "question", Options: []string{"A", "B"}, Source: "e2e", Title: "[e2e] " + name + " " + r, Key: "e2e:" + name + ":" + r}
	if extra.Kind != "" {
		in.Kind = extra.Kind
	}
	if extra.Options != nil {
		in.Options = extra.Options
	}
	in.Context, in.Supersedes = extra.Context, extra.Supersedes
	return in
}

// purgeE2EInbox removes probe items from every node's mirror; a fresh context lets a cancelled run still clean up.
func purgeE2EInbox(in *Inbox, ids ...string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var errs []error
	for _, id := range ids {
		errs = append(errs, in.kv.Purge(ctx, id))
	}
	return errors.Join(errs...)
}

func e2eInboxItem(ctx context.Context, in *Inbox, id string) (InboxItem, error) {
	entry, err := in.kv.Get(ctx, id)
	if err != nil {
		return InboxItem{}, err
	}
	var item InboxItem
	err = json.Unmarshal(entry.Value(), &item)
	return item, err
}

// e2eSignIn signs the throwaway user in once; sessions work on every node.
func e2eSignIn(ctx context.Context, base string, u *e2eUser) (string, error) {
	cookie, status, _, err := signIn(ctx, base, u.name, u.password)
	if err != nil {
		return "", err
	}
	if status != http.StatusSeeOther || cookie == "" {
		return "", fmt.Errorf("sign-in on %s: HTTP %d, want 303 with a session", base, status)
	}
	return cookie, nil
}

// waitInboxPage polls a node's /inbox until want accepts it, for up to 10 s.
// The open marker is the answer form action /inbox/<id>/answer, which only open items render.
func waitInboxPage(ctx context.Context, base, cookie string, want func(body string) bool) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for {
		status, _, _, body, err := e2eRequest(ctx, "GET", base+"/inbox", cookie, nil)
		if err == nil && status == http.StatusOK && want(body) {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("GET %s/inbox: not as expected within 10s (HTTP %d, %v)", base, status, err)
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// pollE2E calls check every interval until it reports done or timeout passes; a check error stops it at once.
func pollE2E(ctx context.Context, timeout, every time.Duration, check func(context.Context) (bool, error)) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		done, err := check(ctx)
		if err != nil || done {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(every):
		}
	}
}

func checkInboxAddVisible(ctx context.Context, in *Inbox, _ *Bus, urls []string, u *e2eUser) (evidence string, err error) {
	var ids []string
	defer func() { err = errors.Join(err, purgeE2EInbox(in, ids...)) }()
	cookie, err := e2eSignIn(ctx, urls[0], u)
	if err != nil {
		return "", err
	}
	start := time.Now()
	item, err := in.Add(ctx, e2eInboxInput("add-visible", InboxInput{}))
	if err != nil {
		return "", err
	}
	ids = append(ids, item.ID)
	open := "/inbox/" + item.ID + "/answer"
	for _, base := range urls {
		if err := waitInboxPage(ctx, base, cookie, func(body string) bool { return strings.Contains(body, open) }); err != nil {
			return "", err
		}
	}
	return fmt.Sprintf("filed on %s, open on %s in %s", in.host, strings.Join(e2eHosts, ", "), time.Since(start).Round(time.Millisecond)), nil
}

func checkInboxAnswerCloses(ctx context.Context, in *Inbox, bus *Bus, urls []string, u *e2eUser) (evidence string, err error) {
	var ids []string
	defer func() { err = errors.Join(err, purgeE2EInbox(in, ids...)) }()
	cookie, err := e2eSignIn(ctx, urls[0], u)
	if err != nil {
		return "", err
	}
	item, err := in.Add(ctx, e2eInboxInput("answer-closes", InboxInput{}))
	if err != nil {
		return "", err
	}
	ids = append(ids, item.ID)
	leader, err := bus.Leader(ctx)
	if err != nil {
		return "", err
	}
	idx := slices.IndexFunc(e2eHosts, func(h string) bool { return h != leader })
	status, _, _, _, err := e2eRequest(ctx, "POST", urls[idx]+"/inbox/"+item.ID+"/answer", cookie,
		url.Values{"option": {"A"}, "text": {"e2e answer"}}, "Datastar-Request", "true")
	if err != nil {
		return "", err
	}
	if status != http.StatusNoContent {
		return "", fmt.Errorf("answer on %s: HTTP %d, want 204", e2eHosts[idx], status)
	}
	stored, err := e2eInboxItem(ctx, in, item.ID)
	if err != nil {
		return "", err
	}
	if stored.Status != "answered" || stored.Answer == nil || stored.Answer.Option != "A" || stored.ClosedOn != e2eHosts[idx] {
		return "", fmt.Errorf("stored item %s = status %q, answer %+v, closed on %q; want answered with option A on %s", item.ID, stored.Status, stored.Answer, stored.ClosedOn, e2eHosts[idx])
	}
	open := "/inbox/" + item.ID + "/answer"
	for _, base := range urls {
		if err := waitInboxPage(ctx, base, cookie, func(body string) bool { return !strings.Contains(body, open) }); err != nil {
			return "", err
		}
	}
	return fmt.Sprintf("leader %s; answered on %s (non-leader), closed there and gone from every /inbox", leader, e2eHosts[idx]), nil
}

func checkInboxConcurrentAnswer(ctx context.Context, in *Inbox, _ *Bus, urls []string, u *e2eUser) (evidence string, err error) {
	var ids []string
	defer func() { err = errors.Join(err, purgeE2EInbox(in, ids...)) }()
	cookie, err := e2eSignIn(ctx, urls[0], u)
	if err != nil {
		return "", err
	}
	item, err := in.Add(ctx, e2eInboxInput("concurrent-answer", InboxInput{}))
	if err != nil {
		return "", err
	}
	ids = append(ids, item.ID)
	options := []string{"A", "B", "A"}
	statuses := make([]int, len(urls))
	errs := make([]error, len(urls))
	var wg sync.WaitGroup
	for i, base := range urls {
		wg.Go(func() {
			statuses[i], _, _, _, errs[i] = e2eRequest(ctx, "POST", base+"/inbox/"+item.ID+"/answer", cookie,
				url.Values{"option": {options[i]}}, "Datastar-Request", "true")
		})
	}
	wg.Wait()
	var winners []int
	for i, status := range statuses {
		if errs[i] != nil {
			return "", errs[i]
		}
		switch status {
		case http.StatusNoContent:
			winners = append(winners, i)
		case http.StatusConflict:
		default:
			return "", fmt.Errorf("concurrent answer on %s: HTTP %d, want 204 or 409", urls[i], status)
		}
	}
	if len(winners) != 1 {
		return "", fmt.Errorf("%d concurrent answers won, want exactly 1 (statuses %v)", len(winners), statuses)
	}
	stored, err := e2eInboxItem(ctx, in, item.ID)
	if err != nil {
		return "", err
	}
	win := winners[0]
	if stored.Answer == nil || stored.Answer.Option != options[win] {
		return "", fmt.Errorf("stored answer %+v, want the winner's option %s", stored.Answer, options[win])
	}
	return fmt.Sprintf("winner %s with option %s; statuses %v for %v", e2eHosts[win], options[win], statuses, e2eHosts), nil
}

func checkInboxDedup(ctx context.Context, in *Inbox, _ *Bus, _ []string, _ *e2eUser) (evidence string, err error) {
	var ids []string
	defer func() { err = errors.Join(err, purgeE2EInbox(in, ids...)) }()
	first := e2eInboxInput("dedup", InboxInput{Context: "first"})
	old, err := in.Add(ctx, first)
	if err != nil {
		return "", err
	}
	ids = append(ids, old.ID)
	first.Context = "second"
	same, err := in.Add(ctx, first)
	if err != nil {
		return "", err
	}
	if same.ID != old.ID || same.Status != "open" {
		return "", fmt.Errorf("same key filed %s (%s), want the open item %s", same.ID, same.Status, old.ID)
	}
	stored, err := e2eInboxItem(ctx, in, old.ID)
	if err != nil {
		return "", err
	}
	if stored.Context != "second" {
		return "", fmt.Errorf("stored context %q after the same key, want \"second\"", stored.Context)
	}
	newer, err := in.Add(ctx, e2eInboxInput("dedup-newer", InboxInput{Supersedes: []string{old.ID}}))
	if err != nil {
		return "", err
	}
	ids = append(ids, newer.ID)
	superseded, err := e2eInboxItem(ctx, in, old.ID)
	if err != nil {
		return "", err
	}
	if want := "Superseded by: " + newer.Title; superseded.Status != "superseded" || superseded.Note != want {
		return "", fmt.Errorf("old item = status %q, note %q; want superseded with note %q", superseded.Status, superseded.Note, want)
	}
	return "same key updated one open item; supersedes closed the old one with its note", nil
}

// checkInboxNeedsUserForward has a probe worker ask a NEEDS-USER question, answers it on a non-leader and waits for the reply.
func checkInboxNeedsUserForward(ctx context.Context, in *Inbox, bus *Bus, urls []string, u *e2eUser) (evidence string, err error) {
	host := in.host
	raw := make([]byte, 4)
	rand.Read(raw)
	r := hex.EncodeToString(raw)
	question := "[e2e] probe " + r + ": which option?"
	// ts-cos exists on every COS host (selfchange launches there too). The root workspace is never touched: the probe runs no tools.
	input, err := json.Marshal(map[string]any{
		"projectTitle":   "ts-cos",
		"title":          "[e2e] TS-815 needs-user probe " + r,
		"modelSelection": map[string]any{"instanceId": "claudeAgent", "model": "claude-opus-5-5", "options": []map[string]string{{"id": "effort", "value": "high"}}},
		"message":        "This is an automated ts-cos e2e probe. Do not run any tools. Reply with exactly these three lines and nothing else, then stop:\nNEEDS-USER: " + question + "\nOPTIONS: A | B\nWHY: e2e probe waits on this answer.\nWhen a message starting with \"USER ANSWER\" arrives, reply with one line: \"PROBE GOT: \" followed by the answer text, then stop.",
	})
	if err != nil {
		return "", err
	}
	var threadID, itemID string
	defer func() {
		cctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		var errs []error
		if threadID != "" {
			errs = append(errs, bus.Call(cctx, host, "delete", []string{threadID}, nil))
		}
		if itemID != "" {
			errs = append(errs, purgeE2EInbox(in, itemID))
		}
		err = errors.Join(err, errors.Join(errs...))
	}()
	var launched struct {
		ThreadID string `json:"threadId"`
	}
	if err := bus.Call(ctx, host, "launch", []string{string(input)}, &launched); err != nil {
		return "", err
	}
	if launched.ThreadID == "" {
		return "", fmt.Errorf("launch on %s returned no thread ID", host)
	}
	threadID = launched.ThreadID
	start := time.Now()
	// Found by thread, not by a precomputed ID: the model may not echo the question byte for byte.
	err = pollE2E(ctx, 3*time.Minute, 2*time.Second, func(ctx context.Context) (bool, error) {
		keys, err := in.kv.ListKeys(ctx)
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		defer keys.Stop()
		for k := range keys.Keys() {
			item, err := e2eInboxItem(ctx, in, k)
			if errors.Is(err, jetstream.ErrKeyNotFound) {
				continue
			}
			if err != nil {
				return false, err
			}
			if item.Thread == threadID && item.Host == host && item.Status == "open" {
				itemID = item.ID
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil {
		return "", fmt.Errorf("probe %s on %s: no open item for the thread within 3m: %w", threadID, host, err)
	}
	filed := time.Since(start)
	leader, err := bus.Leader(ctx)
	if err != nil {
		return "", err
	}
	idx := slices.IndexFunc(e2eHosts, func(h string) bool { return h != leader })
	cookie, err := e2eSignIn(ctx, urls[0], u)
	if err != nil {
		return "", err
	}
	status, _, _, _, err := e2eRequest(ctx, "POST", urls[idx]+"/inbox/"+itemID+"/answer", cookie,
		url.Values{"option": {"A"}, "text": {"e2e forward"}}, "Datastar-Request", "true")
	if err != nil {
		return "", err
	}
	if status != http.StatusNoContent {
		return "", fmt.Errorf("answer on %s: HTTP %d, want 204", e2eHosts[idx], status)
	}
	answered := time.Now()
	// The leader retries failed forwards every minute, so a forward error is not final until the wait runs out.
	var forwardErr string
	err = pollE2E(ctx, 90*time.Second, 2*time.Second, func(ctx context.Context) (bool, error) {
		stored, err := e2eInboxItem(ctx, in, itemID)
		if err != nil {
			return false, err
		}
		if stored.ForwardError != "" {
			forwardErr = stored.ForwardError
		}
		return stored.Forwarded, nil
	})
	if err != nil {
		return "", fmt.Errorf("answer %s not forwarded to thread %s within 90s (last forward error: %q)", itemID, threadID, forwardErr)
	}
	err = pollE2E(ctx, 3*time.Minute, 5*time.Second, func(ctx context.Context) (bool, error) {
		var out struct {
			LastReply string `json:"lastReply"`
		}
		if err := bus.Call(ctx, host, "thread", []string{threadID}, &out); err != nil {
			return false, err
		}
		return strings.Contains(out.LastReply, "PROBE GOT") && strings.Contains(out.LastReply, "e2e forward"), nil
	})
	if err != nil {
		return "", fmt.Errorf("thread %s did not reply PROBE GOT with the answer within 3m: %w", threadID, err)
	}
	return fmt.Sprintf("probe %s on %s: filed in %s, answered on %s (non-leader), forwarded, worker replied in %s",
		threadID, host, filed.Round(time.Millisecond), e2eHosts[idx], time.Since(answered).Round(time.Millisecond)), nil
}
