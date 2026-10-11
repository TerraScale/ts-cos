package cos

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// chatProbeWait bounds how long each node's open stream may take to repaint the probe.
const chatProbeWait = 15 * time.Second

// checkChatNonLeader puts a system probe on the chat and requires every node to repaint it live.
func checkChatNonLeader(ctx context.Context, e *E2EEnv) (string, error) {
	return accessCheck(func(ctx context.Context, urls []string, u *e2eUser) (string, error) {
		cookies, err := chatCookies(ctx, urls, u)
		if err != nil {
			return "", err
		}
		bus, err := e.bus(ctx)
		if err != nil {
			return "", err
		}
		defer bus.Close()
		return chatProbeAcross(ctx, bus, urls, cookies)
	})(ctx, e)
}

// checkChatReplyAll requires the newest COS reply to be in the first chat repaint of every node.
func checkChatReplyAll(ctx context.Context, e *E2EEnv) (string, error) {
	return accessCheck(func(ctx context.Context, urls []string, u *e2eUser) (string, error) {
		bus, err := e.bus(ctx)
		if err != nil {
			return "", err
		}
		defer bus.Close()
		id, err := newestCOSReply(ctx, bus)
		if errors.Is(err, errNoCOSReply) {
			return "", e2eSkip("no COS reply on the chat yet")
		}
		if err != nil {
			return "", err
		}
		cookies, err := chatCookies(ctx, urls, u)
		if err != nil {
			return "", err
		}
		return replyRepaint(ctx, urls, cookies, id)
	})(ctx, e)
}

// checkChatSpawnThread requires the COS thread to be recorded on the current cluster leader.
func checkChatSpawnThread(ctx context.Context, e *E2EEnv) (string, error) {
	bus, err := e.bus(ctx)
	if err != nil {
		return "", err
	}
	defer bus.Close()
	st, err := liveAgentState(ctx, bus)
	if err != nil {
		return "", err
	}
	leader, err := leaderOf(ctx, bus)
	if err != nil {
		return "", err
	}
	return spawnThread(st, leader)
}

// checkChatFocusRestart requires a focus change made after the thread started to leave a restart pending.
func checkChatFocusRestart(ctx context.Context, e *E2EEnv) (string, error) {
	bus, err := e.bus(ctx)
	if err != nil {
		return "", err
	}
	defer bus.Close()
	st, err := liveAgentState(ctx, bus)
	if err != nil {
		return "", err
	}
	focusAt, err := focusUpdated(ctx, bus)
	if err != nil {
		return "", err
	}
	return focusRestart(st, focusAt)
}

// checkChatWakeBudget requires no rolling hour of COS wakes to hold more digest wakes than the behavior allows.
func checkChatWakeBudget(ctx context.Context, e *E2EEnv) (string, error) {
	bus, err := e.bus(ctx)
	if err != nil {
		return "", err
	}
	defer bus.Close()
	st, err := liveAgentState(ctx, bus)
	if err != nil {
		return "", err
	}
	return wakeBudget(st, CurrentBehavior().Wake.MaxWakesPerHour)
}

// checkChatContext requires the carried-context numbers in the agent state to follow the agent's own restart rules.
func checkChatContext(ctx context.Context, e *E2EEnv) (string, error) {
	bus, err := e.bus(ctx)
	if err != nil {
		return "", err
	}
	defer bus.Close()
	st, err := liveAgentState(ctx, bus)
	if err != nil {
		return "", err
	}
	return contextAgrees(st)
}

// chatCookies signs u in on every node and returns one session cookie per base URL.
func chatCookies(ctx context.Context, urls []string, u *e2eUser) ([]string, error) {
	cookies := make([]string, len(urls))
	for i, base := range urls {
		cookie, status, _, err := signIn(ctx, base, u.name, u.password)
		if err != nil || status != http.StatusSeeOther {
			return nil, fmt.Errorf("login on %s: HTTP %d %v", base, status, err)
		}
		cookies[i] = cookie
	}
	return cookies, nil
}

// chatFeed is one open /chat/events stream.
type chatFeed struct {
	base string
	sc   *bufio.Scanner
	stop context.CancelFunc
	body io.Closer
}

// openChatFeed opens base's chat stream and returns once the connection is accepted.
func openChatFeed(ctx context.Context, base, cookie string) (*chatFeed, error) {
	sctx, stop := context.WithCancel(ctx)
	req, err := http.NewRequestWithContext(sctx, "GET", base+"/chat/events", nil)
	if err != nil {
		stop()
		return nil, err
	}
	req.Header.Set("Accept", "text/event-stream")
	req.AddCookie(&http.Cookie{Name: sessionCookie, Value: cookie})
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		stop()
		return nil, fmt.Errorf("GET %s/chat/events: %w", base, err)
	}
	if res.StatusCode != http.StatusOK {
		res.Body.Close()
		stop()
		return nil, fmt.Errorf("GET %s/chat/events: HTTP %d", base, res.StatusCode)
	}
	return &chatFeed{base: base, sc: newSSEScanner(res.Body), stop: stop, body: res.Body}, nil
}

func (f *chatFeed) close() {
	f.stop()
	f.body.Close()
}

// chatProbeAcross opens every stream first, so the probe reaches each one as a live patch and not as history.
// It then writes the probe through bus, requires each stream to repaint it within chatProbeWait, and deletes it.
func chatProbeAcross(ctx context.Context, bus *Bus, bases, cookies []string) (evidence string, err error) {
	feeds := make([]*chatFeed, len(bases))
	defer func() {
		for _, f := range feeds {
			if f != nil {
				f.close()
			}
		}
	}()
	for i, base := range bases {
		if feeds[i], err = openChatFeed(ctx, base, cookies[i]); err != nil {
			return "", err
		}
		if !scanFor(feeds[i].sc, `id="messages"`) {
			return "", fmt.Errorf("no chat history repaint on %s%s", base, scanErr(feeds[i].sc))
		}
	}
	id := chatID(time.Now().UTC())
	if _, err := PutChat(ctx, bus, ChatEntry{ID: id, Role: "system", Text: "e2e probe " + id}); err != nil {
		return "", err
	}
	// A system entry does not wake the COS agent; the probe is removed whatever the outcome.
	defer func() {
		del, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if derr := deleteChatEntry(del, bus, id); derr != nil && err == nil {
			err = fmt.Errorf("remove probe %s: %w", id, derr)
		}
	}()
	marker := `id="m-` + id + `"`
	for _, f := range feeds {
		deadline := time.AfterFunc(chatProbeWait, f.stop)
		seen := scanFor(f.sc, marker)
		deadline.Stop()
		if !seen {
			return "", fmt.Errorf("probe %s not repainted on %s within %s%s", id, f.base, chatProbeWait, scanErr(f.sc))
		}
	}
	return fmt.Sprintf("probe %s repainted live on %d/%d nodes", id, len(bases), len(bases)), nil
}

// deleteChatEntry removes one entry from the chat bucket; every node's watcher drops it.
func deleteChatEntry(ctx context.Context, b *Bus, id string) error {
	kv, err := openKV(ctx, b, chatBucket)
	if err != nil {
		return err
	}
	return kv.Delete(ctx, id)
}

var errNoCOSReply = errors.New("no COS reply on the chat")

// newestCOSReply returns the id of the newest COS entry. It only reads: the bucket is never created or written.
func newestCOSReply(ctx context.Context, bus *Bus) (string, error) {
	kv, err := readKV(ctx, bus, chatBucket)
	if errors.Is(err, jetstream.ErrBucketNotFound) {
		return "", errNoCOSReply
	}
	if err != nil {
		return "", err
	}
	keys, err := kv.Keys(ctx)
	if errors.Is(err, jetstream.ErrNoKeysFound) {
		return "", errNoCOSReply
	}
	if err != nil {
		return "", err
	}
	slices.Sort(keys) // chatID sorts by creation time
	for i := len(keys) - 1; i >= 0; i-- {
		e, err := kv.Get(ctx, keys[i])
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue // deleted since the listing
		}
		if err != nil {
			return "", err
		}
		var entry ChatEntry
		if err := json.Unmarshal(e.Value(), &entry); err != nil {
			return "", err
		}
		if entry.Role == "cos" {
			return entry.ID, nil
		}
	}
	return "", errNoCOSReply
}

// firstRepaint returns the history repaint line of base's chat stream; it must arrive within chatProbeWait.
func firstRepaint(ctx context.Context, base, cookie string) (string, error) {
	f, err := openChatFeed(ctx, base, cookie)
	if err != nil {
		return "", err
	}
	defer f.close()
	deadline := time.AfterFunc(chatProbeWait, f.stop)
	defer deadline.Stop()
	if !scanFor(f.sc, `id="messages"`) {
		return "", fmt.Errorf("no chat history repaint on %s%s", base, scanErr(f.sc))
	}
	return f.sc.Text(), nil
}

// replyRepaint requires the reply id to be in the first repaint of every node's chat stream.
func replyRepaint(ctx context.Context, bases, cookies []string, id string) (string, error) {
	marker := `id="m-` + id + `"`
	for i, base := range bases {
		first, err := firstRepaint(ctx, base, cookies[i])
		if err != nil {
			return "", err
		}
		if !strings.Contains(first, marker) {
			return "", fmt.Errorf("reply %s missing from the first repaint on %s", id, base)
		}
	}
	return fmt.Sprintf("reply %s in the first repaint on %d/%d nodes", id, len(bases), len(bases)), nil
}

// readKV binds an existing bucket without creating it: the live run observes and never changes state.
func readKV(ctx context.Context, bus *Bus, bucket string) (jetstream.KeyValue, error) {
	js, err := bus.JetStream()
	if err != nil {
		return nil, err
	}
	return js.KeyValue(ctx, bucket)
}

// liveAgentState reads the replicated COS state; with no state yet it returns the zero state.
func liveAgentState(ctx context.Context, bus *Bus) (AgentState, error) {
	var st AgentState
	kv, err := readKV(ctx, bus, agentBucket)
	if errors.Is(err, jetstream.ErrBucketNotFound) {
		return st, nil
	}
	if err != nil {
		return st, err
	}
	e, err := kv.Get(ctx, "state")
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return st, nil
	}
	if err != nil {
		return st, err
	}
	return st, json.Unmarshal(e.Value(), &st)
}

// focusUpdated returns when the focus context key was last written, or the zero time if it was never set.
func focusUpdated(ctx context.Context, bus *Bus) (time.Time, error) {
	kv, err := readKV(ctx, bus, contextBucket)
	if errors.Is(err, jetstream.ErrBucketNotFound) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, err
	}
	e, err := kv.Get(ctx, "focus")
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, err
	}
	return e.Created(), nil
}

// spawnThread requires the recorded thread to sit on the cluster leader. It does not query T3 for the thread.
func spawnThread(st AgentState, leader string) (string, error) {
	if st.ThreadID == "" {
		return "", e2eSkip("no COS thread recorded yet")
	}
	if st.Host != leader {
		return "", fmt.Errorf("thread %s recorded on host %q, cluster leader is %q", st.ThreadID, st.Host, leader)
	}
	return fmt.Sprintf("thread %s recorded on leader %s (existence on T3 not queried: no read-only path from the runner)", st.ThreadID, leader), nil
}

// focusRestart requires a focus change made after the thread started to be named by a pending restart.
// A focus wake that already ran since the change also counts, since it clears the restart it served.
func focusRestart(st AgentState, focusAt time.Time) (string, error) {
	if st.Started.IsZero() {
		return "", e2eSkip("no COS agent state yet")
	}
	if focusAt.IsZero() || !focusAt.After(st.Started) {
		return "focus unchanged since the thread started " + st.Started.Format(time.RFC3339), nil
	}
	if strings.Contains(st.Restart, "focus") {
		return fmt.Sprintf("focus changed %s, after thread start %s: restart pending", focusAt.Format(time.RFC3339), st.Started.Format(time.RFC3339)), nil
	}
	for _, w := range st.Wakes {
		if strings.Contains(w.Reason, "focus") && !w.At.Before(focusAt) {
			return fmt.Sprintf("focus changed %s: a focus wake already ran at %s", focusAt.Format(time.RFC3339), w.At.Format(time.RFC3339)), nil
		}
	}
	return "", fmt.Errorf("focus changed %s, after thread start %s, but no focus restart is pending", focusAt.Format(time.RFC3339), st.Started.Format(time.RFC3339))
}

// maxDigestsPerHour is the most digest wakes that fall within one hour, counted from each digest wake.
func maxDigestsPerHour(wakes []WakeRecord) int {
	var digests []time.Time
	for _, w := range wakes {
		if strings.Contains(w.Reason, "digest") {
			digests = append(digests, w.At)
		}
	}
	most := 0
	for i, start := range digests {
		n := 0
		for _, at := range digests[i:] {
			if at.Sub(start) > time.Hour {
				break
			}
			n++
		}
		most = max(most, n)
	}
	return most
}

// wakeBudget requires no rolling hour of wakes to hold more digest wakes than limit allows.
func wakeBudget(st AgentState, limit int) (string, error) {
	if len(st.Wakes) == 0 {
		return "", e2eSkip("no COS wakes yet")
	}
	most := maxDigestsPerHour(st.Wakes)
	if most > limit {
		return "", fmt.Errorf("%d digest wakes within one hour, limit %d", most, limit)
	}
	return fmt.Sprintf("most digest wakes in one hour %d, limit %d", most, limit), nil
}

// contextAgrees requires the carried-context numbers to match the restart rule in agent.go. It is a consistency
// check on the recorded state; it does not show that a compaction ran.
func contextAgrees(st AgentState) (string, error) {
	if st.ThreadID == "" && st.Compacted != "" {
		return "", fmt.Errorf("compaction %s recorded with no COS thread", st.Compacted)
	}
	if st.CarriedTokens > 0 && (st.BriefTokens == 0 || st.CarriedTokens <= agentFreshMin || st.CarriedTokens <= 2*st.BriefTokens) {
		return "", fmt.Errorf("carried %d tokens does not pass the restart rule (over %d and twice the %d-token brief)", st.CarriedTokens, agentFreshMin, st.BriefTokens)
	}
	return fmt.Sprintf("carried %d tokens, brief %d tokens: consistent with the restart rule", st.CarriedTokens, st.BriefTokens), nil
}
