package cos

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

// chatProbeWait bounds how long each node's open stream may take to repaint the probe.
const chatProbeWait = 15 * time.Second

// checkChatNonLeader puts a system probe on the chat and requires every node to repaint it live.
func checkChatNonLeader(ctx context.Context, e *E2EEnv) (string, error) {
	return accessCheck(func(ctx context.Context, urls []string, u *e2eUser) (string, error) {
		cookies := make([]string, len(urls))
		for i, base := range urls {
			cookie, status, _, err := signIn(ctx, base, u.name, u.password)
			if err != nil || status != http.StatusSeeOther {
				return "", fmt.Errorf("login on %s: HTTP %d %v", base, status, err)
			}
			cookies[i] = cookie
		}
		bus, err := e.bus(ctx)
		if err != nil {
			return "", err
		}
		defer bus.Close()
		return chatProbeAcross(ctx, bus, urls, cookies)
	})(ctx, e)
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
	// The probe writes as the deploy health user, whose dashboard session is the one the feeds open.
	ctx = WithUser(ctx, probeUser)
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
	scoped, err := UserKV(ctx, kv)
	if err != nil {
		return err
	}
	return scoped.Delete(ctx, id)
}
