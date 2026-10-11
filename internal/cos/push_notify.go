package cos

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	pushConsumer    = "cos_push"
	pushMaxDeliver  = 5
	pushCloseKeep   = 20
	pushReplyWindow = 10 * time.Minute
	pushLastKey     = "last.inbox"
	pushDownLoud    = 2 * time.Minute // a consumer outage longer than this is an error
	pushRetryMax    = 30 * time.Second
)

// pushLast is the last Inbox item the notifier handled, read back by the push.inbox-notify e2e check.
type pushLast struct {
	Event    string    `json:"event"`
	Item     string    `json:"item"`
	Severity string    `json:"severity"`
	Sent     int       `json:"sent"`
	At       time.Time `json:"at"`
}

// pushSubjects are the events that become notifications or close one.
var pushSubjects = []string{
	"cos.events.inbox.inbox.item_added",
	"cos.events.inbox.inbox.item_closed",
	"cos.events.user.user.answer",
	"cos.events.deploy.>",
	"cos.events.node.>",
}

// inboxTitles name the Inbox item kinds in a notification title.
var inboxTitles = map[string]string{"approval": "Approval needed", "question": "Question", "decision": "Decision needed", "blocker": "Blocked"}

// notify runs under the cluster lease: a durable consumer turns events into pushes and a chat watcher pushes COS replies. A dropped consumer resubscribes with backoff (1 s doubling to 30 s) and logs an error only after 2 minutes down.
func (p *Push) notify(ctx context.Context) {
	var replies sync.WaitGroup
	replies.Go(func() { p.chatReplies(ctx) })
	defer replies.Wait()
	var down time.Time
	delay := time.Second
	for ctx.Err() == nil {
		start := time.Now()
		err := p.consume(ctx)
		if ctx.Err() != nil {
			return
		}
		if time.Since(start) > time.Minute { // it was healthy for a while: this is a fresh outage
			down, delay = time.Time{}, time.Second
		}
		if down.IsZero() {
			down = time.Now()
		}
		logErr(ctx, pushOutageLevel(time.Since(down)), "push notifier", err, "down", time.Since(down).Round(time.Second))
		select {
		case <-ctx.Done():
		case <-time.After(delay):
		}
		delay = min(delay*2, pushRetryMax)
	}
}

// pushOutageLevel keeps rollout restarts and heartbeat blips at warn; only an outage that outlasts pushDownLoud is an error.
func pushOutageLevel(down time.Duration) slog.Level {
	if down > pushDownLoud {
		return slog.LevelError
	}
	return slog.LevelWarn
}

// consume pushes events in order until the consumer or ctx fails. A closed item is held and carried by the next push.
func (p *Push) consume(ctx context.Context) error {
	setup, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := EnsureEventStreams(setup, p.js); err != nil {
		return err
	}
	consumer, err := p.js.CreateOrUpdateConsumer(setup, EventsStream, jetstream.ConsumerConfig{
		Durable: pushConsumer, FilterSubjects: pushSubjects, DeliverPolicy: jetstream.DeliverNewPolicy,
		AckPolicy: jetstream.AckExplicitPolicy, AckWait: time.Minute, MaxDeliver: pushMaxDeliver,
	})
	if err != nil {
		return err
	}
	msgs, err := consumer.Messages()
	if err != nil {
		return err
	}
	stop := context.AfterFunc(ctx, msgs.Stop)
	defer stop()
	var closed []string
	for {
		m, err := msgs.Next()
		if err != nil {
			return err
		}
		var e Event
		if err := json.Unmarshal(m.Data(), &e); err != nil {
			_ = m.Term() // malformed events never become valid
			continue
		}
		if e.Item != "" && (e.Kind == KindInboxClosed || e.Kind == KindUserAnswer) {
			closed = append(closed, "inbox-"+e.Item)
			closed = closed[max(0, len(closed)-pushCloseKeep):]
			_ = m.Ack()
			continue
		}
		n, ok := noticeFor(e)
		if !ok {
			p.recordLast(ctx, e, 0)
			_ = m.Ack()
			continue
		}
		n.Close = closed
		sent, err := p.Send(ctx, n)
		if err != nil {
			logErr(ctx, slog.LevelError, "push notice", err, "event", e.ID)
			_ = m.NakWithDelay(30 * time.Second)
			continue
		}
		closed = nil
		p.recordLast(ctx, e, sent)
		_ = m.Ack()
	}
}

// recordLast notes an Inbox item once it is handled. A failed write only logs: the item was already handled, so NAKing would push it twice.
func (p *Push) recordLast(ctx context.Context, e Event, sent int) {
	if e.Kind != KindInboxItemAdded {
		return
	}
	raw, err := json.Marshal(pushLast{Event: e.ID, Item: e.Item, Severity: string(e.Severity), Sent: sent, At: time.Now().UTC()})
	if err == nil {
		_, err = p.kv.Put(ctx, pushLastKey, raw)
	}
	if err != nil {
		logErr(ctx, slog.LevelWarn, "push record last", err, "event", e.ID)
	}
}

// noticeFor decides whether an event deserves a notification and what it says.
func noticeFor(e Event) (Notice, bool) {
	switch {
	case e.Kind == KindInboxItemAdded:
		if e.Severity == SeverityInfo {
			return Notice{}, false
		}
		var data struct {
			Kind string `json:"kind"`
		}
		_ = json.Unmarshal(e.Data, &data)
		title := "Inbox"
		if t, ok := inboxTitles[data.Kind]; ok {
			title = t
		}
		return Notice{Category: "inbox", Title: title, Body: e.Summary, URL: "/inbox#inbox-" + e.Item, Tag: "inbox-" + e.Item, Urgent: e.Severity == SeverityUrgent}, true
	case e.Kind == "deploy.completed":
		return Notice{Category: "done", Title: "Updated", Body: e.Summary, URL: "/", Tag: "deploy"}, true
	case e.Kind == "deploy.rolled_back":
		return Notice{Category: "done", Title: "Rolled back", Body: e.Summary, URL: "/", Tag: "deploy"}, true
	case e.Source == "node" && (e.Severity == SeverityWarn || e.Severity == SeverityUrgent):
		return Notice{Category: "node", Title: "Node alert · " + e.Host, Body: e.Summary, URL: "/nodes#" + e.Host, Tag: "node-" + e.Host, Urgent: e.Severity == SeverityUrgent}, true
	}
	return Notice{}, false
}

// chatReplies pushes each COS reply once, when it lands final in the chat mirror. Replies already final at start are not pushed.
func (p *Push) chatReplies(ctx context.Context) {
	sub, unsubscribe := p.chat.SubscribeAll()
	defer unsubscribe()
	seen := map[string]bool{}
	for key, e := range p.chat.entriesAll() {
		if !e.Streaming {
			seen[key] = true
		}
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-sub.wake:
		}
		for _, key := range p.chat.Changed(sub) {
			e, ok := p.chat.entryByKey(key)
			if !ok || e.Role != "cos" || e.Streaming || seen[key] || time.Since(e.At) > pushReplyWindow {
				continue
			}
			seen[key] = true
			if _, err := p.Send(ctx, Notice{Category: "chat", Title: "COS", Body: firstLine(e.Text), URL: "/#m-" + e.ID, Tag: "chat"}); err != nil {
				logErr(ctx, slog.LevelError, "push COS reply", err)
			}
		}
	}
}

// firstLine is the first non-empty line of a reply, without the markdown markers that lead it.
func firstLine(text string) string {
	for _, line := range strings.Split(text, "\n") {
		if line = strings.TrimSpace(strings.TrimLeft(strings.TrimSpace(line), "#*->")); line != "" {
			return line
		}
	}
	return ""
}
