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
)

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

// notify runs under the cluster lease: a durable consumer turns events into pushes and a chat watcher pushes COS replies.
func (p *Push) notify(ctx context.Context) {
	var replies sync.WaitGroup
	replies.Go(func() { p.chatReplies(ctx) })
	defer replies.Wait()
	for ctx.Err() == nil {
		if err := p.consume(ctx); err != nil && ctx.Err() == nil {
			slog.Error("push notifier", "error", err)
		}
		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
		}
	}
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
			_ = m.Ack()
			continue
		}
		n.Close = closed
		if _, err := p.Send(ctx, n); err != nil {
			slog.Error("push notice", "event", e.ID, "error", err)
			_ = m.NakWithDelay(30 * time.Second)
			continue
		}
		closed = nil
		_ = m.Ack()
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
	sub, unsubscribe := p.chat.Subscribe()
	defer unsubscribe()
	seen := map[string]bool{}
	for _, e := range p.chat.Entries() {
		if !e.Streaming {
			seen[e.ID] = true
		}
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-sub.wake:
		}
		for _, key := range p.chat.Changed(sub) {
			e, ok := p.chat.Entry(key)
			if !ok || e.Role != "cos" || e.Streaming || seen[key] || time.Since(e.At) > pushReplyWindow {
				continue
			}
			seen[key] = true
			if _, err := p.Send(ctx, Notice{Category: "chat", Title: "COS", Body: firstLine(e.Text), URL: "/#m-" + key, Tag: "chat"}); err != nil {
				slog.Error("push COS reply", "error", err)
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
