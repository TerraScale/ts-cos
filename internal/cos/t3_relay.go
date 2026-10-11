package cos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

// The T3 relay lets any COS node follow any host's threads over NATS. A client asks
// cos.<host>.stream to publish a thread's (or the shell's) events to its inbox; it renews the
// lease on <inbox>.ka and the host stops when the lease lapses, the client says stop, or T3 ends.
const (
	relayLease     = 30 * time.Second
	relayKeepalive = 10 * time.Second
	relayMax       = 64
	relayEndHeader = "Cos-Stream-End"
	// shortcut: relay snapshots keep the last 50 messages so one event fits a NATS message;
	// read older history with `cos thread`.
	relaySnapshotMessages = 50
)

type relayRequest struct {
	ThreadID string `json:"threadId,omitempty"`
	Inbox    string `json:"inbox"`
}

// ServeT3Streams registers this host's relay endpoint; open connects to the local T3.
func (b *Bus) ServeT3Streams(host string, open func(context.Context) (*T3, error)) error {
	if b.service == nil || !validHost(host) {
		return errors.New("T3 relay needs a started COS service")
	}
	var mu sync.Mutex
	running := 0
	return b.service.AddEndpoint("stream", micro.HandlerFunc(func(req micro.Request) {
		var r relayRequest
		if json.Unmarshal(req.Data(), &r) != nil || !strings.HasPrefix(r.Inbox, "_INBOX.") || strings.ContainsAny(r.Inbox, " *>\t\r\n") || len(r.ThreadID) > 512 {
			_ = req.Error("400", "invalid stream request", nil)
			return
		}
		mu.Lock()
		if running >= relayMax {
			mu.Unlock()
			_ = req.Error("503", "too many T3 streams on "+host, nil)
			return
		}
		running++
		mu.Unlock()
		ctx, cancel := context.WithCancel(b.ctx)
		lease := time.AfterFunc(relayLease, cancel)
		ka, err := b.conn.Subscribe(r.Inbox+".ka", func(m *nats.Msg) {
			if string(m.Data) == "stop" {
				cancel()
			} else {
				lease.Reset(relayLease)
			}
		})
		if err != nil {
			cancel()
			mu.Lock()
			running--
			mu.Unlock()
			_ = req.Error("500", "stream subscription failed", nil)
			return
		}
		_ = req.Respond([]byte("ok"))
		go func() {
			defer func() {
				lease.Stop()
				_ = ka.Unsubscribe()
				cancel()
				mu.Lock()
				running--
				mu.Unlock()
			}()
			err := b.relay(ctx, r, open)
			end := nats.NewMsg(r.Inbox)
			end.Header.Set(relayEndHeader, "done")
			if err != nil && ctx.Err() == nil {
				message := "T3 stream failed"
				var public interface{ PublicMessage() string }
				if errors.As(err, &public) {
					message = public.PublicMessage()
				}
				end.Header.Set(relayEndHeader, message)
			}
			_ = b.conn.PublishMsg(end)
		}()
	}), micro.WithEndpointSubject("cos."+host+".stream"))
}

func (b *Bus) relay(ctx context.Context, r relayRequest, open func(context.Context) (*T3, error)) error {
	c, err := open(ctx)
	if err != nil {
		return err
	}
	publish := func(v any) error {
		data, err := json.Marshal(v)
		if err != nil {
			return err
		}
		if int64(len(data)) > b.conn.MaxPayload() {
			return OperationError("T3 event too large to relay")
		}
		return b.conn.Publish(r.Inbox, data)
	}
	if r.ThreadID == "" {
		return c.SubscribeShell(ctx, func(e ShellEvent) error { return publish(e) })
	}
	return c.SubscribeThread(ctx, r.ThreadID, func(e ThreadEvent) error {
		if p := e.Snapshot; p != nil {
			p.ProviderTurns, p.TurnItems = nil, pendingItems(p)
			if n := len(p.Messages); n > relaySnapshotMessages {
				p.Messages = p.Messages[n-relaySnapshotMessages:]
			}
		}
		return publish(e)
	})
}

// pendingItems keeps the turn items that carry the questions of pending runtime requests.
func pendingItems(p *Projection) []TurnItem {
	out := []TurnItem{}
	for _, item := range p.TurnItems {
		if item.RequestID != "" && item.Status != "completed" {
			out = append(out, item)
		}
	}
	return out
}

// StreamThread follows a thread on any host; it returns when ctx ends, fn fails, or the host stops.
func (b *Bus) StreamThread(ctx context.Context, host, threadID string, fn func(ThreadEvent) error) error {
	if threadID == "" {
		return errors.New("thread ID required")
	}
	return streamRelay(ctx, b, host, threadID, fn)
}

// StreamShell follows the thread list of any host.
func (b *Bus) StreamShell(ctx context.Context, host string, fn func(ShellEvent) error) error {
	return streamRelay(ctx, b, host, "", fn)
}

func streamRelay[E any](ctx context.Context, b *Bus, host, threadID string, fn func(E) error) error {
	if !validHost(host) {
		return errors.New("invalid COS host")
	}
	inbox := nats.NewInbox()
	sub, err := b.conn.SubscribeSync(inbox)
	if err != nil {
		return errors.New("COS stream subscription failed")
	}
	defer sub.Unsubscribe()
	defer b.conn.Publish(inbox+".ka", []byte("stop"))
	data, _ := json.Marshal(relayRequest{ThreadID: threadID, Inbox: inbox})
	start, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	reply, err := b.conn.RequestWithContext(start, "cos."+host+".stream", data)
	if err != nil {
		if errors.Is(err, nats.ErrNoResponders) {
			return fmt.Errorf("COS node %s unavailable", host)
		}
		return fmt.Errorf("COS node %s did not start the stream: %w", host, err)
	}
	if message := reply.Header.Get(micro.ErrorHeader); message != "" {
		return errors.New(message)
	}
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	go func() {
		keepalive := time.NewTicker(relayKeepalive)
		defer keepalive.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-keepalive.C:
				_ = b.conn.Publish(inbox+".ka", nil)
			}
		}
	}()
	for {
		m, err := sub.NextMsgWithContext(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("COS stream from %s interrupted", host)
		}
		if end := m.Header.Get(relayEndHeader); end == "done" {
			return fmt.Errorf("COS node %s ended the stream", host)
		} else if end != "" {
			return errors.New(end)
		}
		var e E
		if err := json.Unmarshal(m.Data, &e); err != nil {
			return errors.New("COS stream event could not be decoded")
		}
		if err := fn(e); err != nil {
			return err
		}
	}
}
