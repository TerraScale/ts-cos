package cos

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	boardEventsKeep  = 30 * 24 * time.Hour
	boardLiveTTL     = 30 * time.Second
	mirrorRetry      = 5 * time.Second
	mirrorRetryMax   = 30 * time.Second
	mirrorErrorAfter = 5
	mirrorHealthy    = time.Minute
	kindDeployDone   = "deploy.completed"

	// Fixed width so SQLite's text order matches time order.
	eventTimeFormat = "2006-01-02T15:04:05.000000000Z"
)

// eventMirror copies COS_EVENTS into the events table and attributes each event to a ticket.
type eventMirror struct {
	store   *Store
	live    Live
	prs     map[string]string // repo#number -> ticket; keeps merged PRs that left the open list
	at      time.Time
	handled int // messages mirrored by the current run
}

// RunEventMirror keeps the events table in step with COS_EVENTS so the board never calls T3 or Forgejo.
func RunEventMirror(ctx context.Context, bus *Bus, store *Store) {
	m := &eventMirror{store: store, prs: map[string]string{}}
	var prune sync.WaitGroup
	defer prune.Wait()
	prune.Go(func() {
		tick := time.NewTicker(time.Hour)
		defer tick.Stop()
		for {
			if err := store.pruneEvents(ctx, time.Now()); err != nil && ctx.Err() == nil {
				logErr(ctx, slog.LevelError, "prune events", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
		}
	})
	var failures int
	for ctx.Err() == nil {
		m.handled = 0
		start := time.Now()
		err := m.run(ctx, bus)
		if m.handled > 0 || time.Since(start) >= mirrorHealthy {
			failures = 0
		}
		wait := mirrorRetry
		if err != nil && ctx.Err() == nil {
			failures++
			var level slog.Level
			wait, level = mirrorBackoff(failures)
			slog.Log(ctx, level, "mirror events failed; retrying", "error", err, "in", wait, "failures", failures)
		}
		select {
		case <-ctx.Done():
		case <-time.After(wait):
		}
	}
}

// mirrorBackoff is the wait after the nth consecutive mirror failure and how loudly to log it.
func mirrorBackoff(failures int) (time.Duration, slog.Level) {
	wait := min(mirrorRetry<<min(max(failures-1, 0), 3), mirrorRetryMax)
	if failures < mirrorErrorAfter {
		return wait, slog.LevelWarn
	}
	return wait, slog.LevelError
}

func (m *eventMirror) run(ctx context.Context, bus *Bus) error {
	js, err := bus.JetStream()
	if err != nil {
		return err
	}
	var last int64
	if err := m.store.DB.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq),0) FROM events`).Scan(&last); err != nil {
		return err
	}
	stream, err := js.Stream(ctx, EventsStream)
	if err != nil {
		return err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return err
	}
	if info.State.LastSeq < uint64(last) {
		// A recreated stream restarts its sequences, so the mirrored rows no longer match it.
		if _, err := m.store.DB.ExecContext(ctx, `DELETE FROM events`); err != nil {
			return err
		}
		slog.Warn("COS events stream restarted; re-mirroring")
		m.prs, last = map[string]string{}, 0
	}
	cfg := jetstream.OrderedConsumerConfig{FilterSubjects: []string{EventsSubjects}, DeliverPolicy: jetstream.DeliverAllPolicy}
	if last > 0 {
		cfg.DeliverPolicy, cfg.OptStartSeq = jetstream.DeliverByStartSequencePolicy, uint64(last+1)
	}
	cons, err := js.OrderedConsumer(ctx, EventsStream, cfg)
	if err != nil {
		return err
	}
	it, err := cons.Messages()
	if err != nil {
		return err
	}
	defer it.Stop()
	defer context.AfterFunc(ctx, it.Stop)()
	for {
		msg, err := it.Next()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if err := m.handle(ctx, msg); err != nil {
			return err
		}
	}
}

func (m *eventMirror) handle(ctx context.Context, msg jetstream.Msg) error {
	var e Event
	if err := json.Unmarshal(msg.Data(), &e); err != nil || e.ID == "" || e.Time.IsZero() {
		slog.Warn("skip undecodable event", "subject", msg.Subject(), "error", err)
		return nil
	}
	md, err := msg.Metadata()
	if err != nil {
		return err
	}
	ticket, err := m.ticket(ctx, e)
	if err != nil {
		return err
	}
	if err := m.store.insertEvent(ctx, int64(md.Sequence.Stream), e, ticket); err != nil {
		return err
	}
	m.handled++
	return nil
}

func (m *eventMirror) ticket(ctx context.Context, e Event) (string, error) {
	if time.Since(m.at) >= boardLiveTTL {
		live, err := m.store.Live(ctx)
		if err != nil {
			return "", err
		}
		m.live, m.at = live, time.Now()
		for _, p := range live.PRs {
			if ts := prTickets(p); len(ts) > 0 {
				m.prs[prKey(forgejoFullName(p.Repo), p.Number)] = ts[0]
			}
		}
	}
	ticket := eventTicket(e, m.live, m.prs)
	if ticket != "" && e.Repo != "" && e.PR != 0 {
		m.prs[prKey(e.Repo, e.PR)] = ticket
	}
	return ticket, nil
}

// eventTicket names the ticket an event belongs to: its own, else its PR's, else its thread's.
func eventTicket(e Event, live Live, prs map[string]string) string {
	if e.Ticket != "" {
		return strings.ToUpper(e.Ticket)
	}
	if e.PR != 0 {
		if t := prs[prKey(e.Repo, e.PR)]; t != "" {
			return t
		}
	}
	if e.Thread != "" {
		for _, h := range live.Fleet {
			for _, th := range h.Threads {
				if h.Host == e.Host && th.ThreadID == e.Thread {
					if ts := threadTickets(th); len(ts) > 0 {
						return ts[0]
					}
					return ""
				}
			}
		}
	}
	return ""
}

func prKey(repo string, number int) string {
	return repo + "#" + strconv.Itoa(number)
}

func (s *Store) insertEvent(ctx context.Context, seq int64, e Event, ticket string) error {
	e.Ticket = ticket
	payload, err := json.Marshal(e)
	if err != nil {
		return err
	}
	// Plain Exec, not change: mirrored events must not bump the dashboard revision.
	_, err = s.DB.ExecContext(ctx, `INSERT OR IGNORE INTO events(seq,id,time,ticket,kind,payload) VALUES(?,?,?,?,?,?)`,
		seq, e.ID, e.Time.UTC().Format(eventTimeFormat), ticket, string(e.Kind), string(payload))
	return err
}

func (s *Store) pruneEvents(ctx context.Context, now time.Time) error {
	_, err := s.DB.ExecContext(ctx, `DELETE FROM events WHERE time < ?`, now.Add(-boardEventsKeep).UTC().Format(eventTimeFormat))
	return err
}

// TicketEvents returns a ticket's events, newest first.
func (s *Store) TicketEvents(ctx context.Context, ticket string, limit int) ([]Event, error) {
	return s.queryEvents(ctx, `SELECT payload FROM events WHERE ticket=? ORDER BY time DESC, seq DESC LIMIT ?`, strings.ToUpper(ticket), limit)
}

// EventsOfKind returns one kind's events since a time, oldest first.
func (s *Store) EventsOfKind(ctx context.Context, kind EventKind, since time.Time) ([]Event, error) {
	return s.queryEvents(ctx, `SELECT payload FROM events WHERE kind=? AND time>=? ORDER BY time`, string(kind), since.UTC().Format(eventTimeFormat))
}

// LatestEvents summarizes every ticket's events and the latest cluster-wide deploy.
func (s *Store) LatestEvents(ctx context.Context) (map[string]BoardEventSpan, time.Time, error) {
	events, err := s.queryEvents(ctx, `SELECT payload FROM events WHERE ticket<>'' ORDER BY time, seq`)
	if err != nil {
		return nil, time.Time{}, err
	}
	spans := map[string]BoardEventSpan{}
	for _, e := range events {
		span := spans[e.Ticket]
		if span.First.IsZero() {
			span.First = e.Time
		}
		span.Last, span.LastKind, span.LastSummary = e.Time, string(e.Kind), e.Summary
		if e.Kind == KindPRMerged {
			span.Merged = append(span.Merged, mergedPR{Repo: e.Repo, Number: e.PR, Time: e.Time})
		}
		spans[e.Ticket] = span
	}
	var raw string
	err = s.DB.QueryRowContext(ctx, `SELECT time FROM events WHERE kind=? ORDER BY time DESC LIMIT 1`, kindDeployDone).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return spans, time.Time{}, nil
	}
	if err != nil {
		return nil, time.Time{}, err
	}
	deployed, err := time.Parse(eventTimeFormat, raw)
	return spans, deployed, err
}

func (s *Store) queryEvents(ctx context.Context, query string, args ...any) ([]Event, error) {
	rows, err := s.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Event{}
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var e Event
		if err := json.Unmarshal([]byte(raw), &e); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
