package cos

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Every source publishes normalized events to cos.events.<source>.<kind>; the rules
// engine and the inbox consume them. Events are deduplicated by ID within the window.
const (
	EventsStream    = "COS_EVENTS"
	EventsSubjects  = "cos.events.>"
	eventsMaxAge    = 7 * 24 * time.Hour
	eventsDedup     = 24 * time.Hour
	eventsMaxBytes  = 256 << 20
	eventsMaxMsg    = 16 << 10
	eventSummaryMax = 200
	eventDataMax    = 4 << 10

	// A's webhook ingress publishes each verified request body unchanged with its
	// HTTP headers copied and Nats-Msg-Id set to the delivery ID; E normalizes it.
	IngestStream   = "COS_INGEST"
	IngestSubjects = "cos.ingest.>"
	IngestForgejo  = "cos.ingest.forgejo"
	ingestMaxAge   = 7 * 24 * time.Hour
	ingestMaxBytes = 256 << 20
	ingestMaxMsg   = 1 << 20

	// Every COS node holds a replica so any node keeps working when another is offline.
	eventsReplicas = 3
)

type EventSource string

const (
	SourceT3      EventSource = "t3"
	SourceForgejo EventSource = "forgejo"
	SourceUser    EventSource = "user"
	SourceInbox   EventSource = "inbox"
	SourceDeploy  EventSource = "deploy"
)

type EventKind string

const (
	// T3 live thread events, one per host.
	KindTurnCompleted   EventKind = "turn.completed"
	KindTurnFailed      EventKind = "turn.failed"
	KindTurnInterrupted EventKind = "turn.interrupted"
	KindRequestPending  EventKind = "request.pending" // runtime question or approval
	KindContextHigh     EventKind = "context.high"    // Data: {"percent": n}, once per high-water mark
	KindNeedsUser       EventKind = "worker.needs_user"

	// Forgejo, only PRs authored by mariogk.
	KindPROpened       EventKind = "pr.opened"
	KindPRSynchronized EventKind = "pr.synchronized"
	KindPRMergeable    EventKind = "pr.mergeable"
	KindPRConflict     EventKind = "pr.conflict"
	KindPRMerged       EventKind = "pr.merged"
	KindPRClosed       EventKind = "pr.closed"
	KindCIStatus       EventKind = "ci.status" // Data: {"state": "success|failure|pending|error", "context": ...}
	KindReview         EventKind = "review.submitted"
	KindPushMain       EventKind = "push.main"

	// Published by the dashboard chat (B) and the user Inbox (F).
	KindChatMessage    EventKind = "chat.message"
	KindUserAnswer     EventKind = "user.answer" // Data: AnswerData
	KindInboxItemAdded EventKind = "inbox.item_added"
	KindInboxClosed    EventKind = "inbox.item_closed"

	// Published by the rollout leader (TS-818). Data: {"rollout", "to", "change", "hosts", "from", "reason"}.
	KindDeployStarted    EventKind = "deploy.started"
	KindNodeUpdated      EventKind = "node.updated"
	KindGateFailed       EventKind = "gate.failed"
	KindDeployRolledBack EventKind = "deploy.rolled_back"
	KindDeployCompleted  EventKind = "deploy.completed"
)

var eventKinds = map[EventKind]EventSource{
	KindTurnCompleted: SourceT3, KindTurnFailed: SourceT3, KindTurnInterrupted: SourceT3,
	KindRequestPending: SourceT3, KindContextHigh: SourceT3, KindNeedsUser: SourceT3,
	KindPROpened: SourceForgejo, KindPRSynchronized: SourceForgejo, KindPRMergeable: SourceForgejo,
	KindPRConflict: SourceForgejo, KindPRMerged: SourceForgejo, KindPRClosed: SourceForgejo,
	KindCIStatus: SourceForgejo, KindReview: SourceForgejo, KindPushMain: SourceForgejo,
	KindChatMessage: SourceUser, KindUserAnswer: SourceUser,
	KindInboxItemAdded: SourceInbox, KindInboxClosed: SourceInbox,
	KindDeployStarted: SourceDeploy, KindNodeUpdated: SourceDeploy, KindGateFailed: SourceDeploy,
	KindDeployRolledBack: SourceDeploy, KindDeployCompleted: SourceDeploy,
}

// Severity is the source's hint; the rules engine makes the final classification.
type Severity string

const (
	SeverityInfo   Severity = "info"
	SeverityWarn   Severity = "warn"
	SeverityUrgent Severity = "urgent"
)

// Event is the normalized record. ID is "<source>:<source event id>" and becomes
// Nats-Msg-Id, so redelivered webhooks and reconciler re-emits are dropped.
type Event struct {
	ID        string          `json:"id"`
	Source    EventSource     `json:"source"`
	Kind      EventKind       `json:"kind"`
	Time      time.Time       `json:"time"`
	Severity  Severity        `json:"severity"`
	Summary   string          `json:"summary"`
	Host      string          `json:"host,omitempty"`
	Ticket    string          `json:"ticket,omitempty"`
	Thread    string          `json:"thread,omitempty"`
	Repo      string          `json:"repo,omitempty"` // owner/name
	PR        int             `json:"pr,omitempty"`
	Item      string          `json:"item,omitempty"` // Inbox item ID
	Synthetic bool            `json:"synthetic,omitempty"`
	Data      json.RawMessage `json:"data,omitempty"`
}

// AnswerData is the payload of user.answer: free text, a chosen option, or both.
type AnswerData struct {
	Text   string `json:"text,omitempty"`
	Option string `json:"option,omitempty"`
}

func (e Event) Subject() string {
	return "cos.events." + string(e.Source) + "." + string(e.Kind)
}

func (e Event) Validate() error {
	switch {
	case len(e.ID) > 256 || !validEventID(e.ID, e.Source):
		return errors.New("event ID must be <source>:<source event id>, at most 256 bytes")
	case eventKinds[e.Kind] == "" || eventKinds[e.Kind] != e.Source:
		return fmt.Errorf("unknown event kind %q for source %q", e.Kind, e.Source)
	case e.Time.IsZero():
		return errors.New("event time is required")
	case e.Severity != SeverityInfo && e.Severity != SeverityWarn && e.Severity != SeverityUrgent:
		return fmt.Errorf("unknown event severity %q", e.Severity)
	case e.Summary == "" || utf8.RuneCountInString(e.Summary) > eventSummaryMax || strings.IndexFunc(e.Summary, notGraphic) >= 0:
		return fmt.Errorf("event summary must be one printable line of at most %d characters", eventSummaryMax)
	case len(e.Host) > 64 || (e.Host != "" && !validHost(e.Host)):
		return errors.New("invalid event host")
	case len(e.Ticket) > 64 || len(e.Thread) > 256 || len(e.Repo) > 256 || len(e.Item) > 256:
		return errors.New("event ticket, thread, repo or item reference is too long")
	case e.PR < 0 || (e.PR != 0 && e.Repo == ""):
		return errors.New("event PR requires a repo")
	case e.Kind == KindUserAnswer && (e.Item == "" || !hasAnswer(e.Data)):
		return errors.New("user.answer requires an Inbox item and answer text or a chosen option")
	case strings.HasPrefix(string(e.Kind), "pr.") && e.PR == 0:
		return fmt.Errorf("%s requires a repo and PR number", e.Kind)
	case len(e.Data) > eventDataMax || (len(e.Data) != 0 && !json.Valid(e.Data)):
		return fmt.Errorf("event data must be JSON of at most %d bytes", eventDataMax)
	}
	return nil
}

// notGraphic rejects control, format and line/paragraph separator runes, which can
// break lines or inject terminal escapes when a summary is printed.
func notGraphic(r rune) bool { return !unicode.IsGraphic(r) }

// validEventID requires a non-empty printable source event ID, since it becomes Nats-Msg-Id.
func validEventID(id string, source EventSource) bool {
	rest, ok := strings.CutPrefix(id, string(source)+":")
	return ok && rest != "" && strings.IndexFunc(rest, notGraphic) < 0
}

func hasAnswer(data json.RawMessage) bool {
	var a AnswerData
	return json.Unmarshal(data, &a) == nil && (a.Text != "" || a.Option != "")
}

// EventStreamConfigs are the fixed definitions of the event and ingest streams.
func EventStreamConfigs() []jetstream.StreamConfig {
	return []jetstream.StreamConfig{{
		Name: EventsStream, Subjects: []string{EventsSubjects},
		Retention: jetstream.LimitsPolicy, Discard: jetstream.DiscardOld,
		Storage: jetstream.FileStorage, Replicas: eventsReplicas,
		MaxAge: eventsMaxAge, MaxBytes: eventsMaxBytes, MaxMsgSize: eventsMaxMsg,
		Duplicates: eventsDedup, DenyDelete: true, DenyPurge: true,
	}, {
		// Work queue: each raw delivery is normalized once by whichever node pulls it.
		// When full, new deliveries are refused so the ingress fails visibly and the
		// heartbeat reconciler re-fetches, instead of silently dropping the oldest.
		Name: IngestStream, Subjects: []string{IngestSubjects},
		Retention: jetstream.WorkQueuePolicy, Discard: jetstream.DiscardNew,
		Storage: jetstream.FileStorage, Replicas: eventsReplicas,
		MaxAge: ingestMaxAge, MaxBytes: ingestMaxBytes, MaxMsgSize: ingestMaxMsg,
		Duplicates: eventsDedup,
	}}
}

// EnsureEventStreams creates or updates both streams; it needs the JetStream meta
// leader, so callers retry while the cluster forms.
func EnsureEventStreams(ctx context.Context, js jetstream.JetStream) error {
	for _, cfg := range EventStreamConfigs() {
		if _, err := js.CreateOrUpdateStream(ctx, cfg); err != nil {
			return fmt.Errorf("COS stream %s: %w", cfg.Name, err)
		}
	}
	return nil
}

// PublishEvent validates e and stores it once; duplicate reports a deduplicated ID.
func PublishEvent(ctx context.Context, js jetstream.JetStream, e Event) (duplicate bool, err error) {
	if err := e.Validate(); err != nil {
		return false, err
	}
	data, err := json.Marshal(e)
	if err != nil {
		return false, err
	}
	msg := nats.NewMsg(e.Subject())
	msg.Data = data
	ack, err := js.PublishMsg(ctx, msg, jetstream.WithMsgID(e.ID), jetstream.WithExpectStream(EventsStream))
	if err != nil {
		return false, fmt.Errorf("COS event publish: %w", err)
	}
	return ack.Duplicate, nil
}

// JetStream exposes the bus connection's JetStream API to event producers and consumers.
func (b *Bus) JetStream() (jetstream.JetStream, error) {
	return jetstream.New(b.conn)
}

// NewEventID joins source event ID parts, hashing IDs too long for Nats-Msg-Id.
func NewEventID(source EventSource, parts ...string) string {
	id := string(source) + ":" + strings.Join(parts, ":")
	if len(id) > 256 || strings.IndexFunc(id, notGraphic) >= 0 {
		sum := sha256.Sum256([]byte(id))
		id = string(source) + ":" + hex.EncodeToString(sum[:20])
	}
	return id
}

// EventText makes external text a valid one-line summary or payload field of at most n runes.
func EventText(s string, n int) string {
	s = strings.Join(strings.FieldsFunc(s, func(r rune) bool { return unicode.IsSpace(r) || notGraphic(r) }), " ")
	if r := []rune(s); len(r) > n {
		s = string(r[:n-1]) + "…"
	}
	return s
}
