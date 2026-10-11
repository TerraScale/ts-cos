package cos

import (
	"context"
	"encoding/json"
	"log/slog"
	"reflect"
	"testing"
	"time"
)

func TestPushRecordLast(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	buses, _ := chatTestCluster(t, ctx)
	p := pushTestStores(t, ctx, buses)[0]
	p.recordLast(ctx, Event{Source: SourceInbox, Kind: KindInboxItemAdded, ID: "ev1", Item: "a1", Severity: SeverityWarn}, 2)
	p.recordLast(ctx, Event{Source: "deploy", Kind: "deploy.completed", ID: "ev2"}, 5)
	var got pushLast
	chatTestWait(t, "last inbox record", func() bool {
		entry, err := p.kv.Get(ctx, pushLastKey)
		return err == nil && json.Unmarshal(entry.Value(), &got) == nil && got.Event == "ev1"
	})
	if got.Item != "a1" || got.Severity != "warn" || got.Sent != 2 || got.At.IsZero() {
		t.Fatalf("last inbox record = %+v", got)
	}
}

func TestNoticeFor(t *testing.T) {
	approval := json.RawMessage(`{"kind":"approval"}`)
	owner := inboxTestUser().ID
	cases := []struct {
		name  string
		event Event
		want  Notice
		ok    bool
	}{
		{"approval", Event{Source: SourceInbox, Kind: KindInboxItemAdded, Severity: SeverityUrgent, Summary: "Deploy?", Item: "a1", User: owner, Data: approval},
			Notice{Category: "inbox", User: owner, Title: "Approval needed", Body: "Deploy?", URL: "/inbox#inbox-a1", Tag: "inbox-a1", Urgent: true}, true},
		{"untitled item", Event{Source: SourceInbox, Kind: KindInboxItemAdded, Severity: SeverityWarn, Item: "a3", User: owner},
			Notice{Category: "inbox", User: owner, Title: "Inbox", URL: "/inbox#inbox-a3", Tag: "inbox-a3"}, true},
		{"inbox item without its user is dropped", Event{Source: SourceInbox, Kind: KindInboxItemAdded, Severity: SeverityWarn, Item: "a4"}, Notice{}, false},
		{"info item is silent", Event{Source: SourceInbox, Kind: KindInboxItemAdded, Severity: SeverityInfo, Item: "a2"}, Notice{}, false},
		{"deploy", Event{Source: "deploy", Kind: "deploy.completed", Summary: "cos 1.2"},
			Notice{Category: "done", Title: "Updated", Body: "cos 1.2", URL: "/", Tag: "deploy"}, true},
		{"rollback", Event{Source: "deploy", Kind: "deploy.rolled_back", Summary: "cos 1.2"},
			Notice{Category: "done", Title: "Rolled back", Body: "cos 1.2", URL: "/", Tag: "deploy"}, true},
		{"node warning", Event{Source: "node", Kind: "node.alert", Severity: SeverityWarn, Host: "delta", Summary: "disk 91%"},
			Notice{Category: "node", Title: "Node alert · delta", Body: "disk 91%", URL: "/nodes#delta", Tag: "node-delta"}, true},
		{"node info is silent", Event{Source: "node", Kind: "node.alert", Severity: SeverityInfo, Host: "delta"}, Notice{}, false},
		{"answers only close", Event{Source: SourceUser, Kind: KindUserAnswer, Item: "a1", Severity: SeverityUrgent}, Notice{}, false},
	}
	for _, tc := range cases {
		got, ok := noticeFor(tc.event)
		if ok != tc.ok || !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: got %+v, %v; want %+v, %v", tc.name, got, ok, tc.want, tc.ok)
		}
	}
}

func TestFirstLine(t *testing.T) {
	for in, want := range map[string]string{
		"":                     "",
		"## Status\nall good":  "Status",
		"\n\n> quoted reply":   "quoted reply",
		"- first item\nsecond": "first item",
		"plain sentence":       "plain sentence",
	} {
		if got := firstLine(in); got != want {
			t.Errorf("firstLine(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestPushOutageLevel(t *testing.T) {
	for down, want := range map[time.Duration]slog.Level{
		0:                           slog.LevelWarn,
		2 * time.Minute:             slog.LevelWarn,
		2*time.Minute + time.Second: slog.LevelError,
	} {
		if got := pushOutageLevel(down); got != want {
			t.Errorf("pushOutageLevel(%s) = %s, want %s", down, got, want)
		}
	}
}
