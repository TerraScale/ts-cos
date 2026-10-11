package cos

import (
	"bytes"
	"context"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestClockText(t *testing.T) {
	now := time.Now().UTC()
	today := time.Date(now.Year(), now.Month(), now.Day(), 9, 5, 0, 0, time.UTC)
	other := time.Date(now.Year(), now.Month(), now.Day()-2, 14, 30, 0, 0, time.UTC)
	for _, c := range []struct {
		name string
		at   time.Time
		want string
	}{
		{"zero", time.Time{}, "—"},
		{"today", today, "09:05 UTC"},
		{"another day", other, other.Format("Jan 2") + " 14:30 UTC"},
	} {
		if got := clock(c.at); got != c.want {
			t.Errorf("%s: clock = %q, want %q", c.name, got, c.want)
		}
	}
}

// Header text must not show a raw RFC3339 stamp; the datetime attribute may.
var rawStampText = regexp.MustCompile(`>[^<]*T\d\d:\d\d:\d\dZ`)

func TestChatHeaderLocalTimes(t *testing.T) {
	beat := time.Now().UTC().Add(-5 * time.Minute).Truncate(time.Second)
	var buf bytes.Buffer
	if err := chatHead(chatHeader{Autopilot: AutopilotState{Interval: 15, LastBeat: beat.Format(time.RFC3339)}}, "chat").Render(context.Background(), &buf); err != nil {
		t.Fatal(err)
	}
	head := buf.String()
	if !strings.Contains(head, `<time datetime="`) || rawStampText.MatchString(head) {
		t.Fatalf("header with a beat:\n%s", head)
	}
	buf.Reset()
	if err := chatHead(chatHeader{Autopilot: AutopilotState{Interval: 15}}, "chat").Render(context.Background(), &buf); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "after the leader's first check") {
		t.Fatalf("header before the first beat:\n%s", buf.String())
	}
}

func TestSettingsLocalTimes(t *testing.T) {
	beat := time.Now().UTC().Add(-5 * time.Minute).Truncate(time.Second)
	a := AutopilotState{Interval: 15, LastBeat: beat.Format(time.RFC3339), Log: []AutopilotEntry{{At: beat.Format(time.RFC3339), Host: "delta", Kind: "beat", Action: "Heartbeat"}}}
	var buf bytes.Buffer
	if err := settingsPage(chatHeader{}, a, budgetPanelData{}, PushState{}).Render(context.Background(), &buf); err != nil {
		t.Fatal(err)
	}
	page := buf.String()
	// One for the meta line's last and next, one for the Recent actions row.
	if n := strings.Count(page, `<time datetime="`); n != 3 {
		t.Fatalf("settings has %d <time> elements, want 3", n)
	}
	if rawStampText.MatchString(page) {
		t.Fatal("settings shows a raw RFC3339 stamp as text")
	}
	buf.Reset()
	if err := settingsPage(chatHeader{}, AutopilotState{Interval: 15}, budgetPanelData{}, PushState{}).Render(context.Background(), &buf); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "last —") {
		t.Fatal("settings with no beat does not show a dash for last")
	}
}

func TestTopbarCurrentPage(t *testing.T) {
	for _, page := range []string{"chat", "inbox", "work", "nodes", "tokens", "settings"} {
		var buf bytes.Buffer
		if err := chatHead(chatHeader{}, page).Render(context.Background(), &buf); err != nil {
			t.Fatal(err)
		}
		html := buf.String()
		if !strings.Contains(html, `<header id="chat-head" class="ts-topbar"`) {
			t.Fatalf("%s: top bar markup missing", page)
		}
		if n := strings.Count(html, `aria-current="page"`); n != 1 {
			t.Fatalf("%s: %d current links, want 1", page, n)
		}
	}
}
