package cos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestChatTabsApply(t *testing.T) {
	c := NewChat()
	tab, err := json.Marshal(ChatTab{Name: "Deploys", Created: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	closed, err := json.Marshal(ChatTab{Name: "Old", Closed: true})
	if err != nil {
		t.Fatal(err)
	}
	tabbed, err := json.Marshal(ChatEntry{Role: "user", Text: "ship it", Tab: "0000000000000005-t"})
	if err != nil {
		t.Fatal(err)
	}
	c.apply(fakeChatEntry{key: chatTabPrefix + "0000000000000005-t", value: tab, op: jetstream.KeyValuePut})
	c.apply(fakeChatEntry{key: chatTabPrefix + "0000000000000004-o", value: closed, op: jetstream.KeyValuePut})
	c.apply(fakeChatEntry{key: "0000000000000006-a", value: tabbed, op: jetstream.KeyValuePut})
	c.apply(chatTestPut(t, "0000000000000007-b", "main"))
	c.apply(nil)

	tabs := c.Tabs()
	if len(tabs) != 2 || tabs[0].ID != "0000000000000004-o" || !tabs[0].Closed || tabs[1].ID != "0000000000000005-t" || tabs[1].Name != "Deploys" {
		t.Fatal(tabs)
	}
	if got, ok := c.Tab("0000000000000005-t"); !ok || got.Name != "Deploys" {
		t.Fatal(got, ok)
	}
	if _, ok := c.Entry(chatTabPrefix + "0000000000000005-t"); ok {
		t.Fatal("tab key stored as an entry")
	}
	if got := c.Entries("0000000000000005-t"); len(got) != 1 || got[0].Text != "ship it" {
		t.Fatal(got)
	}
	if got := c.Entries(chatMainTab); len(got) != 1 || got[0].ID != "0000000000000007-b" || got[0].Tab != chatMainTab {
		t.Fatal(got)
	}
	c.apply(fakeChatEntry{key: chatTabPrefix + "0000000000000005-t", op: jetstream.KeyValueDelete})
	if _, ok := c.Tab("0000000000000005-t"); ok {
		t.Fatal("deleted tab still present")
	}
}

func TestValidTab(t *testing.T) {
	for _, tc := range []struct {
		name, focus string
		ok          bool
	}{
		{"Deploys", "", true},
		{strings.Repeat("n", chatTabNameMax), strings.Repeat("f", chatTabFocusMax), true},
		{strings.Repeat("é", chatTabNameMax), "line one\nline two", true},
		{"", "", false},
		{strings.Repeat("n", chatTabNameMax+1), "", false},
		{"two\nlines", "", false},
		{"tab\x00", "", false},
		{"Deploys", strings.Repeat("f", chatTabFocusMax+1), false},
	} {
		err := validTab(ChatTab{Name: tc.name, Focus: tc.focus})
		if (err == nil) != tc.ok {
			t.Errorf("validTab(%q, %d focus runes) = %v, want ok=%v", tc.name, len([]rune(tc.focus)), err, tc.ok)
		}
	}
}

// TestChatTabGuards checks the refusals that need no bucket: the main tab, an empty id, a never-read tab and a store still starting.
func TestChatTabGuards(t *testing.T) {
	ctx := context.Background()
	c := NewChat()
	c.apply(chatTestPut(t, "0000000000000007-b", "main"))

	if err := c.DeleteTab(ctx, chatMainTab); err == nil {
		t.Fatal("main tab was deleted")
	}
	if err := c.DeleteTab(ctx, ""); err == nil {
		t.Fatal("empty tab id was deleted")
	}
	if got := c.Entries(chatMainTab); len(got) != 1 {
		t.Fatal("main entries swept by a refused delete", got)
	}

	kv := &fakeKV{data: map[string][]byte{}}
	c.kv = kv
	if err := c.UpdateTab(ctx, ChatTab{ID: chatMainTab, Name: "COS", rev: 3}); err == nil {
		t.Fatal("main tab was updated")
	}
	if err := c.UpdateTab(ctx, ChatTab{ID: "0000000000000005-t", Name: "Deploys"}); err == nil {
		t.Fatal("tab without a revision was updated")
	}
	if len(kv.data) != 0 {
		t.Fatal("refused update created a key", kv.data)
	}

	if _, err := c.CreateTab(ctx, "Deploys", ""); err == nil {
		t.Fatal("tab created before the store was ready")
	}
}

// closeFailT3 is an agent whose T3 cannot close the COS thread.
type closeFailT3 struct{ *agentTestT3 }

func (closeFailT3) CloseTree(context.Context, string, bool) (TreeResult, error) {
	return TreeResult{}, errors.New("T3 offline")
}

func TestAgentCloseSurfacesT3Error(t *testing.T) {
	ctx := context.Background()
	state, err := json.Marshal(AgentState{Host: "delta", ThreadID: "thread-1"})
	if err != nil {
		t.Fatal(err)
	}
	a := &Agent{Host: "delta", Tab: mainTab, Chat: NewChat(), KV: &fakeKV{data: map[string][]byte{"state": state}},
		T3: func(context.Context) (agentT3, error) { return closeFailT3{&agentTestT3{}}, nil }}
	err = a.close(ctx)
	if err == nil || !strings.Contains(err.Error(), "settle COS thread thread-1") || !strings.Contains(err.Error(), "T3 offline") {
		t.Fatal(err)
	}
}

// TestChatTabStore covers the replicated tab bucket: the open-tab limit, revision fencing and deleting a tab with its entries.
func TestChatTabStore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	nodes, _ := chatTestCluster(t, ctx)
	c := NewChat()
	go c.Run(ctx, nodes[0])
	// Every node follows the chat: the buckets are created by whichever node leads JetStream.
	for _, n := range nodes[1:] {
		go NewChat().Run(ctx, n)
	}
	chatTestWait(t, "chat ready", c.Ready)

	var first ChatTab
	for i := range chatMaxTabs {
		tab, err := c.CreateTab(ctx, fmt.Sprintf("tab %d", i), "")
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			first = tab
		}
		chatTestWait(t, "tab cached", func() bool { _, ok := c.Tab(tab.ID); return ok })
	}
	if _, err := c.CreateTab(ctx, "one too many", ""); err == nil {
		t.Fatal("tab number 13 was accepted")
	}

	renamed := first
	renamed.Name = "renamed"
	if err := c.UpdateTab(ctx, renamed); err != nil {
		t.Fatal(err)
	}
	if err := c.UpdateTab(ctx, first); err == nil {
		t.Fatal("rename with a stale revision was accepted")
	}

	cur, _ := c.Tab(first.ID)
	cur.Closed = true
	if err := c.UpdateTab(ctx, cur); err != nil {
		t.Fatal(err)
	}
	entry, err := c.Put(ctx, ChatEntry{Role: "user", Text: "hi", Tab: first.ID})
	if err != nil {
		t.Fatal(err)
	}
	chatTestWait(t, "tab entry cached", func() bool { _, ok := c.Entry(entry.ID); return ok })
	if err := c.DeleteTab(ctx, first.ID); err != nil {
		t.Fatal(err)
	}
	chatTestWait(t, "tab and its entries gone", func() bool {
		_, tabOK := c.Tab(first.ID)
		_, entryOK := c.Entry(entry.ID)
		return !tabOK && !entryOK
	})
}

func TestChatTabAgent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	e := newAgentTestEnv(t, ctx)
	e.reset(t)
	created, err := e.chat.CreateTab(ctx, "Deploys", "watch the release")
	if err != nil {
		t.Fatal(err)
	}
	chatTestWait(t, "tab cached", func() bool { _, ok := e.chat.Tab(created.ID); return ok })
	tab, _ := e.chat.Tab(created.ID)
	ta := e.a.forTab(tab)

	// The tab's message reaches only the tab's COS: the main COS launches nothing for it.
	entry, err := e.chat.Put(ctx, ChatEntry{Role: "user", Text: "what's deploying?", Tab: tab.ID})
	if err != nil {
		t.Fatal(err)
	}
	chatTestWait(t, "tab message in chat", func() bool { _, ok := e.chat.Entry(entry.ID); return ok })
	e.stepAgent(e.a)
	e.expectLaunches(0)
	// The first step claims the tab for this host; the next one launches its thread.
	e.stepAgent(ta)
	e.stepAgent(ta)
	e.expectLaunches(1)
	if title := e.fake.launches[0]["title"]; title != "COS · Deploys" {
		t.Fatal(title)
	}
	e.expectBrief(`You are the COS of the chat tab "Deploys". Focus: watch the release.`)
	if _, err := e.agentKV.Get(ctx, "state."+tab.ID); err != nil {
		t.Fatal(err)
	}
	if st := e.state(); st.ThreadID != "" {
		t.Fatal("main COS state took the tab's thread", st.ThreadID)
	}

	// The tab's reply is mirrored into the tab only.
	e.fake.projection = agentTestThread("completed", 0, "[]")
	e.stepAgent(ta)
	chatTestWait(t, "tab reply in chat", func() bool {
		for _, reply := range e.chat.Entries(tab.ID) {
			if reply.Role == "cos" && strings.Contains(reply.Text, "Hello") {
				return true
			}
		}
		return false
	})
	if n := len(e.cosEntries()); n != 0 {
		t.Fatalf("tab reply leaked into the main tab: %d entries", n)
	}

	// A main message goes to the main COS only, so the tab's COS sends nothing.
	e.say("status?")
	e.stepAgent(e.a)
	e.expectLaunches(2)
	if title := e.fake.launches[1]["title"]; title != "COS" {
		t.Fatal(title)
	}
	e.stepAgent(ta)
	if len(e.fake.sends) != 0 {
		t.Fatal("tab COS received a main message", e.fake.sends)
	}

	// Closing settles the tab's thread; the agent stays until the thread is settled.
	e.fake.skipClose = true
	cur, _ := e.chat.Tab(tab.ID)
	cur.Closed = true
	if err := e.chat.UpdateTab(ctx, cur); err != nil {
		t.Fatal(err)
	}
	chatTestWait(t, "closed tab cached", func() bool { got, ok := e.chat.Tab(tab.ID); return ok && got.Closed })
	agents := map[string]*Agent{tab.ID: ta}
	e.a.stepTabs(ctx, agents)
	if _, ok := agents[tab.ID]; !ok {
		t.Fatal("tab agent dropped while its thread still runs")
	}
	e.fake.skipClose = false
	e.a.stepTabs(ctx, agents)
	if _, ok := agents[tab.ID]; ok {
		t.Fatal("closed tab's agent still running")
	}
	if got := e.fake.closes[len(e.fake.closes)-1]; got.Thread != "thread-1" || got.Delete {
		t.Fatalf("closed thread = %+v, want thread-1 settled without delete", got)
	}
	if _, err := e.agentKV.Get(ctx, "state."+tab.ID); !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatal("tab state survived close:", err)
	}
	chatTestWait(t, "closed tab gone with its entries", func() bool {
		_, ok := e.chat.Tab(tab.ID)
		return !ok && len(e.chat.Entries(tab.ID)) == 0
	})
}
