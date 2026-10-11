package cos

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// chatTestWait polls like busTestReady with room for JetStream elections on a loaded CI runner.
func chatTestWait(t *testing.T, description string, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for !check() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", description)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// chatTestCluster starts three embedded COS nodes routed together.
func chatTestCluster(t *testing.T, ctx context.Context) ([]*Bus, []Config) {
	t.Helper()
	return chatTestClusterHandled(t, ctx, func(context.Context, string, []string) (any, error) { return nil, nil })
}

// chatTestClusterHandled is chatTestCluster with the operations every node answers.
func chatTestClusterHandled(t *testing.T, ctx context.Context, handle OperationHandler) ([]*Bus, []Config) {
	t.Helper()
	ports := busTestPorts(t, 6)
	hosts := []string{"delta", "beta", "mariogk-dev"}
	nodes := make([]*Bus, 3)
	configs := make([]Config, 3)
	nodesCfg := make([]busNode, 3)
	for i, host := range hosts {
		var peers []int
		for j := range hosts {
			if i != j {
				peers = append(peers, ports[j+3])
			}
		}
		var bn busNode
		configs[i], bn = busTestConfig(t, host, ports[i], ports[i+3], peers)
		nodesCfg[i] = bn
	}
	for i := range hosts {
		bus, err := startBus(ctx, configs[i], nodesCfg[i], handle)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(bus.Close)
		nodes[i] = bus
	}
	return nodes, configs
}

// chatTestUser is a COS user whose chat a test writes and reads.
func chatTestUser() User { return User{ID: newULID(), Name: "owner", Role: roleMember} }

// asUserHandler serves h with u in every request's context, as the dashboard's sign-in does.
func asUserHandler(u User, h http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		h.ServeHTTP(rw, r.WithContext(WithUser(r.Context(), u)))
	})
}

// chatTestAwait waits for check to hold, re-evaluating it on every change the chat notifies
// (puts, deletes, ready, resets), so the outcome does not depend on wall-clock polling.
func chatTestAwait(t *testing.T, c *Chat, description string, check func() bool) {
	t.Helper()
	sub, stop := c.SubscribeAll()
	defer stop()
	timeout := time.After(2 * time.Minute) // generous: 3-node elections are slow on a loaded runner
	for !check() {
		select {
		case <-sub.wake:
			c.Changed(sub)
		case <-timeout:
			t.Fatalf("timeout waiting for %s", description)
		}
	}
}

func TestChatReplicatesAcrossNodes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	nodes, _ := chatTestCluster(t, ctx)
	u := chatTestUser()
	uctx := WithUser(ctx, u)
	chats := make([]*Chat, len(nodes))
	for i, bus := range nodes {
		chats[i] = NewChat()
		chats[i].recheck = 100 * time.Millisecond
		go chats[i].Run(ctx, bus)
	}
	for _, c := range chats {
		chatTestAwait(t, c, "chat ready", c.Ready)
	}
	e, err := chats[0].Put(uctx, ChatEntry{Role: "user", Text: "what's running?"})
	if err != nil {
		t.Fatal(err)
	}
	// The raw bucket holds the entry under its owner's prefix.
	raw, err := openKV(ctx, nodes[0], chatBucket)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Get(ctx, u.Key(e.ID)); err != nil {
		t.Fatal("entry is not under the owner's key:", err)
	}
	chatTestAwait(t, chats[2], "entry on another node", func() bool {
		got, ok := chats[2].Entry(uctx, e.ID)
		return ok && got.Text == "what's running?"
	})

	// A deleted and recreated bucket (re-bootstrap) must not freeze any node's mirror.
	js, err := jetstream.New(nodes[0].conn)
	if err != nil {
		t.Fatal(err)
	}
	if err := js.DeleteKeyValue(ctx, chatBucket); err != nil {
		t.Fatal(err)
	}
	// Every node notices the lost bucket and drops its mirror; the leader then recreates the bucket.
	for _, c := range chats {
		chatTestAwait(t, c, "mirror reset after the bucket was deleted", func() bool {
			_, ok := c.Entry(uctx, e.ID)
			return !ok
		})
	}
	for _, c := range chats {
		chatTestAwait(t, c, "chat ready on the recreated bucket", c.Ready)
	}
	again, err := chats[1].Put(uctx, ChatEntry{Role: "user", Text: "still there?"})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range chats {
		chatTestAwait(t, c, "new entry on every node", func() bool {
			_, ok := c.Entry(uctx, again.ID)
			return ok
		})
	}
}

// fakeChatEntry is a KeyValueEntry for feeding Chat.apply without NATS.
type fakeChatEntry struct {
	key   string
	value []byte
	op    jetstream.KeyValueOp
}

func (f fakeChatEntry) Bucket() string                  { return chatBucket }
func (f fakeChatEntry) Key() string                     { return f.key }
func (f fakeChatEntry) Value() []byte                   { return f.value }
func (f fakeChatEntry) Revision() uint64                { return 1 }
func (f fakeChatEntry) Created() time.Time              { return time.Time{} }
func (f fakeChatEntry) Delta() uint64                   { return 0 }
func (f fakeChatEntry) Operation() jetstream.KeyValueOp { return f.op }

func chatTestPut(t *testing.T, key, text string) fakeChatEntry {
	t.Helper()
	data, err := json.Marshal(ChatEntry{Role: "user", Text: text})
	if err != nil {
		t.Fatal(err)
	}
	return fakeChatEntry{key: key, value: data, op: jetstream.KeyValuePut}
}

func TestChatApplyAndOrder(t *testing.T) {
	u := chatTestUser()
	ctx := WithUser(context.Background(), u)
	c := NewChat()
	c.apply(chatTestPut(t, u.Key("0000000000000002-b"), "second"))
	c.apply(chatTestPut(t, u.Key("0000000000000001-a"), "first"))
	if c.Ready() {
		t.Fatal("ready before initial values marker")
	}
	c.apply(nil)
	if !c.Ready() {
		t.Fatal("not ready after initial values marker")
	}
	entries := c.Entries(ctx)
	if len(entries) != 2 || entries[0].ID != "0000000000000001-a" || entries[1].ID != "0000000000000002-b" {
		t.Fatal(entries)
	}
	if e, ok := c.Entry(ctx, "0000000000000002-b"); !ok || e.Text != "second" {
		t.Fatal(e, ok)
	}
	c.apply(fakeChatEntry{key: u.Key("0000000000000001-a"), op: jetstream.KeyValueDelete})
	if _, ok := c.Entry(ctx, "0000000000000001-a"); ok {
		t.Fatal("deleted entry still present")
	}
	c.apply(fakeChatEntry{key: u.Key("0000000000000003-c"), value: []byte("{not json"), op: jetstream.KeyValuePut})
	if _, ok := c.Entry(ctx, "0000000000000003-c"); ok {
		t.Fatal("invalid entry was stored")
	}
	for i := range 205 {
		c.apply(chatTestPut(t, u.Key(fmt.Sprintf("%016x-n", i+10)), fmt.Sprint(i)))
	}
	entries = c.Entries(ctx)
	if len(entries) != chatShown || entries[0].ID != fmt.Sprintf("%016x-n", 15) || entries[len(entries)-1].ID != fmt.Sprintf("%016x-n", 214) {
		t.Fatal(len(entries), entries[0].ID, entries[len(entries)-1].ID)
	}
	sub, stop := c.Subscribe(ctx)
	defer stop()
	c.apply(chatTestPut(t, u.Key("0000000000000009-z"), "changed"))
	select {
	case <-sub.wake:
	default:
		t.Fatal("no wake signal after put")
	}
	if got := c.Changed(sub); !slices.Equal(got, []string{"0000000000000009-z"}) {
		t.Fatal(got)
	}
	if got := c.Changed(sub); len(got) != 0 {
		t.Fatal("changed keys not cleared", got)
	}
}

// TestChatUsersAreIsolated: one user's entries, wake-ups and reads never reach another user.
func TestChatUsersAreIsolated(t *testing.T) {
	a, b := chatTestUser(), chatTestUser()
	actx, bctx := WithUser(context.Background(), a), WithUser(context.Background(), b)
	c := NewChat()
	c.apply(nil)
	c.apply(chatTestPut(t, a.Key("0000000000000001-a"), "for a"))
	c.apply(chatTestPut(t, "0000000000000009-legacy", "unscoped"))
	if got := c.Entries(bctx); len(got) != 0 {
		t.Fatal("B sees A's entries:", got)
	}
	if _, ok := c.Entry(bctx, "0000000000000001-a"); ok {
		t.Fatal("B reads A's entry")
	}
	if got := c.Entries(actx); len(got) != 1 || got[0].ID != "0000000000000001-a" || got[0].Text != "for a" {
		t.Fatal(got)
	}
	if _, ok := c.entriesAll()["0000000000000009-legacy"]; ok {
		t.Fatal("a legacy key was applied before migration")
	}
	bsub, stopB := c.Subscribe(bctx)
	defer stopB()
	asub, stopA := c.Subscribe(actx)
	defer stopA()
	c.apply(chatTestPut(t, a.Key("0000000000000002-a"), "more"))
	select {
	case <-bsub.wake:
		t.Fatal("A's change woke B")
	default:
	}
	if got := c.Changed(asub); !slices.Equal(got, []string{"0000000000000002-a"}) {
		t.Fatal("A's change keys:", got)
	}
	if _, err := c.Put(context.Background(), ChatEntry{Role: "user", Text: "no user"}); !errors.Is(err, ErrNoUser) {
		t.Fatal("Put without a user:", err)
	}
}

// TestChatWebIsolation: a signed-in user's pages, answers and posts never reach another user's chat.
func TestChatWebIsolation(t *testing.T) {
	bus, _ := releaseTestBus(t)
	ctx := t.Context()
	chat := NewChat()
	go chat.Run(ctx, bus)
	chatTestWait(t, "chat ready", chat.Ready)
	store, err := OpenStore(filepath.Join(t.TempDir(), "cos.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	web := NewWeb(store, nil)
	web.Chat = chat
	routes := web.routes()
	a, b := chatTestUser(), chatTestUser()
	actx, bctx := WithUser(ctx, a), WithUser(ctx, b)
	ask := ChatAsk{Host: "delta", ThreadID: "thread-1", RequestID: "q1", Questions: []ChatQuestion{{ID: "pick", Header: "Deploy", Text: "Which window?", Options: []string{"Now", "Tonight"}}}}
	e, err := chat.Put(actx, ChatEntry{Role: "cos", Text: "marker-from-a", Ask: &ask})
	if err != nil {
		t.Fatal(err)
	}
	chatTestWait(t, "A's entry", func() bool { _, ok := chat.Entry(actx, e.ID); return ok })

	page := httptest.NewRequest("GET", "/", nil).WithContext(bctx)
	rec := httptest.NewRecorder()
	routes.ServeHTTP(rec, page)
	if strings.Contains(rec.Body.String(), "marker-from-a") {
		t.Fatal("B's page shows A's entry")
	}

	answer := httptest.NewRequest("POST", "/chat/answer?e="+e.ID+"&q=pick&o=1", nil).WithContext(bctx)
	answer.RemoteAddr = "127.0.0.1:4000"
	answer.Header.Set("Datastar-Request", "true")
	rec = httptest.NewRecorder()
	routes.ServeHTTP(rec, answer)
	if rec.Code != http.StatusNotFound {
		t.Fatal("B answered A's question:", rec.Code)
	}
	if cur, _ := chat.Entry(actx, e.ID); len(cur.Ask.Answers) != 0 {
		t.Fatal("A's question changed:", cur.Ask.Answers)
	}

	post := httptest.NewRequest("POST", "/chat", strings.NewReader(`{"msg":"hi from b"}`)).WithContext(bctx)
	post.Header.Set("Datastar-Request", "true")
	rec = httptest.NewRecorder()
	routes.ServeHTTP(rec, post)
	chatTestWait(t, "B's message in B's chat", func() bool {
		for _, entry := range chat.Entries(bctx) {
			if entry.Text == "hi from b" {
				return true
			}
		}
		return false
	})
	for _, entry := range chat.Entries(actx) {
		if entry.Text == "hi from b" {
			t.Fatal("B's message reached A's chat")
		}
	}
	raw, err := openKV(ctx, bus, chatBucket)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := raw.Keys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(keys, func(k string) bool { return strings.HasPrefix(k, b.Key("")) }) {
		t.Fatal("B's message is not under B's prefix:", keys)
	}
}

func TestChatSSEPatches(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store, err := OpenStore(filepath.Join(t.TempDir(), "cos.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	web := NewWeb(store, nil)
	u := chatTestUser()
	web.ownerID.Store(&u.ID) // the owner's stream also patches the node-T3 header, KPIs and Work
	// Seed one entry first: an append needs a non-empty client view, otherwise the stream repaints.
	web.Chat.apply(chatTestPut(t, u.Key("0000000000000001-seed"), "seed"))
	web.Chat.apply(nil)
	server := httptest.NewServer(asUserHandler(u, web.routes()))
	defer server.Close()
	req, _ := http.NewRequestWithContext(ctx, "GET", server.URL+"/chat/events", nil)
	response, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	reader := bufio.NewReader(response.Body)
	readEvent := func() string {
		t.Helper()
		var event strings.Builder
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				t.Fatal(err)
			}
			event.WriteString(line)
			if line == "\n" {
				return event.String()
			}
		}
	}
	if header := readEvent(); !strings.Contains(header, `<header id="chat-head"`) {
		t.Fatal(header)
	}
	if tiles := readEvent(); !strings.Contains(tiles, `id="kpis"`) {
		t.Fatal(tiles)
	}
	for _, badge := range []string{`id="side-inbox"`, `id="tab-inbox"`} {
		if event := readEvent(); !strings.Contains(event, badge) {
			t.Fatal(event)
		}
	}
	if nodes := readEvent(); !strings.Contains(nodes, `id="pane-nodes"`) {
		t.Fatal(nodes)
	}
	if work := readEvent(); !strings.Contains(work, `id="pane-work"`) {
		t.Fatal(work)
	}
	if inboxPane := readEvent(); !strings.Contains(inboxPane, `id="inbox"`) {
		t.Fatal(inboxPane)
	}
	if messages := readEvent(); !strings.Contains(messages, `id="m-0000000000000001-seed"`) {
		t.Fatal(messages)
	}

	key := "0000000000000002-new"
	web.Chat.apply(chatTestPut(t, u.Key(key), "hello"))
	appended := readEvent()
	if !strings.Contains(appended, "data: selector #messages\n") || !strings.Contains(appended, "data: mode append\n") || !strings.Contains(appended, `id="m-`+key+`"`) {
		t.Fatal(appended)
	}
	// Each batch ends with the chatTick that scrolls the log.
	if tick := readEvent(); !strings.Contains(tick, "chatTick") {
		t.Fatal(tick)
	}

	web.Chat.apply(chatTestPut(t, u.Key(key), "hello again"))
	morphed := readEvent()
	if !strings.Contains(morphed, `id="m-`+key+`"`) || !strings.Contains(morphed, "hello again") || strings.Contains(morphed, "mode append") {
		t.Fatal(morphed)
	}
	readEvent()

	web.Chat.apply(fakeChatEntry{key: u.Key(key), op: jetstream.KeyValueDelete})
	removed := readEvent()
	if !strings.Contains(removed, "data: selector #m-"+key+"\n") || !strings.Contains(removed, "data: mode remove\n") {
		t.Fatal(removed)
	}
	readEvent()
}

func TestRenderMarkdownSafe(t *testing.T) {
	got := renderMarkdown("**b** <script>alert(1)</script> [x](javascript:alert(1))")
	if !strings.Contains(got, "<strong>b</strong>") || strings.Contains(got, "<script>") || strings.Contains(got, "javascript:") {
		t.Fatal(got)
	}
	for _, link := range []string{"[x](JavaScript:alert(1))", "[x](DATA:text/html,hi)", "![x](VBScript:x)", "<JavaScript:alert(1)>"} {
		if got := strings.ToLower(renderMarkdown(link)); strings.Contains(got, "javascript:") && strings.Contains(got, "href") || strings.Contains(got, `href="data:`) || strings.Contains(got, `src="vbscript:`) {
			t.Fatal(link, got)
		}
	}
	if got := renderMarkdown("[ok](https://git.terrascale.tech/x) [rel](/status)"); !strings.Contains(got, `href="https://git.terrascale.tech/x"`) || !strings.Contains(got, `href="/status"`) {
		t.Fatal(got)
	}
	if table := renderMarkdown("| a | b |\n| - | - |\n| 1 | 2 |"); !strings.Contains(table, "<table>") {
		t.Fatal(table)
	}
}

func TestRenderMarkdownLinkSchemes(t *testing.T) {
	attr := regexp.MustCompile(`(?:href|src)="([^"]*)"`)
	// An empty or colon-free value is relative, so it cannot carry a scheme.
	safe := func(v string) bool {
		if !strings.Contains(v, ":") {
			return true
		}
		for _, prefix := range []string{"https://", "http://", "mailto:", "#", "/"} {
			if strings.HasPrefix(v, prefix) {
				return true
			}
		}
		return false
	}
	unsafe := []struct{ name, markdown string }{
		{"javascript", "[x](javascript:alert(1))"},
		{"javascript mixed case", "[x](JavaScript:alert(1))"},
		{"javascript upper case", "[x](JAVASCRIPT:alert(1))"},
		{"javascript leading spaces", "[x](  javascript:alert(1))"},
		{"javascript angle brackets", "[x](<javascript:alert(1)>)"},
		{"javascript decimal entity", "[x](java&#115;cript:alert(1))"},
		{"javascript hex entity", "[x](java&#x73;cript:alert(1))"},
		{"javascript encoded colon", "[x](javascript&#58;alert(1))"},
		{"javascript named entity colon", "[x](javascript&colon;alert(1))"},
		{"javascript percent letter", "[x](%6Aavascript:alert(1))"},
		{"data base64", "[x](data:text/html;base64,PHNjcmlwdD5hbGVydCgxKTwvc2NyaXB0Pg==)"},
		{"data upper case", "[x](DATA:text/html,<script>alert(1)</script>)"},
		{"data image svg", "![x](data:image/svg+xml;base64,PHN2Zz48L3N2Zz4=)"},
		{"vbscript", "[x](vbscript:msgbox(1))"},
		{"vbscript mixed case", "[x](VBScript:msgbox(1))"},
		{"file", "[x](file:///etc/passwd)"},
		{"autolink javascript", "<javascript:alert(1)>"},
		{"autolink mixed case", "<JavaScript:alert(1)>"},
		{"reference definition", "[ref]\n\n[ref]: javascript:alert(1)"},
		{"raw html anchor", `<a href="javascript:alert(1)">x</a>`},
		{"raw html image", "<img src=x onerror=alert(1)>"},
	}
	for _, tc := range unsafe {
		t.Run(tc.name, func(t *testing.T) {
			got := renderMarkdown(tc.markdown)
			for _, m := range attr.FindAllStringSubmatch(got, -1) {
				v := strings.ToLower(html.UnescapeString(m[1]))
				v = strings.Map(func(r rune) rune {
					if r <= ' ' || r == 0x7f {
						return -1
					}
					return r
				}, v)
				if decoded, err := url.PathUnescape(v); err == nil {
					v = decoded
				}
				if !safe(v) {
					t.Fatalf("%q rendered unsafe attribute %q (decoded %q)\noutput: %s", tc.markdown, m[1], v, got)
				}
			}
		})
	}
	t.Run("raw html is escaped", func(t *testing.T) {
		got := renderMarkdown(`<a href="javascript:alert(1)">x</a> <img src=x onerror=alert(1)>`)
		if strings.Contains(got, `<a href="javascript`) || strings.Contains(got, "<img") {
			t.Fatal(got)
		}
	})

	safeCases := []struct{ name, markdown, want string }{
		{"https link", "[ok](https://git.terrascale.tech/TerraScale/ts-cos/pulls/8)", `href="https://git.terrascale.tech/TerraScale/ts-cos/pulls/8"`},
		{"mailto link", "[mail](mailto:a@b.c)", `href="mailto:a@b.c"`},
		{"anchor link", "[anchor](#inbox)", `href="#inbox"`},
		{"root relative link", "[rel](/status)", `href="/status"`},
		{"relative path link", "[plain](docs/readme.md)", `href="docs/readme.md"`},
		{"https autolink", "<https://example.com>", `href="https://example.com"`},
	}
	for _, tc := range safeCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := renderMarkdown(tc.markdown); !strings.Contains(got, tc.want) {
				t.Fatalf("%q lost %s\noutput: %s", tc.markdown, tc.want, got)
			}
		})
	}
}

func TestChatAnswerPartial(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	u := chatTestUser()
	uctx := WithUser(ctx, u)
	nodes, _ := chatTestCluster(t, ctx)
	chat := NewChat()
	go chat.Run(ctx, nodes[0])
	for _, n := range nodes[1:] {
		go NewChat().Run(ctx, n)
	}
	chatTestWait(t, "chat ready", chat.Ready)
	store, err := OpenStore(filepath.Join(t.TempDir(), "cos.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	web := NewWeb(store, nil)
	web.Chat = chat
	questions := []ChatQuestion{
		{ID: "pick", Header: "Deploy", Text: "Which window?", Options: []string{"Now", "Tonight"}},
		{ID: "scope", Header: "Scope", Text: "How much?", Options: []string{"Small", "Large"}},
	}
	put := func(ask ChatAsk) string {
		t.Helper()
		e, err := chat.Put(uctx, ChatEntry{Role: "cos", Ask: &ask})
		if err != nil {
			t.Fatal(err)
		}
		chatTestWait(t, "ask entry", func() bool { _, ok := chat.Entry(uctx, e.ID); return ok })
		return e.ID
	}
	post := func(query string, datastar bool) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest("POST", "/chat/answer?"+query, nil)
		req.RemoteAddr = "127.0.0.1:4000"
		if datastar {
			req.Header.Set("Datastar-Request", "true")
		}
		rec := httptest.NewRecorder()
		web.routes().ServeHTTP(rec, req.WithContext(uctx))
		return rec
	}
	id := put(ChatAsk{Host: "delta", ThreadID: "thread-1", RequestID: "q1", Questions: questions})

	t.Run("first answer is kept and the ask stays open", func(t *testing.T) {
		if rec := post("e="+id+"&q=pick&o=1", true); rec.Code != http.StatusNoContent {
			t.Fatal(rec.Code, rec.Body.String())
		}
		chatTestWait(t, "partial answer", func() bool {
			e, ok := chat.Entry(uctx, id)
			return ok && e.Ask.Answers["pick"] == "Tonight" && !e.Ask.Done
		})
	})

	t.Run("missing Datastar header is forbidden", func(t *testing.T) {
		if rec := post("e="+id+"&q=scope&o=0", false); rec.Code != http.StatusForbidden {
			t.Fatal(rec.Code)
		}
		e, _ := chat.Entry(uctx, id)
		if _, ok := e.Ask.Answers["scope"]; ok {
			t.Fatal("rejected request stored an answer")
		}
	})

	t.Run("unknown option or question is a bad request", func(t *testing.T) {
		if rec := post("e="+id+"&q=scope&o=5", true); rec.Code != http.StatusBadRequest {
			t.Fatal("unknown option:", rec.Code)
		}
		if rec := post("e="+id+"&q=nope&o=0", true); rec.Code != http.StatusBadRequest {
			t.Fatal("unknown question:", rec.Code)
		}
	})

	t.Run("answered ask is a conflict", func(t *testing.T) {
		done := put(ChatAsk{Host: "delta", ThreadID: "thread-1", RequestID: "q2", Questions: questions, Done: true})
		if rec := post("e="+done+"&q=pick&o=0", true); rec.Code != http.StatusConflict {
			t.Fatal(rec.Code)
		}
	})

	t.Run("answers to one ask go through back to back", func(t *testing.T) {
		three := append(slices.Clone(questions), ChatQuestion{ID: "when", Header: "When", Text: "Which day?", Options: []string{"Mon", "Tue"}})
		multi := put(ChatAsk{Host: "delta", ThreadID: "thread-1", RequestID: "q3", Questions: three})
		for _, q := range []string{"pick", "scope"} {
			if rec := post("e="+multi+"&q="+q+"&o=0", true); rec.Code != http.StatusNoContent {
				t.Fatal(q, rec.Code, rec.Body.String())
			}
		}
		if rec := post("e="+multi+"&q=pick&o=1", true); rec.Code != http.StatusConflict {
			t.Fatal("re-answer", rec.Code, rec.Body.String())
		}
		e, _ := chat.Entry(uctx, multi)
		if len(e.Ask.Answers) != 2 || e.Ask.Done {
			t.Fatalf("ask = %+v", e.Ask)
		}
	})

	t.Run("last answer needs the COS bus", func(t *testing.T) {
		seen, _ := chat.Entry(uctx, id)
		bare := NewChat()
		bare.kv = chat.kv
		bare.entries[u.Key(id)] = seen
		web.Chat = bare
		defer func() { web.Chat = chat }()
		if rec := post("e="+id+"&q=scope&o=0", true); rec.Code != http.StatusServiceUnavailable {
			t.Fatal(rec.Code)
		}
		// The answer was fenced in, then rolled back because T3 was never reached.
		e, _ := bare.Entry(uctx, id)
		if e.Ask.Done || len(e.Ask.Answers) != 1 {
			t.Fatalf("ask = %+v", e.Ask)
		}
	})
}

// Two answers to the last question race: the revision fence lets one write win, and only the winner reaches T3.
func TestChatAnswerFence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	u := chatTestUser()
	uctx := WithUser(ctx, u)
	var mu sync.Mutex
	responds := 0
	nodes, _ := chatTestClusterHandled(t, ctx, func(_ context.Context, command string, _ []string) (any, error) {
		if command == "respond" {
			mu.Lock()
			responds++
			mu.Unlock()
		}
		return nil, nil
	})
	chat := NewChat()
	go chat.Run(ctx, nodes[0])
	for _, n := range nodes[1:] {
		go NewChat().Run(ctx, n)
	}
	chatTestWait(t, "chat ready", chat.Ready)
	store, err := OpenStore(filepath.Join(t.TempDir(), "cos.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	web := NewWeb(store, nil)
	web.Chat = chat
	questions := []ChatQuestion{
		{ID: "pick", Header: "Deploy", Text: "Which window?", Options: []string{"Now", "Tonight"}},
		{ID: "scope", Header: "Scope", Text: "How much?", Options: []string{"Small", "Large"}},
	}
	e, err := chat.Put(uctx, ChatEntry{Role: "cos", Ask: &ChatAsk{Host: "delta", ThreadID: "thread-1", RequestID: "q1", Questions: questions}})
	if err != nil {
		t.Fatal(err)
	}
	chatTestWait(t, "ask entry", func() bool { _, ok := chat.Entry(uctx, e.ID); return ok })
	post := func(query string) int {
		t.Helper()
		req := httptest.NewRequest("POST", "/chat/answer?"+query, nil)
		req.RemoteAddr = "127.0.0.1:4000"
		req.Header.Set("Datastar-Request", "true")
		rec := httptest.NewRecorder()
		web.routes().ServeHTTP(rec, req.WithContext(uctx))
		return rec.Code
	}
	if code := post("e=" + e.ID + "&q=pick&o=1"); code != http.StatusNoContent {
		t.Fatal("first answer:", code)
	}
	codes := make(chan int, 2)
	var wg sync.WaitGroup
	for _, o := range []string{"0", "1"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			codes <- post("e=" + e.ID + "&q=scope&o=" + o)
		}()
	}
	wg.Wait()
	close(codes)
	var got []int
	for code := range codes {
		got = append(got, code)
	}
	slices.Sort(got)
	if !slices.Equal(got, []int{http.StatusNoContent, http.StatusConflict}) {
		t.Fatal("racing answers:", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if responds != 1 {
		t.Fatalf("respond deliveries = %d, want 1", responds)
	}
}
