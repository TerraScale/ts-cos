package cos

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestChatTrimRemovesWindowedOut checks that an entry leaving the chatShown window is removed from the open log, and stays gone when edited.
func TestChatTrimRemovesWindowedOut(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store, err := OpenStore(filepath.Join(t.TempDir(), "cos.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	web := NewWeb(store, nil)
	const oldest = "0000000000000000-oldest"
	web.Chat.apply(chatTestPut(t, oldest, "oldest"))
	web.Chat.apply(nil)
	server := httptest.NewServer(web.routes())
	defer server.Close()
	req, _ := http.NewRequestWithContext(ctx, "GET", server.URL+"/chat/events", nil)
	response, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	reader := bufio.NewReader(response.Body)
	// readUntil returns every event read up to and including the first one containing marker.
	readUntil := func(marker string) string {
		t.Helper()
		var read, event strings.Builder
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				t.Fatalf("stream ended before %q: %v", marker, err)
			}
			event.WriteString(line)
			if line != "\n" {
				continue
			}
			read.WriteString(event.String())
			if strings.Contains(event.String(), marker) {
				return read.String()
			}
			event.Reset()
		}
	}
	readUntil(`id="m-` + oldest + `"`)

	for i := 1; i <= chatShown; i++ {
		web.Chat.apply(chatTestPut(t, fmt.Sprintf("%016x-new", i), "new"))
	}
	removed := readUntil("selector #m-" + oldest + "\n")
	if !strings.Contains(removed, "data: mode remove\n") {
		t.Fatal(removed)
	}

	// The cache still holds the trimmed entry; an edit must not bring it back into the log.
	const after = "ffffffffffffffff-after"
	web.Chat.apply(chatTestPut(t, oldest, "edited"))
	web.Chat.apply(chatTestPut(t, after, "after"))
	if seen := readUntil(`id="m-` + after + `"`); strings.Contains(seen, `id="m-`+oldest+`"`) {
		t.Fatal("trimmed entry re-appended on edit:\n" + seen)
	}
}

// TestHomeScrollAttributes checks the log keeps itself at the newest message and the composer has its notice slot.
func TestHomeScrollAttributes(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "cos.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	server := httptest.NewServer(NewWeb(store, nil).routes())
	defer server.Close()
	res, err := server.Client().Get(server.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	out, _ := io.ReadAll(res.Body)
	body := string(out)
	for _, want := range []string{
		`data-signals="{stick:true,chatTick:0}"`,
		`data-on:scroll="$stick = el.scrollHeight`,
		`data-effect="$chatTick; $stick`,
		`<p id="chat-notice" class="toast" role="status"></p>`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("home lacks %s", want)
		}
	}
}
