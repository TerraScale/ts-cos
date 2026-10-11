package cos

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestChangeText(t *testing.T) {
	for _, tc := range []struct{ note, subject, want string }{
		{"", "TS-818: Rolling canary deploys (#16)", "Rolling canary deploys"},
		{"The Inbox groups items by ticket", "TS-817: Group inbox (#30)", "The Inbox groups items by ticket"},
		{"", "Fix login\x1b[31m", "Fix login [31m"},
		{"", "TS-1: x (#2) and more", "x (#2) and more"},
	} {
		if got := changeText(tc.note, tc.subject); got != tc.want {
			t.Fatalf("changeText(%q, %q) = %q, want %q", tc.note, tc.subject, got, tc.want)
		}
	}
}

func readSSE(t *testing.T, lines *bufio.Scanner) string {
	t.Helper()
	var event []string
	for lines.Scan() {
		if lines.Text() == "" {
			return strings.Join(event, "\n")
		}
		event = append(event, lines.Text())
	}
	t.Fatal("SSE stream ended")
	return ""
}

func TestReleaseFeedStreamsOnlyKnownReleases(t *testing.T) {
	f := &releaseFeed{clients: map[chan struct{}]struct{}{}}
	srv := httptest.NewServer(http.HandlerFunc(f.events))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	lines := bufio.NewScanner(resp.Body)
	if got := readSSE(t, lines); got != ": release unknown" {
		t.Fatalf("first event %q; a tab must not learn a release before the cluster has one", got)
	}
	f.set(Release{Revision: testRevision(2), Change: "New thing"})
	want := `event: datastar-patch-signals` + "\n" + `data: signals {"_release":"` + testRevision(2) + `"}`
	if got := readSSE(t, lines); got != want {
		t.Fatalf("event %q, want %q", got, want)
	}
}

func TestVersionEndpointAndReleaseWatch(t *testing.T) {
	r, _ := bootstrapTest(t)
	f := &releaseFeed{cfg: Config{Home: strings.TrimSuffix(string(r), "/.local/share/cos"), LocalHost: "beta"}, clients: map[chan struct{}]struct{}{}}
	f.set(Release{Revision: testRevision(3), Change: "Deploys reload open tabs"})
	rec := httptest.NewRecorder()
	f.version(rec, httptest.NewRequest(http.MethodGet, "/version", nil))
	var v struct {
		Build   Build   `json:"build"`
		Release Release `json:"release"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil || v.Build.Host != "beta" || v.Release.Revision != testRevision(3) {
		t.Fatalf("version = %s, %v", rec.Body.String(), err)
	}

	old := clusterRelease
	defer func() { clusterRelease = old }()
	clusterRelease = f
	var page bytes.Buffer
	if err := releaseWatch().Render(context.Background(), &page); err != nil {
		t.Fatal(err)
	}
	html := page.String()
	for _, want := range []string{`/release&#39;, {retry: &#39;always&#39;, retryMaxCount: 1000}`, `&#34;_releaseSeen&#34;:&#34;` + testRevision(3), "Updated: Deploys reload open tabs", "location.reload()"} {
		if !strings.Contains(html, want) {
			t.Fatalf("release watch lacks %q:\n%s", want, html)
		}
	}
}
