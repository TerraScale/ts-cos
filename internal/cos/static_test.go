package cos

import (
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"testing"
)

func TestCSSServedPublicWithCacheHeaders(t *testing.T) {
	h := (&Auth{}).Protect(http.NotFoundHandler(), http.NotFoundHandler())
	get := func(path string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest("GET", path, nil)
		req.RemoteAddr = "127.0.0.1:1234"
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}
	if !regexp.MustCompile(`^/static/cos\.[0-9a-f]{12}\.css$`).MatchString(cssPath) {
		t.Fatal("bad cssPath", cssPath)
	}
	if rec := get(cssPath); rec.Code != 200 || rec.Header().Get("Content-Type") != "text/css; charset=utf-8" ||
		rec.Header().Get("Cache-Control") != "public, max-age=31536000, immutable" || rec.Body.String() != string(cosCSS) {
		t.Fatalf("GET %s = %d %v", cssPath, rec.Code, rec.Header())
	}
	// A stale hash from another node during a rolling deploy is served uncached.
	if rec := get("/static/cos.000000000000.css"); rec.Code != 200 || rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("stale hash = %d %q", rec.Code, rec.Header().Get("Cache-Control"))
	}
	if rec := get("/static/other.js"); rec.Code != http.StatusNotFound {
		t.Fatalf("other static file = %d", rec.Code)
	}
	if rec := get("/static/cos.css"); rec.Code != http.StatusNotFound {
		t.Fatalf("unhashed css = %d", rec.Code)
	}
	if rec := get("/static/..%2fstatic.go"); rec.Code != http.StatusNotFound {
		t.Fatalf("traversal = %d", rec.Code)
	}
	if rec := get("/static/missing.png"); rec.Code != http.StatusNotFound {
		t.Fatalf("missing png = %d", rec.Code)
	}
	for file, typ := range map[string]string{"icon.svg": "image/svg+xml", "icon-192.png": "image/png", "badge.svg": "image/svg+xml", "badge-96.png": "image/png"} {
		rec := get("/static/" + file)
		if rec.Code != 200 || rec.Header().Get("Content-Type") != typ || rec.Header().Get("Cache-Control") != "public, max-age=86400" ||
			rec.Header().Get("X-Content-Type-Options") != "nosniff" {
			t.Fatalf("GET %s = %d %v", file, rec.Code, rec.Header())
		}
	}
}

func TestNoExternalStylesheets(t *testing.T) {
	files, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".templ") {
			continue
		}
		data, err := os.ReadFile(file.Name())
		if err != nil {
			t.Fatal(err)
		}
		src := strings.ToLower(string(data))
		for _, banned := range []string{"pico", "<style", `rel="stylesheet" href="`} {
			if strings.Contains(src, banned) {
				t.Errorf("%s contains %q; link cos.css with href={ templ.SafeURL(cssPath) }", file.Name(), banned)
			}
		}
	}
}
