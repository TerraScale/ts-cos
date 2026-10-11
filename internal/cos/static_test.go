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
	// Not a {file} match, so it falls to the auth gate; it must never be served as a 200.
	if rec := get("/static/css/tokens.css"); rec.Code == http.StatusOK {
		t.Fatalf("unbundled css served = %d", rec.Code)
	}
	if rec := get("/static/oxanium-latin-wght-normal.woff2"); rec.Code != 200 || rec.Header().Get("Content-Type") != "font/woff2" {
		t.Fatalf("font = %d %q", rec.Code, rec.Header().Get("Content-Type"))
	}
	for file, typ := range map[string]string{"icon.svg": "image/svg+xml", "icon-192.png": "image/png", "badge.svg": "image/svg+xml", "badge-96.png": "image/png"} {
		rec := get("/static/" + file)
		if rec.Code != 200 || rec.Header().Get("Content-Type") != typ || rec.Header().Get("Cache-Control") != "public, max-age=86400" ||
			rec.Header().Get("X-Content-Type-Options") != "nosniff" {
			t.Fatalf("GET %s = %d %v", file, rec.Code, rec.Header())
		}
	}
}

func TestCSSBundleLayersAndFiles(t *testing.T) {
	const order = "@layer base, layout, legacy, components, features, utilities;\n"
	if !strings.HasPrefix(string(cosCSS), order) {
		t.Fatalf("bundle starts with %q", string(cosCSS)[:min(len(cosCSS), 80)])
	}
	// The first rule of each file must reach the bundle, so a missing file or wrong wrapper fails here.
	firstRule := regexp.MustCompile(`(?m)^[^\s/*][^\n]*\{$`)
	for _, file := range []string{"tokens.css", "base.css", "layout.css", "legacy.css", "shell.css", "controls.css", "surfaces.css", "data.css", "overlays.css", "auth.css", "cos.css"} {
		data, err := staticFS.ReadFile("static/css/" + file)
		if err != nil {
			t.Fatal(err)
		}
		rule := firstRule.FindString(string(data))
		if rule == "" || !strings.Contains(string(cosCSS), rule) {
			t.Errorf("%s: first rule %q missing from bundle", file, rule)
		}
	}
	for _, wrap := range []string{"@layer base {", "@layer layout {", "@layer legacy {", "@layer components {", "@layer features {"} {
		if !strings.Contains(string(cosCSS), wrap) {
			t.Errorf("bundle lacks %q", wrap)
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
		// Datastar's data-show needs an initial display:none; any other inline style belongs in cos.css.
		if strings.Count(src, "style=") != strings.Count(src, `style="display:none"`) {
			t.Errorf("%s has an inline style=; use cos.css classes", file.Name())
		}
	}
}
