package cos

import (
	"encoding/json"
	"fmt"
	"image/png"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPWAPublicAssets(t *testing.T) {
	handler := NewAuth(t.TempDir(), nil).Protect(http.NotFoundHandler(), http.NotFoundHandler())
	get := func(path string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest("GET", path, nil) // no session cookie: installers are anonymous
		req.RemoteAddr = "127.0.0.1:1234"
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: %d %s", path, rec.Code, rec.Body)
		}
		if rec.Header().Get("X-Content-Type-Options") != "nosniff" {
			t.Fatalf("%s: missing nosniff", path)
		}
		return rec
	}

	sw := get("/sw.js")
	if sw.Header().Get("Content-Type") != "text/javascript; charset=utf-8" || sw.Header().Get("Cache-Control") != "no-cache" || sw.Header().Get("Service-Worker-Allowed") != "/" {
		t.Fatal(sw.Header())
	}
	if strings.Contains(sw.Body.String(), "__VERSION__") {
		t.Fatal("__VERSION__ placeholder left in sw.js")
	}

	var manifest struct {
		StartURL string `json:"start_url"`
		Scope    string `json:"scope"`
		Display  string `json:"display"`
		Icons    []struct {
			Src   string `json:"src"`
			Sizes string `json:"sizes"`
			Type  string `json:"type"`
		} `json:"icons"`
	}
	rec := get("/manifest.webmanifest")
	if rec.Header().Get("Content-Type") != "application/manifest+json" {
		t.Fatal(rec.Header())
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.StartURL != "/" || manifest.Scope != "/" || manifest.Display != "standalone" || len(manifest.Icons) != 4 {
		t.Fatal(manifest)
	}
	for _, icon := range manifest.Icons {
		asset := get(icon.Src)
		if asset.Header().Get("Content-Type") != icon.Type {
			t.Fatal(icon, asset.Header())
		}
		if icon.Type == "image/png" {
			cfg, err := png.DecodeConfig(asset.Body)
			if err != nil || icon.Sizes != fmt.Sprintf("%dx%d", cfg.Width, cfg.Height) {
				t.Fatal(icon, cfg, err)
			}
		}
	}

	if offline := get("/pwa/offline"); offline.Header().Get("Content-Type") != "text/html; charset=utf-8" {
		t.Fatal(offline.Header())
	}
}
