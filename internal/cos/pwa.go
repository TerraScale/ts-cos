package cos

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
)

//go:embed pwa/sw.js
var pwaSource string

//go:embed pwa/offline.html
var pwaOffline string

//go:embed pwa/push.js
var pwaPush string

// pwaSW binds the cache name to a hash of everything a visitor caches, so a content change installs a new worker.
var pwaSW = func() string {
	sum := sha256.Sum256([]byte(pwaSource + pwaOffline + pwaPush))
	return strings.ReplaceAll(pwaSource, "__VERSION__", hex.EncodeToString(sum[:])[:12])
}()

// Icons are the design system's files under /static/, so the manifest and the app share one set.
var pwaManifest, _ = json.Marshal(map[string]any{
	"id":               "/",
	"name":             "COS · TerraScale",
	"short_name":       "COS",
	"description":      "TerraScale Chief of Staff",
	"start_url":        "/",
	"scope":            "/",
	"display":          "standalone",
	"background_color": "#f7f8fa",
	"theme_color":      "#f7f8fa",
	"icons": []map[string]string{
		{"src": "/static/icon-192.png", "sizes": "192x192", "type": "image/png", "purpose": "any"},
		{"src": "/static/icon-512.png", "sizes": "512x512", "type": "image/png", "purpose": "any"},
		{"src": "/static/icon-maskable-512.png", "sizes": "512x512", "type": "image/png", "purpose": "maskable"},
		{"src": "/static/icon.svg", "sizes": "any", "type": "image/svg+xml", "purpose": "any"},
	},
})

const pwaDay = "public, max-age=86400"

// servePWA registers installer assets. They are public because installers fetch them without cookies; none carries data.
func servePWA(mux *http.ServeMux) {
	asset := func(contentType, cache string, body func() []byte) http.HandlerFunc {
		return func(rw http.ResponseWriter, r *http.Request) {
			rw.Header().Set("X-Content-Type-Options", "nosniff")
			rw.Header().Set("Content-Type", contentType)
			rw.Header().Set("Cache-Control", cache)
			rw.Write(body())
		}
	}
	text := func(s string) func() []byte { return func() []byte { return []byte(s) } }
	sw := asset("text/javascript; charset=utf-8", "no-cache", text(pwaSW))
	mux.HandleFunc("GET /sw.js", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Service-Worker-Allowed", "/")
		sw(rw, r)
	})
	mux.HandleFunc("GET /manifest.webmanifest", asset("application/manifest+json", pwaDay, func() []byte { return pwaManifest }))
	mux.HandleFunc("GET /pwa/offline", asset("text/html; charset=utf-8", "no-cache", text(pwaOffline)))
	mux.HandleFunc("GET /pwa/push.js", asset("text/javascript; charset=utf-8", "no-cache", text(pwaPush)))
}
