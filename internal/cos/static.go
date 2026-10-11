package cos

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"mime"
	"net/http"
	"path"
	"strings"
)

//go:embed static
var staticFS embed.FS

var cosCSS, _ = staticFS.ReadFile("static/cos.css") // embedded at build time, cannot fail

// cssPath changes with the content, so browsers and Cloudflare may cache it forever.
var cssPath = func() string {
	sum := sha256.Sum256(cosCSS)
	return "/static/cos." + hex.EncodeToString(sum[:6]) + ".css"
}()

// serveStatic is public: the login page needs it. During a rolling deploy another
// node may ask for an older or newer hash; it gets this node's file uncached.
func serveStatic(rw http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/static/")
	if name == "cos.css" || strings.Contains(name, "/") || strings.HasPrefix(name, ".") {
		http.NotFound(rw, r)
		return
	}
	if strings.HasPrefix(name, "cos.") && strings.HasSuffix(name, ".css") {
		rw.Header().Set("Content-Type", "text/css; charset=utf-8")
		rw.Header().Set("X-Content-Type-Options", "nosniff")
		if r.URL.Path == cssPath {
			rw.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		}
		_, _ = rw.Write(cosCSS)
		return
	}
	data, err := staticFS.ReadFile("static/" + name)
	if err != nil {
		http.NotFound(rw, r)
		return
	}
	rw.Header().Set("Content-Type", mime.TypeByExtension(path.Ext(name)))
	rw.Header().Set("X-Content-Type-Options", "nosniff")
	rw.Header().Set("Cache-Control", "public, max-age=86400")
	_, _ = rw.Write(data)
}
