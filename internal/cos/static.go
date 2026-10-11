package cos

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"net/http"
	"strings"
)

//go:embed static/cos.css
var cosCSS []byte

// cssPath changes with the content, so browsers and Cloudflare may cache it forever.
var cssPath = func() string {
	sum := sha256.Sum256(cosCSS)
	return "/static/cos." + hex.EncodeToString(sum[:6]) + ".css"
}()

// serveCSS is public: the login page needs it. During a rolling deploy another
// node may ask for an older or newer hash; it gets this node's file uncached.
func serveCSS(rw http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, "/static/cos.") || !strings.HasSuffix(r.URL.Path, ".css") {
		http.NotFound(rw, r)
		return
	}
	rw.Header().Set("Content-Type", "text/css; charset=utf-8")
	rw.Header().Set("X-Content-Type-Options", "nosniff")
	if r.URL.Path == cssPath {
		rw.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	}
	_, _ = rw.Write(cosCSS)
}
