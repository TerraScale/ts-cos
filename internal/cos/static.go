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

// cosCSS is the whole stylesheet in one file, bundled here because ts-cos has no npm build step.
var cosCSS = buildCSS()

// buildCSS wraps each ts-dashboard file in its cascade layer; tokens stay unlayered so every layer can read them.
func buildCSS() []byte {
	var b strings.Builder
	b.WriteString("@layer base, layout, legacy, components, features, utilities;\n")
	b.Write(readCSS("tokens.css"))
	for _, layer := range []struct {
		name  string
		files []string
	}{
		{"base", []string{"base.css"}},
		{"layout", []string{"layout.css"}},
		{"legacy", []string{"legacy.css"}},
		{"components", []string{"controls.css", "surfaces.css", "data.css"}},
	} {
		for _, file := range layer.files {
			b.WriteString("@layer " + layer.name + " {\n")
			b.Write(readCSS(file))
			b.WriteString("\n}\n")
		}
	}
	return []byte(b.String())
}

func readCSS(file string) []byte {
	data, err := staticFS.ReadFile("static/css/" + file)
	if err != nil {
		panic(err) // embedded at build time: a missing file is a build bug
	}
	return data
}

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
	typ := mime.TypeByExtension(path.Ext(name))
	if path.Ext(name) == ".woff2" {
		typ = "font/woff2" // not in every system's mime table
	}
	rw.Header().Set("Content-Type", typ)
	rw.Header().Set("X-Content-Type-Options", "nosniff")
	rw.Header().Set("Cache-Control", "public, max-age=86400")
	_, _ = rw.Write(data)
}
