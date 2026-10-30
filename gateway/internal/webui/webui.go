// Package webui serves the browser UI that web/ builds into build/dist and
// the gateway embeds. Same origin as the API, so no CORS.
package webui

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"
)

// The pattern always matches build/README.md, so the package compiles on a
// fresh clone with no UI built (see that file).
//
//go:embed all:build
var embedded embed.FS

// csp lets the page load only its own scripts, styles, fonts and images,
// talk only to its own origin, and never be framed. No 'unsafe-inline':
// even a key rendered as markup by mistake could not run a script.
const csp = "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self'; font-src 'self'; " +
	"connect-src 'self'; object-src 'none'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"

// Handler serves the embedded build: index.html at / and hashed files under
// /assets/.
func Handler() http.Handler {
	dist, err := fs.Sub(embedded, "build/dist")
	if err != nil {
		panic(err) // fs.Sub fails only for an invalid path, which this is not
	}
	return handler(dist)
}

// Built reports whether the embedded build has an index.html.
func Built() bool {
	_, err := fs.Stat(embedded, "build/dist/index.html")
	return err == nil
}

func handler(dist fs.FS) http.Handler {
	assets := http.FileServerFS(dist)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", csp)
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "no-referrer")
		if r.URL.Path == "/" {
			page, err := fs.ReadFile(dist, "index.html")
			if err != nil {
				http.Error(w, "web UI not built: run make web, then rebuild the gateway", http.StatusServiceUnavailable)
				return
			}
			// Always revalidated, so a new build's asset names are picked up.
			h.Set("Cache-Control", "no-cache")
			h.Set("Content-Type", "text/html; charset=utf-8")
			w.Write(page)
			return
		}
		// Under /assets/: files only, never a directory listing.
		if strings.HasSuffix(r.URL.Path, "/") {
			http.NotFound(w, r)
			return
		}
		// Vite puts a content hash in every asset name: a name never changes meaning.
		h.Set("Cache-Control", "public, max-age=31536000, immutable")
		assets.ServeHTTP(w, r)
	})
}
