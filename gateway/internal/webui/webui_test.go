package webui

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
)

func serve(t *testing.T, dist fstest.MapFS, path string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	handler(dist).ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
	return w
}

var built = fstest.MapFS{
	"index.html":           {Data: []byte("<!doctype html><title>Object store</title>")},
	"assets/index-abc.js":  {Data: []byte("console.log(1)")},
	"assets/index-abc.css": {Data: []byte("body{}")},
}

func TestServesIndexAndAssets(t *testing.T) {
	tests := []struct {
		path        string
		status      int
		cache, body string
	}{
		{"/", http.StatusOK, "no-cache", "<title>Object store</title>"},
		{"/assets/index-abc.js", http.StatusOK, "immutable", "console.log(1)"},
		{"/assets/missing.js", http.StatusNotFound, "", ""},
		{"/assets/", http.StatusNotFound, "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			w := serve(t, built, tt.path)
			if w.Code != tt.status {
				t.Fatalf("status %d, want %d", w.Code, tt.status)
			}
			if !strings.Contains(w.Header().Get("Cache-Control"), tt.cache) {
				t.Errorf("Cache-Control %q, want %q", w.Header().Get("Cache-Control"), tt.cache)
			}
			if !strings.Contains(w.Body.String(), tt.body) {
				t.Errorf("body %q, want it to contain %q", w.Body.String(), tt.body)
			}
		})
	}
}

// Mutation: the CSP allows inline scripts.
func TestEveryResponseCarriesTheSecurityHeaders(t *testing.T) {
	for _, path := range []string{"/", "/assets/index-abc.css", "/assets/missing.js"} {
		w := serve(t, built, path)
		policy := w.Header().Get("Content-Security-Policy")
		if !strings.Contains(policy, "script-src 'self'") || strings.Contains(policy, "unsafe-inline") ||
			!strings.Contains(policy, "frame-ancestors 'none'") {
			t.Errorf("%s: Content-Security-Policy %q", path, policy)
		}
		if w.Header().Get("X-Content-Type-Options") != "nosniff" {
			t.Errorf("%s: no nosniff", path)
		}
	}
}

func TestUnbuiltUIExplainsItself(t *testing.T) {
	w := serve(t, fstest.MapFS{}, "/")
	if w.Code != http.StatusServiceUnavailable || !strings.Contains(w.Body.String(), "web UI not built") {
		t.Fatalf("got %d %q", w.Code, w.Body.String())
	}
}
