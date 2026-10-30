package httpapi

import (
	"bytes"
	"net/http"
	"strings"
	"testing"
)

func TestParseAllowedHosts(t *testing.T) {
	tests := []struct {
		in      string
		want    []string
		wantErr bool
	}{
		{"localhost:9081,127.0.0.1:9081", []string{"localhost:9081", "127.0.0.1:9081"}, false},
		{" LocalHost:9081 , ", []string{"localhost:9081"}, false},
		{"[::1]:9081", []string{"[::1]:9081"}, false},
		{"", nil, true},
		{" , ", nil, true},
		{"localhost", nil, true},
		{":9081", nil, true},
		{"localhost:http", nil, true},
		{"localhost:0", nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := ParseAllowedHosts(tt.in)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err %v, wantErr %v", err, tt.wantErr)
			}
			if strings.Join(got, ",") != strings.Join(tt.want, ",") {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

// A DNS-rebinding page reaches the gateway with its own domain in Host.
// Every route refuses it, reads included. Mutation: the Host check is
// skipped.
func TestHostNotAllowedIsRefused(t *testing.T) {
	c := newCluster(t, options{})
	if resp := putObject(t, c, "k", []byte("secret")); resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT: %s", resp.Status)
	}
	port := c.srv.Listener.Addr().String()[strings.LastIndex(c.srv.Listener.Addr().String(), ":"):]
	for _, host := range []string{"evil.example" + port, "evil.example", "localhost.evil.example" + port, "127.0.0.1"} {
		for _, target := range []struct{ method, path string }{
			{"GET", "/objects/k"}, {"GET", "/objects/k/stat"}, {"GET", "/objects"}, {"GET", "/cluster"},
			{"GET", "/healthz"}, {"DELETE", "/objects/k"}, {"PUT", "/objects/k"},
		} {
			req, err := http.NewRequest(target.method, c.srv.URL+target.path, bytes.NewReader(nil))
			if err != nil {
				t.Fatal(err)
			}
			req.Host = host
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != http.StatusForbidden || errorCode(t, resp) != "HOST_NOT_ALLOWED" {
				t.Errorf("%s %s with Host %q: %s, want 403 HOST_NOT_ALLOWED", target.method, target.path, host, resp.Status)
			}
			resp.Body.Close()
		}
	}
	if _, body, err := getObject(t, c, "k"); err != nil || string(body) != "secret" {
		t.Fatalf("the object must be untouched: %q, %v", body, err)
	}
}

// Mutation: PUT and DELETE skip the cross-origin check.
func TestCrossOriginMutationIsRefused(t *testing.T) {
	c := newCluster(t, options{})
	putObject(t, c, "k", []byte("data"))
	tests := []struct {
		name    string
		method  string
		headers map[string]string
	}{
		{"DELETE, Sec-Fetch-Site cross-site", "DELETE", map[string]string{"Sec-Fetch-Site": "cross-site"}},
		{"DELETE, Sec-Fetch-Site same-site", "DELETE", map[string]string{"Sec-Fetch-Site": "same-site"}},
		{"PUT, Sec-Fetch-Site cross-site", "PUT", map[string]string{"Sec-Fetch-Site": "cross-site"}},
		{"DELETE, foreign Origin", "DELETE", map[string]string{"Origin": "http://evil.example"}},
		{"PUT, foreign Origin", "PUT", map[string]string{"Origin": "http://evil.example:9081"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(tt.method, objectURL(c, "k"), strings.NewReader("evil"))
			if err != nil {
				t.Fatal(err)
			}
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusForbidden || errorCode(t, resp) != "CROSS_ORIGIN" {
				t.Fatalf("%s, want 403 CROSS_ORIGIN", resp.Status)
			}
		})
	}
	if _, body, err := getObject(t, c, "k"); err != nil || string(body) != "data" {
		t.Fatalf("the object must be untouched: %q, %v", body, err)
	}
}

// The UI's own requests (same origin) and non-browser clients (no headers)
// pass; a cross-site page may still read (GET is not a write).
func TestSameOriginAndHeaderlessMutationsAllowed(t *testing.T) {
	c := newCluster(t, options{})
	tests := []struct {
		name    string
		headers map[string]string
	}{
		{"no browser headers", nil},
		{"Sec-Fetch-Site same-origin", map[string]string{"Sec-Fetch-Site": "same-origin"}},
		{"Origin matches Host", map[string]string{"Origin": c.srv.URL}},
		{"Sec-Fetch-Site none (typed by the user)", map[string]string{"Sec-Fetch-Site": "none"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, method := range []string{"PUT", "DELETE"} {
				req, err := http.NewRequest(method, objectURL(c, "k"), strings.NewReader("data"))
				if err != nil {
					t.Fatal(err)
				}
				for k, v := range tt.headers {
					req.Header.Set(k, v)
				}
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					t.Fatalf("%s: %s, want 200", method, resp.Status)
				}
			}
		})
	}
}

// Mutation: the ASCII fallback keeps quotes, backslashes and %.
func TestContentDispositionEncodesKey(t *testing.T) {
	tests := []struct {
		key, want string
	}{
		{"photo.bin", `attachment; filename="photo.bin"; filename*=UTF-8''photo.bin`},
		{"demo/photo-archive.bin", `attachment; filename="photo-archive.bin"; filename*=UTF-8''photo-archive.bin`},
		{`say "hi"\now.txt`, `attachment; filename="say _hi__now.txt"; filename*=UTF-8''say%20%22hi%22%5Cnow.txt`},
		{"a;b=c.txt", `attachment; filename="a;b=c.txt"; filename*=UTF-8''a%3Bb%3Dc.txt`},
		{"résumé 100%.pdf", `attachment; filename="r_sum_ 100_.pdf"; filename*=UTF-8''r%C3%A9sum%C3%A9%20100%25.pdf`},
		{"logs/", `attachment; filename="download"; filename*=UTF-8''download`},
		{"..", `attachment; filename="download"; filename*=UTF-8''download`},
		{"line\r\nbreak", `attachment; filename="line__break"; filename*=UTF-8''line%0D%0Abreak`},
	}
	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			if got := contentDisposition(tt.key); got != tt.want {
				t.Fatalf("got  %s\nwant %s", got, tt.want)
			}
		})
	}
}

// The header reaches the client on GET and HEAD, with nosniff, and the
// object is never offered inline. Mutation: inline instead of attachment.
func TestDownloadIsAnAttachment(t *testing.T) {
	c := newCluster(t, options{})
	putObject(t, c, "site/index.html", []byte("<script>alert(1)</script>"))
	for _, method := range []string{"GET", "HEAD"} {
		req, _ := http.NewRequest(method, objectURL(c, "site/index.html"), nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		cd := resp.Header.Get("Content-Disposition")
		if !strings.HasPrefix(cd, "attachment;") || !strings.Contains(cd, `filename="index.html"`) {
			t.Errorf("%s: Content-Disposition %q", method, cd)
		}
		if resp.Header.Get("X-Content-Type-Options") != "nosniff" ||
			resp.Header.Get("Content-Type") != "application/octet-stream" {
			t.Errorf("%s: Content-Type %q, X-Content-Type-Options %q", method,
				resp.Header.Get("Content-Type"), resp.Header.Get("X-Content-Type-Options"))
		}
	}
}

// The UI is served only when configured, behind the same Host check.
func TestUIRoutes(t *testing.T) {
	ui := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ui:" + r.URL.Path)) })
	cfg := defaultConfig()
	cfg.UI = ui
	c := newCluster(t, options{cfg: cfg})
	for path, want := range map[string]string{"/": "ui:/", "/assets/x.js": "ui:/assets/x.js"} {
		resp, err := http.Get(c.srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		var b bytes.Buffer
		b.ReadFrom(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || b.String() != want {
			t.Errorf("%s: %s %q", path, resp.Status, b.String())
		}
	}
	resp, err := http.Get(c.srv.URL + "/other")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("/other: %s, want 404", resp.Status)
	}
}
