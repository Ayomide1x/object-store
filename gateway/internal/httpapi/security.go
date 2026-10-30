package httpapi

import (
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
)

// ParseAllowedHosts parses GATEWAY_ALLOWED_HOSTS: comma-separated host:port
// entries, such as "localhost:9081,127.0.0.1:9081". An empty list is an
// error: the gateway refuses to start rather than serve every Host.
func ParseAllowedHosts(s string) ([]string, error) {
	var hosts []string
	for _, entry := range strings.Split(s, ",") {
		entry = strings.ToLower(strings.TrimSpace(entry))
		if entry == "" {
			continue
		}
		host, port, err := net.SplitHostPort(entry)
		if err != nil || host == "" {
			return nil, fmt.Errorf("allowed host %q must be host:port", entry)
		}
		if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
			return nil, fmt.Errorf("allowed host %q has a bad port", entry)
		}
		hosts = append(hosts, entry)
	}
	if len(hosts) == 0 {
		return nil, fmt.Errorf("at least one allowed host:port is required")
	}
	return hosts, nil
}

// AllowHosts refuses, with 403, every request whose Host header is not in
// hosts (compared without case, port included). It guards against DNS
// rebinding: a page on a hostile domain that resolves to 127.0.0.1 is
// same-origin with the gateway as far as the browser is concerned, but its
// requests carry that domain in Host. It applies to every route, reads
// included (a rebinding read would leak object bytes and keys) and /healthz
// (whose probe sends an allowed Host).
func AllowHosts(hosts []string, next http.Handler) http.Handler {
	allowed := make(map[string]bool, len(hosts))
	for _, h := range hosts {
		allowed[strings.ToLower(h)] = true
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !allowed[strings.ToLower(r.Host)] {
			w.Header().Set("Connection", "close")
			writeError(w, http.StatusForbidden, "HOST_NOT_ALLOWED",
				fmt.Sprintf("Host %q is not one this gateway serves (GATEWAY_ALLOWED_HOSTS)", r.Host))
			return
		}
		next.ServeHTTP(w, r)
	})
}

// sameOriginWrites refuses cross-origin PUT and DELETE (any method but GET,
// HEAD and OPTIONS) with 403, using the standard library's check: the
// browser's Sec-Fetch-Site header, or else Origin against Host. Requests
// with neither header (curl, scripts, osctl-like clients) are allowed, as
// they are not from a browser. Defence in depth: a cross-site page cannot
// send PUT or DELETE without a CORS preflight the gateway never approves,
// and a rebinding page is refused by AllowHosts.
func sameOriginWrites(next http.Handler) http.Handler {
	cop := http.NewCrossOriginProtection()
	cop.SetDenyHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Connection", "close")
		writeError(w, http.StatusForbidden, "CROSS_ORIGIN",
			r.Method+" from another origin is not allowed")
	}))
	return cop.Handler(next)
}

// contentDisposition names the download after the key's last "/" segment:
// an ASCII fallback in filename (RFC 6266) for old clients, and the exact
// name in filename* (RFC 5987, UTF-8, percent-encoded). Always attachment,
// never inline: an uploaded HTML file opened inline would run on the UI's
// origin.
func contentDisposition(key string) string {
	name := key[strings.LastIndex(key, "/")+1:]
	if name == "" || name == "." || name == ".." {
		name = "download"
	}
	var fallback strings.Builder
	for _, r := range name {
		// Quotes and backslashes end or escape the quoted string; % is
		// decoded by some browsers; the rest is not printable ASCII.
		if r < 0x20 || r > 0x7e || r == '"' || r == '\\' || r == '%' {
			fallback.WriteByte('_')
		} else {
			fallback.WriteRune(r)
		}
	}
	var exact strings.Builder
	for _, b := range []byte(name) {
		if isAttrChar(b) {
			exact.WriteByte(b)
		} else {
			fmt.Fprintf(&exact, "%%%02X", b)
		}
	}
	return fmt.Sprintf(`attachment; filename="%s"; filename*=UTF-8''%s`, fallback.String(), exact.String())
}

// isAttrChar reports whether b may appear unencoded in an RFC 5987 value.
func isAttrChar(b byte) bool {
	switch {
	case 'a' <= b && b <= 'z', 'A' <= b && b <= 'Z', '0' <= b && b <= '9':
		return true
	}
	return strings.IndexByte("!#$&+-.^_`|~", b) >= 0
}
