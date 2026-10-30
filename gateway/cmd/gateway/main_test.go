package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Ayomide1x/object-store/gateway/internal/httpapi"
)

// The container healthcheck runs "gateway health" against a gateway whose
// Host check is on. The probe must send an allowed Host, or every healthy
// gateway is reported unhealthy. Mutation: the probe sends the listen
// address as Host.
func TestHealthProbeSendsAnAllowedHost(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
	srv := httptest.NewServer(httpapi.AllowHosts([]string{"localhost:9081"}, ok))
	defer srv.Close()
	listen := strings.TrimPrefix(srv.URL, "http://")

	tests := []struct {
		name, allowed string
		wantErr       string
	}{
		{"first entry is allowed", "localhost:9081,127.0.0.1:9081", ""},
		{"first entry is not served", "other.test:9081", "403"},
		{"no allowlist", "", "GATEWAY_ALLOWED_HOSTS"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkHealth(listen, tt.allowed)
			if tt.wantErr == "" && err != nil {
				t.Fatalf("unhealthy: %v", err)
			}
			if tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)) {
				t.Fatalf("got %v, want an error containing %q", err, tt.wantErr)
			}
		})
	}
}
