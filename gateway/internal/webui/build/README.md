# Web UI build output

`npm run build` in `web/` writes the UI to `dist/` here, and the gateway
embeds this directory (`//go:embed all:build`). `dist/` is not committed.

This file is: it keeps the embed pattern matching on a fresh clone, so
`go build`, `go vet` and `go test ./...` work before the UI is built. A
gateway built without `dist/` serves 503 "web UI not built" at `/`. The
gateway image always builds the UI first, and fails if it is missing.
