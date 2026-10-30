# Build context is the repo root: the gateway imports gen/go and the shared
# upload and download packages from the root module, and embeds the web UI.

# The web UI: npm ci from the committed lockfile, then Vite's production
# build into gateway/internal/webui/build/dist.
FROM node:24 AS web
WORKDIR /src/web
COPY web/package.json web/package-lock.json web/.npmrc ./
RUN --mount=type=cache,target=/root/.npm npm ci
COPY web ./
RUN npm run build && test -f /src/gateway/internal/webui/build/dist/index.html

FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY gen gen
COPY internal internal
COPY gateway gateway
# This image always carries the UI; a binary without it serves 503 at /.
COPY --from=web /src/gateway/internal/webui/build/dist gateway/internal/webui/build/dist
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -o /out/gateway ./gateway/cmd/gateway

# Stateless: no volume, nothing written to disk.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/gateway /gateway
EXPOSE 9081
ENTRYPOINT ["/gateway"]
