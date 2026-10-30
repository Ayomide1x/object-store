# Build context is the repo root: the gateway imports gen/go and the shared
# upload and download packages from the root module.
FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY gen gen
COPY internal internal
COPY gateway gateway
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -o /out/gateway ./gateway/cmd/gateway

# Stateless: no volume, nothing written to disk.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/gateway /gateway
EXPOSE 9081
ENTRYPOINT ["/gateway"]
