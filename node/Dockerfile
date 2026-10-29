# Build context is the repo root: the node imports gen/go from the root module.
FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY gen gen
COPY internal internal
COPY node node
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -o /out/node ./node/cmd/node
# distroless has no shell to mkdir, so create the data dir here, owned by nonroot.
RUN mkdir -p /out/data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/node /node
COPY --from=build --chown=65532:65532 /out/data /data
EXPOSE 9101
ENTRYPOINT ["/node"]
