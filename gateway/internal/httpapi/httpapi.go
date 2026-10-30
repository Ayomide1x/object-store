// Package httpapi is the gateway's HTTP API. The gateway is just a client
// (invariant 11): every request becomes the same coordinator and node RPCs
// osctl makes, through the same shared upload and download code, and nothing
// is kept between requests.
//
// A key is exactly one URL path segment, percent-encoded: "/" in a key is
// sent as %2F (url.PathEscape, or JavaScript's encodeURIComponent). So
// /objects/demo%2Fphoto.bin/stat is unambiguously the stat of key
// "demo/photo.bin".
package httpapi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"

	objectstorev1 "github.com/Ayomide1x/object-store/gen/go/objectstore/v1"
	"github.com/Ayomide1x/object-store/internal/download"
	"github.com/Ayomide1x/object-store/internal/upload"
)

// Config holds the gateway's bounds.
type Config struct {
	// Uploads and downloads in flight at once; more get 503 at once.
	MaxUploads   int
	MaxDownloads int
	// How long a transfer may make no progress: the PUT body must deliver
	// each next chunk, and a GET's client must accept each next chunk, within
	// this. A transfer that keeps moving is never cut off, whatever its size.
	StallTimeout time.Duration
	// Deadlines for each replica write and read, connection included.
	ChunkWriteTimeout time.Duration
	ChunkReadTimeout  time.Duration
	// Host header values served (host:port, from ParseAllowedHosts); every
	// other Host gets 403. Empty refuses everything.
	AllowedHosts []string
	// The web UI, served at / and /assets/; nil serves no UI.
	UI http.Handler
}

// Server serves the HTTP API.
type Server struct {
	coord         objectstorev1.CoordinatorServiceClient
	cfg           Config
	log           *slog.Logger
	uploadSlots   chan struct{}
	downloadSlots chan struct{}
	nodeDialOpts  []grpc.DialOption
}

// NodeDialOptions bound what one connection to a node can buffer when it
// receives: a chunk read gets a fixed 1 MiB window rather than grpc-go's
// BDP-driven window, which can grow to 16 MiB per stream and per connection.
func NodeDialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithStaticStreamWindowSize(1 << 20),
		grpc.WithStaticConnWindowSize(1 << 20),
	}
}

// New returns a Server that reaches the coordinator through coord.
func New(coord objectstorev1.CoordinatorServiceClient, cfg Config, log *slog.Logger) *Server {
	return &Server{
		coord:         coord,
		cfg:           cfg,
		log:           log,
		uploadSlots:   make(chan struct{}, cfg.MaxUploads),
		downloadSlots: make(chan struct{}, cfg.MaxDownloads),
		nodeDialOpts:  NodeDialOptions(),
	}
}

// Handler returns the routes, behind the Host allowlist and the cross-origin
// check for writes. {key} matches one path segment, so a key with "/" in it
// must arrive as %2F; ServeMux matches on the escaped path and hands the
// handler the unescaped key.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /objects/{key}", s.put)
	mux.HandleFunc("GET /objects/{key}", s.get) // also HEAD
	mux.HandleFunc("DELETE /objects/{key}", s.rm)
	mux.HandleFunc("GET /objects/{key}/stat", s.stat)
	mux.HandleFunc("GET /objects", s.list)
	mux.HandleFunc("GET /cluster", s.cluster)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/objects/{rest...}", s.unmatched)
	if s.cfg.UI != nil {
		mux.Handle("GET /{$}", s.cfg.UI)
		mux.Handle("GET /assets/", s.cfg.UI)
	}
	return AllowHosts(s.cfg.AllowedHosts, sameOriginWrites(mux))
}

// unmatched explains requests under /objects/ that no route takes: an empty
// key, a raw "/" in a key, or a method the path doesn't support.
func (s *Server) unmatched(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.EscapedPath(), "/objects/")
	switch {
	case rest == "":
		writeError(w, http.StatusBadRequest, "BAD_KEY", "key is required: /objects/{key}")
	case !strings.Contains(rest, "/"):
		w.Header().Set("Allow", "GET, HEAD, PUT, DELETE")
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", r.Method+" is not supported on an object")
	case strings.Count(rest, "/") == 1 && strings.HasSuffix(rest, "/stat"):
		w.Header().Set("Allow", "GET, HEAD")
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", r.Method+" is not supported on stat")
	default:
		writeError(w, http.StatusBadRequest, "BAD_KEY",
			"a key is one path segment: encode \"/\" in keys as %2F, as in /objects/demo%2Fphoto.bin")
	}
}

// acquire takes a slot without waiting. When none is free it answers 503 and
// returns false: overload is refused at once, not queued.
func acquire(w http.ResponseWriter, log *slog.Logger, slots chan struct{}, what string) bool {
	select {
	case slots <- struct{}{}:
		return true
	default:
		log.Warn(what+" rejected: concurrency limit reached", "limit", cap(slots))
		w.Header().Set("Retry-After", "1")
		// An upload's body is not read: close rather than drain it (see put).
		w.Header().Set("Connection", "close")
		writeError(w, http.StatusServiceUnavailable, "BUSY",
			fmt.Sprintf("gateway is serving %d %ss; retry later", cap(slots), what))
		return false
	}
}

// errDeadline is what a body read returns once its read deadline passes.
var errDeadline = os.ErrDeadlineExceeded

// bodySource is the PUT body as an upload.Source. Before each chunk is read
// it moves the body's read deadline StallTimeout ahead, so a client that
// stops sending is cut off while a slow but moving one is not.
type bodySource struct {
	*upload.StreamSource
	rc    *http.ResponseController
	stall time.Duration
}

func (b bodySource) Chunk(offset, size int64) (io.ReaderAt, int64, error) {
	if err := b.rc.SetReadDeadline(time.Now().Add(b.stall)); err != nil {
		return nil, 0, fmt.Errorf("set read deadline: %w", err)
	}
	return b.StreamSource.Chunk(offset, size)
}

func (s *Server) put(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	log := s.log.With("key", key)
	// Every error response closes the connection. Otherwise net/http, before
	// sending it, drains up to 256 KiB of any unread body to reuse the
	// connection, with no deadline: a client that sent headers and then
	// stalls would hold the handler forever.
	refuse := func(code int, reason, msg string) {
		w.Header().Set("Connection", "close")
		writeError(w, code, reason, msg)
	}
	// Go reports ContentLength 0 both for "Content-Length: 0" and for no
	// header at all, and -1 for chunked bodies; only the header says the
	// client declared a size.
	if r.ContentLength < 0 || len(r.Header.Values("Content-Length")) == 0 {
		refuse(http.StatusLengthRequired, "LENGTH_REQUIRED",
			"Content-Length is required (chunked uploads are not supported)")
		return
	}
	if r.ContentLength > upload.MaxObjectSize {
		refuse(http.StatusRequestEntityTooLarge, "TOO_LARGE",
			fmt.Sprintf("object of %d bytes exceeds the %d-byte limit", r.ContentLength, int64(upload.MaxObjectSize)))
		return
	}
	// Checked before the body is read: a client that sent Expect:
	// 100-continue never transfers a body we refuse.
	if !acquire(w, log, s.uploadSlots, "upload") {
		return
	}
	defer func() { <-s.uploadSlots }()

	rc := http.NewResponseController(w)
	src := bodySource{StreamSource: upload.NewStreamSource(r.Body, r.ContentLength), rc: rc, stall: s.cfg.StallTimeout}
	objectID, err := upload.Put(r.Context(), s.coord, log, key, r.ContentLength, src, upload.Options{
		ChunkWriteTimeout: s.cfg.ChunkWriteTimeout,
		NodeAddresses:     objectstorev1.NodeAddressSet_NODE_ADDRESS_SET_INTERNAL,
	})
	if err != nil {
		// Nothing was completed, so nothing committed: the upload stays
		// PENDING and garbage collection removes what was written.
		log.Warn("upload failed; nothing committed", "error", err)
		w.Header().Set("Connection", "close")
		if errors.Is(err, upload.ErrSource) {
			// Answer even if the client may be gone (net/http cancels the
			// request context on any body read error, a stall included):
			// writing to a closed connection fails harmlessly.
			if errors.Is(err, errDeadline) {
				writeError(w, http.StatusRequestTimeout, "BODY_STALLED",
					fmt.Sprintf("request body made no progress for %s", s.cfg.StallTimeout))
				return
			}
			writeError(w, http.StatusBadRequest, "BODY_INCOMPLETE", "request body ended before Content-Length bytes")
			return
		}
		writeUpstreamError(w, err)
		return
	}
	// The whole body is read: the connection can be reused without a deadline.
	_ = rc.SetReadDeadline(time.Time{})
	writeJSON(w, http.StatusOK, putResponse{Key: key, ObjectID: objectID, SizeBytes: r.ContentLength})
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	log := s.log.With("key", key)
	if !acquire(w, log, s.downloadSlots, "download") {
		return
	}
	defer func() { <-s.downloadSlots }()

	obj, err := download.Fetch(r.Context(), s.coord, key, objectstorev1.NodeAddressSet_NODE_ADDRESS_SET_INTERNAL)
	if err != nil {
		writeUpstreamError(w, err)
		return
	}
	log = log.With("object_id", obj.ID)
	out := &responseStream{w: w, rc: http.NewResponseController(w), stall: s.cfg.StallTimeout, size: obj.Size,
		objectID: obj.ID, key: key}
	if r.Method == http.MethodHead {
		out.start()
		return
	}

	d := download.New(s.cfg.ChunkReadTimeout, download.Shuffled, log, s.nodeDialOpts...)
	err = d.Stream(r.Context(), obj.Chunks, obj.Size, out)
	// Reports go out whatever happened, on their own deadlines (stage 5).
	download.SendReports(s.coord, log, d.Reports())
	var writeErr *download.WriteError
	switch {
	case err == nil:
		out.start() // a 0-byte object has written nothing yet
		log.Info("download complete", "bytes", obj.Size, "chunks", len(obj.Chunks))
	case errors.As(err, &writeErr):
		log.Warn("download ended: client stopped reading", "bytes_sent", out.written, "error", err)
	case !out.started:
		// Nothing sent yet: a proper status is still possible.
		log.Warn("download failed before the first byte", "error", err)
		writeUpstreamError(w, err)
	default:
		// The status line and Content-Length are out and some verified bytes
		// with them. Ending normally would leave a short body that some
		// clients accept; aborting closes the connection (HTTP/1.1) or resets
		// the stream (HTTP/2), so the client sees a broken transfer.
		log.Error("download failed mid-stream; aborting the connection",
			"bytes_sent", out.written, "bytes_declared", obj.Size, "error", err)
		panic(http.ErrAbortHandler)
	}
}

// responseStream sends the response headers with the first verified chunk,
// so every failure up to then still gets a proper status, then writes and
// flushes each chunk as it is verified, with a write deadline per chunk.
type responseStream struct {
	w        http.ResponseWriter
	rc       *http.ResponseController
	stall    time.Duration
	size     int64
	objectID string
	key      string
	started  bool
	written  int64
}

func (o *responseStream) start() {
	if o.started {
		return
	}
	o.started = true
	h := o.w.Header()
	h.Set("Content-Length", strconv.FormatInt(o.size, 10))
	h.Set("Content-Type", "application/octet-stream")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Content-Disposition", contentDisposition(o.key))
	h.Set("X-Object-Id", o.objectID)
	o.w.WriteHeader(http.StatusOK)
}

func (o *responseStream) Write(p []byte) (int, error) {
	o.start()
	if err := o.rc.SetWriteDeadline(time.Now().Add(o.stall)); err != nil {
		return 0, fmt.Errorf("set write deadline: %w", err)
	}
	n, err := o.w.Write(p)
	o.written += int64(n)
	if err != nil {
		return n, err
	}
	// Out now: a later failure must find these bytes on the wire, not in a
	// buffer the abort would discard.
	if err := o.rc.Flush(); err != nil {
		return n, fmt.Errorf("flush: %w", err)
	}
	return n, nil
}

func (s *Server) rm(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	resp, err := s.coord.DeleteObject(r.Context(), &objectstorev1.DeleteObjectRequest{Key: key})
	if err != nil {
		writeUpstreamError(w, err)
		return
	}
	s.log.Info("key removed; files are deleted after the grace period", "key", key, "object_id", resp.GetObjectId())
	writeJSON(w, http.StatusOK, rmResponse{Key: key, ObjectID: resp.GetObjectId()})
}

func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := 0
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "BAD_LIMIT", fmt.Sprintf("limit must be an integer, got %q", v))
			return
		}
		limit = n
	}
	if int64(limit) != int64(int32(limit)) {
		writeError(w, http.StatusBadRequest, "BAD_LIMIT", fmt.Sprintf("limit %d is out of range", limit))
		return
	}
	resp, err := s.coord.ListObjects(r.Context(), &objectstorev1.ListObjectsRequest{
		Prefix: q.Get("prefix"), StartAfter: q.Get("start_after"), Limit: int32(limit),
	})
	if err != nil {
		writeUpstreamError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toListResponse(resp))
}

func (s *Server) stat(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	resp, err := s.coord.StatObject(r.Context(), &objectstorev1.StatObjectRequest{Key: key})
	if err != nil {
		writeUpstreamError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toStatResponse(key, resp))
}

func (s *Server) cluster(w http.ResponseWriter, r *http.Request) {
	resp, err := s.coord.GetClusterStatus(r.Context(), &objectstorev1.GetClusterStatusRequest{})
	if err != nil {
		writeUpstreamError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toClusterResponse(resp))
}

// CoordinatorDeadline returns a unary interceptor that gives every
// coordinator call a deadline of d unless its context has a sooner one.
func CoordinatorDeadline(d time.Duration) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx, cancel := context.WithTimeout(ctx, d)
		defer cancel()
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}
