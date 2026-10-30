package httpapi

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	objectstorev1 "github.com/Ayomide1x/object-store/gen/go/objectstore/v1"
)

func randomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

// escapeKey encodes a key as the one path segment the API expects:
// url.PathEscape (which turns "/" into %2F), and a key that is exactly "." or
// ".." with its dots as %2E, since a client would otherwise resolve those
// segments away before sending.
func escapeKey(key string) string {
	if key == "." || key == ".." {
		return strings.ReplaceAll(key, ".", "%2E")
	}
	return url.PathEscape(key)
}

func objectURL(c *cluster, key string) string {
	return c.srv.URL + "/objects/" + escapeKey(key)
}

func putObject(t *testing.T, c *cluster, key string, data []byte) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, objectURL(c, key), bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT %s: %v", key, err)
	}
	resp.Body.Close()
	return resp
}

// getObject returns the response, the body read so far, and the error that
// ended the read (nil for a complete body).
func getObject(t *testing.T, c *cluster, key string) (*http.Response, []byte, error) {
	t.Helper()
	resp, err := http.Get(objectURL(c, key))
	if err != nil {
		t.Fatalf("GET %s: %v", key, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	return resp, body, err
}

// raw sends request text on a new connection and returns the parsed
// response and the open connection (closed at test end).
func raw(t *testing.T, c *cluster, request string) (*http.Response, net.Conn) {
	t.Helper()
	conn, err := net.Dial("tcp", c.srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	if _, err := io.WriteString(conn, request); err != nil {
		t.Fatal(err)
	}
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return resp, conn
}

func errorCode(t *testing.T, resp *http.Response) string {
	t.Helper()
	var e errorBody
	if err := json.NewDecoder(resp.Body).Decode(&e); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	return e.Code
}

func TestRoundTrip(t *testing.T) {
	c := newCluster(t, options{})
	for _, size := range []int{0, 1, 4 << 20, 4<<20 + 1, 9 << 20} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			data := randomBytes(t, size)
			key := fmt.Sprintf("size-%d", size)
			if resp := putObject(t, c, key, data); resp.StatusCode != http.StatusOK {
				t.Fatalf("PUT: %s", resp.Status)
			}
			resp, body, err := getObject(t, c, key)
			if err != nil || resp.StatusCode != http.StatusOK {
				t.Fatalf("GET: %s, %v", resp.Status, err)
			}
			if !bytes.Equal(body, data) {
				t.Fatalf("got %d bytes back, not the %d uploaded", len(body), len(data))
			}
			if resp.ContentLength != int64(size) || resp.Header.Get("X-Object-Id") == "" {
				t.Fatalf("Content-Length %d, X-Object-Id %q", resp.ContentLength, resp.Header.Get("X-Object-Id"))
			}
		})
	}
}

// Mutation: the gateway asks for client addresses. The fake coordinator's
// client addresses are unreachable, so only the internal set works.
func TestGatewayAsksForInternalAddresses(t *testing.T) {
	c := newCluster(t, options{})
	data := randomBytes(t, 100)
	if resp := putObject(t, c, "k", data); resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT: %s", resp.Status)
	}
	if resp, _, _ := getObject(t, c, "k"); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET: %s", resp.Status)
	}
	c.coord.mu.Lock()
	defer c.coord.mu.Unlock()
	if len(c.coord.addresses) != 2 {
		t.Fatalf("coordinator was asked for addresses %d times, want 2", len(c.coord.addresses))
	}
	for _, a := range c.coord.addresses {
		if a != objectstorev1.NodeAddressSet_NODE_ADDRESS_SET_INTERNAL {
			t.Fatalf("asked for %s, want INTERNAL", a)
		}
	}
}

// Mutation: a PUT without Content-Length is accepted.
func TestPutRequiresContentLength(t *testing.T) {
	cases := []struct {
		name    string
		request string
	}{
		{"no header", "PUT /objects/k HTTP/1.1\r\nHost: x\r\n\r\n"},
		{"chunked", "PUT /objects/k HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n0\r\n\r\n"},
		{"both", "PUT /objects/k HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n0\r\n\r\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newCluster(t, options{})
			resp, _ := raw(t, c, tc.request)
			if resp.StatusCode != http.StatusLengthRequired {
				t.Fatalf("got %s, want 411", resp.Status)
			}
			if code := errorCode(t, resp); code != "LENGTH_REQUIRED" {
				t.Fatalf("code %q", code)
			}
			if n, _ := c.coord.counts(); n != 0 {
				t.Fatalf("coordinator saw %d initiates, want 0", n)
			}
		})
	}
	// An explicit Content-Length: 0 is a 0-byte object, not a missing length.
	c := newCluster(t, options{})
	resp, _ := raw(t, c, "PUT /objects/empty HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\n\r\n")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Content-Length: 0 got %s, want 200", resp.Status)
	}
}

func TestPutTooLarge(t *testing.T) {
	c := newCluster(t, options{})
	// With Expect: 100-continue a client sends the body only after "100
	// Continue"; the 413 must come first, so the body is never transferred.
	resp, _ := raw(t, c, fmt.Sprintf("PUT /objects/k HTTP/1.1\r\nHost: x\r\nContent-Length: %d\r\nExpect: 100-continue\r\n\r\n", int64(5<<30)+1))
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("got %s, want 413", resp.Status)
	}
	if n, _ := c.coord.counts(); n != 0 {
		t.Fatalf("coordinator saw %d initiates, want 0", n)
	}
	// Exactly the limit is not refused by the gateway (the request is then
	// abandoned here, before any body).
	resp, _ = raw(t, c, fmt.Sprintf("PUT /objects/k HTTP/1.1\r\nHost: x\r\nContent-Length: %d\r\nExpect: 100-continue\r\n\r\n", int64(5<<30)))
	if resp.StatusCode != http.StatusContinue {
		t.Fatalf("at the limit got %s, want 100 Continue", resp.Status)
	}
}

func TestKeyEncoding(t *testing.T) {
	c := newCluster(t, options{})
	for _, key := range []string{"demo/photo-archive.bin", "a/../b", "a//b", "..", "space and?query#frag", "%2F-literal", "ünïcode"} {
		t.Run(key, func(t *testing.T) {
			data := randomBytes(t, 10)
			if resp := putObject(t, c, key, data); resp.StatusCode != http.StatusOK {
				t.Fatalf("PUT: %s", resp.Status)
			}
			c.coord.mu.Lock()
			_, stored := c.coord.objects[key]
			c.coord.mu.Unlock()
			if !stored {
				t.Fatalf("coordinator has no key %q", key)
			}
			if _, body, err := getObject(t, c, key); err != nil || !bytes.Equal(body, data) {
				t.Fatalf("GET: %v", err)
			}
			resp, err := http.Get(objectURL(c, key) + "/stat")
			if err != nil {
				t.Fatal(err)
			}
			var st statResponse
			json.NewDecoder(resp.Body).Decode(&st)
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK || st.Key != key {
				t.Fatalf("stat: %s, key %q", resp.Status, st.Key)
			}
		})
	}

	for _, tc := range []struct {
		method, path string
		want         int
	}{
		{"GET", "/objects/demo/photo-archive.bin", http.StatusBadRequest},
		{"PUT", "/objects/demo/photo-archive.bin", http.StatusBadRequest},
		{"GET", "/objects/", http.StatusBadRequest},
		{"POST", "/objects/k", http.StatusMethodNotAllowed},
		{"POST", "/objects/k/stat", http.StatusMethodNotAllowed},
	} {
		req, _ := http.NewRequest(tc.method, c.srv.URL+tc.path, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != tc.want {
			t.Errorf("%s %s: got %s, want %d", tc.method, tc.path, resp.Status, tc.want)
		}
	}
}

// Mutation: NOT_FOUND is not mapped (it falls through to 502).
func TestErrorMapping(t *testing.T) {
	cases := []struct {
		code  codes.Code
		want  int
		retry bool
	}{
		{codes.InvalidArgument, http.StatusBadRequest, false},
		{codes.NotFound, http.StatusNotFound, false},
		{codes.FailedPrecondition, http.StatusServiceUnavailable, true},
		{codes.Unavailable, http.StatusServiceUnavailable, true},
		{codes.ResourceExhausted, http.StatusServiceUnavailable, true},
		{codes.DataLoss, http.StatusInternalServerError, false},
		{codes.DeadlineExceeded, http.StatusGatewayTimeout, false},
		{codes.Internal, http.StatusBadGateway, false},
		{codes.Unknown, http.StatusBadGateway, false},
	}
	c := newCluster(t, options{})
	for _, tc := range cases {
		t.Run(tc.code.String(), func(t *testing.T) {
			for _, call := range []struct {
				method, rpc, path string
			}{
				{"GET", "GetObject", "/objects/k"},
				{"DELETE", "DeleteObject", "/objects/k"},
				{"GET", "StatObject", "/objects/k/stat"},
				{"GET", "ListObjects", "/objects"},
				{"GET", "GetClusterStatus", "/cluster"},
				{"PUT", "CompleteUpload", "/objects/k"},
			} {
				c.coord.mu.Lock()
				c.coord.errs = map[string]error{call.rpc: status.Error(tc.code, "fake failure")}
				c.coord.mu.Unlock()
				var body io.Reader
				if call.method == "PUT" {
					body = strings.NewReader("abc")
				}
				req, _ := http.NewRequest(call.method, c.srv.URL+call.path, body)
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				resp.Body.Close()
				if resp.StatusCode != tc.want || (resp.Header.Get("Retry-After") != "") != tc.retry {
					t.Errorf("%s %s with %s: got %s, Retry-After %q; want %d, retry %v",
						call.method, call.path, tc.code, resp.Status, resp.Header.Get("Retry-After"), tc.want, tc.retry)
				}
			}
		})
	}
}

func TestQuorumFailureIs503AndNothingCompletes(t *testing.T) {
	c := newCluster(t, options{})
	// Two of three nodes stop: no chunk can reach 2 replicas.
	c.nodes[0].addr, c.nodes[1].addr = "127.0.0.1:1", "127.0.0.1:1"
	resp := putObject(t, c, "k", randomBytes(t, 100))
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("got %s, Retry-After %q; want 503 with Retry-After", resp.Status, resp.Header.Get("Retry-After"))
	}
	if _, n := c.coord.counts(); n != 0 {
		t.Fatalf("%d completes, want 0", n)
	}
}

// startHeld starts n requests that block at the fake nodes and waits until
// each has reached a node.
func startHeld(t *testing.T, c *cluster, n, arrivalsEach int, do func()) *sync.WaitGroup {
	t.Helper()
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			do()
		}()
	}
	for range n * arrivalsEach {
		select {
		case <-c.nodes[0].arrived:
		case <-c.nodes[1].arrived:
		case <-c.nodes[2].arrived:
		case <-time.After(10 * time.Second):
			t.Fatal("held requests did not reach the nodes")
		}
	}
	return &wg
}

// Mutation: no upload limit.
func TestUploadConcurrencyLimit(t *testing.T) {
	cfg := defaultConfig()
	cfg.MaxUploads = 2
	c := newCluster(t, options{cfg: cfg})
	hold := make(chan struct{})
	for _, n := range c.nodes {
		n.holdWrites, n.arrived = hold, make(chan string, 100)
	}
	statuses := make(chan int, 2)
	wg := startHeld(t, c, 2, 3, func() { statuses <- putObject(t, c, fmt.Sprint(time.Now().UnixNano()), []byte("held")).StatusCode })

	resp := putObject(t, c, "third", []byte("rejected"))
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") != "1" {
		t.Fatalf("third upload: got %s, Retry-After %q; want 503, 1", resp.Status, resp.Header.Get("Retry-After"))
	}
	close(hold)
	wg.Wait()
	for range 2 {
		if code := <-statuses; code != http.StatusOK {
			t.Fatalf("held upload: %d", code)
		}
	}
	if resp := putObject(t, c, "after", []byte("ok")); resp.StatusCode != http.StatusOK {
		t.Fatalf("after the slots freed: %s", resp.Status)
	}
}

// Mutation: no download limit.
func TestDownloadConcurrencyLimit(t *testing.T) {
	cfg := defaultConfig()
	cfg.MaxDownloads = 2
	c := newCluster(t, options{cfg: cfg})
	if resp := putObject(t, c, "k", []byte("data")); resp.StatusCode != http.StatusOK {
		t.Fatal(resp.Status)
	}
	hold := make(chan struct{})
	for _, n := range c.nodes {
		n.holdReads["*"], n.arrived = hold, make(chan string, 100)
	}
	wg := startHeld(t, c, 2, 1, func() { getObject(t, c, "k") })

	resp, _, _ := getObject(t, c, "k")
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") != "1" {
		t.Fatalf("third download: got %s, Retry-After %q; want 503, 1", resp.Status, resp.Header.Get("Retry-After"))
	}
	close(hold)
	wg.Wait()
}

// Mutations: complete the response after a mid-stream failure; no flush per
// chunk (the verified bytes stay in a buffer the abort discards); no reports
// after a failed download. Chunks are small, well under net/http's 4 KiB
// write buffer, so only a flush gets chunk 0 onto the wire.
func TestGetAbortsMidStream(t *testing.T) {
	c := newCluster(t, options{chunkSize: 1000})
	data := randomBytes(t, 2500) // chunks of 1000, 1000, 500
	if resp := putObject(t, c, "k", data); resp.StatusCode != http.StatusOK {
		t.Fatal(resp.Status)
	}
	chunk1 := "obj-1-chunk-1"
	for _, n := range c.nodes {
		n.corrupt[chunk1] = true
	}

	resp, body, err := getObject(t, c, "k")

	if resp.StatusCode != http.StatusOK || resp.ContentLength != 2500 {
		t.Fatalf("got %s, Content-Length %d; want 200 and the full length declared", resp.Status, resp.ContentLength)
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("body read ended with %v, want a broken transfer (unexpected EOF)", err)
	}
	if !bytes.Equal(body, data[:1000]) {
		t.Fatalf("got %d bytes, want exactly the verified chunk 0 (1000 bytes)", len(body))
	}
	c.waitHandled(t, 1)
	c.coord.mu.Lock()
	defer c.coord.mu.Unlock()
	if len(c.coord.reports) != 3 {
		t.Fatalf("%d bad-replica reports, want 3 (one per replica of chunk 1)", len(c.coord.reports))
	}
	for _, r := range c.coord.reports {
		if r.GetChunkId() != chunk1 || r.GetReason() != objectstorev1.ReportBadReplicaRequest_REASON_CHECKSUM_MISMATCH {
			t.Fatalf("report %v", r)
		}
	}
}

// Mutation: the headers go out before chunk 0 is verified.
func TestFirstChunkFailureIs502(t *testing.T) {
	c := newCluster(t, options{chunkSize: 1000})
	if resp := putObject(t, c, "k", randomBytes(t, 2500)); resp.StatusCode != http.StatusOK {
		t.Fatal(resp.Status)
	}
	for _, n := range c.nodes {
		n.corrupt["obj-1-chunk-0"] = true
	}
	resp, err := http.Get(objectURL(c, "k"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway || resp.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("got %s (%s), want 502 with a JSON error", resp.Status, resp.Header.Get("Content-Type"))
	}
	if code := errorCode(t, resp); code != "UPSTREAM_FAILED" {
		t.Fatalf("code %q", code)
	}
}

func TestHeadSendsHeadersOnly(t *testing.T) {
	c := newCluster(t, options{})
	if resp := putObject(t, c, "k", randomBytes(t, 1234)); resp.StatusCode != http.StatusOK {
		t.Fatal(resp.Status)
	}
	for _, n := range c.nodes {
		n.corrupt["obj-1-chunk-0"] = true // a HEAD that read chunks would fail
	}
	resp, err := http.Head(objectURL(c, "k"))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || resp.ContentLength != 1234 {
		t.Fatalf("got %s, Content-Length %d", resp.Status, resp.ContentLength)
	}
}

// Mutation: the whole body is buffered before any chunk is written. The
// body is a pipe that delivers chunk 0 and then waits until chunk 0 is on
// the nodes, so a gateway that waits for the end of the body never gets it.
func TestPutStreamsChunkByChunk(t *testing.T) {
	c := newCluster(t, options{})
	data := randomBytes(t, 9<<20) // chunks of 4, 4 and 1 MiB
	pr, pw := io.Pipe()
	req, _ := http.NewRequest(http.MethodPut, objectURL(c, "streamed"), pr)
	req.ContentLength = int64(len(data))
	done := make(chan *http.Response, 1)
	go func() {
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Errorf("PUT: %v", err)
			done <- nil
			return
		}
		resp.Body.Close()
		done <- resp
	}()

	if _, err := pw.Write(data[:4<<20]); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for !(c.nodes[0].has("obj-1-chunk-0") && c.nodes[1].has("obj-1-chunk-0") && c.nodes[2].has("obj-1-chunk-0")) {
		if time.Now().After(deadline) {
			pw.CloseWithError(errors.New("test gave up"))
			t.Fatal("chunk 0 was not written to the nodes while the rest of the body was pending")
		}
		time.Sleep(10 * time.Millisecond)
	}
	pw.Write(data[4<<20:])
	pw.Close()
	if resp := <-done; resp == nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT did not succeed: %v", resp)
	}
}

// The first chunk reaches the client while the second is still being read
// from a node.
func TestGetStreamsChunkByChunk(t *testing.T) {
	c := newCluster(t, options{})
	data := randomBytes(t, 5<<20)
	if resp := putObject(t, c, "k", data); resp.StatusCode != http.StatusOK {
		t.Fatal(resp.Status)
	}
	hold := make(chan struct{})
	for _, n := range c.nodes {
		n.holdReads["obj-1-chunk-1"] = hold
	}
	resp, err := http.Get(objectURL(c, "k"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	first := make([]byte, 4<<20)
	got := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(resp.Body, first)
		got <- err
	}()
	select {
	case err := <-got:
		if err != nil || !bytes.Equal(first, data[:4<<20]) {
			t.Fatalf("first chunk: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("chunk 0 did not arrive while chunk 1 was held")
	}
	close(hold)
	rest, err := io.ReadAll(resp.Body)
	if err != nil || !bytes.Equal(rest, data[4<<20:]) {
		t.Fatalf("rest: %v", err)
	}
}

// The client hangs up halfway through the body: nothing completes. Two
// things guarantee it: net/http cancels the request context on the body read
// error, and the stream source refuses a short read
// (TestStreamSourceFailsOnShortRead guards the second on its own).
func TestClientDisconnectNeverCommits(t *testing.T) {
	c := newCluster(t, options{chunkSize: 1000})
	conn, err := net.Dial("tcp", c.srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	// Registered after the server's cleanup, so it runs first: a failing
	// test must not leave a handler waiting on this body.
	t.Cleanup(func() { conn.Close() })
	fmt.Fprintf(conn, "PUT /objects/k HTTP/1.1\r\nHost: x\r\nContent-Length: 3000\r\n\r\n")
	conn.Write(randomBytes(t, 1500))
	deadline := time.Now().Add(5 * time.Second)
	for !c.nodes[0].has("obj-1-chunk-0") {
		if time.Now().After(deadline) {
			t.Fatal("chunk 0 never reached the nodes")
		}
		time.Sleep(10 * time.Millisecond)
	}
	conn.Close()
	c.waitHandled(t, 1)

	if _, n := c.coord.counts(); n != 0 {
		t.Fatalf("%d completes after the client hung up, want 0", n)
	}
	c.coord.mu.Lock()
	defer c.coord.mu.Unlock()
	if _, ok := c.coord.objects["k"]; ok {
		t.Fatal("key k resolves after an incomplete upload")
	}
}

// Mutation: no stall deadline on the body. The client sends half the body
// and then nothing, without hanging up.
func TestStalledBodyAborted(t *testing.T) {
	cfg := defaultConfig()
	cfg.StallTimeout = 300 * time.Millisecond
	c := newCluster(t, options{cfg: cfg, chunkSize: 1000})
	start := time.Now()
	resp, _ := raw(t, c, "PUT /objects/k HTTP/1.1\r\nHost: x\r\nContent-Length: 3000\r\n\r\n"+strings.Repeat("x", 1500))
	if resp.StatusCode != http.StatusRequestTimeout {
		t.Fatalf("got %s, want 408", resp.Status)
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("took %s to give up on a stalled body", took)
	}
	if _, n := c.coord.counts(); n != 0 {
		t.Fatalf("%d completes, want 0", n)
	}
}

func TestDeleteAndList(t *testing.T) {
	c := newCluster(t, options{})
	for _, k := range []string{"p/1", "p/2", "p/3", "q/1"} {
		putObject(t, c, k, []byte(k))
	}
	resp, err := http.Get(c.srv.URL + "/objects?prefix=p%2F&start_after=p%2F1&limit=1")
	if err != nil {
		t.Fatal(err)
	}
	var page listResponse
	json.NewDecoder(resp.Body).Decode(&page)
	resp.Body.Close()
	if len(page.Objects) != 1 || page.Objects[0].Key != "p/2" || page.NextStartAfter != "p/2" {
		t.Fatalf("page %+v", page)
	}
	c.coord.mu.Lock()
	last := c.coord.lists[len(c.coord.lists)-1]
	c.coord.mu.Unlock()
	if last.GetPrefix() != "p/" || last.GetStartAfter() != "p/1" || last.GetLimit() != 1 {
		t.Fatalf("passed through %v", last)
	}
	if resp, _ := http.Get(c.srv.URL + "/objects?limit=abc"); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("limit=abc: %s", resp.Status)
	}

	req, _ := http.NewRequest(http.MethodDelete, objectURL(c, "p/1"), nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("DELETE: %v %v", resp.Status, err)
	}
	resp, _ = http.DefaultClient.Do(req)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("second DELETE: %s, want 404", resp.Status)
	}
	if resp, _, _ := getObject(t, c, "p/1"); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET after DELETE: %s", resp.Status)
	}
}

func TestClusterJSON(t *testing.T) {
	c := newCluster(t, options{})
	resp, err := http.Get(c.srv.URL + "/cluster")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var cl clusterResponse
	if err := json.NewDecoder(resp.Body).Decode(&cl); err != nil {
		t.Fatal(err)
	}
	if len(cl.Nodes) != 3 || cl.Nodes[0].Liveness != "ALIVE" || cl.UnderReplicatedChunks != 2 ||
		cl.PendingDeletions != 5 || cl.LastRepairPass == nil || cl.LastRepairPass.CopiesStarted != 1 || cl.LastGcPass != nil {
		t.Fatalf("cluster %+v", cl)
	}
}

// Mutation: the busy response leaves the connection open, so net/http first
// drains the unread body. A client that sent headers and then nothing must
// still get its 503 at once.
func TestRefusedUploadDoesNotWaitForItsBody(t *testing.T) {
	cfg := defaultConfig()
	cfg.MaxUploads = 1
	c := newCluster(t, options{cfg: cfg})
	hold := make(chan struct{})
	for _, n := range c.nodes {
		n.holdWrites, n.arrived = hold, make(chan string, 100)
	}
	wg := startHeld(t, c, 1, 3, func() { putObject(t, c, "held", []byte("held")) })
	defer func() {
		close(hold)
		wg.Wait()
	}()

	conn, err := net.Dial("tcp", c.srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	io.WriteString(conn, "PUT /objects/k HTTP/1.1\r\nHost: x\r\nContent-Length: 1000\r\n\r\n") // and no body
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("no response to a refused upload whose body never came: %v", err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("got %s, want 503", resp.Status)
	}
}
