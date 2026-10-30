package httpapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	objectstorev1 "github.com/Ayomide1x/object-store/gen/go/objectstore/v1"
)

// fakeNode stores chunks in memory. corrupt makes it serve a chunk with one
// byte flipped; holdWrites and holdReads make every write or read wait until
// the channel is closed, after announcing itself on arrived.
type fakeNode struct {
	objectstorev1.UnimplementedStorageNodeServiceServer
	id   string
	addr string

	mu         sync.Mutex
	chunks     map[string][]byte
	corrupt    map[string]bool
	holdWrites chan struct{}
	holdReads  map[string]chan struct{} // by chunk ID; "*" holds every read
	arrived    chan string
}

func (n *fakeNode) WriteChunk(stream objectstorev1.StorageNodeService_WriteChunkServer) error {
	n.mu.Lock()
	hold, arrived := n.holdWrites, n.arrived
	n.mu.Unlock()
	if arrived != nil {
		arrived <- "write"
	}
	if hold != nil {
		select {
		case <-hold:
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
	var id string
	var data bytes.Buffer
	var trailer []byte
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		switch f := req.GetFrame().(type) {
		case *objectstorev1.WriteChunkRequest_Header:
			id = f.Header.GetChunkId()
		case *objectstorev1.WriteChunkRequest_Data:
			data.Write(f.Data)
		case *objectstorev1.WriteChunkRequest_Trailer:
			trailer = f.Trailer.GetSha256()
		}
	}
	sum := sha256.Sum256(data.Bytes())
	if !bytes.Equal(sum[:], trailer) {
		return status.Error(codes.InvalidArgument, "checksum mismatch")
	}
	n.mu.Lock()
	n.chunks[id] = data.Bytes()
	n.mu.Unlock()
	return stream.SendAndClose(&objectstorev1.WriteChunkResponse{ChunkId: id, BytesWritten: int64(data.Len()), Sha256: sum[:]})
}

func (n *fakeNode) ReadChunk(req *objectstorev1.ReadChunkRequest, stream objectstorev1.StorageNodeService_ReadChunkServer) error {
	n.mu.Lock()
	data, ok := n.chunks[req.GetChunkId()]
	corrupt := n.corrupt[req.GetChunkId()]
	hold := n.holdReads[req.GetChunkId()]
	if hold == nil {
		hold = n.holdReads["*"]
	}
	arrived := n.arrived
	n.mu.Unlock()
	if hold != nil {
		if arrived != nil {
			arrived <- "read " + req.GetChunkId()
		}
		select {
		case <-hold:
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
	if !ok {
		return status.Error(codes.NotFound, "no such chunk")
	}
	if corrupt {
		data = bytes.Clone(data)
		data[len(data)/2] ^= 0xff
	}
	for off := 0; off < len(data) || off == 0; off += 1 << 20 {
		if err := stream.Send(&objectstorev1.ReadChunkResponse{Data: data[off:min(off+1<<20, len(data))]}); err != nil {
			return err
		}
		if len(data) == 0 {
			break
		}
	}
	return nil
}

func (n *fakeNode) has(chunkID string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	_, ok := n.chunks[chunkID]
	return ok
}

type fakeChunk struct {
	id   string
	size int64
	sha  []byte
}

type fakeObject struct {
	id     string
	size   int64
	chunks []fakeChunk
}

// fakeCoordinator places every chunk on all three fake nodes, commits when
// every chunk has a hash, and resolves keys to committed objects. errs makes
// a method fail with the given error instead.
type fakeCoordinator struct {
	objectstorev1.UnimplementedCoordinatorServiceServer
	nodes     []*fakeNode
	chunkSize int64

	mu        sync.Mutex
	nextID    int
	pending   map[string]fakeObject
	objects   map[string]fakeObject // by key
	completes int
	initiates int
	addresses []objectstorev1.NodeAddressSet // as asked, by InitiateUpload and GetObject
	reports   []*objectstorev1.ReportBadReplicaRequest
	lists     []*objectstorev1.ListObjectsRequest
	errs      map[string]error
}

func (c *fakeCoordinator) fail(method string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.errs[method]
}

func (c *fakeCoordinator) locations(addresses objectstorev1.NodeAddressSet) []*objectstorev1.NodeLocation {
	var out []*objectstorev1.NodeLocation
	for _, n := range c.nodes {
		addr := n.addr
		if addresses != objectstorev1.NodeAddressSet_NODE_ADDRESS_SET_INTERNAL {
			addr = "client-address-unreachable:1" // only the internal set is reachable here
		}
		out = append(out, &objectstorev1.NodeLocation{NodeId: n.id, Address: addr, Liveness: objectstorev1.NodeLiveness_NODE_LIVENESS_ALIVE})
	}
	return out
}

func (c *fakeCoordinator) InitiateUpload(_ context.Context, req *objectstorev1.InitiateUploadRequest) (*objectstorev1.InitiateUploadResponse, error) {
	if err := c.fail("InitiateUpload"); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.initiates++
	c.addresses = append(c.addresses, req.GetNodeAddresses())
	c.nextID++
	obj := fakeObject{id: fmt.Sprintf("obj-%d", c.nextID), size: req.GetSizeBytes()}
	resp := &objectstorev1.InitiateUploadResponse{ObjectId: obj.id}
	for i, off := 0, int64(0); off < req.GetSizeBytes(); i, off = i+1, off+c.chunkSize {
		size := min(c.chunkSize, req.GetSizeBytes()-off)
		id := fmt.Sprintf("%s-chunk-%d", obj.id, i)
		obj.chunks = append(obj.chunks, fakeChunk{id: id, size: size})
		resp.Chunks = append(resp.Chunks, &objectstorev1.ChunkPlacement{ChunkId: id, Index: int32(i), SizeBytes: size,
			Nodes: c.locations(req.GetNodeAddresses())})
	}
	c.pending[obj.id] = obj
	return resp, nil
}

func (c *fakeCoordinator) CompleteUpload(_ context.Context, req *objectstorev1.CompleteUploadRequest) (*objectstorev1.CompleteUploadResponse, error) {
	if err := c.fail("CompleteUpload"); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.completes++
	obj, ok := c.pending[req.GetObjectId()]
	if !ok || len(req.GetChunks()) != len(obj.chunks) {
		return nil, status.Error(codes.InvalidArgument, "chunk list does not match the plan")
	}
	for i, ch := range req.GetChunks() {
		obj.chunks[i].sha = ch.GetSha256()
	}
	delete(c.pending, obj.id)
	c.objects[req.GetKey()] = obj
	return &objectstorev1.CompleteUploadResponse{}, nil
}

func (c *fakeCoordinator) GetObject(_ context.Context, req *objectstorev1.GetObjectRequest) (*objectstorev1.GetObjectResponse, error) {
	if err := c.fail("GetObject"); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.addresses = append(c.addresses, req.GetNodeAddresses())
	obj, ok := c.objects[req.GetKey()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "key '%s' not found", req.GetKey())
	}
	resp := &objectstorev1.GetObjectResponse{ObjectId: obj.id, SizeBytes: obj.size}
	for i, ch := range obj.chunks {
		resp.Chunks = append(resp.Chunks, &objectstorev1.ChunkInfo{ChunkId: ch.id, Index: int32(i), SizeBytes: ch.size,
			Sha256: ch.sha, Replicas: c.locations(req.GetNodeAddresses())})
	}
	return resp, nil
}

func (c *fakeCoordinator) ReportBadReplica(_ context.Context, req *objectstorev1.ReportBadReplicaRequest) (*objectstorev1.ReportBadReplicaResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reports = append(c.reports, req)
	return &objectstorev1.ReportBadReplicaResponse{Verdict: objectstorev1.ReportBadReplicaResponse_VERDICT_MARKED_BAD}, nil
}

func (c *fakeCoordinator) DeleteObject(_ context.Context, req *objectstorev1.DeleteObjectRequest) (*objectstorev1.DeleteObjectResponse, error) {
	if err := c.fail("DeleteObject"); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	obj, ok := c.objects[req.GetKey()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "key '%s' not found", req.GetKey())
	}
	delete(c.objects, req.GetKey())
	return &objectstorev1.DeleteObjectResponse{ObjectId: obj.id}, nil
}

// ListObjects pages by key exactly as the coordinator does: after
// start_after, up to limit (0 means 1000), cursor only when more follow.
func (c *fakeCoordinator) ListObjects(_ context.Context, req *objectstorev1.ListObjectsRequest) (*objectstorev1.ListObjectsResponse, error) {
	if err := c.fail("ListObjects"); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lists = append(c.lists, req)
	var keys []string
	for k := range c.objects {
		if strings.HasPrefix(k, req.GetPrefix()) && k > req.GetStartAfter() {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	limit := int(req.GetLimit())
	if limit == 0 || limit > 1000 {
		limit = 1000
	}
	resp := &objectstorev1.ListObjectsResponse{}
	if len(keys) > limit {
		keys = keys[:limit]
		resp.NextStartAfter = keys[limit-1]
	}
	for _, k := range keys {
		resp.Objects = append(resp.Objects, &objectstorev1.ObjectSummary{Key: k, ObjectId: c.objects[k].id, SizeBytes: c.objects[k].size})
	}
	return resp, nil
}

func (c *fakeCoordinator) StatObject(_ context.Context, req *objectstorev1.StatObjectRequest) (*objectstorev1.StatObjectResponse, error) {
	if err := c.fail("StatObject"); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	obj, ok := c.objects[req.GetKey()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "key '%s' not found", req.GetKey())
	}
	resp := &objectstorev1.StatObjectResponse{ObjectId: obj.id, SizeBytes: obj.size}
	for i, ch := range obj.chunks {
		cs := &objectstorev1.ChunkStatus{ChunkId: ch.id, Index: int32(i), SizeBytes: ch.size, Sha256: ch.sha}
		for _, n := range c.nodes {
			cs.Replicas = append(cs.Replicas, &objectstorev1.ReplicaStatus{NodeId: n.id, State: objectstorev1.ReplicaStatus_STATE_HEALTHY,
				Origin: "upload", Liveness: objectstorev1.NodeLiveness_NODE_LIVENESS_ALIVE})
		}
		cs.CountedReplicas = int32(len(cs.Replicas))
		resp.Chunks = append(resp.Chunks, cs)
	}
	return resp, nil
}

func (c *fakeCoordinator) GetClusterStatus(context.Context, *objectstorev1.GetClusterStatusRequest) (*objectstorev1.GetClusterStatusResponse, error) {
	if err := c.fail("GetClusterStatus"); err != nil {
		return nil, err
	}
	resp := &objectstorev1.GetClusterStatusResponse{UnderReplicatedChunks: 2, PendingDeletions: 5,
		LastRepairPass: &objectstorev1.RepairPass{PassId: 9, CopiesStarted: 1}, UnderReplicatedAsOfUnixMillis: 1234,
		LivenessEventsDropped: 3, RecentLivenessEvents: []*objectstorev1.LivenessEvent{
			{EventId: 2, NodeId: "node2", From: objectstorev1.NodeLiveness_NODE_LIVENESS_ALIVE,
				To: objectstorev1.NodeLiveness_NODE_LIVENESS_SUSPECT, AtUnixMillis: 99},
			{EventId: 1, NodeId: "node2", To: objectstorev1.NodeLiveness_NODE_LIVENESS_UNKNOWN, AtUnixMillis: 98},
		}}
	for _, n := range c.nodes {
		resp.Nodes = append(resp.Nodes, &objectstorev1.NodeStatus{NodeId: n.id, Liveness: objectstorev1.NodeLiveness_NODE_LIVENESS_ALIVE})
	}
	return resp, nil
}

func (c *fakeCoordinator) counts() (initiates, completes int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.initiates, c.completes
}

// cluster is a gateway under test, with its fake coordinator and nodes.
type cluster struct {
	coord *fakeCoordinator
	nodes []*fakeNode
	srv   *httptest.Server
	// handled receives once each time a request's handler returns.
	handled chan struct{}
}

// options for newCluster.
type options struct {
	cfg       Config
	chunkSize int64
}

func defaultConfig() Config {
	return Config{MaxUploads: 4, MaxDownloads: 4, StallTimeout: 5 * time.Second,
		ChunkWriteTimeout: 5 * time.Second, ChunkReadTimeout: 5 * time.Second}
}

func listen(t *testing.T) net.Listener {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return lis
}

func newCluster(t *testing.T, o options) *cluster {
	t.Helper()
	if o.cfg.MaxUploads == 0 {
		o.cfg = defaultConfig()
	}
	if o.chunkSize == 0 {
		o.chunkSize = 4 << 20
	}
	c := &cluster{handled: make(chan struct{}, 100)}
	for i := 1; i <= 3; i++ {
		lis := listen(t)
		n := &fakeNode{id: fmt.Sprintf("node%d", i), addr: lis.Addr().String(), chunks: map[string][]byte{},
			corrupt: map[string]bool{}, holdReads: map[string]chan struct{}{}}
		srv := grpc.NewServer()
		objectstorev1.RegisterStorageNodeServiceServer(srv, n)
		go srv.Serve(lis)
		t.Cleanup(srv.Stop)
		c.nodes = append(c.nodes, n)
	}
	c.coord = &fakeCoordinator{nodes: c.nodes, chunkSize: o.chunkSize, pending: map[string]fakeObject{},
		objects: map[string]fakeObject{}, errs: map[string]error{}}
	lis := listen(t)
	gsrv := grpc.NewServer()
	objectstorev1.RegisterCoordinatorServiceServer(gsrv, c.coord)
	go gsrv.Serve(lis)
	t.Cleanup(gsrv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(CoordinatorDeadline(5*time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	log := slog.New(slog.NewTextHandler(testLogOut, nil))
	c.srv = httptest.NewUnstartedServer(nil)
	if o.cfg.AllowedHosts == nil {
		// The test server's address, as http.Client sends it, and "x", the
		// Host of the hand-written requests.
		o.cfg.AllowedHosts = []string{c.srv.Listener.Addr().String(), "x"}
	}
	h := New(objectstorev1.NewCoordinatorServiceClient(conn), o.cfg, log).Handler()
	c.srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { c.handled <- struct{}{} }()
		h.ServeHTTP(w, r)
	})
	c.srv.Start()
	t.Cleanup(c.srv.Close)
	return c
}

// waitHandled waits for n handlers to return.
func (c *cluster) waitHandled(t *testing.T, n int) {
	t.Helper()
	for range n {
		select {
		case <-c.handled:
		case <-time.After(10 * time.Second):
			t.Fatal("handler did not return within 10s")
		}
	}
}

var testLogOut io.Writer = io.Discard
