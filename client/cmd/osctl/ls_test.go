package main

import (
	"bytes"
	"context"
	"net"
	"sort"
	"strings"
	"testing"

	"google.golang.org/grpc"

	objectstorev1 "github.com/Ayomide1x/object-store/gen/go/objectstore/v1"
)

// pagingCoordinator lists keys two at a time, as the coordinator pages them.
type pagingCoordinator struct {
	objectstorev1.UnimplementedCoordinatorServiceServer
	keys  []string
	calls int
}

func (p *pagingCoordinator) ListObjects(_ context.Context, req *objectstorev1.ListObjectsRequest) (*objectstorev1.ListObjectsResponse, error) {
	p.calls++
	var match []string
	for _, k := range p.keys {
		if strings.HasPrefix(k, req.GetPrefix()) && k > req.GetStartAfter() {
			match = append(match, k)
		}
	}
	sort.Strings(match)
	resp := &objectstorev1.ListObjectsResponse{}
	if len(match) > 2 {
		match = match[:2]
		resp.NextStartAfter = match[1]
	}
	for _, k := range match {
		resp.Objects = append(resp.Objects, &objectstorev1.ObjectSummary{Key: k, ObjectId: "id-" + k, SizeBytes: 1})
	}
	return resp, nil
}

// Mutation: ls prints only the first page.
func TestLsFollowsEveryPage(t *testing.T) {
	fake := &pagingCoordinator{keys: []string{"p/a", "p/b", "p/c", "p/d", "p/e", "q/a"}}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	objectstorev1.RegisterCoordinatorServiceServer(srv, fake)
	go srv.Serve(lis)
	defer srv.Stop()

	var out bytes.Buffer
	if err := ls(context.Background(), &out, lis.Addr().String(), "p/"); err != nil {
		t.Fatal(err)
	}

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	var keys []string
	for _, l := range lines[1:] {
		keys = append(keys, strings.Fields(l)[0])
	}
	if !strings.HasPrefix(lines[0], "KEY") || strings.Join(keys, ",") != "p/a,p/b,p/c,p/d,p/e" {
		t.Fatalf("output:\n%s", out.String())
	}
	if fake.calls != 3 {
		t.Fatalf("%d ListObjects calls, want 3 pages", fake.calls)
	}
}
