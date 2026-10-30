package upload

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

// Mutation: the stream source ignores a short read. A body that ends early
// must fail the chunk, never hand the replicas a buffer holding old bytes.
func TestStreamSourceFailsOnShortRead(t *testing.T) {
	src := NewStreamSource(strings.NewReader(strings.Repeat("a", 10)+"bcd"), 20)
	r, at, err := src.Chunk(0, 10)
	if err != nil {
		t.Fatalf("first chunk: %v", err)
	}
	got := make([]byte, 10)
	if _, err := r.ReadAt(got, at); err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte(strings.Repeat("a", 10))) {
		t.Fatalf("first chunk %q", got)
	}
	if _, _, err := src.Chunk(10, 10); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("second chunk (3 of 10 bytes available): err %v, want unexpected EOF", err)
	}
}

func TestStreamSourceBufferIsSizedToTheObject(t *testing.T) {
	if n := len(NewStreamSource(strings.NewReader(""), 10).buf); n != 10 {
		t.Fatalf("buffer of %d bytes for a 10-byte object", n)
	}
	if n := len(NewStreamSource(strings.NewReader(""), 5<<30).buf); n != MaxChunkSize {
		t.Fatalf("buffer of %d bytes for a 5 GiB object, want one chunk", n)
	}
}
