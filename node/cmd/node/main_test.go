package main

import (
	"crypto/sha256"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/Ayomide1x/object-store/node/internal/chunkstore"
)

const (
	tombstonedID = "0b6f0d8e-7a64-4a8e-9b3a-2a1c7f0e5d11"
	liveID       = "7d1e2f3a-4b5c-4d6e-8f70-112233445566"
)

// TestRecoverStore is the node's startup cleanup: a stray temp file and a
// final file that has a tombstone are both removed before the node listens;
// a live chunk is kept.
func TestRecoverStore(t *testing.T) {
	dir := t.TempDir()
	store, err := chunkstore.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{tombstonedID, liveID} {
		w, err := store.NewWriter(t.Context(), id)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(nil)
		if err := w.Commit(t.Context(), sum[:]); err != nil {
			t.Fatal(err)
		}
	}
	// As left by a process that died between a racing write's link and its
	// tombstone check.
	if err := os.WriteFile(filepath.Join(dir, "tombstones", tombstonedID), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	stray := filepath.Join(dir, "tmp", "chunk-"+liveID+"-1")
	if err := os.WriteFile(stray, []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := recoverStore(t.Context(), store, slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		path string
		want bool
	}{
		{"tombstoned final file removed", filepath.Join(dir, "chunks", tombstonedID), false},
		{"stray temp removed", stray, false},
		{"live chunk kept", filepath.Join(dir, "chunks", liveID), true},
	}
	for _, tt := range tests {
		_, err := os.Stat(tt.path)
		if got := !errors.Is(err, os.ErrNotExist); got != tt.want {
			t.Errorf("%s: exists = %v, want %v", tt.name, got, tt.want)
		}
	}
}
