package chunkstore

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

const testChunkID = "0b6f0d8e-7a64-4a8e-9b3a-2a1c7f0e5d11"

// TestCommitDurabilityOrder records the filesystem calls Commit makes. The
// order is what makes a write crash-safe: data synced before it gets a final
// name, the name made durable before the write is acknowledged, and the temp
// name removed last. No test can observe a missing fsync on its own (the page
// cache hides it), so this checks the calls are made, in order.
func TestCommitDurabilityOrder(t *testing.T) {
	dir := t.TempDir()
	s, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	var calls []string
	real := s.ops
	s.ops = ops{
		syncFile: func(f *os.File) error { calls = append(calls, "sync temp"); return real.syncFile(f) },
		link: func(oldname, newname string) error {
			calls = append(calls, "link "+filepath.Base(filepath.Dir(oldname))+" -> "+rel(dir, newname))
			return real.link(oldname, newname)
		},
		syncDir: func(path string) error { calls = append(calls, "sync dir "+rel(dir, path)); return real.syncDir(path) },
		remove: func(path string) error {
			calls = append(calls, "unlink "+filepath.Base(filepath.Dir(path)))
			return real.remove(path)
		},
	}

	w, err := s.NewWriter(t.Context(), testChunkID)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("durable bytes")
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	if err := w.Commit(t.Context(), sum[:]); err != nil {
		t.Fatal(err)
	}

	want := []string{
		"sync temp",
		"link tmp -> chunks/" + testChunkID,
		"sync dir chunks",
		"unlink tmp",
	}
	if !slices.Equal(calls, want) {
		t.Errorf("calls =\n  %q\nwant\n  %q", calls, want)
	}
}

func TestCleanTemps(t *testing.T) {
	tests := []struct {
		name  string
		temps []string
	}{
		{"no temps", nil},
		{"one temp", []string{"chunk-" + testChunkID + "-123"}},
		{"several, one with an odd name", []string{"chunk-" + testChunkID + "-1", "chunk-" + testChunkID + "-2", "junk"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			s, err := New(dir)
			if err != nil {
				t.Fatal(err)
			}
			for _, name := range tt.temps {
				if err := os.WriteFile(filepath.Join(dir, "tmp", name), []byte("partial"), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			// A committed chunk must survive cleanup.
			w, err := s.NewWriter(t.Context(), testChunkID)
			if err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256(nil)
			if err := w.Commit(t.Context(), sum[:]); err != nil {
				t.Fatal(err)
			}

			removed, err := s.CleanTemps(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if len(removed) != len(tt.temps) {
				t.Errorf("removed %q, want %q", removed, tt.temps)
			}
			left, err := os.ReadDir(filepath.Join(dir, "tmp"))
			if err != nil {
				t.Fatal(err)
			}
			if len(left) != 0 {
				t.Errorf("tmp still has %d entries", len(left))
			}
			if _, err := s.Read(t.Context(), testChunkID); err != nil {
				t.Errorf("committed chunk unreadable after cleanup: %v", err)
			}
		})
	}
}

func TestChunkIDFromTempName(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{"chunk-" + testChunkID + "-123456", testChunkID},
		{"chunk-not-a-uuid", ""},
		{"other-" + testChunkID, ""},
	}
	for _, tt := range tests {
		if got := ChunkIDFromTempName(tt.name); got != tt.want {
			t.Errorf("ChunkIDFromTempName(%q) = %q, want %q", tt.name, got, tt.want)
		}
	}
}

func rel(base, path string) string {
	r, err := filepath.Rel(base, path)
	if err != nil {
		return path
	}
	return r
}
