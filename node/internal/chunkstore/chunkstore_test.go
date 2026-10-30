package chunkstore

import (
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

const (
	testChunkID  = "0b6f0d8e-7a64-4a8e-9b3a-2a1c7f0e5d11"
	otherChunkID = "7d1e2f3a-4b5c-4d6e-8f70-112233445566"
	thirdChunkID = "11111111-2222-4333-8444-555555555555"
)

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

// commit writes data as chunk id and commits it.
func commit(t *testing.T, s *Store, id string, data []byte) error {
	t.Helper()
	w, err := s.NewWriter(t.Context(), id)
	if err != nil {
		return err
	}
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	return w.Commit(t.Context(), sum[:])
}

func exists(t *testing.T, path string) bool {
	t.Helper()
	_, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false
	}
	if err != nil {
		t.Fatal(err)
	}
	return true
}

// TestDeleteOrder records the calls Delete makes: the tombstone is durable
// before the file is unlinked, and the unlink is made durable before Delete
// returns. With the tombstone first, a crash at any point leaves either the
// file with its tombstone (swept at startup) or no file.
func TestDeleteOrder(t *testing.T) {
	dir := t.TempDir()
	s, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := commit(t, s, testChunkID, []byte("doomed")); err != nil {
		t.Fatal(err)
	}
	var calls []string
	real := s.ops
	s.ops.syncDir = func(path string) error { calls = append(calls, "sync dir "+rel(dir, path)); return real.syncDir(path) }
	s.ops.remove = func(path string) error { calls = append(calls, "unlink "+rel(dir, path)); return real.remove(path) }

	removed, err := s.Delete(t.Context(), []Deletion{{ChunkID: testChunkID, Tombstone: true}})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(removed, []bool{true}) {
		t.Errorf("removed = %v, want [true]", removed)
	}
	want := []string{"sync dir tombstones", "unlink chunks/" + testChunkID, "sync dir chunks"}
	if !slices.Equal(calls, want) {
		t.Errorf("calls =\n  %q\nwant\n  %q", calls, want)
	}
}

func TestDelete(t *testing.T) {
	tests := []struct {
		name      string
		tombstone bool
		// whether a later write of the same ID succeeds
		writeAfter error
	}{
		{"with tombstone: the ID can never be written again", true, ErrDeleted},
		{"without tombstone: a good copy can be written again", false, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			s, err := New(dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := commit(t, s, testChunkID, []byte("first copy")); err != nil {
				t.Fatal(err)
			}
			d := []Deletion{{ChunkID: testChunkID, Tombstone: tt.tombstone}}
			removed, err := s.Delete(t.Context(), d)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(removed, []bool{true}) {
				t.Errorf("first delete: removed = %v, want [true]", removed)
			}
			if exists(t, filepath.Join(dir, "chunks", testChunkID)) {
				t.Error("file still exists after delete")
			}
			// Idempotent: a second delete succeeds and reports the file absent.
			removed, err = s.Delete(t.Context(), d)
			if err != nil {
				t.Fatalf("second delete: %v", err)
			}
			if !slices.Equal(removed, []bool{false}) {
				t.Errorf("second delete: removed = %v, want [false]", removed)
			}

			err = commit(t, s, testChunkID, []byte("second copy"))
			if !errors.Is(err, tt.writeAfter) || (tt.writeAfter == nil) != (err == nil) {
				t.Errorf("write after delete: err = %v, want %v", err, tt.writeAfter)
			}
			if tt.writeAfter != nil && exists(t, filepath.Join(dir, "chunks", testChunkID)) {
				t.Error("a refused write left a file")
			}
		})
	}

	t.Run("an invalid ID changes nothing", func(t *testing.T) {
		dir := t.TempDir()
		s, err := New(dir)
		if err != nil {
			t.Fatal(err)
		}
		if err := commit(t, s, testChunkID, []byte("keep me")); err != nil {
			t.Fatal(err)
		}
		_, err = s.Delete(t.Context(), []Deletion{{ChunkID: testChunkID, Tombstone: true}, {ChunkID: "../x"}})
		if !errors.Is(err, ErrInvalidID) {
			t.Errorf("err = %v, want ErrInvalidID", err)
		}
		if !exists(t, filepath.Join(dir, "chunks", testChunkID)) {
			t.Error("the valid ID in the batch was deleted")
		}
	})
}

// TestWriteRacingDelete runs a whole Delete in the window between a write's
// fast-path check and its link: the delete finds no file (ALREADY_ABSENT),
// then the write links one. Commit's check after the link must find the
// tombstone and remove the file, or it would be an orphan nothing tracks.
func TestWriteRacingDelete(t *testing.T) {
	dir := t.TempDir()
	s, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	w, err := s.NewWriter(t.Context(), testChunkID) // passes the fast path: no tombstone yet
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("late write")
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	real := s.ops.link
	var removed []bool
	s.ops.link = func(oldname, newname string) error {
		var err error
		removed, err = s.Delete(t.Context(), []Deletion{{ChunkID: testChunkID, Tombstone: true}})
		if err != nil {
			t.Fatal(err)
		}
		return real(oldname, newname)
	}
	sum := sha256.Sum256(data)
	err = w.Commit(t.Context(), sum[:])
	if !errors.Is(err, ErrDeleted) {
		t.Errorf("commit: err = %v, want ErrDeleted", err)
	}
	if !slices.Equal(removed, []bool{false}) {
		t.Errorf("delete in the window: removed = %v, want [false] (no file yet)", removed)
	}
	if exists(t, filepath.Join(dir, "chunks", testChunkID)) {
		t.Error("the racing write left a file under the final name")
	}
	if _, _, err := s.Stat(t.Context(), testChunkID); !errors.Is(err, ErrNotFound) {
		t.Errorf("stat: err = %v, want ErrNotFound", err)
	}
}

func TestSweepTombstoned(t *testing.T) {
	dir := t.TempDir()
	s, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	// A process died between linking otherChunkID and seeing its tombstone.
	if err := commit(t, s, otherChunkID, []byte("orphan")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "tombstones", otherChunkID), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	// A live chunk without a tombstone, and a tombstone without a file.
	if err := commit(t, s, testChunkID, []byte("live")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "tombstones", thirdChunkID), nil, 0o644); err != nil {
		t.Fatal(err)
	}

	swept, err := s.SweepTombstoned(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(swept, []string{otherChunkID}) {
		t.Errorf("swept %q, want [%s]", swept, otherChunkID)
	}
	if exists(t, filepath.Join(dir, "chunks", otherChunkID)) {
		t.Error("tombstoned file survived the sweep")
	}
	if _, err := s.Read(t.Context(), testChunkID); err != nil {
		t.Errorf("live chunk unreadable after the sweep: %v", err)
	}
}

// TestOpenReadSurvivesDelete checks the property Delete relies on: unlinking
// removes the name, not the data, so a read that already opened the file
// gets every byte. (Store.Read opens and then reads, so this is its window.)
func TestOpenReadSurvivesDelete(t *testing.T) {
	dir := t.TempDir()
	s, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("read me while I go")
	if err := commit(t, s, testChunkID, data); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(filepath.Join(dir, "chunks", testChunkID))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := s.Delete(t.Context(), []Deletion{{ChunkID: testChunkID, Tombstone: true}}); err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	if string(b[HeaderSize:]) != string(data) {
		t.Errorf("open reader got %q, want %q", b[HeaderSize:], data)
	}
}

func rel(base, path string) string {
	r, err := filepath.Rel(base, path)
	if err != nil {
		return path
	}
	return r
}
