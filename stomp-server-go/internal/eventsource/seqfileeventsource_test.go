package eventsource

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

func TestSeqFileSource_StartsAtOneWhenFileMissing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seq.txt")

	s, err := newSeqFileSourceState(path, discardLogger())
	if err != nil {
		t.Fatalf("newSeqFileSourceState: %v", err)
	}
	if got := s.next(); got != "Persistent Event 1" {
		t.Errorf("next() = %q, want %q", got, "Persistent Event 1")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if string(data) != "1" {
		t.Errorf("file contents = %q, want %q", string(data), "1")
	}
}

func TestSeqFileSource_ResumesFromExistingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seq.txt")
	if err := os.WriteFile(path, []byte("41"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}

	s, err := newSeqFileSourceState(path, discardLogger())
	if err != nil {
		t.Fatalf("newSeqFileSourceState: %v", err)
	}
	if got := s.next(); got != "Persistent Event 42" {
		t.Errorf("next() after resume = %q, want %q", got, "Persistent Event 42")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if string(data) != "42" {
		t.Errorf("file contents after resume = %q, want %q", string(data), "42")
	}
}

func TestSeqFileSource_TreatsEmptyFileAsZero(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seq.txt")
	if err := os.WriteFile(path, []byte("  \n"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}

	s, err := newSeqFileSourceState(path, discardLogger())
	if err != nil {
		t.Fatalf("newSeqFileSourceState: %v", err)
	}
	if got := s.next(); got != "Persistent Event 1" {
		t.Errorf("next() = %q, want %q", got, "Persistent Event 1")
	}
}

func TestSeqFileSource_RejectsCorruptFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seq.txt")
	if err := os.WriteFile(path, []byte("not-a-number"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}

	if _, err := newSeqFileSourceState(path, discardLogger()); err == nil {
		t.Fatal("expected newSeqFileSourceState to reject a corrupt sequence file")
	}
}

func TestSeqFileSource_ResumesAcrossSuccessiveInstances(t *testing.T) {
	// Simulates several restarts in a row, each a brand-new SeqFileSource
	// reading whatever the previous one left on disk -- without ever
	// running two instances' background goroutines concurrently against the
	// same file.
	path := filepath.Join(t.TempDir(), "seq.txt")

	for i, want := range []string{"Persistent Event 1", "Persistent Event 2", "Persistent Event 3"} {
		s, err := newSeqFileSourceState(path, discardLogger())
		if err != nil {
			t.Fatalf("instance %d: newSeqFileSourceState: %v", i, err)
		}
		if got := s.next(); got != want {
			t.Errorf("instance %d: next() = %q, want %q", i, got, want)
		}
	}
}

func TestSeqFileSource_RealTickerWiring(t *testing.T) {
	// A lightweight end-to-end sanity check that NewSeqFileSource wires its
	// ticker and channel correctly. Deliberately does not use t.TempDir():
	// this type has no Stop() (matching TickerSource), so its background
	// goroutine keeps ticking indefinitely after the test returns;
	// t.TempDir()'s cleanup racing that goroutine's next write is exactly
	// the flakiness the other tests in this file avoid by driving next()
	// directly instead of a real ticker.
	dir, err := os.MkdirTemp("", "seqfilesource-wiring-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	path := filepath.Join(dir, "seq.txt")

	s, err := NewSeqFileSource(path, time.Millisecond, discardLogger())
	if err != nil {
		t.Fatalf("NewSeqFileSource: %v", err)
	}
	select {
	case got := <-s.Events():
		if got != "Persistent Event 1" {
			t.Errorf("first event = %q, want %q", got, "Persistent Event 1")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the first event")
	}
}
