package eventsource

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func expectEvent(t *testing.T, s *FSNotifySource, want string) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case got := <-s.Events():
			if got == want {
				return
			}
		case <-deadline:
			t.Fatalf("timed out waiting for event %q", want)
		}
	}
}

func TestFSNotifySource_CreateAndModify(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFSNotifySource(dir, discardLogger())
	if err != nil {
		t.Fatalf("NewFSNotifySource: %v", err)
	}
	f := filepath.Join(dir, "a.txt")
	if err := os.WriteFile(f, []byte("one"), 0o644); err != nil {
		t.Fatal(err)
	}
	expectEvent(t, s, f)

	// Drain anything left from the create, then modify.
	time.Sleep(100 * time.Millisecond)
	for len(s.Events()) > 0 {
		<-s.Events()
	}
	if err := os.WriteFile(f, []byte("two"), 0o644); err != nil {
		t.Fatal(err)
	}
	expectEvent(t, s, f)
}

func TestFSNotifySource_NewSubdirectory(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFSNotifySource(dir, discardLogger())
	if err != nil {
		t.Fatalf("NewFSNotifySource: %v", err)
	}
	sub := filepath.Join(dir, "sub")
	if err := os.Mkdir(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	f := filepath.Join(sub, "b.txt")
	if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	expectEvent(t, s, f)
}

func TestFSNotifySource_MissingPath(t *testing.T) {
	if _, err := NewFSNotifySource(filepath.Join(t.TempDir(), "nope"), discardLogger()); err == nil {
		t.Fatal("expected error for missing path")
	}
}
