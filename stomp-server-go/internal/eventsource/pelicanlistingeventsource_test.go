package eventsource

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"stomp-server-go/internal/clientqueue"
)

// fakePelicanLister is a pelicanListingRunner test double: for each
// directory URL it's asked to list, it returns responses from a
// per-directory queue (one per call to that directory, repeating the last
// once exhausted), and records every directory it was asked to list so
// tests can assert on what actually got polled.
type fakePelicanLister struct {
	mu        sync.Mutex
	responses map[string][]func() ([]pelicanObject, error)
	calls     map[string]int
	callLog   []string
}

func newFakePelicanLister() *fakePelicanLister {
	return &fakePelicanLister{
		responses: map[string][]func() ([]pelicanObject, error){},
		calls:     map[string]int{},
	}
}

func (f *fakePelicanLister) queue(dir string, resp func() ([]pelicanObject, error)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.responses[dir] = append(f.responses[dir], resp)
}

func (f *fakePelicanLister) list(ctx context.Context, dir string) ([]pelicanObject, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.callLog = append(f.callLog, dir)
	resps := f.responses[dir]
	if len(resps) == 0 {
		return nil, nil
	}
	i := f.calls[dir]
	if i >= len(resps) {
		i = len(resps) - 1
	}
	f.calls[dir]++
	return resps[i]()
}

func (f *fakePelicanLister) callCount(dir string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[dir]
}

func (f *fakePelicanLister) totalCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.callLog)
}

func objectsResponse(objects ...pelicanObject) func() ([]pelicanObject, error) {
	return func() ([]pelicanObject, error) { return objects, nil }
}

func errorResponse(err error) func() ([]pelicanObject, error) {
	return func() ([]pelicanObject, error) { return nil, err }
}

func file(name string) pelicanObject {
	return pelicanObject{Name: name, Size: 12, ModTime: time.Date(2025, 7, 1, 18, 50, 4, 0, time.UTC)}
}

func dir(name string) pelicanObject {
	return pelicanObject{Name: name, IsCollection: true}
}

// queueWithParams is a clientqueue.Queue for tests that only care about
// Params() (used to drive QueueAdded/ShouldNotify) -- the clientID is
// irrelevant, and each call returns a distinct instance (MemoryQueue is a
// pointer type), so two queues built with the same params are still
// distinct tracked entries, matching two different real clients watching
// the same directory.
func queueWithParams(params string) clientqueue.Queue {
	return clientqueue.NewMemoryQueue("test-client", params)
}

func TestPelicanDirectoryFromParams(t *testing.T) {
	cases := []struct {
		params  string
		wantDir string
		wantOK  bool
	}{
		{"osdf/vdc/public/pelican_protocol", "osdf://vdc/public/pelican_protocol", true},
		{"pelican/foo/bar", "pelican://foo/bar", true},
		{"osdf", "", false},        // no "/" at all
		{"osdf/", "", false},       // empty object path
		{"/vdc/public", "", false}, // empty protocol
		{"", "", false},
	}
	for _, c := range cases {
		dir, ok := pelicanDirectoryFromParams(c.params)
		if dir != c.wantDir || ok != c.wantOK {
			t.Errorf("pelicanDirectoryFromParams(%q) = (%q, %v), want (%q, %v)", c.params, dir, ok, c.wantDir, c.wantOK)
		}
	}
}

func TestPelicanURLDir(t *testing.T) {
	cases := []struct {
		url  string
		want string
	}{
		{"osdf://vdc/public/pelican_protocol/hello.txt", "osdf://vdc/public/pelican_protocol"},
		{"osdf://vdc/public/pelican_protocol/sub/hello.txt", "osdf://vdc/public/pelican_protocol/sub"},
		{"no-scheme-here", "no-scheme-here"},
	}
	for _, c := range cases {
		if got := pelicanURLDir(c.url); got != c.want {
			t.Errorf("pelicanURLDir(%q) = %q, want %q", c.url, got, c.want)
		}
	}
}

func TestPelicanListingSource_NoTrackedQueuesPollsNothing(t *testing.T) {
	lister := newFakePelicanLister()
	statePath := filepath.Join(t.TempDir(), "state.json")
	s, err := newPelicanListingSourceState(statePath, lister.list, discardLogger())
	if err != nil {
		t.Fatalf("newPelicanListingSourceState: %v", err)
	}

	s.poll()

	if got := lister.totalCalls(); got != 0 {
		t.Errorf("lister called %d times with no tracked queues, want 0", got)
	}
}

func TestPelicanListingSource_DiscoversDirectoryFromTrackedQueueParams(t *testing.T) {
	lister := newFakePelicanLister()
	lister.queue("osdf://vdc/public/pelican_protocol", objectsResponse(file("/vdc/public/pelican_protocol/a.txt")))
	statePath := filepath.Join(t.TempDir(), "state.json")
	s, err := newPelicanListingSourceState(statePath, lister.list, discardLogger())
	if err != nil {
		t.Fatalf("newPelicanListingSourceState: %v", err)
	}

	s.QueueAdded(queueWithParams("osdf/vdc/public/pelican_protocol"))
	s.poll()

	if got := lister.callCount("osdf://vdc/public/pelican_protocol"); got != 1 {
		t.Errorf("lister called %d times for the tracked queue's directory, want 1", got)
	}
}

func TestPelicanListingSource_UnionsMultipleClientsSameDirectory(t *testing.T) {
	lister := newFakePelicanLister()
	lister.queue("osdf://vdc/public/pelican_protocol", objectsResponse(file("/vdc/public/pelican_protocol/a.txt")))
	statePath := filepath.Join(t.TempDir(), "state.json")
	s, err := newPelicanListingSourceState(statePath, lister.list, discardLogger())
	if err != nil {
		t.Fatalf("newPelicanListingSourceState: %v", err)
	}

	s.QueueAdded(queueWithParams("osdf/vdc/public/pelican_protocol"))
	s.QueueAdded(queueWithParams("osdf/vdc/public/pelican_protocol"))
	s.poll()

	if got := lister.callCount("osdf://vdc/public/pelican_protocol"); got != 1 {
		t.Errorf("lister called %d times for a directory watched by 2 clients, want 1 (union, not per-client)", got)
	}
}

func TestPelicanListingSource_WatchesMultipleDistinctDirectories(t *testing.T) {
	lister := newFakePelicanLister()
	lister.queue("osdf://vdc/public/a", objectsResponse(file("/vdc/public/a/x.txt")))
	lister.queue("osdf://vdc/public/b", objectsResponse(file("/vdc/public/b/y.txt")))
	statePath := filepath.Join(t.TempDir(), "state.json")
	s, err := newPelicanListingSourceState(statePath, lister.list, discardLogger())
	if err != nil {
		t.Fatalf("newPelicanListingSourceState: %v", err)
	}

	s.QueueAdded(queueWithParams("osdf/vdc/public/a"))
	s.QueueAdded(queueWithParams("osdf/vdc/public/b"))
	s.poll()

	if got := lister.callCount("osdf://vdc/public/a"); got != 1 {
		t.Errorf("directory a called %d times, want 1", got)
	}
	if got := lister.callCount("osdf://vdc/public/b"); got != 1 {
		t.Errorf("directory b called %d times, want 1", got)
	}
}

func TestPelicanListingSource_QueueRemovedStopsWatchingDirectoryWithNoOtherClient(t *testing.T) {
	lister := newFakePelicanLister()
	lister.queue("osdf://vdc/public/pelican_protocol", objectsResponse(file("/vdc/public/pelican_protocol/a.txt")))
	statePath := filepath.Join(t.TempDir(), "state.json")
	s, err := newPelicanListingSourceState(statePath, lister.list, discardLogger())
	if err != nil {
		t.Fatalf("newPelicanListingSourceState: %v", err)
	}

	q := queueWithParams("osdf/vdc/public/pelican_protocol")
	s.QueueAdded(q)
	s.poll()
	if got := lister.callCount("osdf://vdc/public/pelican_protocol"); got != 1 {
		t.Fatalf("directory called %d times before removal, want 1", got)
	}

	s.QueueRemoved(q)
	s.poll()

	if got := lister.callCount("osdf://vdc/public/pelican_protocol"); got != 1 {
		t.Errorf("directory called %d times after its only client was removed, want still 1 (not polled again)", got)
	}
}

func TestPelicanListingSource_FirstPollOfDirectorySeedsBaselineWithoutEmitting(t *testing.T) {
	lister := newFakePelicanLister()
	lister.queue("osdf://vdc/public/pelican_protocol", objectsResponse(
		dir("/vdc/public/pelican_protocol/subdir"),
		file("/vdc/public/pelican_protocol/a.txt"),
		file("/vdc/public/pelican_protocol/b.txt"),
	))
	statePath := filepath.Join(t.TempDir(), "state.json")
	s, err := newPelicanListingSourceState(statePath, lister.list, discardLogger())
	if err != nil {
		t.Fatalf("newPelicanListingSourceState: %v", err)
	}

	s.QueueAdded(queueWithParams("osdf/vdc/public/pelican_protocol"))
	s.poll()

	select {
	case ev := <-s.Events():
		t.Fatalf("first poll emitted an event, want none (baseline seed): %q", ev)
	default:
	}

	data, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatalf("read state file: %v", err)
	}
	var persisted map[string][]string
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatalf("state file is not valid JSON: %v", err)
	}
	names := persisted["osdf://vdc/public/pelican_protocol"]
	if len(names) != 2 {
		t.Fatalf("persisted names = %v, want exactly a.txt and b.txt (no subdir)", names)
	}
}

func TestPelicanListingSource_EmitsOneEventPerNewFileOnSubsequentPoll(t *testing.T) {
	lister := newFakePelicanLister()
	lister.queue("osdf://vdc/public/pelican_protocol", objectsResponse(file("/vdc/public/pelican_protocol/a.txt"), file("/vdc/public/pelican_protocol/b.txt")))
	lister.queue("osdf://vdc/public/pelican_protocol", objectsResponse(file("/vdc/public/pelican_protocol/a.txt"), file("/vdc/public/pelican_protocol/b.txt"), file("/vdc/public/pelican_protocol/c.txt")))
	statePath := filepath.Join(t.TempDir(), "state.json")
	s, err := newPelicanListingSourceState(statePath, lister.list, discardLogger())
	if err != nil {
		t.Fatalf("newPelicanListingSourceState: %v", err)
	}
	s.QueueAdded(queueWithParams("osdf/vdc/public/pelican_protocol"))

	s.poll() // seeds baseline: a.txt, b.txt
	select {
	case ev := <-s.Events():
		t.Fatalf("baseline poll emitted an event, want none: %q", ev)
	default:
	}

	s.poll() // c.txt is new

	select {
	case ev := <-s.Events():
		var got pelicanFileEvent
		if err := json.Unmarshal([]byte(ev), &got); err != nil {
			t.Fatalf("event %q is not valid JSON: %v", ev, err)
		}
		if got.Name != "c.txt" {
			t.Errorf("event name = %q, want c.txt", got.Name)
		}
		if got.URL != "osdf://vdc/public/pelican_protocol/c.txt" {
			t.Errorf("event url = %q, want osdf://vdc/public/pelican_protocol/c.txt", got.URL)
		}
		if got.Size != 12 {
			t.Errorf("event size = %d, want 12", got.Size)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the new-file event")
	}

	select {
	case ev := <-s.Events():
		t.Fatalf("got an unexpected second event: %q", ev)
	default:
	}
}

func TestPelicanListingSource_ResumesFromExistingStateFileAcrossRestart(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "state.json")
	seed := map[string][]string{
		"osdf://vdc/public/pelican_protocol": {"/vdc/public/pelican_protocol/a.txt", "/vdc/public/pelican_protocol/b.txt"},
	}
	data, err := json.Marshal(seed)
	if err != nil {
		t.Fatalf("marshal seed: %v", err)
	}
	if err := os.WriteFile(statePath, data, 0o644); err != nil {
		t.Fatalf("seed state file: %v", err)
	}

	lister := newFakePelicanLister()
	lister.queue("osdf://vdc/public/pelican_protocol", objectsResponse(file("/vdc/public/pelican_protocol/a.txt"), file("/vdc/public/pelican_protocol/b.txt"), file("/vdc/public/pelican_protocol/c.txt")))
	// A fresh instance, simulating a process restart, reads the pre-existing
	// state file rather than starting from an empty baseline.
	s, err := newPelicanListingSourceState(statePath, lister.list, discardLogger())
	if err != nil {
		t.Fatalf("newPelicanListingSourceState: %v", err)
	}
	s.QueueAdded(queueWithParams("osdf/vdc/public/pelican_protocol"))

	s.poll()

	select {
	case ev := <-s.Events():
		var got pelicanFileEvent
		if err := json.Unmarshal([]byte(ev), &got); err != nil {
			t.Fatalf("event %q is not valid JSON: %v", ev, err)
		}
		if got.Name != "c.txt" {
			t.Errorf("event name = %q, want c.txt (a.txt/b.txt were already known before this restart)", got.Name)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the new-file event")
	}

	select {
	case ev := <-s.Events():
		t.Fatalf("got an unexpected second event: %q", ev)
	default:
	}
}

func TestPelicanListingSource_IgnoresEmptyResultWhenPreviouslyNonEmpty(t *testing.T) {
	lister := newFakePelicanLister()
	lister.queue("osdf://vdc/public/pelican_protocol", objectsResponse(file("/vdc/public/pelican_protocol/a.txt"), file("/vdc/public/pelican_protocol/b.txt")))
	lister.queue("osdf://vdc/public/pelican_protocol", objectsResponse()) // anomalous empty result
	lister.queue("osdf://vdc/public/pelican_protocol", objectsResponse(file("/vdc/public/pelican_protocol/a.txt"), file("/vdc/public/pelican_protocol/b.txt")))
	statePath := filepath.Join(t.TempDir(), "state.json")
	s, err := newPelicanListingSourceState(statePath, lister.list, discardLogger())
	if err != nil {
		t.Fatalf("newPelicanListingSourceState: %v", err)
	}
	s.QueueAdded(queueWithParams("osdf/vdc/public/pelican_protocol"))

	s.poll() // seeds baseline: a.txt, b.txt
	s.poll() // anomalous empty result -- must be ignored, not treated as mass deletion

	select {
	case ev := <-s.Events():
		t.Fatalf("anomalous empty poll emitted an event, want none: %q", ev)
	default:
	}
	s.mu.Lock()
	seenCount := len(s.seen["osdf://vdc/public/pelican_protocol"])
	s.mu.Unlock()
	if seenCount != 2 {
		t.Fatalf("seen set after anomalous empty poll = %d entries, want still 2 (unaffected)", seenCount)
	}

	s.poll() // back to normal: a.txt/b.txt must NOT be re-announced as new

	select {
	case ev := <-s.Events():
		t.Fatalf("a.txt/b.txt were re-announced as new after the anomalous poll: %q", ev)
	default:
	}
}

func TestPelicanListingSource_ListErrorForOneDirectoryDoesNotAffectOthers(t *testing.T) {
	lister := newFakePelicanLister()
	lister.queue("osdf://vdc/public/broken", errorResponse(errors.New("federation unreachable")))
	lister.queue("osdf://vdc/public/ok", objectsResponse(file("/vdc/public/ok/a.txt")))
	statePath := filepath.Join(t.TempDir(), "state.json")
	s, err := newPelicanListingSourceState(statePath, lister.list, discardLogger())
	if err != nil {
		t.Fatalf("newPelicanListingSourceState: %v", err)
	}
	s.QueueAdded(queueWithParams("osdf/vdc/public/broken"))
	s.QueueAdded(queueWithParams("osdf/vdc/public/ok"))

	s.poll()

	s.mu.Lock()
	_, brokenSeeded := s.seen["osdf://vdc/public/broken"]
	_, okSeeded := s.seen["osdf://vdc/public/ok"]
	s.mu.Unlock()
	if brokenSeeded {
		t.Error("the broken directory got a baseline despite its list() call failing")
	}
	if !okSeeded {
		t.Error("the ok directory's baseline was never seeded -- one directory's error should not block another's poll")
	}
}

func TestPelicanListingSource_ListErrorDoesNotModifyState(t *testing.T) {
	lister := newFakePelicanLister()
	lister.queue("osdf://vdc/public/pelican_protocol", errorResponse(errors.New("federation unreachable")))
	statePath := filepath.Join(t.TempDir(), "state.json")
	s, err := newPelicanListingSourceState(statePath, lister.list, discardLogger())
	if err != nil {
		t.Fatalf("newPelicanListingSourceState: %v", err)
	}
	s.QueueAdded(queueWithParams("osdf/vdc/public/pelican_protocol"))

	s.poll()

	select {
	case ev := <-s.Events():
		t.Fatalf("a failed list emitted an event: %q", ev)
	default:
	}
	if _, err := os.Stat(statePath); !os.IsNotExist(err) {
		t.Errorf("state file was created/modified despite the list failing: err=%v", err)
	}
}

func TestPelicanListingSource_ShouldNotify_OnlyNotifiesClientSubscribedToMatchingDirectory(t *testing.T) {
	lister := newFakePelicanLister()
	statePath := filepath.Join(t.TempDir(), "state.json")
	s, err := newPelicanListingSourceState(statePath, lister.list, discardLogger())
	if err != nil {
		t.Fatalf("newPelicanListingSourceState: %v", err)
	}

	event, err := json.Marshal(pelicanFileEvent{Name: "hello.txt", URL: "osdf://vdc/public/pelican_protocol/hello.txt"})
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}

	matching := queueWithParams("osdf/vdc/public/pelican_protocol")
	other := queueWithParams("osdf/vdc/public/some_other_protocol")

	if !s.ShouldNotify(string(event), matching) {
		t.Error("ShouldNotify() = false for the client subscribed to the event's own directory, want true")
	}
	if s.ShouldNotify(string(event), other) {
		t.Error("ShouldNotify() = true for a client subscribed to a different directory, want false")
	}
}

func TestPelicanListingSource_ShouldNotify_RejectsClientWithUnparsableParams(t *testing.T) {
	lister := newFakePelicanLister()
	statePath := filepath.Join(t.TempDir(), "state.json")
	s, err := newPelicanListingSourceState(statePath, lister.list, discardLogger())
	if err != nil {
		t.Fatalf("newPelicanListingSourceState: %v", err)
	}

	event, err := json.Marshal(pelicanFileEvent{Name: "hello.txt", URL: "osdf://vdc/public/pelican_protocol/hello.txt"})
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}

	if s.ShouldNotify(string(event), queueWithParams("not-a-directory-spec")) {
		t.Error("ShouldNotify() = true for a client whose params don't name a Pelican directory, want false")
	}
}

func TestPelicanListingSource_RealTickerWiring(t *testing.T) {
	// A lightweight sanity check that run() actually wires a real ticker to
	// poll() and to the Events() channel, and that QueueAdded takes effect
	// on the very next tick.
	lister := newFakePelicanLister()
	lister.queue("osdf://vdc/public/pelican_protocol", objectsResponse(file("/vdc/public/pelican_protocol/a.txt")))
	lister.queue("osdf://vdc/public/pelican_protocol", objectsResponse(file("/vdc/public/pelican_protocol/a.txt"), file("/vdc/public/pelican_protocol/b.txt")))
	statePath := filepath.Join(t.TempDir(), "state.json")
	s, err := newPelicanListingSourceState(statePath, lister.list, discardLogger())
	if err != nil {
		t.Fatalf("newPelicanListingSourceState: %v", err)
	}
	s.QueueAdded(queueWithParams("osdf/vdc/public/pelican_protocol"))
	go s.run(time.Millisecond)

	select {
	case ev := <-s.Events():
		var got pelicanFileEvent
		if err := json.Unmarshal([]byte(ev), &got); err != nil {
			t.Fatalf("event %q is not valid JSON: %v", ev, err)
		}
		if got.Name != "b.txt" {
			t.Errorf("event name = %q, want b.txt", got.Name)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the new-file event")
	}
}
