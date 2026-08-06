package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fakePelicanLister is a pelicanListingRunner test double: it returns
// whatever fixed responses were queued for it, one per call, so tests can
// drive successive polls without a real pelican binary or federation.
type fakePelicanLister struct {
	responses []func() ([]pelicanObject, error)
	calls     int
}

func (f *fakePelicanLister) list(ctx context.Context, listingURL string) ([]pelicanObject, error) {
	i := f.calls
	if i >= len(f.responses) {
		i = len(f.responses) - 1 // repeat the last response forever
	}
	f.calls++
	return f.responses[i]()
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

func TestPelicanListingEventSource_FirstPollSeedsBaselineWithoutEmitting(t *testing.T) {
	lister := &fakePelicanLister{responses: []func() ([]pelicanObject, error){
		objectsResponse(
			dir("/vdc/public/pelican_protocol/subdir"),
			file("/vdc/public/pelican_protocol/a.txt"),
			file("/vdc/public/pelican_protocol/b.txt"),
		),
	}}
	statePath := filepath.Join(t.TempDir(), "state.txt")
	s, err := newPelicanListingEventSourceState("osdf://vdc/public/pelican_protocol", statePath, lister.list, discardLogger())
	if err != nil {
		t.Fatalf("newPelicanListingEventSourceState: %v", err)
	}

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
	for _, want := range []string{"/vdc/public/pelican_protocol/a.txt", "/vdc/public/pelican_protocol/b.txt"} {
		if !strings.Contains(string(data), want) {
			t.Errorf("state file %q does not contain %q", data, want)
		}
	}
	if strings.Contains(string(data), "subdir") {
		t.Errorf("state file %q should not contain the subdirectory entry", data)
	}
}

func TestPelicanListingEventSource_EmitsOneEventPerNewFileOnSubsequentPoll(t *testing.T) {
	lister := &fakePelicanLister{responses: []func() ([]pelicanObject, error){
		objectsResponse(file("/vdc/public/pelican_protocol/a.txt"), file("/vdc/public/pelican_protocol/b.txt")),
		objectsResponse(file("/vdc/public/pelican_protocol/a.txt"), file("/vdc/public/pelican_protocol/b.txt"), file("/vdc/public/pelican_protocol/c.txt")),
	}}
	statePath := filepath.Join(t.TempDir(), "state.txt")
	s, err := newPelicanListingEventSourceState("osdf://vdc/public/pelican_protocol", statePath, lister.list, discardLogger())
	if err != nil {
		t.Fatalf("newPelicanListingEventSourceState: %v", err)
	}

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

func TestPelicanListingEventSource_ResumesFromExistingStateFileAcrossRestart(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "state.txt")
	if err := os.WriteFile(statePath, []byte("/vdc/public/pelican_protocol/a.txt\n/vdc/public/pelican_protocol/b.txt\n"), 0o644); err != nil {
		t.Fatalf("seed state file: %v", err)
	}

	lister := &fakePelicanLister{responses: []func() ([]pelicanObject, error){
		objectsResponse(file("/vdc/public/pelican_protocol/a.txt"), file("/vdc/public/pelican_protocol/b.txt"), file("/vdc/public/pelican_protocol/c.txt")),
	}}
	// A fresh instance, simulating a process restart, reads the pre-existing
	// state file rather than starting from an empty baseline.
	s, err := newPelicanListingEventSourceState("osdf://vdc/public/pelican_protocol", statePath, lister.list, discardLogger())
	if err != nil {
		t.Fatalf("newPelicanListingEventSourceState: %v", err)
	}

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

func TestPelicanListingEventSource_IgnoresEmptyResultWhenPreviouslyNonEmpty(t *testing.T) {
	lister := &fakePelicanLister{responses: []func() ([]pelicanObject, error){
		objectsResponse(file("/vdc/public/pelican_protocol/a.txt"), file("/vdc/public/pelican_protocol/b.txt")),
		objectsResponse(), // anomalous empty result
		objectsResponse(file("/vdc/public/pelican_protocol/a.txt"), file("/vdc/public/pelican_protocol/b.txt")),
	}}
	statePath := filepath.Join(t.TempDir(), "state.txt")
	s, err := newPelicanListingEventSourceState("osdf://vdc/public/pelican_protocol", statePath, lister.list, discardLogger())
	if err != nil {
		t.Fatalf("newPelicanListingEventSourceState: %v", err)
	}

	s.poll() // seeds baseline: a.txt, b.txt
	s.poll() // anomalous empty result -- must be ignored, not treated as mass deletion

	select {
	case ev := <-s.Events():
		t.Fatalf("anomalous empty poll emitted an event, want none: %q", ev)
	default:
	}
	if len(s.seen) != 2 {
		t.Fatalf("seen set after anomalous empty poll = %d entries, want still 2 (unaffected)", len(s.seen))
	}

	s.poll() // back to normal: a.txt/b.txt must NOT be re-announced as new

	select {
	case ev := <-s.Events():
		t.Fatalf("a.txt/b.txt were re-announced as new after the anomalous poll: %q", ev)
	default:
	}
}

func TestPelicanListingEventSource_ListErrorDoesNotModifyState(t *testing.T) {
	lister := &fakePelicanLister{responses: []func() ([]pelicanObject, error){
		errorResponse(errors.New("federation unreachable")),
	}}
	statePath := filepath.Join(t.TempDir(), "state.txt")
	s, err := newPelicanListingEventSourceState("osdf://vdc/public/pelican_protocol", statePath, lister.list, discardLogger())
	if err != nil {
		t.Fatalf("newPelicanListingEventSourceState: %v", err)
	}

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

func TestPelicanListingEventSource_RejectsSchemelessURL(t *testing.T) {
	if _, err := newPelicanListingEventSourceState("not-a-url", filepath.Join(t.TempDir(), "state.txt"), (&fakePelicanLister{}).list, discardLogger()); err == nil {
		t.Fatal("expected an error for a URL without a scheme")
	}
}

func TestPelicanListingEventSource_RealTickerWiring(t *testing.T) {
	// A lightweight sanity check that run() actually wires a real ticker to
	// poll() and to the Events() channel, rather than testing poll() in
	// isolation like the other tests here.
	lister := &fakePelicanLister{responses: []func() ([]pelicanObject, error){
		objectsResponse(file("/vdc/public/pelican_protocol/a.txt")),
		objectsResponse(file("/vdc/public/pelican_protocol/a.txt"), file("/vdc/public/pelican_protocol/b.txt")),
	}}
	statePath := filepath.Join(t.TempDir(), "state.txt")
	s, err := newPelicanListingEventSourceState("osdf://vdc/public/pelican_protocol", statePath, lister.list, discardLogger())
	if err != nil {
		t.Fatalf("newPelicanListingEventSourceState: %v", err)
	}
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
