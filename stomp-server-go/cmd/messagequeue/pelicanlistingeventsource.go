package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path"
	"sort"
	"strings"
	"time"
)

// pelicanListTimeout bounds a single poll's invocation of the pelican
// binary, so a stalled federation/network can't block this source's
// goroutine indefinitely.
const pelicanListTimeout = 30 * time.Second

// pelicanObject is one entry from `pelican object ls --json --long`'s
// output. Field names are matched case-insensitively against that JSON, so
// no struct tags are needed.
type pelicanObject struct {
	Name         string
	Size         int64
	ModTime      time.Time
	IsCollection bool
}

// pelicanFileEvent is the JSON body emitted for each newly-observed file.
type pelicanFileEvent struct {
	Name    string    `json:"name"`
	URL     string    `json:"url"`
	Size    int64     `json:"size"`
	ModTime time.Time `json:"mod_time"`
}

// pelicanListingRunner lists the objects at listingURL. Production code uses
// execPelicanListing, which shells out to the pelican CLI; tests substitute
// a fake so they don't need a real pelican binary or federation/network
// access.
type pelicanListingRunner func(ctx context.Context, listingURL string) ([]pelicanObject, error)

// execPelicanListing returns a pelicanListingRunner that shells out to
// pelicanBinary's "object ls --json --long" subcommand, which prints exactly
// the object metadata (name, size, mod time, whether it's a collection) this
// source needs as a single JSON array.
func execPelicanListing(pelicanBinary string) pelicanListingRunner {
	return func(ctx context.Context, listingURL string) ([]pelicanObject, error) {
		cmd := exec.CommandContext(ctx, pelicanBinary, "object", "ls", "--json", "--long", listingURL)
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			msg := strings.TrimSpace(stderr.String())
			if msg == "" {
				msg = err.Error()
			}
			return nil, fmt.Errorf("%s object ls: %s", pelicanBinary, msg)
		}
		var objects []pelicanObject
		if err := json.Unmarshal(stdout.Bytes(), &objects); err != nil {
			return nil, fmt.Errorf("parse %s object ls output: %w", pelicanBinary, err)
		}
		return objects, nil
	}
}

// pelicanListingEventSource polls a Pelican federation directory (e.g. an
// osdf:// URL) on a fixed interval, diffs the files it lists against what
// was last observed, and emits one event per file that's newly appeared.
// The observed set is persisted to statePath after every poll that changes
// it, so a restarted process resumes from where it left off instead of
// re-announcing every file already on the server as new.
//
// This only looks at the one directory named by listingURL: entries with
// IsCollection true are subdirectories and are skipped, not recursed into
// (pelican object ls is non-recursive by default, matching that scope).
//
// The very first poll ever run against a given statePath (i.e. no state
// file exists yet) seeds the baseline silently, without emitting any
// events -- otherwise every process's first startup against an established
// directory would flood every connected client with one event per
// pre-existing file. "New" specifically means "appeared since the last
// poll", not "exists".
type pelicanListingEventSource struct {
	listingURL string
	statePath  string
	list       pelicanListingRunner
	log        *slog.Logger
	ch         chan string
	seen       map[string]bool

	// baselineSeeded is true once this instance has established a starting
	// point to diff against -- either statePath already existed at
	// construction time, or an earlier poll() call set it.
	baselineSeeded bool
}

// newPelicanListingEventSourceState loads statePath's previously-observed
// file set (empty if the file doesn't exist yet) and returns a
// pelicanListingEventSource ready to use, without starting its background
// poll loop -- split out from newPelicanListingEventSource so tests can call
// poll() directly with a fake pelicanListingRunner on a compressed,
// non-realtime schedule.
func newPelicanListingEventSourceState(listingURL, statePath string, list pelicanListingRunner, log *slog.Logger) (*pelicanListingEventSource, error) {
	if !strings.Contains(listingURL, "://") {
		return nil, fmt.Errorf("invalid listing URL (missing scheme): %s", listingURL)
	}
	seen, existed, err := readFileSet(statePath)
	if err != nil {
		return nil, err
	}
	return &pelicanListingEventSource{
		listingURL:     listingURL,
		statePath:      statePath,
		list:           list,
		log:            log,
		ch:             make(chan string, 1),
		seen:           seen,
		baselineSeeded: existed,
	}, nil
}

// newPelicanListingEventSource loads statePath's previously-observed file
// set and starts polling listingURL every interval via pelicanBinary's
// "object ls", persisting the observed set back to statePath after every
// poll.
func newPelicanListingEventSource(listingURL string, interval time.Duration, statePath, pelicanBinary string, log *slog.Logger) (*pelicanListingEventSource, error) {
	s, err := newPelicanListingEventSourceState(listingURL, statePath, execPelicanListing(pelicanBinary), log)
	if err != nil {
		return nil, err
	}
	go s.run(interval)
	return s, nil
}

func (s *pelicanListingEventSource) Events() <-chan string { return s.ch }

func (s *pelicanListingEventSource) run(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		s.poll()
	}
}

// poll lists the current directory contents, emits one event per file not
// already in s.seen, and persists the updated set. Any error (listing,
// unexpected-empty-result, or persist) is logged and leaves s.seen
// unmodified, so the next tick retries the diff fresh rather than forgetting
// or double-reporting history.
func (s *pelicanListingEventSource) poll() {
	ctx, cancel := context.WithTimeout(context.Background(), pelicanListTimeout)
	defer cancel()
	objects, err := s.list(ctx, s.listingURL)
	if err != nil {
		s.log.Error("failed to list Pelican directory", "url", s.listingURL, "error", err)
		return
	}

	var files []pelicanObject
	for _, o := range objects {
		if !o.IsCollection {
			files = append(files, o)
		}
	}

	// A directory that legitimately has zero files is possible, but so is a
	// misbehaving federation component returning an empty/unexpected result.
	// Treating that as "everything was deleted" would wipe s.seen, and the
	// next successful poll would then re-announce every real file as
	// newly-appeared. Refuse to accept an empty result once a non-empty
	// baseline has been established.
	if len(files) == 0 && len(s.seen) > 0 {
		s.log.Warn("Pelican directory listing came back empty; ignoring this poll rather than treating it as every file being deleted",
			"url", s.listingURL, "previously_observed", len(s.seen))
		return
	}

	current := make(map[string]bool, len(files))
	for _, f := range files {
		current[f.Name] = true
	}

	if !s.baselineSeeded {
		s.seen = current
		s.baselineSeeded = true
		if err := writeFileSet(s.statePath, current); err != nil {
			s.log.Error("failed to persist observed file list", "path", s.statePath, "error", err)
		}
		s.log.Info("seeded Pelican listing baseline without emitting events for pre-existing files",
			"url", s.listingURL, "files", len(current))
		return
	}

	var newFiles []pelicanObject
	for _, f := range files {
		if !s.seen[f.Name] {
			newFiles = append(newFiles, f)
		}
	}

	// Emit before persisting: if the process dies between the two, a
	// restart re-diffs against the same not-yet-updated state and re-emits
	// these same files as new again -- an at-least-once duplicate here is
	// far preferable to silently never announcing a file that arrived.
	scheme := listingURLScheme(s.listingURL)
	for _, f := range newFiles {
		payload, err := json.Marshal(pelicanFileEvent{
			Name:    path.Base(f.Name),
			URL:     scheme + "://" + strings.TrimPrefix(f.Name, "/"),
			Size:    f.Size,
			ModTime: f.ModTime,
		})
		if err != nil {
			s.log.Error("failed to encode new-file event", "name", f.Name, "error", err)
			continue
		}
		s.ch <- string(payload)
	}

	s.seen = current
	if err := writeFileSet(s.statePath, current); err != nil {
		s.log.Error("failed to persist observed file list", "path", s.statePath, "error", err)
	}
}

// listingURLScheme returns the scheme prefix of a Pelican object URL (e.g.
// "osdf" for "osdf://vdc/public/pelican_protocol"). Pelican's federation
// URLs (osdf://, pelican://, stash://) aren't meaningfully split into
// host/path by net/url.Parse for reconstruction purposes -- e.g.
// "osdf://vdc/public/x" parses with Host="vdc", but pelican object ls's own
// Name field for a file under that directory is the flat absolute path
// "/vdc/public/x/file.txt", with "vdc" already part of it -- so this just
// grabs the literal scheme text instead.
func listingURLScheme(listingURL string) string {
	if scheme, _, ok := strings.Cut(listingURL, "://"); ok {
		return scheme
	}
	return "osdf"
}

// readFileSet reads a newline-delimited list of file names from path,
// returning an empty set and existed=false if the file doesn't exist yet
// (first run). existed is false only for a genuinely missing file -- one
// that exists but is empty still counts, since that represents a previous
// poll having confirmed the directory was empty at the time, not "no poll
// has ever succeeded yet."
func readFileSet(path string) (set map[string]bool, existed bool, err error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return map[string]bool{}, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("read state file: %w", err)
	}
	set = map[string]bool{}
	for _, line := range strings.Split(string(data), "\n") {
		if line != "" {
			set[line] = true
		}
	}
	return set, true, nil
}

// writeFileSet persists set to path as a newline-delimited, sorted list of
// file names -- sorted so the file's contents are deterministic and easy to
// diff/inspect by hand, not because order matters to readFileSet.
func writeFileSet(path string, set map[string]bool) error {
	names := make([]string, 0, len(set))
	for name := range set {
		names = append(names, name)
	}
	sort.Strings(names)
	return os.WriteFile(path, []byte(strings.Join(names, "\n")+"\n"), 0o644)
}
