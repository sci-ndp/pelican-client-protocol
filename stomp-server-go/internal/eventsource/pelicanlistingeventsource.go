package eventsource

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
	"sync"
	"time"

	"stomp-server-go/internal/clientqueue"
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

// pelicanDirectoryFromParams parses a client's subscription parameters of
// the form "<protocol>/<object path>" -- e.g.
// "osdf/vdc/public/pelican_protocol" -- into a Pelican federation directory
// URL, e.g. "osdf://vdc/public/pelican_protocol". ok is false if params
// doesn't contain a "/" with a non-empty protocol and path on either side,
// meaning that client isn't naming a Pelican directory at all.
func pelicanDirectoryFromParams(params string) (dir string, ok bool) {
	protocol, objectPath, found := strings.Cut(params, "/")
	if !found || protocol == "" || objectPath == "" {
		return "", false
	}
	return protocol + "://" + objectPath, true
}

// pelicanURLDir returns the directory portion of a Pelican federation
// object URL, e.g. "osdf://vdc/public/pelican_protocol/hello.txt" ->
// "osdf://vdc/public/pelican_protocol". Operates on the path portion only
// (after stripping "scheme://") since path.Dir would otherwise collapse the
// "//" right after the scheme (path.Dir treats runs of slashes as plain
// separators, e.g. path.Dir("osdf://a/b") == "osdf:/a", silently corrupting
// the scheme).
func pelicanURLDir(u string) string {
	scheme, rest, ok := strings.Cut(u, "://")
	if !ok {
		return u
	}
	return scheme + "://" + path.Dir(rest)
}

// PelicanListingSource dynamically discovers which Pelican federation
// directories to watch from the subscription parameters of every currently
// active client queue (see clientqueue.Queue.Params, pelicanDirectoryFromParams),
// rather than a single directory fixed at construction. It polls the union
// of those directories on a fixed interval via the pelican CLI, diffs each
// directory's files against what was last observed there, and emits one
// event per file that's newly appeared in any watched directory.
// ShouldNotify then filters delivery back down so a client only ever sees
// events for its own directory, never another client's.
//
// A client queue becomes tracked (and its directory starts being watched)
// via QueueAdded, and stops via QueueRemoved. A queue whose Params don't
// parse as "<protocol>/<path>" contributes no directory.
//
// This only looks at the one directory named by each client's params:
// entries with IsCollection true are subdirectories and are skipped, not
// recursed into (pelican object ls is non-recursive by default, matching
// that scope).
//
// The very first poll of a given directory (no prior state for it) seeds
// that directory's baseline silently, without emitting any events --
// otherwise a client subscribing to an already-established directory would
// be flooded with one event per pre-existing file. "New" specifically means
// "appeared since the last poll of this directory", not "exists".
type PelicanListingSource struct {
	statePath string
	list      pelicanListingRunner
	log       *slog.Logger
	ch        chan string

	mu      sync.Mutex
	tracked map[clientqueue.Queue]bool // currently active client queues
	seen    map[string]map[string]bool // directory URL -> observed file names; a directory's key exists once its baseline has been seeded
}

// newPelicanListingSourceState loads statePath's previously-observed
// per-directory file sets (empty if the file doesn't exist yet) and returns
// a PelicanListingSource ready to use, without starting its background poll
// loop -- split out from NewPelicanListingSource so tests can call poll()
// directly with a fake pelicanListingRunner on a compressed, non-realtime
// schedule.
func newPelicanListingSourceState(statePath string, list pelicanListingRunner, log *slog.Logger) (*PelicanListingSource, error) {
	seen, err := readDirectorySets(statePath)
	if err != nil {
		return nil, err
	}
	return &PelicanListingSource{
		statePath: statePath,
		list:      list,
		log:       log,
		ch:        make(chan string, 1),
		tracked:   map[clientqueue.Queue]bool{},
		seen:      seen,
	}, nil
}

// NewPelicanListingSource loads statePath's previously-observed
// per-directory file sets and starts polling the union of watched
// directories every interval via pelicanBinary's "object ls", persisting
// the observed sets back to statePath after every poll.
func NewPelicanListingSource(interval time.Duration, statePath, pelicanBinary string, log *slog.Logger) (*PelicanListingSource, error) {
	s, err := newPelicanListingSourceState(statePath, execPelicanListing(pelicanBinary), log)
	if err != nil {
		return nil, err
	}
	go s.run(interval)
	return s, nil
}

func (s *PelicanListingSource) Events() <-chan string { return s.ch }

func (s *PelicanListingSource) run(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		s.poll()
	}
}

// QueueAdded implements EventSource.
func (s *PelicanListingSource) QueueAdded(q clientqueue.Queue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tracked[q] = true
}

// QueueRemoved implements EventSource. The directory q named (if any) stops
// being polled once no other tracked queue still names it; its "seen" state
// simply lingers, unpolled, rather than being cleaned up.
func (s *PelicanListingSource) QueueRemoved(q clientqueue.Queue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.tracked, q)
}

// ShouldNotify implements EventSource: a client only ever sees new-file
// events for the one Pelican directory named by its own subscription
// parameters, never another client's directory.
func (s *PelicanListingSource) ShouldNotify(event string, q clientqueue.Queue) bool {
	dir, ok := pelicanDirectoryFromParams(q.Params())
	if !ok {
		return false
	}
	var e pelicanFileEvent
	if err := json.Unmarshal([]byte(event), &e); err != nil {
		return false
	}
	return pelicanURLDir(e.URL) == dir
}

// watchedDirectories returns the current union of Pelican directories named
// by every tracked client queue's subscription parameters.
func (s *PelicanListingSource) watchedDirectories() []string {
	s.mu.Lock()
	dirSet := map[string]bool{}
	for q := range s.tracked {
		if dir, ok := pelicanDirectoryFromParams(q.Params()); ok {
			dirSet[dir] = true
		}
	}
	s.mu.Unlock()

	dirs := make([]string, 0, len(dirSet))
	for d := range dirSet {
		dirs = append(dirs, d)
	}
	sort.Strings(dirs) // deterministic polling order, easier to reason about/log
	return dirs
}

// poll lists every currently-watched directory and processes each
// independently, so one directory's listing failure can't prevent the
// others from being checked.
func (s *PelicanListingSource) poll() {
	for _, dir := range s.watchedDirectories() {
		s.pollDirectory(dir)
	}
}

// pollDirectory lists dir, emits one event per file not already observed
// there, and persists the updated state. Any error (listing,
// unexpected-empty-result, or persist) is logged and leaves this
// directory's seen set unmodified, so the next poll retries the diff fresh
// rather than forgetting or double-reporting history.
func (s *PelicanListingSource) pollDirectory(dir string) {
	ctx, cancel := context.WithTimeout(context.Background(), pelicanListTimeout)
	defer cancel()
	objects, err := s.list(ctx, dir)
	if err != nil {
		s.log.Error("failed to list Pelican directory", "url", dir, "error", err)
		return
	}

	var files []pelicanObject
	for _, o := range objects {
		if !o.IsCollection {
			files = append(files, o)
		}
	}

	s.mu.Lock()
	previouslySeen, baselineSeeded := s.seen[dir]
	s.mu.Unlock()

	// A directory that legitimately has zero files is possible, but so is a
	// misbehaving federation component returning an empty/unexpected result.
	// Treating that as "everything was deleted" would wipe this directory's
	// seen set, and the next successful poll would then re-announce every
	// real file as newly-appeared. Refuse to accept an empty result once a
	// non-empty baseline has been established.
	if len(files) == 0 && len(previouslySeen) > 0 {
		s.log.Warn("Pelican directory listing came back empty; ignoring this poll rather than treating it as every file being deleted",
			"url", dir, "previously_observed", len(previouslySeen))
		return
	}

	current := make(map[string]bool, len(files))
	for _, f := range files {
		current[f.Name] = true
	}

	if !baselineSeeded {
		s.mu.Lock()
		s.seen[dir] = current
		s.mu.Unlock()
		if err := s.persist(); err != nil {
			s.log.Error("failed to persist observed file list", "path", s.statePath, "error", err)
		}
		s.log.Info("seeded Pelican listing baseline without emitting events for pre-existing files",
			"url", dir, "files", len(current))
		return
	}

	var newFiles []pelicanObject
	for _, f := range files {
		if !previouslySeen[f.Name] {
			newFiles = append(newFiles, f)
		}
	}

	// Emit before persisting: if the process dies between the two, a
	// restart re-diffs against the same not-yet-updated state and re-emits
	// these same files as new again -- an at-least-once duplicate here is
	// far preferable to silently never announcing a file that arrived.
	scheme, _, _ := strings.Cut(dir, "://")
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

	s.mu.Lock()
	s.seen[dir] = current
	s.mu.Unlock()
	if err := s.persist(); err != nil {
		s.log.Error("failed to persist observed file list", "path", s.statePath, "error", err)
	}
}

// persist writes every watched directory's currently-observed file set to
// statePath as JSON (directory URL -> sorted file names), so a restarted
// process resumes each directory from where it left off instead of
// re-announcing every file already there as new.
func (s *PelicanListingSource) persist() error {
	s.mu.Lock()
	snapshot := make(map[string][]string, len(s.seen))
	for dir, names := range s.seen {
		list := make([]string, 0, len(names))
		for name := range names {
			list = append(list, name)
		}
		sort.Strings(list)
		snapshot[dir] = list
	}
	s.mu.Unlock()

	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("encode state: %w", err)
	}
	return os.WriteFile(s.statePath, data, 0o644)
}

// readDirectorySets reads the per-directory observed file sets persisted by
// persist, returning an empty map if path doesn't exist yet (first run: no
// directory has a baseline yet).
func readDirectorySets(path string) (map[string]map[string]bool, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return map[string]map[string]bool{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read state file: %w", err)
	}
	var raw map[string][]string
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parse state file %s: %w", path, err)
	}
	seen := make(map[string]map[string]bool, len(raw))
	for dir, names := range raw {
		set := make(map[string]bool, len(names))
		for _, n := range names {
			set[n] = true
		}
		seen[dir] = set
	}
	return seen, nil
}
