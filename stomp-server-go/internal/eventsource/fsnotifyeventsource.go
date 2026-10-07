package eventsource

import (
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/fsnotify/fsnotify"
)

// FSNotifySource is an EventSource that watches a directory tree on the local
// filesystem and emits one event, the file's path, for every file created or
// modified anywhere beneath it. Subdirectories (including ones created after
// startup) are watched too, since fsnotify itself is not recursive.
type FSNotifySource struct {
	defaultNotifier
	root    string
	log     *slog.Logger
	watcher *fsnotify.Watcher
	ch      chan string
}

// NewFSNotifySource starts watching path (which must be an existing
// directory) on a background goroutine that runs for the life of the process.
func NewFSNotifySource(path string, log *slog.Logger) (*FSNotifySource, error) {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("create fsnotify watcher: %w", err)
	}
	s := &FSNotifySource{root: path, log: log, watcher: w, ch: make(chan string, 1)}
	if err := s.addTree(path); err != nil {
		w.Close()
		return nil, err
	}
	go s.run()
	return s, nil
}

func (s *FSNotifySource) Events() <-chan string { return s.ch }

// addTree watches dir and every directory beneath it. Errors for the root are
// fatal to the caller; errors deeper down (e.g. a directory removed mid-walk)
// are logged and skipped.
func (s *FSNotifySource) addTree(dir string) error {
	return filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if p == s.root {
				return fmt.Errorf("watch %s: %w", p, err)
			}
			s.log.Warn("skipping unreadable path", "path", p, "error", err)
			return nil
		}
		if !d.IsDir() {
			return nil
		}
		if err := s.watcher.Add(p); err != nil {
			if p == s.root {
				return fmt.Errorf("watch %s: %w", p, err)
			}
			s.log.Warn("failed to watch directory", "path", p, "error", err)
		}
		return nil
	})
}

func (s *FSNotifySource) run() {
	for {
		select {
		case ev, ok := <-s.watcher.Events:
			if !ok {
				return
			}
			s.handle(ev)
		case err, ok := <-s.watcher.Errors:
			if !ok {
				return
			}
			s.log.Error("fsnotify error", "error", err)
		}
	}
}

func (s *FSNotifySource) handle(ev fsnotify.Event) {
	if !ev.Has(fsnotify.Create) && !ev.Has(fsnotify.Write) {
		return
	}
	info, err := os.Stat(ev.Name)
	if err != nil {
		// Already gone again; nothing to report.
		return
	}
	if info.IsDir() {
		if ev.Has(fsnotify.Create) {
			// Files may land in the new directory before the watch is in
			// place, so pick up anything already inside it.
			if err := s.addTree(ev.Name); err != nil {
				s.log.Warn("failed to watch new directory", "path", ev.Name, "error", err)
			}
			s.emitExisting(ev.Name)
		}
		return
	}
	s.ch <- ev.Name
}

// emitExisting emits an event for each file already present under dir.
func (s *FSNotifySource) emitExisting(dir string) {
	filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			s.ch <- p
		}
		return nil
	})
}
