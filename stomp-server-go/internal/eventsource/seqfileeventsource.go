package eventsource

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// SeqFileSource is a demo EventSource like TickerSource, except its sequence
// number survives process restarts: it's read from path on construction and
// re-written to path after every increment, so a restarted process resumes
// counting up from where it left off instead of starting over at 1.
type SeqFileSource struct {
	defaultNotifier
	path string
	log  *slog.Logger
	ch   chan string
	n    int
}

// newSeqFileSourceState reads the current sequence number from path (0 if
// the file doesn't exist yet or is empty) and returns a SeqFileSource ready
// to use, without starting its background ticker -- split out from
// NewSeqFileSource so tests can drive next() directly on a compressed,
// non-realtime schedule.
func newSeqFileSourceState(path string, log *slog.Logger) (*SeqFileSource, error) {
	n, err := readSeq(path)
	if err != nil {
		return nil, err
	}
	return &SeqFileSource{path: path, log: log, ch: make(chan string, 1), n: n}, nil
}

// NewSeqFileSource reads the current sequence number from path (0 if the
// file doesn't exist yet or is empty) and starts emitting
// "Persistent Event <n>" on interval counting up from n+1, persisting n back
// to path after every increment.
func NewSeqFileSource(path string, interval time.Duration, log *slog.Logger) (*SeqFileSource, error) {
	s, err := newSeqFileSourceState(path, log)
	if err != nil {
		return nil, err
	}
	go s.run(interval)
	return s, nil
}

func (s *SeqFileSource) Events() <-chan string { return s.ch }

func (s *SeqFileSource) run(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		s.ch <- s.next()
	}
}

// next increments the sequence number, persists it, and returns the event
// string to emit.
func (s *SeqFileSource) next() string {
	s.n++
	// Persist before emitting: if the process dies between the two, a
	// restart re-emits the same number it already wrote here, but never
	// double-writes a number it already emitted.
	if err := writeSeq(s.path, s.n); err != nil {
		s.log.Error("failed to persist sequence number", "path", s.path, "error", err)
	}
	return fmt.Sprintf("Persistent Event %d", s.n)
}

func readSeq(path string) (int, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read sequence file: %w", err)
	}
	text := strings.TrimSpace(string(data))
	if text == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(text)
	if err != nil {
		return 0, fmt.Errorf("sequence file %s has invalid content %q: %w", path, text, err)
	}
	return n, nil
}

func writeSeq(path string, n int) error {
	return os.WriteFile(path, []byte(strconv.Itoa(n)), 0o644)
}
