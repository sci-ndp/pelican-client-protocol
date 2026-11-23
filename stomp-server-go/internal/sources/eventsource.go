// Package sources provides this repo's concrete eventsource.EventSource
// implementations: a plain demo ticker, a restart-durable sequence counter,
// and a Pelican federation directory watcher.
package sources

import (
	"fmt"
	"github.com/sci-ndp/pelican-client-protocol/stomp-server-go/eventsource"
	"time"
)

// TickerSource is a demo EventSource that emits an incrementing "Event <n>"
// string on a fixed interval.
type TickerSource struct {
	eventsource.DefaultNotifier
	ch chan string
}

// NewTickerSource starts emitting immediately on a background goroutine that
// runs for the life of the process.
func NewTickerSource(interval time.Duration) *TickerSource {
	s := &TickerSource{ch: make(chan string, 1)}
	go s.run(interval)
	return s
}

func (s *TickerSource) Events() <-chan string { return s.ch }

func (s *TickerSource) run(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	n := 0
	for range ticker.C {
		n++
		s.ch <- fmt.Sprintf("Event %d", n)
	}
}
