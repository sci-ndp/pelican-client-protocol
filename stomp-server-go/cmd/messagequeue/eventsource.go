package main

import (
	"fmt"
	"time"
)

// EventSource produces events that get fanned out to every registered
// client queue. Name is used as the second path segment of every client's
// destination: "<subscription-header-value>/<Name()>".
type EventSource interface {
	Name() string
	// Events returns a channel that emits one string per event, for the
	// lifetime of the process. It has a single consumer (see
	// messageQueueApp.run), so implementations need no internal locking.
	Events() <-chan string
}

// tickerEventSource is a demo EventSource that emits an incrementing
// "Event <n>" string on a fixed interval.
type tickerEventSource struct {
	name string
	ch   chan string
}

// newTickerEventSource starts emitting immediately on a background
// goroutine that runs for the life of the process.
func newTickerEventSource(name string, interval time.Duration) *tickerEventSource {
	s := &tickerEventSource{name: name, ch: make(chan string, 1)}
	go s.run(interval)
	return s
}

func (s *tickerEventSource) Name() string         { return s.name }
func (s *tickerEventSource) Events() <-chan string { return s.ch }

func (s *tickerEventSource) run(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	n := 0
	for range ticker.C {
		n++
		s.ch <- fmt.Sprintf("Event %d", n)
	}
}
