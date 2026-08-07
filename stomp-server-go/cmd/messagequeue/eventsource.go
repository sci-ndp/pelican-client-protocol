package main

import (
	"fmt"
	"time"
)

// EventSource produces events that get fanned out to every registered
// client queue ShouldNotify accepts.
type EventSource interface {
	// Events returns a channel that emits one string per event, for the
	// lifetime of the process. It has a single consumer (see
	// messageQueueApp.run), so implementations need no internal locking.
	Events() <-chan string

	// ShouldNotify reports whether event should be delivered to the client
	// owning q. Called once per registered client queue for every event
	// this source produces, so an EventSource can filter delivery per
	// client (e.g. based on that client's subscription parameters) instead
	// of every event always fanning out to every client. Runs on
	// messageQueueApp's single event-processing goroutine while a.mu is
	// held, so it must not block or call back into messageQueueApp.
	ShouldNotify(event string, q clientQueue) bool
}

// defaultNotifier is embedded by EventSource implementations that don't need
// per-client filtering: it always says yes, matching the original
// fan-out-to-every-client behavior. Embed it to get ShouldNotify for free;
// override it (define your own ShouldNotify method) to filter.
type defaultNotifier struct{}

func (defaultNotifier) ShouldNotify(event string, q clientQueue) bool { return true }

// tickerEventSource is a demo EventSource that emits an incrementing
// "Event <n>" string on a fixed interval.
type tickerEventSource struct {
	defaultNotifier
	ch chan string
}

// newTickerEventSource starts emitting immediately on a background
// goroutine that runs for the life of the process.
func newTickerEventSource(interval time.Duration) *tickerEventSource {
	s := &tickerEventSource{ch: make(chan string, 1)}
	go s.run(interval)
	return s
}

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
