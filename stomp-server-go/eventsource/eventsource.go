// Package eventsource defines the EventSource interface that feeds events
// into a notification server, plus DefaultNotifier for implementations that
// don't need per-client filtering.
package eventsource

import "github.com/sci-ndp/pelican-client-protocol/stomp-server-go/clientqueue"

// EventSource produces events that get fanned out to every registered
// client queue ShouldNotify accepts.
type EventSource interface {
	// Events returns a channel that emits one string per event, for the
	// lifetime of the process. It has a single consumer, so implementations
	// need no internal locking.
	Events() <-chan string

	// ShouldNotify reports whether event should be delivered to the client
	// owning q. Called once per registered client queue for every event
	// this source produces, so an EventSource can filter delivery per
	// client (e.g. based on that client's subscription parameters) instead
	// of every event always fanning out to every client. Runs on the
	// caller's single event-processing goroutine while its own lock is
	// held, so it must not block or call back into the caller.
	ShouldNotify(event string, q clientqueue.Queue) bool

	// QueueAdded is called once, synchronously, whenever the caller creates
	// a brand-new client queue -- a client's first-ever subscribe in this
	// process's lifetime, not a resubscribe/reconnect that resumes an
	// existing queue. Lets an EventSource that needs to know which clients
	// exist (e.g. to derive what to watch from their subscription
	// parameters) stay up to date without reaching back into the caller,
	// which doesn't exist yet at the point an EventSource is itself
	// constructed. Must not block or call back into the caller.
	QueueAdded(q clientqueue.Queue)

	// QueueRemoved is called once, synchronously, whenever the caller
	// permanently deletes a client queue (an explicit UNSUBSCRIBE, see
	// clientqueue.Queue.Delete) -- not on a mere disconnect, since a queue
	// persists across those so the client can reconnect and resume it.
	// Must not block or call back into the caller.
	QueueRemoved(q clientqueue.Queue)
}

// DefaultNotifier is embedded by EventSource implementations that don't need
// per-client filtering or queue tracking: ShouldNotify always says yes,
// matching the original fan-out-to-every-client behavior, and
// QueueAdded/QueueRemoved are no-ops. Embed it to get all three for free;
// define your own method(s) to override just the ones you need.
type DefaultNotifier struct{}

func (DefaultNotifier) ShouldNotify(event string, q clientqueue.Queue) bool { return true }
func (DefaultNotifier) QueueAdded(q clientqueue.Queue)                      {}
func (DefaultNotifier) QueueRemoved(q clientqueue.Queue)                    {}
