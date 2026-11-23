// Package clientqueue provides messagequeue's per-client durable event
// queue abstraction (Queue) and its two implementations: an in-memory one
// and a SQLite-backed one that survives process restarts.
package clientqueue

// Queue is one client's durable, ordered event queue: oldest-first,
// not-yet-acknowledged event bodies. It intentionally does not cover
// in-flight delivery/retry-timer bookkeeping -- that's inherently tied to a
// live *time.Timer and can't meaningfully be persisted, so callers track it
// separately, in memory, regardless of which Queue implementation is in use.
type Queue interface {
	// Enqueue appends event to the back of the queue, then trims from the
	// front until at most cap events remain. overflowed reports whether
	// anything was dropped by that trim.
	Enqueue(event string, cap int) (overflowed bool, err error)

	// PeekFront returns the oldest event without removing it. ok is false
	// if the queue is empty.
	PeekFront() (event string, ok bool, err error)

	// PopFront removes the oldest event. It is a no-op if the queue is
	// empty.
	PopFront() error

	// Len reports how many events are currently queued.
	Len() (int, error)

	// Delete permanently removes every retained event for this client,
	// including any durable on-disk rows. Unlike simply dropping the
	// in-process handle (e.g. when a client merely disconnects, and may
	// reconnect and resume its queue later), Delete represents the client
	// explicitly saying it's done -- called when it UNSUBSCRIBEs.
	Delete() error

	// Params returns the subscription parameters this queue was created
	// with (see Factory) -- the destination's contents after the client's
	// own leading "<clientID>/" segment.
	Params() string
}

// Factory creates the Queue for a client id the caller has not yet built an
// in-process handle for, given the subscription parameters that client most
// recently subscribed with. A durable implementation (like the SQLite one)
// may transparently resume rows a prior process run already wrote for that
// client id.
type Factory func(clientID string, params string) Queue

// MemoryQueue is a Queue backed by a plain in-process slice. Its contents
// are lost when the process exits.
type MemoryQueue struct {
	events []string // oldest first
	params string   // subscription parameters this queue was created with (see Factory and Params)
}

// NewMemoryQueue is a Factory.
func NewMemoryQueue(clientID string, params string) Queue {
	return &MemoryQueue{params: params}
}

func (q *MemoryQueue) Enqueue(event string, cap int) (bool, error) {
	q.events = append(q.events, event)
	overflowed := len(q.events) > cap
	if overflowed {
		q.events = q.events[len(q.events)-cap:]
	}
	return overflowed, nil
}

func (q *MemoryQueue) PeekFront() (string, bool, error) {
	if len(q.events) == 0 {
		return "", false, nil
	}
	return q.events[0], true, nil
}

func (q *MemoryQueue) PopFront() error {
	if len(q.events) > 0 {
		q.events = q.events[1:]
	}
	return nil
}

func (q *MemoryQueue) Len() (int, error) {
	return len(q.events), nil
}

func (q *MemoryQueue) Delete() error {
	q.events = nil
	return nil
}

func (q *MemoryQueue) Params() string { return q.params }

// Snapshot returns a copy of this queue's currently-held events, oldest
// first. Exported purely so callers outside this package (e.g. tests
// asserting on exact queue contents) can inspect it without reaching into
// an unexported field.
func (q *MemoryQueue) Snapshot() []string {
	return append([]string(nil), q.events...)
}
