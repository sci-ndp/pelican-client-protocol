package main

// clientQueue is one client's durable, ordered event queue: oldest-first,
// not-yet-acknowledged event bodies. It intentionally does not cover
// in-flight delivery/retry-timer bookkeeping (inFlightDelivery) -- that's
// inherently tied to a live *time.Timer and can't meaningfully be persisted,
// so messageQueueApp tracks it separately, in memory, regardless of which
// clientQueue implementation is in use.
type clientQueue interface {
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
}

// queueFactory creates the clientQueue for a client id the app has not yet
// built an in-process handle for. A durable implementation (like
// sqliteClientQueue) may transparently resume rows a prior process run
// already wrote for that client id.
type queueFactory func(clientID string) clientQueue

// memoryClientQueue is a clientQueue backed by a plain in-process slice.
// Its contents are lost when the process exits.
type memoryClientQueue struct {
	events []string // oldest first
}

// newMemoryClientQueue is a queueFactory.
func newMemoryClientQueue(clientID string) clientQueue {
	return &memoryClientQueue{}
}

func (q *memoryClientQueue) Enqueue(event string, cap int) (bool, error) {
	q.events = append(q.events, event)
	overflowed := len(q.events) > cap
	if overflowed {
		q.events = q.events[len(q.events)-cap:]
	}
	return overflowed, nil
}

func (q *memoryClientQueue) PeekFront() (string, bool, error) {
	if len(q.events) == 0 {
		return "", false, nil
	}
	return q.events[0], true, nil
}

func (q *memoryClientQueue) PopFront() error {
	if len(q.events) > 0 {
		q.events = q.events[1:]
	}
	return nil
}

func (q *memoryClientQueue) Len() (int, error) {
	return len(q.events), nil
}

func (q *memoryClientQueue) Delete() error {
	q.events = nil
	return nil
}
