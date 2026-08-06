package main

import (
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"stomp-server-go/internal/stomp"
)

const (
	initialRetryDelay  = 1 * time.Second
	retryBackoffFactor = 1.2
	maxRetries         = 16
	maxQueueLen        = 100
)

// messageQueueConfig holds the retry/cap tuning knobs, broken out so tests
// can run the retry loop on a compressed timescale instead of real wall-clock
// delays.
type messageQueueConfig struct {
	initialRetryDelay  time.Duration
	retryBackoffFactor float64
	maxRetries         int
	maxQueueLen        int
}

var defaultMessageQueueConfig = messageQueueConfig{
	initialRetryDelay:  initialRetryDelay,
	retryBackoffFactor: retryBackoffFactor,
	maxRetries:         maxRetries,
	maxQueueLen:        maxQueueLen,
}

// inFlightDelivery is one delivered-but-not-yet-acked MESSAGE and its retry
// timer. It is entirely local bookkeeping: this app never NACKs and never
// relies on the server's own NACK-triggered redelivery (server.go's
// handleNack). It is cleared on ack, and replaced fresh (never reused) on
// every (re)subscribe, so retry state never survives a disconnect the way
// queue contents do. Unlike clientQueue, it is never persisted -- a
// *time.Timer can't survive a restart, so this stays in-memory regardless of
// which clientQueue implementation is in use.
type inFlightDelivery struct {
	messageID string // stable across retries; only the ack id changes
	attempt   int    // retries already attempted; 0 before the first retry
	delay     time.Duration
	timer     *time.Timer
}

// messageQueueApp is a small application layer built entirely on the
// StompServer interface: it never touches Server's internal session or
// subscription state. Each client gets a durable, per-client event queue
// (keyed by its `subscription` header, surviving reconnects) drained one
// message at a time with ack-gated, exponentially-backed-off retries.
// Deliveries always go back to whatever destination the client's SUBSCRIBE
// actually named -- there is no required relationship between that
// destination and the client's `subscription` header value.
type messageQueueApp struct {
	srv      stomp.StompServer
	log      *slog.Logger
	source   EventSource
	cfg      messageQueueConfig
	newQueue queueFactory

	mu           sync.Mutex
	queues       map[string]clientQueue       // client's `subscription` header value -> queue
	inFlights    map[string]*inFlightDelivery // client's `subscription` header value -> in-flight delivery, if any
	destinations map[string]string            // client's `subscription` header value -> destination it last subscribed to
	clientIDs    map[string]string            // destination -> client's `subscription` header value (reverse of destinations)
}

// newMessageQueueApp registers itself as a subscriber to srv's SUBSCRIBE and
// ACK events, starts reading source, and returns the running app. newQueue
// is called once per distinct client id to create that client's clientQueue.
func newMessageQueueApp(srv stomp.StompServer, log *slog.Logger, source EventSource, newQueue queueFactory) *messageQueueApp {
	return newMessageQueueAppWithConfig(srv, log, source, defaultMessageQueueConfig, newQueue)
}

func newMessageQueueAppWithConfig(srv stomp.StompServer, log *slog.Logger, source EventSource, cfg messageQueueConfig, newQueue queueFactory) *messageQueueApp {
	a := &messageQueueApp{
		srv:          srv,
		log:          log,
		source:       source,
		cfg:          cfg,
		newQueue:     newQueue,
		queues:       map[string]clientQueue{},
		inFlights:    map[string]*inFlightDelivery{},
		destinations: map[string]string{},
		clientIDs:    map[string]string{},
	}
	srv.OnSubscribe(a.onSubscribe)
	srv.OnAck(a.onAck)
	go a.run()
	return a
}

// run is the single goroutine that ever reads from source.Events(); all
// clientQueue/inFlights mutation elsewhere in this file happens either here
// or on a connection/timer goroutine, always under a.mu, so there is never a
// second reader of the same channel to race against.
func (a *messageQueueApp) run() {
	for event := range a.source.Events() {
		a.onEvent(event)
	}
}

// clearInFlight stops and forgets clientID's in-flight delivery, if any.
// Must be called with a.mu held.
func (a *messageQueueApp) clearInFlight(clientID string) {
	if inFlight := a.inFlights[clientID]; inFlight != nil {
		if inFlight.timer != nil {
			inFlight.timer.Stop()
		}
		delete(a.inFlights, clientID)
	}
}

// onEvent implements the fan-out and 100-cap policy: while there are no
// registered queues, new events are dropped; once at least one exists, the
// event is appended to every queue, capped at cfg.maxQueueLen by dropping the
// oldest entries. A client that's currently connected and whose queue just
// overflowed is disconnected; an offline client is simply trimmed, since
// there's no connection to give up on.
func (a *messageQueueApp) onEvent(event string) {
	a.mu.Lock()
	if len(a.queues) == 0 {
		a.mu.Unlock()
		return
	}

	var toDisconnect []string
	var toDeliver []string
	for clientID, q := range a.queues {
		overflowed, err := q.Enqueue(event, a.cfg.maxQueueLen)
		if err != nil {
			a.log.Error("queue enqueue failed", "subscription", clientID, "error", err)
			continue
		}
		destination := a.destinations[clientID]
		connected := a.srv.Connected(destination)
		if overflowed && connected {
			a.clearInFlight(clientID)
			toDisconnect = append(toDisconnect, destination)
		} else if a.inFlights[clientID] == nil && connected {
			toDeliver = append(toDeliver, clientID)
		}
	}
	a.mu.Unlock()

	for _, d := range toDisconnect {
		a.log.Warn("queue exceeded cap while connected; disconnecting", "destination", d)
		if err := a.srv.Disconnect(d); err != nil {
			a.log.Error("disconnect failed", "destination", d, "error", err)
		}
	}
	for _, clientID := range toDeliver {
		a.tryDeliver(clientID)
	}
}

// hasOwningSegment reports whether destination's first "/"-delimited segment
// is exactly clientID -- i.e. destination is clientID itself, or
// "<clientID>/<anything>". This is what stops one client from subscribing to
// a destination namespaced under a different client's id.
func hasOwningSegment(destination, clientID string) bool {
	segment := destination
	if idx := strings.IndexByte(destination, '/'); idx >= 0 {
		segment = destination[:idx]
	}
	return segment == clientID
}

// onSubscribe creates a new event queue the first time a client's
// `subscription` header is seen, or resumes the existing one if it matches a
// queue kept around from an earlier connection. Either way, any leftover
// retry state is cleared so this connection's retry loop starts fresh, and
// the client's destination is (re)recorded as wherever this SUBSCRIBE named
// -- future deliveries for this client always go back there, regardless of
// what it was on a previous connection. The destination's first path segment
// must be the client's own subscription id (any suffix after that is free
// choice), so one client can never subscribe to a destination namespaced
// under a different client's id.
func (a *messageQueueApp) onSubscribe(headers map[string]string) {
	clientID := headers["subscription"]
	destination := headers["destination"]
	if clientID == "" || destination == "" {
		a.log.Warn("SUBSCRIBE missing subscription/destination header; ignoring", "headers", headers)
		return
	}
	if !hasOwningSegment(destination, clientID) {
		a.log.Warn("SUBSCRIBE destination does not start with the client's own subscription id; ignoring",
			"subscription", clientID, "destination", destination)
		return
	}
	if ack := headers["ack"]; ack != "client-individual" {
		a.log.Warn("client subscribed without ack:client-individual; retries won't work correctly",
			"subscription", clientID, "ack", ack)
	}

	a.mu.Lock()
	if old, ok := a.destinations[clientID]; ok && old != destination {
		delete(a.clientIDs, old)
	}
	a.destinations[clientID] = destination
	a.clientIDs[destination] = clientID

	q, existed := a.queues[clientID]
	if !existed {
		q = a.newQueue(clientID)
		a.queues[clientID] = q
	}
	a.clearInFlight(clientID)
	a.mu.Unlock()

	// existed only tracks whether this process has already built an
	// in-process handle for clientID; a durable queue (e.g. sqliteClientQueue)
	// may have events from a prior process run before this one ever saw
	// clientID, so ask the queue itself whether there's anything to resume.
	if n, err := q.Len(); err != nil {
		a.log.Warn("queue length check failed", "subscription", clientID, "error", err)
	} else if existed || n > 0 {
		a.log.Info("resumed existing event queue", "subscription", clientID, "queued", n)
	} else {
		a.log.Info("created new event queue", "subscription", clientID)
	}
	a.tryDeliver(clientID)
}

// tryDeliver sends the oldest un-acked event in clientID's queue, if any, and
// arms a fresh retry timer. Must not be called with a.mu held.
func (a *messageQueueApp) tryDeliver(clientID string) {
	a.mu.Lock()
	q, ok := a.queues[clientID]
	if !ok || a.inFlights[clientID] != nil {
		a.mu.Unlock()
		return
	}
	body, hasEvent, err := q.PeekFront()
	if err != nil {
		a.mu.Unlock()
		a.log.Error("queue peek failed", "subscription", clientID, "error", err)
		return
	}
	if !hasEvent {
		a.mu.Unlock()
		return
	}
	inFlight := &inFlightDelivery{messageID: uuid.NewString(), delay: a.cfg.initialRetryDelay}
	a.inFlights[clientID] = inFlight
	destination := a.destinations[clientID]
	a.mu.Unlock()

	a.sendAndArm(clientID, destination, body, inFlight)
}

// sendAndArm publishes body to destination and arms inFlight's retry timer.
// The timer is created and assigned to inFlight.timer while holding a.mu so
// that, even if the timer fires immediately (possible with a very short
// retry delay), its callback -- which locks a.mu as its first action --
// cannot observe inFlight.timer before this assignment completes.
func (a *messageQueueApp) sendAndArm(clientID, destination, body string, inFlight *inFlightDelivery) {
	if err := a.srv.Publish(destination, inFlight.messageID, body); err != nil {
		a.log.Error("publish failed", "destination", destination, "error", err)
	}

	a.mu.Lock()
	inFlight.timer = time.AfterFunc(inFlight.delay, func() {
		a.onRetryTimeout(clientID, inFlight)
	})
	a.mu.Unlock()
}

// onRetryTimeout resends the current head-of-queue event with a longer
// backoff, or gives up and disconnects the client once cfg.maxRetries have
// already been attempted without an ack.
func (a *messageQueueApp) onRetryTimeout(clientID string, inFlight *inFlightDelivery) {
	a.mu.Lock()
	q, ok := a.queues[clientID]
	if !ok || a.inFlights[clientID] != inFlight {
		a.mu.Unlock()
		return // superseded by an ack or a fresh resubscribe; ignore
	}
	if inFlight.attempt >= a.cfg.maxRetries {
		delete(a.inFlights, clientID)
		destination := a.destinations[clientID]
		a.mu.Unlock()
		a.log.Warn("retry budget exhausted; disconnecting client", "subscription", clientID, "destination", destination)
		if err := a.srv.Disconnect(destination); err != nil {
			a.log.Error("disconnect failed", "destination", destination, "error", err)
		}
		return
	}
	inFlight.attempt++
	inFlight.delay = time.Duration(float64(inFlight.delay) * a.cfg.retryBackoffFactor)
	body, hasEvent, err := q.PeekFront()
	destination := a.destinations[clientID]
	a.mu.Unlock()
	if err != nil {
		a.log.Error("queue peek failed", "subscription", clientID, "error", err)
		return
	}
	if !hasEvent {
		return
	}

	a.sendAndArm(clientID, destination, body, inFlight)
}

// onAck resolves the in-flight delivery it corresponds to (if any) and
// advances the queue. Acks that don't match the currently in-flight message
// -- because it was already superseded by a retry, a fresh resubscribe, or
// this is a duplicate/stale ack for an unrelated destination -- are ignored.
func (a *messageQueueApp) onAck(headers map[string]string) {
	messageID := headers["message-id"]

	a.mu.Lock()
	clientID, ok := a.clientIDs[headers["destination"]]
	if !ok {
		a.mu.Unlock()
		return // not one of ours
	}
	q, ok := a.queues[clientID]
	inFlight := a.inFlights[clientID]
	if !ok || inFlight == nil || inFlight.messageID != messageID {
		a.mu.Unlock()
		return
	}
	a.clearInFlight(clientID)
	if err := q.PopFront(); err != nil {
		a.mu.Unlock()
		a.log.Error("queue pop failed", "subscription", clientID, "error", err)
		return
	}
	remaining, err := q.Len()
	a.mu.Unlock()
	if err != nil {
		a.log.Error("queue length check failed", "subscription", clientID, "error", err)
		return
	}

	if remaining > 0 {
		a.tryDeliver(clientID)
	}
}
