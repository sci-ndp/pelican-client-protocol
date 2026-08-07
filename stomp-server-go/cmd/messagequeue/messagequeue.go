package main

import (
	"fmt"
	"log/slog"
	"net/http"
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
	messageID   string // stable across retries; only the ack id changes
	attempt     int    // retries already attempted; 0 before the first retry
	delay       time.Duration
	timer       *time.Timer
	firstSentAt time.Time // set once, at the original send; used for ack-latency
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
	metrics  *appMetrics

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
	a.metrics = newAppMetrics(a)
	srv.OnSubscribe(a.onSubscribe)
	srv.OnUnsubscribe(a.onUnsubscribe)
	srv.OnAck(a.onAck)
	srv.OnDisconnect(a.onDisconnect)
	go a.run()
	return a
}

// MetricsHandler serves this app's Prometheus metrics in the text exposition
// format, suitable for mounting at e.g. "/metrics".
func (a *messageQueueApp) MetricsHandler() http.Handler {
	return a.metrics.Handler()
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

// activeQueueCount implements liveQueueState.
func (a *messageQueueApp) activeQueueCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.queues)
}

// connectedClientCount implements liveQueueState. a.clientIDs holds exactly
// the destinations of clients this app currently considers connected: added
// on an accepted SUBSCRIBE, removed only on an eventual onDisconnect (an
// UNSUBSCRIBE deliberately leaves the entry in place -- see onUnsubscribe).
func (a *messageQueueApp) connectedClientCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.clientIDs)
}

// queuedEventCount implements liveQueueState: the true total of events
// still pending delivery, summed fresh from every live queue rather than
// tracked incrementally. Queue snapshots are taken under a.mu, but the
// Len() calls themselves (each a query for a durable clientQueue) run after
// releasing it, so a slow or blocked storage backend can't stall the rest
// of the app.
func (a *messageQueueApp) queuedEventCount() int {
	a.mu.Lock()
	queues := make([]clientQueue, 0, len(a.queues))
	for _, q := range a.queues {
		queues = append(queues, q)
	}
	a.mu.Unlock()

	total := 0
	for _, q := range queues {
		n, err := q.Len()
		if err != nil {
			a.log.Error("queue length check failed during metrics scrape", "error", err)
			a.metrics.queueErrorOccurred("len")
			continue
		}
		total += n
	}
	return total
}

// onEvent implements the fan-out and 100-cap policy: while there are no
// registered queues, new events are dropped; once at least one exists, the
// event is appended to every queue whose client the EventSource's
// ShouldNotify accepts, capped at cfg.maxQueueLen by dropping the oldest
// entries. A client that's currently connected and whose queue just
// overflowed is disconnected; an offline client is simply trimmed, since
// there's no connection to give up on. A client ShouldNotify rejects this
// event for is skipped entirely -- as if the event never happened for them.
func (a *messageQueueApp) onEvent(event string) {
	a.mu.Lock()
	if len(a.queues) == 0 {
		a.mu.Unlock()
		a.metrics.eventDropped()
		return
	}

	var toDisconnect []string
	var toDeliver []string
	for clientID, q := range a.queues {
		if !a.source.ShouldNotify(event, q) {
			continue
		}
		overflowed, err := q.Enqueue(event, a.cfg.maxQueueLen)
		if err != nil {
			a.log.Error("queue enqueue failed", "subscription", clientID, "error", err)
			a.metrics.queueErrorOccurred("enqueue")
			continue
		}
		a.metrics.eventEnqueued()
		destination := a.destinations[clientID]
		connected := a.srv.Connected(destination)
		if overflowed {
			a.metrics.queueOverflowed(connected)
		}
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
		a.metrics.clientDisconnected("queue_overflow")
		a.disconnectWithReason(d, fmt.Sprintf("event queue exceeded %d messages while connected", a.cfg.maxQueueLen))
	}
	for _, clientID := range toDeliver {
		a.tryDeliver(clientID)
	}
}

// disconnectWithReason sends the client at destination an ERROR frame
// explaining why it's being dropped, then disconnects it. Errors from either
// step are logged, not returned, since callers always disconnect regardless
// of whether the ERROR frame made it out.
func (a *messageQueueApp) disconnectWithReason(destination, reason string) {
	if err := a.srv.SendError(destination, reason); err != nil {
		a.log.Error("send error frame failed", "destination", destination, "error", err)
	}
	if err := a.srv.Disconnect(destination); err != nil {
		a.log.Error("disconnect failed", "destination", destination, "error", err)
	}
}

// hasOwningSegment reports whether destination's first "/"-delimited segment
// is exactly clientID -- i.e. destination is clientID itself, or
// "<clientID>/<subscription parameters>". This is what stops one client from
// subscribing to a destination namespaced under a different client's id.
func hasOwningSegment(destination, clientID string) bool {
	segment := destination
	if idx := strings.IndexByte(destination, '/'); idx >= 0 {
		segment = destination[:idx]
	}
	return segment == clientID
}

// subscriptionParams returns destination's subscription parameters -- its
// contents after the client's own leading "<clientID>/" segment (already
// verified separately by hasOwningSegment), or "" if destination has no "/"
// at all. These are passed to queueFactory when a client's queue is first
// created; nothing yet interprets their contents.
func subscriptionParams(destination string) string {
	if idx := strings.IndexByte(destination, '/'); idx >= 0 {
		return destination[idx+1:]
	}
	return ""
}

// onSubscribe creates a new event queue the first time a client's
// `subscription` header is seen, or resumes the existing one if it matches a
// queue kept around from an earlier connection. Either way, any leftover
// retry state is cleared so this connection's retry loop starts fresh, and
// the client's destination is (re)recorded as wherever this SUBSCRIBE named
// -- future deliveries for this client always go back there, regardless of
// what it was on a previous connection. The destination's first path segment
// must be the client's own subscription id; everything after that is this
// client's subscription parameters (see subscriptionParams), so one client
// can never subscribe to a destination namespaced under a different
// client's id.
func (a *messageQueueApp) onSubscribe(headers map[string]string) {
	clientID := headers["subscription"]
	destination := headers["destination"]
	if clientID == "" || destination == "" {
		a.log.Warn("SUBSCRIBE missing subscription/destination header; ignoring", "headers", headers)
		a.metrics.subscribeRejected("missing_headers")
		return
	}
	if !hasOwningSegment(destination, clientID) {
		a.log.Warn("SUBSCRIBE destination does not start with the client's own subscription id; ignoring",
			"subscription", clientID, "destination", destination)
		a.metrics.subscribeRejected("wrong_owner")
		return
	}
	if ack := headers["ack"]; ack != "client-individual" {
		a.log.Warn("client subscribed without ack:client-individual; retries won't work correctly",
			"subscription", clientID, "ack", ack)
	}
	a.metrics.subscribeAccepted()

	a.mu.Lock()
	if old, ok := a.destinations[clientID]; ok && old != destination {
		delete(a.clientIDs, old)
	}
	a.destinations[clientID] = destination
	a.clientIDs[destination] = clientID

	q, existed := a.queues[clientID]
	if !existed {
		q = a.newQueue(clientID, subscriptionParams(destination))
		a.queues[clientID] = q
	}
	a.clearInFlight(clientID)
	a.mu.Unlock()

	// existed only tracks whether this process has already built an
	// in-process handle for clientID; a durable queue (e.g. sqliteClientQueue)
	// may have events from a prior process run before this one ever saw
	// clientID, so ask the queue itself whether there's anything to resume,
	// purely for this log line -- queuedEvents itself reads straight from the
	// queue at scrape time, so there's nothing to seed here.
	if n, err := q.Len(); err != nil {
		a.log.Warn("queue length check failed", "subscription", clientID, "error", err)
		a.metrics.queueErrorOccurred("len")
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
		a.metrics.queueErrorOccurred("peek")
		return
	}
	if !hasEvent {
		a.mu.Unlock()
		return
	}
	inFlight := &inFlightDelivery{messageID: uuid.NewString(), delay: a.cfg.initialRetryDelay, firstSentAt: time.Now()}
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
	a.metrics.messagePublished()

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
		a.metrics.clientDisconnected("retry_exhausted")
		a.disconnectWithReason(destination, fmt.Sprintf("no ACK received after %d retries", a.cfg.maxRetries))
		return
	}
	inFlight.attempt++
	inFlight.delay = time.Duration(float64(inFlight.delay) * a.cfg.retryBackoffFactor)
	body, hasEvent, err := q.PeekFront()
	destination := a.destinations[clientID]
	a.mu.Unlock()
	if err != nil {
		a.log.Error("queue peek failed", "subscription", clientID, "error", err)
		a.metrics.queueErrorOccurred("peek")
		return
	}
	if !hasEvent {
		return
	}
	a.metrics.retryAttempted()

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
	ackLatency := time.Since(inFlight.firstSentAt)
	a.clearInFlight(clientID)
	if err := q.PopFront(); err != nil {
		a.mu.Unlock()
		a.log.Error("queue pop failed", "subscription", clientID, "error", err)
		a.metrics.queueErrorOccurred("pop")
		return
	}
	remaining, err := q.Len()
	a.mu.Unlock()
	a.metrics.acked(ackLatency)
	if err != nil {
		a.log.Error("queue length check failed", "subscription", clientID, "error", err)
		a.metrics.queueErrorOccurred("len")
		return
	}

	if remaining > 0 {
		a.tryDeliver(clientID)
	}
}

// onUnsubscribe permanently forgets the client's queue (including any
// durable on-disk rows via clientQueue.Delete), in-flight delivery/retry
// timer, and delivery-target bookkeeping for the client that was subscribed
// at headers["destination"]. This is deliberately more destructive than
// onDisconnect for that state -- a disconnect only affects connected-clients
// accounting, since the client may reconnect and resume its queue, but an
// explicit UNSUBSCRIBE is this client saying it's done with that queue for
// good.
//
// clientIDs' entry for this destination is deliberately NOT deleted here,
// unlike destinations/queues: it's the only link onDisconnect has back to a
// clientID once the client has unsubscribed from everything, and
// connectedClientCount (queuedEventCount's connected-clients counterpart)
// still needs it later to correctly stop counting this client once the
// connection eventually closes (internal/stomp's OnDisconnect fires for
// every destination a connection ever subscribed to, even ones already
// UNSUBSCRIBEd, precisely so that lookup keeps working). It is finally
// cleaned up by onDisconnect itself.
//
// A destination this app never accepted a subscribe for is silently ignored,
// the same as onAck/onDisconnect already do.
func (a *messageQueueApp) onUnsubscribe(headers map[string]string) {
	destination := headers["destination"]

	a.mu.Lock()
	clientID, ok := a.clientIDs[destination]
	if !ok {
		a.mu.Unlock()
		return
	}
	a.clearInFlight(clientID)
	q := a.queues[clientID]
	delete(a.queues, clientID)
	delete(a.destinations, clientID)
	a.mu.Unlock()

	if q != nil {
		if err := q.Delete(); err != nil {
			a.log.Error("queue delete failed", "subscription", clientID, "error", err)
			a.metrics.queueErrorOccurred("delete")
		}
	}
	a.log.Info("client unsubscribed; queue and bookkeeping deleted", "subscription", clientID, "destination", destination)
}

// onDisconnect stops counting the client subscribed at headers["destination"]
// as connected, by any means (client DISCONNECT, network error, or a
// server-initiated force-close via disconnectWithReason) -- connectedClients
// itself is computed straight from len(a.clientIDs) at scrape time, so this
// need only remove the entry. A destination this app never accepted a
// subscribe for (e.g. one onSubscribe rejected) is a no-op, the same as
// onAck already does for acks it doesn't recognize.
func (a *messageQueueApp) onDisconnect(headers map[string]string) {
	a.mu.Lock()
	delete(a.clientIDs, headers["destination"])
	a.mu.Unlock()
}
