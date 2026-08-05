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
// queue contents do.
type inFlightDelivery struct {
	messageID string // stable across retries; only the ack id changes
	attempt   int    // retries already attempted; 0 before the first retry
	delay     time.Duration
	timer     *time.Timer
}

// clientQueue is one client's persistent event queue plus whatever delivery
// is currently in flight for it, if any.
type clientQueue struct {
	events   []string // oldest first; events[0] is the oldest un-acked event
	inFlight *inFlightDelivery
}

// messageQueueApp is a small application layer built entirely on the
// StompServer interface: it never touches Server's internal session or
// subscription state. Each client gets a durable, per-client event queue
// (keyed by its `subscription` header, surviving reconnects) drained one
// message at a time with ack-gated, exponentially-backed-off retries.
type messageQueueApp struct {
	srv    stomp.StompServer
	log    *slog.Logger
	source EventSource
	cfg    messageQueueConfig

	mu     sync.Mutex
	queues map[string]*clientQueue // client's `subscription` header value -> queue
}

// newMessageQueueApp registers itself as a subscriber to srv's SUBSCRIBE and
// ACK events, starts reading source, and returns the running app.
func newMessageQueueApp(srv stomp.StompServer, log *slog.Logger, source EventSource) *messageQueueApp {
	return newMessageQueueAppWithConfig(srv, log, source, defaultMessageQueueConfig)
}

func newMessageQueueAppWithConfig(srv stomp.StompServer, log *slog.Logger, source EventSource, cfg messageQueueConfig) *messageQueueApp {
	a := &messageQueueApp{
		srv:    srv,
		log:    log,
		source: source,
		cfg:    cfg,
		queues: map[string]*clientQueue{},
	}
	srv.OnSubscribe(a.onSubscribe)
	srv.OnAck(a.onAck)
	go a.run()
	return a
}

// run is the single goroutine that ever reads from source.Events(); all
// clientQueue map/slice mutation elsewhere in this file happens either here
// or on a connection/timer goroutine, always under a.mu, so there is never a
// second reader of the same channel to race against.
func (a *messageQueueApp) run() {
	for event := range a.source.Events() {
		a.onEvent(event)
	}
}

func (a *messageQueueApp) destination(clientID string) string {
	return clientID + "/" + a.source.Name()
}

func (a *messageQueueApp) clientIDFromDestination(destination string) (string, bool) {
	suffix := "/" + a.source.Name()
	if !strings.HasSuffix(destination, suffix) {
		return "", false
	}
	return strings.TrimSuffix(destination, suffix), true
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
		q.events = append(q.events, event)
		overflowed := len(q.events) > a.cfg.maxQueueLen
		if overflowed {
			q.events = q.events[len(q.events)-a.cfg.maxQueueLen:]
		}
		destination := a.destination(clientID)
		connected := a.srv.Connected(destination)
		if overflowed && connected {
			if q.inFlight != nil {
				if q.inFlight.timer != nil {
					q.inFlight.timer.Stop()
				}
				q.inFlight = nil
			}
			toDisconnect = append(toDisconnect, destination)
		} else if q.inFlight == nil && connected {
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

// onSubscribe creates a new event queue the first time a client's
// `subscription` header is seen, or resumes the existing one if it matches a
// queue kept around from an earlier connection. Either way, any leftover
// retry state is cleared so this connection's retry loop starts fresh.
func (a *messageQueueApp) onSubscribe(headers map[string]string) {
	clientID := headers["subscription"]
	destination := headers["destination"]
	if clientID == "" || destination == "" {
		a.log.Warn("SUBSCRIBE missing subscription/destination header; ignoring", "headers", headers)
		return
	}
	if ack := headers["ack"]; ack != "client-individual" {
		a.log.Warn("client subscribed without ack:client-individual; retries won't work correctly",
			"subscription", clientID, "ack", ack)
	}

	a.mu.Lock()
	q, existed := a.queues[clientID]
	if !existed {
		q = &clientQueue{}
		a.queues[clientID] = q
	}
	if q.inFlight != nil {
		if q.inFlight.timer != nil {
			q.inFlight.timer.Stop()
		}
		q.inFlight = nil
	}
	a.mu.Unlock()

	if existed {
		a.log.Info("resumed existing event queue", "subscription", clientID)
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
	if !ok || len(q.events) == 0 || q.inFlight != nil {
		a.mu.Unlock()
		return
	}
	inFlight := &inFlightDelivery{messageID: uuid.NewString(), delay: a.cfg.initialRetryDelay}
	q.inFlight = inFlight
	body := q.events[0]
	a.mu.Unlock()

	a.sendAndArm(clientID, body, inFlight)
}

// sendAndArm publishes body and arms inFlight's retry timer. The timer is
// created and assigned to inFlight.timer while holding a.mu so that, even if
// the timer fires immediately (possible with a very short retry delay), its
// callback -- which locks a.mu as its first action -- cannot observe
// inFlight.timer before this assignment completes.
func (a *messageQueueApp) sendAndArm(clientID, body string, inFlight *inFlightDelivery) {
	destination := a.destination(clientID)
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
	if !ok || q.inFlight != inFlight {
		a.mu.Unlock()
		return // superseded by an ack or a fresh resubscribe; ignore
	}
	if inFlight.attempt >= a.cfg.maxRetries {
		q.inFlight = nil
		a.mu.Unlock()
		destination := a.destination(clientID)
		a.log.Warn("retry budget exhausted; disconnecting client", "subscription", clientID, "destination", destination)
		if err := a.srv.Disconnect(destination); err != nil {
			a.log.Error("disconnect failed", "destination", destination, "error", err)
		}
		return
	}
	inFlight.attempt++
	inFlight.delay = time.Duration(float64(inFlight.delay) * a.cfg.retryBackoffFactor)
	body := q.events[0]
	a.mu.Unlock()

	a.sendAndArm(clientID, body, inFlight)
}

// onAck resolves the in-flight delivery it corresponds to (if any) and
// advances the queue. Acks that don't match the currently in-flight message
// -- because it was already superseded by a retry, a fresh resubscribe, or
// this is a duplicate/stale ack for an unrelated destination -- are ignored.
func (a *messageQueueApp) onAck(headers map[string]string) {
	clientID, ok := a.clientIDFromDestination(headers["destination"])
	if !ok {
		return // not one of ours
	}
	messageID := headers["message-id"]

	a.mu.Lock()
	q, ok := a.queues[clientID]
	if !ok || q.inFlight == nil || q.inFlight.messageID != messageID {
		a.mu.Unlock()
		return
	}
	if q.inFlight.timer != nil {
		q.inFlight.timer.Stop()
	}
	q.inFlight = nil
	q.events = q.events[1:]
	hasMore := len(q.events) > 0
	a.mu.Unlock()

	if hasMore {
		a.tryDeliver(clientID)
	}
}
