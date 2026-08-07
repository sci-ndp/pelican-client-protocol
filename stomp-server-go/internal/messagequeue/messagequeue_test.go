package messagequeue

import (
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"stomp-server-go/internal/clientqueue"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

// publishCall records one Publish invocation the app made against the fake
// server.
type publishCall struct {
	destination string
	messageID   string
	body        string
}

// sentError records one SendError invocation the app made against the fake
// server.
type sentError struct {
	destination string
	message     string
}

// fakeStompServer is a stomp.StompServer test double: it records Publish,
// SendError, and Disconnect calls, lets a test drive the app's
// OnSubscribe/OnAck/OnDisconnect callbacks directly, and lets a test control
// what Connected reports, all without any real network I/O.
type fakeStompServer struct {
	mu               sync.Mutex
	onSubscribeFns   []func(map[string]string)
	onUnsubscribeFns []func(map[string]string)
	onAckFns         []func(map[string]string)
	onDisconnectFns  []func(map[string]string)
	connected        map[string]bool
	publishes        []publishCall
	sentErrors       []sentError
	disconnects      []string
}

func newFakeStompServer() *fakeStompServer {
	return &fakeStompServer{connected: map[string]bool{}}
}

func (f *fakeStompServer) Publish(destination string, messageID string, body any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.publishes = append(f.publishes, publishCall{destination: destination, messageID: messageID, body: body.(string)})
	return nil
}

func (f *fakeStompServer) OnSubscribe(fn func(headers map[string]string)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onSubscribeFns = append(f.onSubscribeFns, fn)
}

func (f *fakeStompServer) OnUnsubscribe(fn func(headers map[string]string)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onUnsubscribeFns = append(f.onUnsubscribeFns, fn)
}

func (f *fakeStompServer) OnAck(fn func(headers map[string]string)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onAckFns = append(f.onAckFns, fn)
}

func (f *fakeStompServer) OnDisconnect(fn func(headers map[string]string)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onDisconnectFns = append(f.onDisconnectFns, fn)
}

func (f *fakeStompServer) SendError(destination string, message string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sentErrors = append(f.sentErrors, sentError{destination: destination, message: message})
	return nil
}

func (f *fakeStompServer) Disconnect(destination string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.disconnects = append(f.disconnects, destination)
	f.connected[destination] = false
	return nil
}

func (f *fakeStompServer) Connected(destination string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.connected[destination]
}

func (f *fakeStompServer) setConnected(destination string, connected bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.connected[destination] = connected
}

func (f *fakeStompServer) snapshotPublishes() []publishCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]publishCall{}, f.publishes...)
}

func (f *fakeStompServer) snapshotDisconnects() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string{}, f.disconnects...)
}

func (f *fakeStompServer) snapshotSentErrors() []sentError {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]sentError{}, f.sentErrors...)
}

// fakeEventSource is an eventsource.EventSource test double whose channel a
// test can feed directly, though most tests below call App's methods
// directly instead, since that avoids any timing dependency on the app's
// background run() goroutine. ShouldNotify defaults to always-true; a test
// that needs to exercise per-client filtering sets shouldNotify directly.
// QueueAdded/QueueRemoved just record every call, for tests that verify
// onSubscribe/onUnsubscribe wire them correctly.
type fakeEventSource struct {
	mu           sync.Mutex
	ch           chan string
	shouldNotify func(event string, q clientqueue.Queue) bool
	added        []clientqueue.Queue
	removed      []clientqueue.Queue
}

func newFakeEventSource() *fakeEventSource {
	return &fakeEventSource{ch: make(chan string, 1)}
}

func (f *fakeEventSource) Events() <-chan string { return f.ch }

func (f *fakeEventSource) ShouldNotify(event string, q clientqueue.Queue) bool {
	if f.shouldNotify != nil {
		return f.shouldNotify(event, q)
	}
	return true
}

func (f *fakeEventSource) QueueAdded(q clientqueue.Queue) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.added = append(f.added, q)
}

func (f *fakeEventSource) QueueRemoved(q clientqueue.Queue) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removed = append(f.removed, q)
}

func (f *fakeEventSource) snapshotAdded() []clientqueue.Queue {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]clientqueue.Queue{}, f.added...)
}

func (f *fakeEventSource) snapshotRemoved() []clientqueue.Queue {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]clientqueue.Queue{}, f.removed...)
}

func newTestApp(cfg Config) (*App, *fakeStompServer) {
	srv := newFakeStompServer()
	source := newFakeEventSource()
	app := NewWithConfig(srv, discardLogger(), source, cfg, clientqueue.NewMemoryQueue)
	return app, srv
}

// queueEvents reads out a clientqueue.MemoryQueue's contents for assertions.
// All tests in this file use clientqueue.NewMemoryQueue as their factory, so
// this type assertion is safe.
func queueEvents(t *testing.T, q clientqueue.Queue) []string {
	t.Helper()
	mq, ok := q.(*clientqueue.MemoryQueue)
	if !ok {
		t.Fatalf("expected a *clientqueue.MemoryQueue, got %T", q)
	}
	return mq.Snapshot()
}

func subscribeHeaders(clientID string) map[string]string {
	return map[string]string{
		"subscription": clientID,
		"destination":  clientID + "/testsource",
		"ack":          "client-individual",
	}
}

func TestOnEvent_DropsWhenNoQueuesRegistered(t *testing.T) {
	app, srv := newTestApp(defaultConfig)

	app.onEvent("Event 1")

	if got := srv.snapshotPublishes(); len(got) != 0 {
		t.Fatalf("Publish calls = %v, want none", got)
	}
}

func TestOnEvent_SkipsClientsShouldNotifyRejects(t *testing.T) {
	app, srv := newTestApp(defaultConfig)
	srv.setConnected("alice/testsource", true)
	srv.setConnected("bob/testsource", true)
	app.onSubscribe(subscribeHeaders("alice"))
	app.onSubscribe(subscribeHeaders("bob"))

	app.mu.Lock()
	bobQueue := app.queues["bob"]
	app.mu.Unlock()

	// ShouldNotify runs while onEvent holds a.mu, so this must not lock it
	// itself -- compare against the queue captured above instead of looking
	// it up again.
	source := app.source.(*fakeEventSource)
	source.shouldNotify = func(event string, q clientqueue.Queue) bool { return q != bobQueue }

	app.onEvent("Event 1")

	publishes := srv.snapshotPublishes()
	if len(publishes) != 1 {
		t.Fatalf("Publish calls = %d, want 1 (only alice): %+v", len(publishes), publishes)
	}
	if publishes[0].destination != "alice/testsource" {
		t.Errorf("destination = %q, want alice/testsource", publishes[0].destination)
	}
	if n, err := bobQueue.Len(); err != nil || n != 0 {
		t.Errorf("bob's queue Len() = (%d, %v), want (0, nil) -- event should never have been enqueued for bob", n, err)
	}
}

func TestOnEvent_ShouldNotifyRejectingEveryoneIsNotCountedAsDropped(t *testing.T) {
	app, srv := newTestApp(defaultConfig)
	srv.setConnected("alice/testsource", true)
	app.onSubscribe(subscribeHeaders("alice"))

	source := app.source.(*fakeEventSource)
	source.shouldNotify = func(event string, q clientqueue.Queue) bool { return false }

	app.onEvent("Event 1")

	if got := srv.snapshotPublishes(); len(got) != 0 {
		t.Fatalf("Publish calls = %v, want none", got)
	}
	if got := testutil.ToFloat64(app.metrics.eventsDropped); got != 0 {
		t.Errorf("eventsDropped = %v, want 0 (queues existed, ShouldNotify just filtered them all)", got)
	}
}

func TestOnSubscribe_CreatesQueueAndDeliversOnceConnected(t *testing.T) {
	app, srv := newTestApp(defaultConfig)
	srv.setConnected("alice/testsource", true)

	app.onSubscribe(subscribeHeaders("alice"))
	app.onEvent("Event 1")

	publishes := srv.snapshotPublishes()
	if len(publishes) != 1 {
		t.Fatalf("Publish calls = %d, want 1: %+v", len(publishes), publishes)
	}
	if publishes[0].destination != "alice/testsource" {
		t.Errorf("destination = %q, want %q", publishes[0].destination, "alice/testsource")
	}
	if publishes[0].body != "Event 1" {
		t.Errorf("body = %q, want %q", publishes[0].body, "Event 1")
	}
	if publishes[0].messageID == "" {
		t.Error("messageID is empty, want a generated id")
	}
}

func TestOnSubscribe_IncrementsConnectedClientsGauge(t *testing.T) {
	app, srv := newTestApp(defaultConfig)
	srv.setConnected("alice/testsource", true)
	srv.setConnected("bob/testsource", true)

	if got := testutil.ToFloat64(app.metrics.connectedClients); got != 0 {
		t.Fatalf("connectedClients before any subscribe = %v, want 0", got)
	}

	app.onSubscribe(subscribeHeaders("alice"))
	if got := testutil.ToFloat64(app.metrics.connectedClients); got != 1 {
		t.Errorf("connectedClients after one subscribe = %v, want 1", got)
	}

	app.onSubscribe(subscribeHeaders("bob"))
	if got := testutil.ToFloat64(app.metrics.connectedClients); got != 2 {
		t.Errorf("connectedClients after a second client subscribes = %v, want 2", got)
	}
}

func TestOnDisconnect_DecrementsConnectedClientsGauge(t *testing.T) {
	app, srv := newTestApp(defaultConfig)
	srv.setConnected("alice/testsource", true)
	app.onSubscribe(subscribeHeaders("alice"))
	if got := testutil.ToFloat64(app.metrics.connectedClients); got != 1 {
		t.Fatalf("connectedClients after subscribe = %v, want 1", got)
	}

	app.onDisconnect(map[string]string{"destination": "alice/testsource"})

	if got := testutil.ToFloat64(app.metrics.connectedClients); got != 0 {
		t.Errorf("connectedClients after onDisconnect = %v, want 0", got)
	}
}

func TestOnDisconnect_IgnoresUnknownDestination(t *testing.T) {
	app, srv := newTestApp(defaultConfig)
	srv.setConnected("alice/testsource", true)
	app.onSubscribe(subscribeHeaders("alice"))

	app.onDisconnect(map[string]string{"destination": "someone/else"})

	if got := testutil.ToFloat64(app.metrics.connectedClients); got != 1 {
		t.Errorf("connectedClients after an unrelated onDisconnect = %v, want still 1 (unaffected)", got)
	}
}

func TestOnDisconnect_ReconnectAfterDisconnectIncrementsAgain(t *testing.T) {
	app, srv := newTestApp(defaultConfig)
	srv.setConnected("alice/testsource", true)

	app.onSubscribe(subscribeHeaders("alice"))
	app.onDisconnect(map[string]string{"destination": "alice/testsource"})
	if got := testutil.ToFloat64(app.metrics.connectedClients); got != 0 {
		t.Fatalf("connectedClients after disconnect = %v, want 0", got)
	}

	// A genuine reconnect (new WebSocket, same subscription header) should
	// bring the gauge back up, not leave it stuck at 0.
	app.onSubscribe(subscribeHeaders("alice"))
	if got := testutil.ToFloat64(app.metrics.connectedClients); got != 1 {
		t.Errorf("connectedClients after reconnecting = %v, want 1", got)
	}
}

func TestOnUnsubscribe_DeletesQueueAndBookkeeping(t *testing.T) {
	app, srv := newTestApp(defaultConfig)
	srv.setConnected("alice/testsource", true)

	app.onSubscribe(subscribeHeaders("alice"))
	app.onEvent("Event 1") // gives the queue something retained to delete

	app.mu.Lock()
	q := app.queues["alice"]
	app.mu.Unlock()
	if q == nil {
		t.Fatal("queue for alice was never created")
	}

	app.onUnsubscribe(map[string]string{"destination": "alice/testsource"})

	app.mu.Lock()
	_, hasQueue := app.queues["alice"]
	_, hasDestination := app.destinations["alice"]
	_, hasInFlight := app.inFlights["alice"]
	app.mu.Unlock()
	if hasQueue {
		t.Error("queues[\"alice\"] still present after onUnsubscribe")
	}
	if hasDestination {
		t.Error("destinations[\"alice\"] still present after onUnsubscribe")
	}
	if hasInFlight {
		t.Error("inFlights[\"alice\"] still present after onUnsubscribe")
	}
	// clientIDs["alice/testsource"] is deliberately NOT asserted gone here:
	// it's kept alive on purpose until onDisconnect, so the connected-clients
	// gauge can still be decremented correctly even after an unsubscribe --
	// see TestOnDisconnect_DecrementsGaugeEvenAfterPriorUnsubscribe.

	// The queue instance itself must have been told to delete its retained
	// events too -- not just dropped from the map -- since a durable
	// (SQLite-backed) queue would otherwise leak that client's rows forever.
	if n, err := q.Len(); err != nil || n != 0 {
		t.Errorf("old queue instance Len() after onUnsubscribe = (%d, %v), want (0, nil)", n, err)
	}
}

func TestOnUnsubscribe_DecrementsActiveQueuesGauge(t *testing.T) {
	app, srv := newTestApp(defaultConfig)
	srv.setConnected("alice/testsource", true)

	app.onSubscribe(subscribeHeaders("alice"))
	if got := testutil.ToFloat64(app.metrics.activeQueues); got != 1 {
		t.Fatalf("activeQueues after subscribe = %v, want 1", got)
	}

	app.onUnsubscribe(map[string]string{"destination": "alice/testsource"})

	if got := testutil.ToFloat64(app.metrics.activeQueues); got != 0 {
		t.Errorf("activeQueues after onUnsubscribe = %v, want 0", got)
	}
}

func TestOnUnsubscribe_CorrectsQueuedEventsForEventsNeverDelivered(t *testing.T) {
	app, _ := newTestApp(defaultConfig)
	// Deliberately offline: onEvent enqueues but never delivers, so the
	// queue still holds the event when it's deleted below.
	app.onSubscribe(subscribeHeaders("alice"))
	app.onEvent("Event 1")
	app.onEvent("Event 2")
	if got := testutil.ToFloat64(app.metrics.queuedEvents); got != 2 {
		t.Fatalf("queuedEvents before unsubscribe = %v, want 2", got)
	}

	app.onUnsubscribe(map[string]string{"destination": "alice/testsource"})

	// Both events are gone without ever being delivered-and-acked; leaving
	// queuedEvents at 2 would permanently overcount a backlog that no
	// longer exists.
	if got := testutil.ToFloat64(app.metrics.queuedEvents); got != 0 {
		t.Errorf("queuedEvents after onUnsubscribe = %v, want 0", got)
	}
}

func TestOnUnsubscribe_IgnoresUnknownDestination(t *testing.T) {
	app, srv := newTestApp(defaultConfig)
	srv.setConnected("alice/testsource", true)
	app.onSubscribe(subscribeHeaders("alice"))

	app.onUnsubscribe(map[string]string{"destination": "someone/else"})

	app.mu.Lock()
	_, hasQueue := app.queues["alice"]
	app.mu.Unlock()
	if !hasQueue {
		t.Error("queues[\"alice\"] was deleted by an unrelated onUnsubscribe")
	}
	if got := testutil.ToFloat64(app.metrics.activeQueues); got != 1 {
		t.Errorf("activeQueues after an unrelated onUnsubscribe = %v, want still 1 (unaffected)", got)
	}
}

func TestOnUnsubscribe_DoesNotAffectConnectedClientsGauge(t *testing.T) {
	app, srv := newTestApp(defaultConfig)
	srv.setConnected("alice/testsource", true)
	app.onSubscribe(subscribeHeaders("alice"))

	// Unsubscribing is not the same as disconnecting: the connection (and
	// this app's per-connection accounting) may still be live even though
	// the client no longer wants this particular queue.
	app.onUnsubscribe(map[string]string{"destination": "alice/testsource"})

	if got := testutil.ToFloat64(app.metrics.connectedClients); got != 1 {
		t.Errorf("connectedClients after onUnsubscribe = %v, want still 1 (unaffected)", got)
	}
}

func TestOnDisconnect_DecrementsGaugeEvenAfterPriorUnsubscribe(t *testing.T) {
	app, srv := newTestApp(defaultConfig)
	srv.setConnected("alice/testsource", true)
	app.onSubscribe(subscribeHeaders("alice"))

	// A client that unsubscribes from its only destination before the
	// connection itself ends must still have connected-clients decremented
	// once that connection finally closes -- internal/stomp's OnDisconnect
	// is expected to still report this destination even though it's no
	// longer live, precisely so this lookup keeps working.
	app.onUnsubscribe(map[string]string{"destination": "alice/testsource"})
	if got := testutil.ToFloat64(app.metrics.connectedClients); got != 1 {
		t.Fatalf("connectedClients after onUnsubscribe = %v, want still 1", got)
	}

	app.onDisconnect(map[string]string{"destination": "alice/testsource"})

	if got := testutil.ToFloat64(app.metrics.connectedClients); got != 0 {
		t.Errorf("connectedClients after onDisconnect (following an earlier onUnsubscribe) = %v, want 0", got)
	}
}

func TestOnUnsubscribe_ThenResubscribeStartsWithAFreshEmptyQueue(t *testing.T) {
	app, srv := newTestApp(defaultConfig)
	srv.setConnected("alice/testsource", true)

	app.onSubscribe(subscribeHeaders("alice"))
	app.onEvent("Event 1")
	app.onUnsubscribe(map[string]string{"destination": "alice/testsource"})

	app.onSubscribe(subscribeHeaders("alice"))
	app.mu.Lock()
	q := app.queues["alice"]
	app.mu.Unlock()
	if n, err := q.Len(); err != nil || n != 0 {
		t.Errorf("Len() on the queue built after resubscribing = (%d, %v), want (0, nil) -- old events must not resurface", n, err)
	}
}

func TestOnAck_AdvancesQueueAndDeliversNext(t *testing.T) {
	app, srv := newTestApp(defaultConfig)
	srv.setConnected("alice/testsource", true)

	app.onSubscribe(subscribeHeaders("alice"))
	app.onEvent("Event 1")
	app.onEvent("Event 2") // queued behind the in-flight delivery of Event 1

	first := srv.snapshotPublishes()
	if len(first) != 1 {
		t.Fatalf("Publish calls after two events = %d, want 1 (second should be queued)", len(first))
	}

	app.onAck(map[string]string{"destination": "alice/testsource", "message-id": first[0].messageID})

	second := srv.snapshotPublishes()
	if len(second) != 2 {
		t.Fatalf("Publish calls after ack = %d, want 2", len(second))
	}
	if second[1].body != "Event 2" {
		t.Errorf("body = %q, want %q", second[1].body, "Event 2")
	}
	if second[1].messageID == second[0].messageID {
		t.Error("second delivery reused the first delivery's message id")
	}
}

func TestOnSubscribe_ResumesExistingQueueAcrossReconnect(t *testing.T) {
	app, srv := newTestApp(defaultConfig)
	headers := subscribeHeaders("bob")

	srv.setConnected("bob/testsource", true)
	app.onSubscribe(headers)
	app.onEvent("Event A") // delivered but never acked

	app.mu.Lock()
	originalQueue := app.queues["bob"]
	app.mu.Unlock()

	// Client goes offline, then reconnects with the same subscription header.
	srv.setConnected("bob/testsource", false)
	srv.setConnected("bob/testsource", true)
	app.onSubscribe(headers)

	app.mu.Lock()
	resumedQueue := app.queues["bob"]
	app.mu.Unlock()
	events := queueEvents(t, resumedQueue)

	if resumedQueue != originalQueue {
		t.Error("resubscribing with the same subscription header created a new queue instead of resuming it")
	}
	if len(events) != 1 || events[0] != "Event A" {
		t.Errorf("events after resume = %v, want [\"Event A\"]", events)
	}

	publishes := srv.snapshotPublishes()
	if len(publishes) != 2 {
		t.Fatalf("Publish calls = %d, want 2 (original delivery + resumed redelivery)", len(publishes))
	}
	if publishes[0].messageID == publishes[1].messageID {
		t.Error("resumed delivery reused the pre-disconnect message id; retry state should reset on resubscribe")
	}
}

func TestOnSubscribe_DeliversToWhateverDestinationClientSubscribedTo(t *testing.T) {
	app, srv := newTestApp(defaultConfig)
	// An arbitrary suffix -- not the old single <event-source-name> segment --
	// but still owned by "alice", satisfying the first-segment-must-match rule.
	const destination = "alice/totally/unrelated/nested/path"
	srv.setConnected(destination, true)

	app.onSubscribe(map[string]string{
		"subscription": "alice",
		"destination":  destination,
		"ack":          "client-individual",
	})
	app.onEvent("Event 1")

	publishes := srv.snapshotPublishes()
	if len(publishes) != 1 {
		t.Fatalf("Publish calls = %d, want 1", len(publishes))
	}
	if publishes[0].destination != destination {
		t.Errorf("destination = %q, want %q (whatever the client subscribed to, not a derived convention)", publishes[0].destination, destination)
	}

	app.onAck(map[string]string{"destination": destination, "message-id": publishes[0].messageID})
	app.onEvent("Event 2")

	second := srv.snapshotPublishes()
	if len(second) != 2 || second[1].body != "Event 2" || second[1].destination != destination {
		t.Fatalf("expected Event 2 delivered to the same arbitrary destination after ack, got %+v", second)
	}
}

func TestOnSubscribe_UpdatesDestinationWhenClientResubscribesElsewhere(t *testing.T) {
	app, srv := newTestApp(defaultConfig)
	const oldDestination = "alice/old-destination"
	const newDestination = "alice/new-destination"
	srv.setConnected(oldDestination, true)

	app.onSubscribe(map[string]string{
		"subscription": "alice",
		"destination":  oldDestination,
		"ack":          "client-individual",
	})

	// Reconnects with the same subscription header but a different destination.
	srv.setConnected(newDestination, true)
	app.onSubscribe(map[string]string{
		"subscription": "alice",
		"destination":  newDestination,
		"ack":          "client-individual",
	})

	app.onEvent("Event 1")
	publishes := srv.snapshotPublishes()
	if len(publishes) != 1 {
		t.Fatalf("Publish calls = %d, want 1", len(publishes))
	}
	if publishes[0].destination != newDestination {
		t.Errorf("destination = %q, want the new destination %q", publishes[0].destination, newDestination)
	}

	app.mu.Lock()
	_, staleMappingRemains := app.clientIDs[oldDestination]
	app.mu.Unlock()
	if staleMappingRemains {
		t.Error("old destination -> client mapping was not cleaned up after resubscribing elsewhere")
	}
}

func TestHasOwningSegment(t *testing.T) {
	cases := []struct {
		destination string
		clientID    string
		want        bool
	}{
		{"alice", "alice", true},         // bare client id, no suffix
		{"alice/foo", "alice", true},     // client id with a suffix
		{"alice/foo/bar", "alice", true}, // client id with a nested suffix
		{"bob/foo", "alice", false},      // owned by a different client entirely
		{"aliceX/foo", "alice", false},   // shares a prefix but is a different segment
		{"alice", "aliceX", false},       // same, other direction
		{"", "alice", false},             // empty destination
		{"/alice/foo", "alice", false},   // leading slash makes the first segment empty
		{"alice/", "alice", true},        // trailing slash: first segment is still "alice"
	}
	for _, c := range cases {
		if got := hasOwningSegment(c.destination, c.clientID); got != c.want {
			t.Errorf("hasOwningSegment(%q, %q) = %v, want %v", c.destination, c.clientID, got, c.want)
		}
	}
}

func TestSubscriptionParams(t *testing.T) {
	cases := []struct {
		destination string
		want        string
	}{
		{"alice", ""},                // bare client id, no params
		{"alice/foo", "foo"},         // simple params
		{"alice/foo/bar", "foo/bar"}, // nested/multi-segment params, kept whole
		{"alice/", ""},               // trailing slash: empty params, not absent
		{"", ""},                     // empty destination
		{"/alice/foo", "alice/foo"},  // leading slash: everything after the first "/"
	}
	for _, c := range cases {
		if got := subscriptionParams(c.destination); got != c.want {
			t.Errorf("subscriptionParams(%q) = %q, want %q", c.destination, got, c.want)
		}
	}
}

func TestOnSubscribe_PassesSubscriptionParamsToQueueFactory(t *testing.T) {
	app, srv := newTestApp(defaultConfig)
	srv.setConnected("alice/topic=weather&qos=1", true)

	var gotClientID, gotParams string
	app.newQueue = func(clientID, params string) clientqueue.Queue {
		gotClientID, gotParams = clientID, params
		return clientqueue.NewMemoryQueue(clientID, params)
	}

	app.onSubscribe(map[string]string{
		"subscription": "alice",
		"destination":  "alice/topic=weather&qos=1",
		"ack":          "client-individual",
	})

	if gotClientID != "alice" {
		t.Errorf("queueFactory clientID = %q, want alice", gotClientID)
	}
	if gotParams != "topic=weather&qos=1" {
		t.Errorf("queueFactory params = %q, want topic=weather&qos=1", gotParams)
	}
}

func TestOnSubscribe_ResubscribeDoesNotRecreateQueueOrReinvokeFactory(t *testing.T) {
	// The factory is only called once per clientID, the first time this
	// process builds an in-process handle -- a resubscribe (even with
	// different params) reuses the existing queue rather than calling the
	// factory again. Documenting this existing, unchanged behavior here
	// since it's now directly relevant to whether new params ever reach an
	// existing queue (they don't, today).
	app, srv := newTestApp(defaultConfig)
	srv.setConnected("alice/first", true)

	calls := 0
	app.newQueue = func(clientID, params string) clientqueue.Queue {
		calls++
		return clientqueue.NewMemoryQueue(clientID, params)
	}

	app.onSubscribe(map[string]string{"subscription": "alice", "destination": "alice/first", "ack": "client-individual"})
	srv.setConnected("alice/second", true)
	app.onSubscribe(map[string]string{"subscription": "alice", "destination": "alice/second", "ack": "client-individual"})

	if calls != 1 {
		t.Errorf("queueFactory called %d times, want 1 (only on first subscribe)", calls)
	}
}

func TestOnSubscribe_CallsQueueAddedOnceForNewQueue(t *testing.T) {
	app, srv := newTestApp(defaultConfig)
	srv.setConnected("alice/first", true)
	srv.setConnected("alice/second", true)
	source := app.source.(*fakeEventSource)

	app.onSubscribe(map[string]string{"subscription": "alice", "destination": "alice/first", "ack": "client-individual"})
	app.onSubscribe(map[string]string{"subscription": "alice", "destination": "alice/second", "ack": "client-individual"})

	added := source.snapshotAdded()
	if len(added) != 1 {
		t.Fatalf("QueueAdded called %d times, want 1 (only on the first-ever subscribe for this clientID)", len(added))
	}
	app.mu.Lock()
	wantQueue := app.queues["alice"]
	app.mu.Unlock()
	if added[0] != wantQueue {
		t.Error("QueueAdded was called with a queue other than the one now stored for this client")
	}
}

func TestRehydrateQueues_PopulatesQueuesAndCallsQueueAdded(t *testing.T) {
	app, _ := newTestApp(defaultConfig)
	source := app.source.(*fakeEventSource)

	app.RehydrateQueues([]clientqueue.PersistedClient{
		{ClientID: "alice", Params: "osdf/vdc/public/pelican_protocol"},
		{ClientID: "bob", Params: "osdf/vdc/public/other"},
	})

	app.mu.Lock()
	aliceQueue, aliceOK := app.queues["alice"]
	bobQueue, bobOK := app.queues["bob"]
	app.mu.Unlock()
	if !aliceOK || !bobOK {
		t.Fatalf("queues after RehydrateQueues: alice=%v bob=%v, want both present", aliceOK, bobOK)
	}
	if aliceQueue.Params() != "osdf/vdc/public/pelican_protocol" {
		t.Errorf("alice's queue Params() = %q, want osdf/vdc/public/pelican_protocol", aliceQueue.Params())
	}

	added := source.snapshotAdded()
	if len(added) != 2 {
		t.Fatalf("QueueAdded called %d times, want 2", len(added))
	}
	if added[0] != aliceQueue || added[1] != bobQueue {
		t.Errorf("QueueAdded calls = %v, want [aliceQueue, bobQueue] in order", added)
	}
}

func TestRehydrateQueues_SkipsClientIDsAlreadyPresent(t *testing.T) {
	app, srv := newTestApp(defaultConfig)
	srv.setConnected("alice/testsource", true)
	source := app.source.(*fakeEventSource)

	app.onSubscribe(subscribeHeaders("alice")) // a real, already-connected subscribe first
	app.mu.Lock()
	originalQueue := app.queues["alice"]
	app.mu.Unlock()

	app.RehydrateQueues([]clientqueue.PersistedClient{{ClientID: "alice", Params: "should-be-ignored"}})

	app.mu.Lock()
	currentQueue := app.queues["alice"]
	app.mu.Unlock()
	if currentQueue != originalQueue {
		t.Error("RehydrateQueues replaced an already-existing queue instead of leaving it alone")
	}
	if got := source.snapshotAdded(); len(got) != 1 {
		t.Errorf("QueueAdded called %d times, want 1 (only from the real subscribe, not the redundant rehydrate)", len(got))
	}
}

func TestRehydrateQueues_DoesNotMarkClientAsConnected(t *testing.T) {
	app, _ := newTestApp(defaultConfig)

	app.RehydrateQueues([]clientqueue.PersistedClient{{ClientID: "alice", Params: "osdf/vdc/public/pelican_protocol"}})

	if got := testutil.ToFloat64(app.metrics.connectedClients); got != 0 {
		t.Errorf("connectedClients after RehydrateQueues = %v, want 0 -- a rehydrated queue has no live connection yet", got)
	}
	app.mu.Lock()
	_, hasDestination := app.destinations["alice"]
	app.mu.Unlock()
	if hasDestination {
		t.Error("destinations[\"alice\"] was set by RehydrateQueues, want it unset until a real SUBSCRIBE")
	}
}

func TestRehydrateQueues_BacklogIsDeliveredOnceTheRealClientSubscribes(t *testing.T) {
	app, srv := newTestApp(defaultConfig)

	app.RehydrateQueues([]clientqueue.PersistedClient{{ClientID: "alice", Params: "testsource"}})
	app.onEvent("Event 1") // enqueued into the rehydrated queue, but not deliverable yet

	if got := srv.snapshotPublishes(); len(got) != 0 {
		t.Fatalf("Publish calls before alice ever subscribes = %v, want none", got)
	}

	srv.setConnected("alice/testsource", true)
	app.onSubscribe(subscribeHeaders("alice")) // the real client finally reconnects

	publishes := srv.snapshotPublishes()
	if len(publishes) != 1 {
		t.Fatalf("Publish calls after the real subscribe = %d, want 1 (the backlog is flushed): %+v", len(publishes), publishes)
	}
	if publishes[0].body != "Event 1" {
		t.Errorf("delivered body = %q, want Event 1", publishes[0].body)
	}
}

func TestOnUnsubscribe_CallsQueueRemoved(t *testing.T) {
	app, srv := newTestApp(defaultConfig)
	srv.setConnected("alice/testsource", true)
	source := app.source.(*fakeEventSource)

	app.onSubscribe(subscribeHeaders("alice"))
	app.mu.Lock()
	aliceQueue := app.queues["alice"]
	app.mu.Unlock()

	app.onUnsubscribe(map[string]string{"destination": "alice/testsource"})

	removed := source.snapshotRemoved()
	if len(removed) != 1 || removed[0] != aliceQueue {
		t.Errorf("QueueRemoved calls = %v, want exactly one call with alice's queue", removed)
	}
}

func TestOnUnsubscribe_UnknownDestinationDoesNotCallQueueRemoved(t *testing.T) {
	app, srv := newTestApp(defaultConfig)
	srv.setConnected("alice/testsource", true)
	source := app.source.(*fakeEventSource)
	app.onSubscribe(subscribeHeaders("alice"))

	app.onUnsubscribe(map[string]string{"destination": "someone/else"})

	if got := source.snapshotRemoved(); len(got) != 0 {
		t.Errorf("QueueRemoved called for an unrelated destination: %v", got)
	}
}

func TestOnSubscribe_RejectsDestinationNotOwnedByClient(t *testing.T) {
	app, srv := newTestApp(defaultConfig)
	srv.setConnected("bob/something", true)

	app.onSubscribe(map[string]string{
		"subscription": "alice",
		"destination":  "bob/something",
		"ack":          "client-individual",
	})
	app.onEvent("Event 1")

	if got := srv.snapshotPublishes(); len(got) != 0 {
		t.Fatalf("Publish calls = %v, want none (subscribe should have been rejected)", got)
	}
	app.mu.Lock()
	_, hasQueue := app.queues["alice"]
	app.mu.Unlock()
	if hasQueue {
		t.Error("a queue was created for alice despite the destination being rejected")
	}
}

func TestOnSubscribe_RejectsDestinationWithMatchingPrefixButDifferentSegment(t *testing.T) {
	app, srv := newTestApp(defaultConfig)
	srv.setConnected("aliceX/foo", true)

	app.onSubscribe(map[string]string{
		"subscription": "alice",
		"destination":  "aliceX/foo",
		"ack":          "client-individual",
	})
	app.onEvent("Event 1")

	if got := srv.snapshotPublishes(); len(got) != 0 {
		t.Fatalf(`Publish calls = %v, want none ("aliceX/foo"'s first segment is not exactly "alice")`, got)
	}
}

func TestOnEvent_DisconnectsConnectedClientOnOverflow(t *testing.T) {
	cfg := defaultConfig
	cfg.MaxQueueLen = 3
	app, srv := newTestApp(cfg)
	srv.setConnected("alice/testsource", true)

	app.onSubscribe(subscribeHeaders("alice"))
	for _, e := range []string{"E1", "E2", "E3", "E4"} {
		app.onEvent(e) // never acked, so the queue keeps growing behind E1
	}

	if got := srv.snapshotDisconnects(); len(got) != 1 || got[0] != "alice/testsource" {
		t.Fatalf("disconnects = %v, want exactly one disconnect of alice/testsource", got)
	}
	if got := srv.snapshotSentErrors(); len(got) != 1 || got[0].destination != "alice/testsource" || got[0].message == "" {
		t.Fatalf("sent errors = %+v, want exactly one non-empty ERROR to alice/testsource before disconnecting", got)
	}

	app.mu.Lock()
	q := app.queues["alice"]
	inFlight := app.inFlights["alice"]
	app.mu.Unlock()
	events := queueEvents(t, q)

	want := []string{"E2", "E3", "E4"}
	if len(events) != len(want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
	for i := range want {
		if events[i] != want[i] {
			t.Errorf("events = %v, want %v", events, want)
			break
		}
	}
	if inFlight != nil {
		t.Error("inFlight should be cleared after a disconnect-on-overflow")
	}
}

func TestOnEvent_TrimsWithoutDisconnectingWhenOffline(t *testing.T) {
	cfg := defaultConfig
	cfg.MaxQueueLen = 3
	app, srv := newTestApp(cfg)

	// The client subscribed once (so the queue exists) but is now offline;
	// events keep arriving via the broadcast fan-out regardless.
	srv.setConnected("alice/testsource", true)
	app.onSubscribe(subscribeHeaders("alice"))
	srv.setConnected("alice/testsource", false)

	for _, e := range []string{"E1", "E2", "E3", "E4"} {
		app.onEvent(e)
	}

	if got := srv.snapshotDisconnects(); len(got) != 0 {
		t.Fatalf("disconnects = %v, want none while offline", got)
	}
	if got := srv.snapshotPublishes(); len(got) != 0 {
		t.Fatalf("publishes = %v, want none while offline", got)
	}
	if got := srv.snapshotSentErrors(); len(got) != 0 {
		t.Fatalf("sent errors = %v, want none while offline", got)
	}

	app.mu.Lock()
	q := app.queues["alice"]
	app.mu.Unlock()
	events := queueEvents(t, q)

	want := []string{"E2", "E3", "E4"}
	if len(events) != len(want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
	for i := range want {
		if events[i] != want[i] {
			t.Errorf("events = %v, want %v", events, want)
			break
		}
	}
}

func TestOnRetryTimeout_BacksOffThenGivesUpAndDisconnects(t *testing.T) {
	cfg := Config{
		InitialRetryDelay:  time.Millisecond,
		RetryBackoffFactor: 1.2,
		MaxRetries:         3,
		MaxQueueLen:        defaultConfig.MaxQueueLen,
	}
	app, srv := newTestApp(cfg)
	srv.setConnected("alice/testsource", true)

	app.onSubscribe(subscribeHeaders("alice"))
	app.onEvent("Event 1") // delivered, then never acked

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(srv.snapshotDisconnects()) > 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	disconnects := srv.snapshotDisconnects()
	if len(disconnects) != 1 || disconnects[0] != "alice/testsource" {
		t.Fatalf("disconnects = %v, want exactly one disconnect of alice/testsource", disconnects)
	}
	if got := srv.snapshotSentErrors(); len(got) != 1 || got[0].destination != "alice/testsource" || got[0].message == "" {
		t.Fatalf("sent errors = %+v, want exactly one non-empty ERROR to alice/testsource before disconnecting", got)
	}

	publishes := srv.snapshotPublishes()
	if len(publishes) != 1+cfg.MaxRetries {
		t.Fatalf("publishes = %d, want %d (1 initial send + %d retries)", len(publishes), 1+cfg.MaxRetries, cfg.MaxRetries)
	}
	for _, p := range publishes {
		if p.body != "Event 1" {
			t.Errorf("retried body = %q, want %q", p.body, "Event 1")
		}
	}

	app.mu.Lock()
	inFlight := app.inFlights["alice"]
	app.mu.Unlock()
	if inFlight != nil {
		t.Error("inFlight should be cleared after retry exhaustion")
	}
}

func TestOnAck_IgnoresMismatchedOrUnknownAck(t *testing.T) {
	app, srv := newTestApp(defaultConfig)
	srv.setConnected("alice/testsource", true)

	app.onSubscribe(subscribeHeaders("alice"))
	app.onEvent("Event 1")

	app.mu.Lock()
	originalInFlight := app.inFlights["alice"]
	app.mu.Unlock()

	// Ack for a message id that doesn't match the current in-flight delivery.
	app.onAck(map[string]string{"destination": "alice/testsource", "message-id": "not-the-real-id"})
	// Ack for a destination this app doesn't own.
	app.onAck(map[string]string{"destination": "somewhere/else", "message-id": "whatever"})

	app.mu.Lock()
	q := app.queues["alice"]
	inFlight := app.inFlights["alice"]
	app.mu.Unlock()
	events := queueEvents(t, q)

	if inFlight != originalInFlight {
		t.Error("a mismatched ack should not clear the in-flight delivery")
	}
	if len(events) != 1 || events[0] != "Event 1" {
		t.Errorf("events = %v, want [\"Event 1\"] (unchanged)", events)
	}
	if len(srv.snapshotPublishes()) != 1 {
		t.Error("a mismatched ack should not trigger another delivery")
	}
}

func TestApp_ResumesSQLiteBackedQueueAfterRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "queue.sqlite3")

	db1, err := clientqueue.OpenDB(dbPath)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	srv1 := newFakeStompServer()
	srv1.setConnected("carol/testsource", true)
	app1 := NewWithConfig(srv1, discardLogger(), newFakeEventSource(), defaultConfig, clientqueue.SQLiteFactory(db1, discardLogger()))
	app1.onSubscribe(subscribeHeaders("carol"))
	app1.onEvent("Event 1") // delivered but never acked
	if got := srv1.snapshotPublishes(); len(got) != 1 {
		t.Fatalf("Publish calls before restart = %d, want 1: %+v", len(got), got)
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("close db1: %v", err)
	}

	// Simulate a full process restart: a brand-new app and server, backed by
	// the same on-disk database file.
	db2, err := clientqueue.OpenDB(dbPath)
	if err != nil {
		t.Fatalf("reopen OpenDB: %v", err)
	}
	t.Cleanup(func() { db2.Close() })
	srv2 := newFakeStompServer()
	srv2.setConnected("carol/testsource", true)
	app2 := NewWithConfig(srv2, discardLogger(), newFakeEventSource(), defaultConfig, clientqueue.SQLiteFactory(db2, discardLogger()))
	app2.onSubscribe(subscribeHeaders("carol"))

	publishes := srv2.snapshotPublishes()
	if len(publishes) != 1 {
		t.Fatalf("Publish calls after restart = %d, want 1 (resumed delivery of the pre-restart event)", len(publishes))
	}
	if publishes[0].body != "Event 1" {
		t.Errorf("resumed body = %q, want %q", publishes[0].body, "Event 1")
	}

	// The resumed backlog (1 event, persisted by app1, never seen as
	// "enqueued" by app2's own metrics) must be reflected in app2's gauge
	// immediately, not just once it's delivered.
	if got := testutil.ToFloat64(app2.metrics.queuedEvents); got != 1 {
		t.Fatalf("queuedEvents after resuming a 1-event backlog = %v, want 1", got)
	}

	app2.onAck(map[string]string{"destination": "carol/testsource", "message-id": publishes[0].messageID})
	if got := testutil.ToFloat64(app2.metrics.queuedEvents); got != 0 {
		t.Errorf("queuedEvents after acking the resumed event = %v, want 0 (must not go negative)", got)
	}
}

func TestOnSubscribe_ReconnectWithoutRestartDoesNotDoubleCountGauge(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "queue.sqlite3")
	db, err := clientqueue.OpenDB(dbPath)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	app, srv := newTestApp(defaultConfig)
	app.newQueue = clientqueue.SQLiteFactory(db, discardLogger())
	srv.setConnected("dave/testsource", true)

	app.onSubscribe(subscribeHeaders("dave"))
	app.onEvent("Event 1") // delivered but never acked

	if got := testutil.ToFloat64(app.metrics.queuedEvents); got != 1 {
		t.Fatalf("queuedEvents after first subscribe+event = %v, want 1", got)
	}

	// Reconnects without a process restart: existed is true this time, so
	// the gauge must not be re-seeded from the durable backlog -- that
	// backlog is the SAME event already counted once, not a new one.
	app.onSubscribe(subscribeHeaders("dave"))

	if got := testutil.ToFloat64(app.metrics.queuedEvents); got != 1 {
		t.Errorf("queuedEvents after in-process reconnect = %v, want still 1 (must not double-count)", got)
	}
}
