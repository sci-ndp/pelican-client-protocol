package main

import (
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
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
	mu              sync.Mutex
	onSubscribeFns  []func(map[string]string)
	onAckFns        []func(map[string]string)
	onDisconnectFns []func(map[string]string)
	connected       map[string]bool
	publishes       []publishCall
	sentErrors      []sentError
	disconnects     []string
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

// fakeEventSource is an EventSource test double whose channel a test can
// feed directly, though most tests below call messageQueueApp's methods
// directly instead, since that avoids any timing dependency on the app's
// background run() goroutine.
type fakeEventSource struct {
	ch chan string
}

func newFakeEventSource() *fakeEventSource {
	return &fakeEventSource{ch: make(chan string, 1)}
}

func (f *fakeEventSource) Events() <-chan string { return f.ch }

func newTestApp(cfg messageQueueConfig) (*messageQueueApp, *fakeStompServer) {
	srv := newFakeStompServer()
	source := newFakeEventSource()
	app := newMessageQueueAppWithConfig(srv, discardLogger(), source, cfg, newMemoryClientQueue)
	return app, srv
}

// queueEvents reads out a memoryClientQueue's contents for assertions. All
// tests in this file use newMemoryClientQueue as their queueFactory, so this
// type assertion is safe.
func queueEvents(t *testing.T, q clientQueue) []string {
	t.Helper()
	mq, ok := q.(*memoryClientQueue)
	if !ok {
		t.Fatalf("expected a *memoryClientQueue, got %T", q)
	}
	return append([]string{}, mq.events...)
}

func subscribeHeaders(clientID string) map[string]string {
	return map[string]string{
		"subscription": clientID,
		"destination":  clientID + "/testsource",
		"ack":          "client-individual",
	}
}

func TestOnEvent_DropsWhenNoQueuesRegistered(t *testing.T) {
	app, srv := newTestApp(defaultMessageQueueConfig)

	app.onEvent("Event 1")

	if got := srv.snapshotPublishes(); len(got) != 0 {
		t.Fatalf("Publish calls = %v, want none", got)
	}
}

func TestOnSubscribe_CreatesQueueAndDeliversOnceConnected(t *testing.T) {
	app, srv := newTestApp(defaultMessageQueueConfig)
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
	app, srv := newTestApp(defaultMessageQueueConfig)
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
	app, srv := newTestApp(defaultMessageQueueConfig)
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
	app, srv := newTestApp(defaultMessageQueueConfig)
	srv.setConnected("alice/testsource", true)
	app.onSubscribe(subscribeHeaders("alice"))

	app.onDisconnect(map[string]string{"destination": "someone/else"})

	if got := testutil.ToFloat64(app.metrics.connectedClients); got != 1 {
		t.Errorf("connectedClients after an unrelated onDisconnect = %v, want still 1 (unaffected)", got)
	}
}

func TestOnDisconnect_ReconnectAfterDisconnectIncrementsAgain(t *testing.T) {
	app, srv := newTestApp(defaultMessageQueueConfig)
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

func TestOnAck_AdvancesQueueAndDeliversNext(t *testing.T) {
	app, srv := newTestApp(defaultMessageQueueConfig)
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
	app, srv := newTestApp(defaultMessageQueueConfig)
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
	app, srv := newTestApp(defaultMessageQueueConfig)
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
	app, srv := newTestApp(defaultMessageQueueConfig)
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

func TestOnSubscribe_RejectsDestinationNotOwnedByClient(t *testing.T) {
	app, srv := newTestApp(defaultMessageQueueConfig)
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
	app, srv := newTestApp(defaultMessageQueueConfig)
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
	cfg := defaultMessageQueueConfig
	cfg.maxQueueLen = 3
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
	cfg := defaultMessageQueueConfig
	cfg.maxQueueLen = 3
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
	cfg := messageQueueConfig{
		initialRetryDelay:  time.Millisecond,
		retryBackoffFactor: 1.2,
		maxRetries:         3,
		maxQueueLen:        defaultMessageQueueConfig.maxQueueLen,
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
	if len(publishes) != 1+cfg.maxRetries {
		t.Fatalf("publishes = %d, want %d (1 initial send + %d retries)", len(publishes), 1+cfg.maxRetries, cfg.maxRetries)
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
	app, srv := newTestApp(defaultMessageQueueConfig)
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

func TestMessageQueueApp_ResumesSQLiteBackedQueueAfterRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "queue.sqlite3")

	db1, err := openQueueDB(dbPath)
	if err != nil {
		t.Fatalf("openQueueDB: %v", err)
	}
	srv1 := newFakeStompServer()
	srv1.setConnected("carol/testsource", true)
	app1 := newMessageQueueAppWithConfig(srv1, discardLogger(), newFakeEventSource(), defaultMessageQueueConfig, sqliteQueueFactory(db1))
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
	db2, err := openQueueDB(dbPath)
	if err != nil {
		t.Fatalf("reopen openQueueDB: %v", err)
	}
	t.Cleanup(func() { db2.Close() })
	srv2 := newFakeStompServer()
	srv2.setConnected("carol/testsource", true)
	app2 := newMessageQueueAppWithConfig(srv2, discardLogger(), newFakeEventSource(), defaultMessageQueueConfig, sqliteQueueFactory(db2))
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
	db, err := openQueueDB(dbPath)
	if err != nil {
		t.Fatalf("openQueueDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	app, srv := newTestApp(defaultMessageQueueConfig)
	app.newQueue = sqliteQueueFactory(db)
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
