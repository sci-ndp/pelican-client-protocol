package main

import (
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMetrics_SubscribeAcceptedAndRejected(t *testing.T) {
	app, srv := newTestApp(defaultMessageQueueConfig)

	app.onSubscribe(subscribeHeaders("alice"))                                                                        // accepted
	app.onSubscribe(map[string]string{"subscription": "", "destination": "", "ack": "client-individual"})             // missing headers
	app.onSubscribe(map[string]string{"subscription": "alice", "destination": "bob/foo", "ack": "client-individual"}) // wrong owner

	if got := testutil.ToFloat64(app.metrics.subscriptions.WithLabelValues("accepted")); got != 1 {
		t.Errorf("subscriptions{outcome=accepted} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(app.metrics.subscriptions.WithLabelValues("missing_headers")); got != 1 {
		t.Errorf("subscriptions{outcome=missing_headers} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(app.metrics.subscriptions.WithLabelValues("wrong_owner")); got != 1 {
		t.Errorf("subscriptions{outcome=wrong_owner} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(app.metrics.activeQueues); got != 1 {
		t.Errorf("activeQueues = %v, want 1 (only the accepted subscribe should create a queue)", got)
	}

	_ = srv // unused directly; onSubscribe is driven straight on the app
}

func TestMetrics_QueueLifecycleCountersAndGauges(t *testing.T) {
	app, srv := newTestApp(defaultMessageQueueConfig)
	srv.setConnected("alice/testsource", true)

	app.onSubscribe(subscribeHeaders("alice"))
	app.onEvent("Event 1")
	app.onEvent("Event 2") // queued behind the in-flight delivery of Event 1

	if got := testutil.ToFloat64(app.metrics.eventsEnqueued); got != 2 {
		t.Errorf("eventsEnqueued = %v, want 2", got)
	}
	if got := testutil.ToFloat64(app.metrics.queuedEvents); got != 2 {
		t.Errorf("queuedEvents = %v, want 2 (nothing acked yet)", got)
	}
	if got := testutil.ToFloat64(app.metrics.messagesPublished); got != 1 {
		t.Errorf("messagesPublished = %v, want 1 (only Event 1 delivered so far)", got)
	}

	publishes := srv.snapshotPublishes()
	if len(publishes) != 1 {
		t.Fatalf("Publish calls = %d, want 1", len(publishes))
	}
	time.Sleep(2 * time.Millisecond) // ensure a nonzero ack latency to observe
	app.onAck(map[string]string{"destination": "alice/testsource", "message-id": publishes[0].messageID})

	if got := testutil.ToFloat64(app.metrics.acks); got != 1 {
		t.Errorf("acks = %v, want 1", got)
	}
	if got := testutil.ToFloat64(app.metrics.queuedEvents); got != 1 {
		t.Errorf("queuedEvents = %v, want 1 (one delivered event, one still queued)", got)
	}
	if got := testutil.CollectAndCount(app.metrics.ackLatency); got != 1 {
		t.Errorf("ackLatency observation count = %d, want 1", got)
	}
	if got := testutil.ToFloat64(app.metrics.messagesPublished); got != 2 {
		t.Errorf("messagesPublished = %v, want 2 (Event 2 should have been delivered after the ack)", got)
	}
}

func TestMetrics_EventDroppedWhenNoQueuesRegistered(t *testing.T) {
	app, _ := newTestApp(defaultMessageQueueConfig)

	app.onEvent("Event 1")

	if got := testutil.ToFloat64(app.metrics.eventsDropped); got != 1 {
		t.Errorf("eventsDropped = %v, want 1", got)
	}
	if got := testutil.ToFloat64(app.metrics.eventsEnqueued); got != 0 {
		t.Errorf("eventsEnqueued = %v, want 0", got)
	}
}

func TestMetrics_QueueOverflowWhileConnectedRecordsOverflowAndDisconnect(t *testing.T) {
	cfg := defaultMessageQueueConfig
	cfg.maxQueueLen = 3
	app, srv := newTestApp(cfg)
	srv.setConnected("alice/testsource", true)

	app.onSubscribe(subscribeHeaders("alice"))
	for _, e := range []string{"E1", "E2", "E3", "E4"} {
		app.onEvent(e)
	}

	if got := testutil.ToFloat64(app.metrics.queueOverflows.WithLabelValues("true")); got != 1 {
		t.Errorf(`queueOverflows{client_connected="true"} = %v, want 1`, got)
	}
	if got := testutil.ToFloat64(app.metrics.clientDisconnects.WithLabelValues("queue_overflow")); got != 1 {
		t.Errorf(`clientDisconnects{reason="queue_overflow"} = %v, want 1`, got)
	}
}

func TestMetrics_QueueOverflowWhileOfflineDoesNotDisconnect(t *testing.T) {
	cfg := defaultMessageQueueConfig
	cfg.maxQueueLen = 3
	app, srv := newTestApp(cfg)

	srv.setConnected("alice/testsource", true)
	app.onSubscribe(subscribeHeaders("alice"))
	srv.setConnected("alice/testsource", false)

	for _, e := range []string{"E1", "E2", "E3", "E4"} {
		app.onEvent(e)
	}

	if got := testutil.ToFloat64(app.metrics.queueOverflows.WithLabelValues("false")); got != 1 {
		t.Errorf(`queueOverflows{client_connected="false"} = %v, want 1`, got)
	}
	if got := testutil.CollectAndCount(app.metrics.clientDisconnects); got != 0 {
		t.Errorf("clientDisconnects series count = %d, want 0 (offline overflow never disconnects)", got)
	}
}

func TestMetrics_RetryExhaustionRecordsRetriesAndDisconnect(t *testing.T) {
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
		if testutil.ToFloat64(app.metrics.clientDisconnects.WithLabelValues("retry_exhausted")) > 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	if got := testutil.ToFloat64(app.metrics.clientDisconnects.WithLabelValues("retry_exhausted")); got != 1 {
		t.Errorf(`clientDisconnects{reason="retry_exhausted"} = %v, want 1`, got)
	}
	if got := testutil.ToFloat64(app.metrics.retries); got != float64(cfg.maxRetries) {
		t.Errorf("retries = %v, want %d", got, cfg.maxRetries)
	}
	if got := testutil.ToFloat64(app.metrics.messagesPublished); got != float64(1+cfg.maxRetries) {
		t.Errorf("messagesPublished = %v, want %d (1 initial send + %d retries)", got, 1+cfg.maxRetries, cfg.maxRetries)
	}
}

// brokenClientQueue is a clientQueue whose every operation fails, used to
// verify queue-operation failures are all reflected in queueErrorOccurred.
type brokenClientQueue struct{}

func (brokenClientQueue) Enqueue(string, int) (bool, error) { return false, errors.New("boom") }
func (brokenClientQueue) PeekFront() (string, bool, error)  { return "", false, errors.New("boom") }
func (brokenClientQueue) PopFront() error                   { return errors.New("boom") }
func (brokenClientQueue) Len() (int, error)                 { return 0, errors.New("boom") }
func (brokenClientQueue) Delete() error                     { return errors.New("boom") }
func (brokenClientQueue) Params() string                    { return "" }

func TestMetrics_QueueErrorsRecordedByOperation(t *testing.T) {
	app, srv := newTestApp(defaultMessageQueueConfig)
	app.newQueue = func(string, string) clientQueue { return brokenClientQueue{} }
	srv.setConnected("alice/testsource", true)

	app.onSubscribe(subscribeHeaders("alice")) // Len() fails during onSubscribe's resume check
	if got := testutil.ToFloat64(app.metrics.queueErrors.WithLabelValues("len")); got != 1 {
		t.Errorf(`queueErrors{operation="len"} = %v, want 1`, got)
	}

	app.onEvent("Event 1") // Enqueue() fails
	if got := testutil.ToFloat64(app.metrics.queueErrors.WithLabelValues("enqueue")); got != 1 {
		t.Errorf(`queueErrors{operation="enqueue"} = %v, want 1`, got)
	}
}

func TestMetrics_HandlerServesPrometheusExpositionFormat(t *testing.T) {
	app, srv := newTestApp(defaultMessageQueueConfig)
	srv.setConnected("alice/testsource", true)
	app.onSubscribe(subscribeHeaders("alice"))
	app.onEvent("Event 1")

	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/metrics", nil)
	app.MetricsHandler().ServeHTTP(rr, req)

	if rr.Code != 200 {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body, err := io.ReadAll(rr.Result().Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	for _, want := range []string{
		"messagequeue_subscriptions_total",
		"messagequeue_active_queues",
		"messagequeue_queued_events",
		"messagequeue_events_enqueued_total",
		"messagequeue_messages_published_total",
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("metrics output missing %q", want)
		}
	}
}
