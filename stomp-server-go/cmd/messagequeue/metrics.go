package main

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// appMetrics is the messagequeue app's Prometheus instrumentation. This file
// is the only place in the package that imports the prometheus client
// library: messageQueueApp's own logic just calls these small,
// semantically-named methods where something happens (a client subscribed,
// an event was enqueued, ...) without knowing or caring that Prometheus is
// what's counting it.
//
// Each appMetrics owns its own registry rather than registering into the
// global default one, so constructing more than one in the same process --
// e.g. one per test -- never collides on duplicate metric registration.
type appMetrics struct {
	registry *prometheus.Registry

	subscriptions     *prometheus.CounterVec
	connectedClients  prometheus.Gauge
	activeQueues      prometheus.Gauge
	queuedEvents      prometheus.Gauge
	eventsEnqueued    prometheus.Counter
	eventsDropped     prometheus.Counter
	queueOverflows    *prometheus.CounterVec
	messagesPublished prometheus.Counter
	retries           prometheus.Counter
	acks              prometheus.Counter
	ackLatency        prometheus.Histogram
	clientDisconnects *prometheus.CounterVec
	queueErrors       *prometheus.CounterVec
}

func newAppMetrics() *appMetrics {
	m := &appMetrics{
		registry: prometheus.NewRegistry(),
		subscriptions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "messagequeue_subscriptions_total",
			Help: "SUBSCRIBE frames handled, by outcome (accepted, or the reason it was rejected).",
		}, []string{"outcome"}),
		connectedClients: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "messagequeue_connected_clients",
			Help: "Number of clients with a currently-open connection.",
		}),
		activeQueues: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "messagequeue_active_queues",
			Help: "Number of client queues currently tracked by this process.",
		}),
		queuedEvents: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "messagequeue_queued_events",
			Help: "Total events currently pending delivery across all client queues.",
		}),
		eventsEnqueued: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "messagequeue_events_enqueued_total",
			Help: "Events successfully appended to a client queue (once per client per fanned-out event).",
		}),
		eventsDropped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "messagequeue_events_dropped_total",
			Help: "Events dropped entirely because no client queues existed yet.",
		}),
		queueOverflows: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "messagequeue_queue_overflows_total",
			Help: "Times a client queue exceeded its capacity, by whether the client was connected at the time.",
		}, []string{"client_connected"}),
		messagesPublished: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "messagequeue_messages_published_total",
			Help: "MESSAGE frames sent to clients, including retries.",
		}),
		retries: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "messagequeue_retries_total",
			Help: "Resend attempts made after a missed ACK.",
		}),
		acks: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "messagequeue_acks_total",
			Help: "ACKs that advanced a client queue.",
		}),
		ackLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "messagequeue_ack_latency_seconds",
			Help:    "Time from a message's first send to it being acked, including any retries.",
			Buckets: prometheus.DefBuckets,
		}),
		clientDisconnects: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "messagequeue_client_disconnects_total",
			Help: "Clients forcibly disconnected for misbehaving, by reason.",
		}, []string{"reason"}),
		queueErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "messagequeue_queue_errors_total",
			Help: "clientQueue operation failures, by operation.",
		}, []string{"operation"}),
	}

	m.registry.MustRegister(
		m.subscriptions,
		m.connectedClients,
		m.activeQueues,
		m.queuedEvents,
		m.eventsEnqueued,
		m.eventsDropped,
		m.queueOverflows,
		m.messagesPublished,
		m.retries,
		m.acks,
		m.ackLatency,
		m.clientDisconnects,
		m.queueErrors,
	)
	return m
}

// Handler serves this instance's metrics in the Prometheus text exposition
// format, suitable for mounting at e.g. "/metrics".
func (m *appMetrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *appMetrics) subscribeAccepted() { m.subscriptions.WithLabelValues("accepted").Inc() }

func (m *appMetrics) subscribeRejected(reason string) {
	m.subscriptions.WithLabelValues(reason).Inc()
}

// sessionStarted records a client successfully subscribing -- i.e. a
// currently-open connection this app now considers active.
func (m *appMetrics) sessionStarted() { m.connectedClients.Inc() }

// sessionEnded records that connection ending, by any means (client
// DISCONNECT, network error, or a server-initiated force-close).
func (m *appMetrics) sessionEnded() { m.connectedClients.Dec() }

func (m *appMetrics) queueCreated() { m.activeQueues.Inc() }

// queueResumed seeds queuedEvents with n events a durable queue already had
// on disk before this process ever built an in-process handle for that
// client. Without this, a backlog resumed from a prior process run is
// invisible to the gauge until it's delivered -- at which point the
// resulting eventDelivered() decrement has no matching eventEnqueued()
// increment (from *this* process) to balance against, driving the gauge
// negative.
func (m *appMetrics) queueResumed(n int) {
	m.queuedEvents.Add(float64(n))
}

// eventEnqueued records one event successfully appended to a client queue.
func (m *appMetrics) eventEnqueued() {
	m.eventsEnqueued.Inc()
	m.queuedEvents.Inc()
}

// eventDropped records an event that never reached any queue because none
// existed yet.
func (m *appMetrics) eventDropped() { m.eventsDropped.Inc() }

// eventTrimmed records one event dropped from the front of a queue to
// enforce its cap.
func (m *appMetrics) eventTrimmed() { m.queuedEvents.Dec() }

// eventDelivered records one event removed from the front of a queue
// because it was acked.
func (m *appMetrics) eventDelivered() { m.queuedEvents.Dec() }

func (m *appMetrics) queueOverflowed(clientConnected bool) {
	m.queueOverflows.WithLabelValues(strconv.FormatBool(clientConnected)).Inc()
}

func (m *appMetrics) messagePublished() { m.messagesPublished.Inc() }

func (m *appMetrics) retryAttempted() { m.retries.Inc() }

func (m *appMetrics) acked(latency time.Duration) {
	m.acks.Inc()
	m.ackLatency.Observe(latency.Seconds())
}

func (m *appMetrics) clientDisconnected(reason string) {
	m.clientDisconnects.WithLabelValues(reason).Inc()
}

func (m *appMetrics) queueErrorOccurred(operation string) {
	m.queueErrors.WithLabelValues(operation).Inc()
}
