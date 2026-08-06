package main

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// liveQueueState is the read-only view into messageQueueApp's own
// authoritative state that appMetrics needs so connectedClients,
// activeQueues, and queuedEvents can be computed directly at scrape time
// (via GaugeFunc) instead of kept in sync by hand at every call site that
// touches queues/clientIDs. That hand-maintained approach is exactly what
// produced this package's past gauge-drift bugs (queued_events oscillating
// negative across restarts, then again after UNSUBSCRIBE deleted a queue
// without correcting it) -- computing from the source of truth on every
// scrape makes that whole class of bug structurally impossible. messageQueueApp
// implements this interface; see its activeQueueCount/connectedClientCount/
// queuedEventCount methods.
type liveQueueState interface {
	activeQueueCount() int
	connectedClientCount() int
	queuedEventCount() int
}

// appMetrics is the messagequeue app's Prometheus instrumentation. This file
// is the only place in the package that imports the prometheus client
// library: messageQueueApp's own logic just calls these small,
// semantically-named methods where something happens (a client subscribed,
// an event was enqueued, ...) without knowing or caring that Prometheus is
// what's counting it -- except for the three GaugeFuncs above, which instead
// read messageQueueApp's own state directly through liveQueueState.
//
// Each appMetrics owns its own registry rather than registering into the
// global default one, so constructing more than one in the same process --
// e.g. one per test -- never collides on duplicate metric registration.
type appMetrics struct {
	registry *prometheus.Registry

	subscriptions     *prometheus.CounterVec
	connectedClients  prometheus.GaugeFunc
	activeQueues      prometheus.GaugeFunc
	queuedEvents      prometheus.GaugeFunc
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

func newAppMetrics(state liveQueueState) *appMetrics {
	m := &appMetrics{
		registry: prometheus.NewRegistry(),
		subscriptions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "messagequeue_subscriptions_total",
			Help: "SUBSCRIBE frames handled, by outcome (accepted, or the reason it was rejected).",
		}, []string{"outcome"}),
		connectedClients: prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "messagequeue_connected_clients",
			Help: "Number of clients with a currently-open connection.",
		}, func() float64 { return float64(state.connectedClientCount()) }),
		activeQueues: prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "messagequeue_active_queues",
			Help: "Number of client queues currently tracked by this process.",
		}, func() float64 { return float64(state.activeQueueCount()) }),
		queuedEvents: prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "messagequeue_queued_events",
			Help: "Total events currently pending delivery across all client queues.",
		}, func() float64 { return float64(state.queuedEventCount()) }),
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

// eventEnqueued records one event successfully appended to a client queue.
func (m *appMetrics) eventEnqueued() { m.eventsEnqueued.Inc() }

// eventDropped records an event that never reached any queue because none
// existed yet.
func (m *appMetrics) eventDropped() { m.eventsDropped.Inc() }

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
