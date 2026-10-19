// Package notificationserver is the embeddable entrypoint to this module: it
// wires a STOMP-over-WebSocket server to a per-client durable event queue
// fed by a caller-supplied eventsource.EventSource, and returns the result
// as an http.Handler for the caller to mount in its own http.Server.
package notificationserver

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/sci-ndp/pelican-client-protocol/stomp-server-go/clientqueue"
	"github.com/sci-ndp/pelican-client-protocol/stomp-server-go/eventsource"
	"github.com/sci-ndp/pelican-client-protocol/stomp-server-go/internal/messagequeue"
	"github.com/sci-ndp/pelican-client-protocol/stomp-server-go/internal/stomp"
)

// NotificationServerArgs configures New.
type NotificationServerArgs struct {
	// EventSource produces the events fanned out to clients. Required.
	EventSource eventsource.EventSource

	// Logger receives all server logs. If nil, slog.Default() is used.
	Logger *slog.Logger

	// HtpasswdFile is the path to an htpasswd file used for HTTP basic auth
	// on the WebSocket endpoint. If empty, connections are unauthenticated.
	HtpasswdFile string

	// QueueDB is the path to a SQLite database file for durable, on-disk
	// client queues; clients persisted by a prior run are rehydrated at
	// startup. If empty (and QueueFactory is nil), queues are kept in
	// memory only. Mutually exclusive with QueueFactory.
	QueueDB string

	// QueueFactory creates per-client queues, e.g. clientqueue.NewMemoryQueue
	// or clientqueue.SQLiteFactory. Mutually exclusive with QueueDB.
	QueueFactory clientqueue.Factory
}

// metricsPath is where New mounts the Prometheus metrics endpoint; every
// other path serves the STOMP WebSocket endpoint.
const metricsPath = "/metrics"

// New builds the notification server and starts its background event
// processing. The returned handler serves the STOMP WebSocket endpoint at
// "/" and Prometheus metrics at "/metrics" (with permissive CORS, so a
// dashboard on another origin can poll it). Its background goroutines run for
// the life of the process.
func New(args NotificationServerArgs) (http.Handler, error) {
	if args.EventSource == nil {
		return nil, errors.New("notificationserver: EventSource is required")
	}
	if args.QueueDB != "" && args.QueueFactory != nil {
		return nil, errors.New("notificationserver: QueueDB and QueueFactory are mutually exclusive")
	}
	logger := args.Logger
	if logger == nil {
		logger = slog.Default()
	}

	var authenticator stomp.Authenticator = stomp.NoAuth{}
	if args.HtpasswdFile != "" {
		a, err := stomp.NewBasicAuth(args.HtpasswdFile, logger)
		if err != nil {
			return nil, fmt.Errorf("load htpasswd file: %w", err)
		}
		authenticator = a
	}

	newQueue := args.QueueFactory
	var persistedClients []clientqueue.PersistedClient
	switch {
	case args.QueueDB != "":
		db, err := clientqueue.OpenDB(args.QueueDB)
		if err != nil {
			return nil, fmt.Errorf("open queue database: %w", err)
		}
		newQueue = clientqueue.SQLiteFactory(db, logger)
		logger.Info("using on-disk client queues", "queue-db", args.QueueDB)
		persistedClients, err = clientqueue.ListPersistedClients(db)
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("list persisted clients: %w", err)
		}
	case newQueue == nil:
		newQueue = clientqueue.NewMemoryQueue
		logger.Info("using in-memory client queues")
	}

	listener := stomp.NewWSListener(stomp.SimpleAddr("notificationserver"))
	srv := stomp.NewServer(logger)
	app := messagequeue.New(srv, logger, args.EventSource, newQueue)
	// Pre-populate queues (and notify the source) for durable clients from a
	// prior process run, before serving any connections.
	app.RehydrateQueues(persistedClients)

	go func() {
		if err := srv.Serve(listener); err != nil {
			logger.Error("stomp server failed", "error", err)
		}
	}()

	mux := http.NewServeMux()
	mux.Handle("/", stomp.RequireAuth(authenticator, listener))
	mux.Handle(metricsPath, allowCORS(app.MetricsHandler()))
	return mux, nil
}

// allowCORS lets any origin GET the wrapped handler's response, for
// endpoints (like /metrics) that are read-only, unauthenticated, and meant
// to be polled from a dashboard served on a different origin/port.
func allowCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		next.ServeHTTP(w, r)
	})
}
