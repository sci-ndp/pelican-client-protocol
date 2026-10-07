package main

import (
	"flag"
	"log/slog"
	"net/http"
	"os"

	"stomp-server-go/internal/clientqueue"
	"stomp-server-go/internal/eventsource"
	"stomp-server-go/internal/messagequeue"
	"stomp-server-go/internal/stomp"
)

// allowCORS lets any origin GET the wrapped handler's response, for
// endpoints (like /metrics) that are read-only, unauthenticated, and meant
// to be polled from a dashboard served on a different origin/port.
func allowCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		next.ServeHTTP(w, r)
	})
}

func main() {
	port := flag.String("port", "8080", "port to listen on")
	debug := flag.Bool("debug", false, "log full frame contents (headers and body) for every frame")
	htpasswdFile := flag.String("htpasswd", "", "path to an htpasswd file; if absent, connections are unauthenticated")
	queueDB := flag.String("queue-db", "", "path to a SQLite database file for durable, on-disk client queues; if absent, queues are kept in memory only")
	watchDir := flag.String("watch-dir", "", "watch this local directory tree and emit an event for every file created or modified in it (required)")
	flag.Parse()

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	var authenticator stomp.Authenticator = stomp.NoAuth{}
	if *htpasswdFile != "" {
		a, err := stomp.NewBasicAuth(*htpasswdFile, logger)
		if err != nil {
			logger.Error("failed to load htpasswd file", "error", err)
			os.Exit(1)
		}
		authenticator = a
	}

	addr := ":" + *port
	listener := stomp.NewWSListener(stomp.SimpleAddr(addr))

	mux := http.NewServeMux()
	mux.Handle("/", stomp.RequireAuth(authenticator, listener))

	newQueue := clientqueue.Factory(clientqueue.NewMemoryQueue)
	var persistedClients []clientqueue.PersistedClient
	if *queueDB != "" {
		db, err := clientqueue.OpenDB(*queueDB)
		if err != nil {
			logger.Error("failed to open queue database", "error", err)
			os.Exit(1)
		}
		newQueue = clientqueue.SQLiteFactory(db, logger)
		logger.Info("using on-disk client queues", "queue-db", *queueDB)

		persistedClients, err = clientqueue.ListPersistedClients(db)
		if err != nil {
			logger.Error("failed to list persisted clients", "error", err)
			os.Exit(1)
		}
	} else {
		logger.Info("using in-memory client queues")
	}

	if *watchDir == "" {
		logger.Error("--watch-dir is required")
		os.Exit(1)
	}
	source, err := eventsource.NewFSNotifySource(*watchDir, logger)
	if err != nil {
		logger.Error("failed to start filesystem watcher", "path", *watchDir, "error", err)
		os.Exit(1)
	}
	logger.Info("watching local directory for new and modified files", "path", *watchDir)

	srv := stomp.NewServer(logger)
	app := messagequeue.New(srv, logger, source, newQueue)
	// Pre-populate queues (and notify source) for durable clients from a
	// prior process run, before serving any connections -- otherwise e.g.
	// the Pelican watcher wouldn't know to watch their directories until
	// each one happens to reconnect.
	app.RehydrateQueues(persistedClients)
	// Dashboards (e.g. server-ui) are typically served from a different
	// origin than this server, so allow cross-origin GETs of this read-only,
	// non-sensitive endpoint.
	mux.Handle("/metrics", allowCORS(app.MetricsHandler()))

	httpServer := &http.Server{Addr: addr, Handler: mux}
	go func() {
		logger.Info("STOMP 1.2 WebSocket server listening", "addr", "ws://0.0.0.0"+addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("http server failed", "error", err)
			os.Exit(1)
		}
	}()

	if err := srv.Serve(listener); err != nil {
		logger.Error("stomp server failed", "error", err)
		os.Exit(1)
	}
}
