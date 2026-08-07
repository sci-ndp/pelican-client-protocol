package main

import (
	"flag"
	"log/slog"
	"net/http"
	"os"
	"time"

	"stomp-server-go/internal/stomp"
)

const eventInterval = 5 * time.Second

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
	pelicanEnabled := flag.Bool("pelican-enabled", false, "watch Pelican federation directories named by clients' own subscription parameters for new files; if set, this is the event source instead of the demo ticker")
	pelicanPollInterval := flag.Duration("pelican-poll-interval", time.Minute, "how often to poll watched Pelican directories")
	pelicanStateFile := flag.String("pelican-state-file", "", "path to a file recording each watched directory's last-observed listing, so a restart doesn't re-announce every existing file as new (required if --pelican-enabled is set)")
	pelicanBinary := flag.String("pelican-binary", "pelican", "path to the pelican CLI binary used to list watched directories")
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

	newQueue := queueFactory(newMemoryClientQueue)
	var persistedClients []persistedClient
	if *queueDB != "" {
		db, err := openQueueDB(*queueDB)
		if err != nil {
			logger.Error("failed to open queue database", "error", err)
			os.Exit(1)
		}
		newQueue = sqliteQueueFactory(db, logger)
		logger.Info("using on-disk client queues", "queue-db", *queueDB)

		persistedClients, err = listPersistedClients(db)
		if err != nil {
			logger.Error("failed to list persisted clients", "error", err)
			os.Exit(1)
		}
	} else {
		logger.Info("using in-memory client queues")
	}

	// Exactly one event source is ever active: the Pelican directory watcher
	// if --pelican-enabled is set, else the persistent file-backed demo
	// source if TEST_EVENT_SEQ_FILE is set, else the plain in-memory demo
	// ticker. The Pelican watcher doesn't take a fixed directory: it derives
	// what to watch dynamically from each client's own subscription
	// parameters (see pelicanDirectoryFromParams), via QueueAdded/
	// QueueRemoved hooks messageQueueApp calls as clients (un)subscribe.
	var source EventSource
	if *pelicanEnabled {
		if *pelicanStateFile == "" {
			logger.Error("--pelican-state-file is required when --pelican-enabled is set")
			os.Exit(1)
		}
		pelicanSource, err := newPelicanListingEventSource(*pelicanPollInterval, *pelicanStateFile, *pelicanBinary, logger)
		if err != nil {
			logger.Error("failed to start Pelican directory listing watcher", "error", err)
			os.Exit(1)
		}
		source = pelicanSource
		logger.Info("watching Pelican directories named by client subscription parameters", "poll-interval", *pelicanPollInterval, "state-file", *pelicanStateFile)
	} else if seqFile := os.Getenv("TEST_EVENT_SEQ_FILE"); seqFile != "" {
		seqSource, err := newSeqFileEventSource(seqFile, eventInterval, logger)
		if err != nil {
			logger.Error("failed to start persistent sequence event source", "path", seqFile, "error", err)
			os.Exit(1)
		}
		source = seqSource
		logger.Info("using persistent sequence event source", "TEST_EVENT_SEQ_FILE", seqFile)
	} else {
		source = newTickerEventSource(eventInterval)
		logger.Info("using in-memory demo ticker event source")
	}

	srv := stomp.NewServer(logger)
	app := newMessageQueueApp(srv, logger, source, newQueue)
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
