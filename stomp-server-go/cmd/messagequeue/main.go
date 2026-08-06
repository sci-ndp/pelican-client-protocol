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

func main() {
	port := flag.String("port", "8080", "port to listen on")
	debug := flag.Bool("debug", false, "log full frame contents (headers and body) for every frame")
	htpasswdFile := flag.String("htpasswd", "", "path to an htpasswd file; if absent, connections are unauthenticated")
	queueDB := flag.String("queue-db", "", "path to a SQLite database file for durable, on-disk client queues; if absent, queues are kept in memory only")
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

	httpServer := &http.Server{Addr: addr, Handler: mux}
	go func() {
		logger.Info("STOMP 1.2 WebSocket server listening", "addr", "ws://0.0.0.0"+addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("http server failed", "error", err)
			os.Exit(1)
		}
	}()

	newQueue := queueFactory(newMemoryClientQueue)
	if *queueDB != "" {
		db, err := openQueueDB(*queueDB)
		if err != nil {
			logger.Error("failed to open queue database", "error", err)
			os.Exit(1)
		}
		newQueue = sqliteQueueFactory(db)
		logger.Info("using on-disk client queues", "queue-db", *queueDB)
	} else {
		logger.Info("using in-memory client queues")
	}

	// Exactly one demo event source: the persistent, file-backed one if
	// TEST_EVENT_SEQ_FILE is set, otherwise the plain in-memory ticker.
	var source EventSource
	if seqFile := os.Getenv("TEST_EVENT_SEQ_FILE"); seqFile != "" {
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
	newMessageQueueApp(srv, logger, source, newQueue)
	if err := srv.Serve(listener); err != nil {
		logger.Error("stomp server failed", "error", err)
		os.Exit(1)
	}
}
