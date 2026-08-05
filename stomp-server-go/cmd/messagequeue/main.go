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

	srv := stomp.NewServer(logger)
	source := newTickerEventSource("demo-events", eventInterval)
	newMessageQueueApp(srv, logger, source)
	if err := srv.Serve(listener); err != nil {
		logger.Error("stomp server failed", "error", err)
		os.Exit(1)
	}
}
