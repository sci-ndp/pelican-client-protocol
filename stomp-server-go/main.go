package main

import (
	"flag"
	"log/slog"
	"net/http"
	"os"
)

// simpleAddr is a minimal net.Addr for wsListener.Addr(); our Server.Serve
// never actually calls it (it only matters for a ListenAndServe-style
// helper, which we bypass in favor of an HTTP-upgrade-fed listener), but
// the net.Listener interface requires an implementation.
type simpleAddr string

func (a simpleAddr) Network() string { return "ws" }
func (a simpleAddr) String() string  { return string(a) }

func main() {
	port := flag.String("port", "8080", "port to listen on")
	debug := flag.Bool("debug", false, "log full frame contents (headers and body) for every frame")
	flag.Parse()

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	addr := ":" + *port
	listener := newWSListener(simpleAddr(addr))

	mux := http.NewServeMux()
	mux.Handle("/", listener)

	httpServer := &http.Server{Addr: addr, Handler: mux}
	go func() {
		logger.Info("STOMP 1.2 WebSocket server listening", "addr", "ws://0.0.0.0"+addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("http server failed", "error", err)
			os.Exit(1)
		}
	}()

	srv := newServer(logger)
	if err := srv.Serve(listener); err != nil {
		logger.Error("stomp server failed", "error", err)
		os.Exit(1)
	}
}
