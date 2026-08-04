package main

import (
	"flag"
	"log"
	"net/http"

	stompserver "github.com/go-stomp/stomp/v3/server"
)

// simpleAddr is a minimal net.Addr for wsListener.Addr(); go-stomp's server
// package never actually calls it (it only matters for the ListenAndServe
// helper, which we bypass in favor of an HTTP-upgrade-fed listener), but the
// net.Listener interface requires an implementation.
type simpleAddr string

func (a simpleAddr) Network() string { return "ws" }
func (a simpleAddr) String() string  { return string(a) }

func main() {
	port := flag.String("port", "8080", "port to listen on")
	flag.Parse()

	addr := ":" + *port
	listener := newWSListener(simpleAddr(addr))

	mux := http.NewServeMux()
	mux.Handle("/", listener)

	httpServer := &http.Server{Addr: addr, Handler: mux}
	go func() {
		log.Printf("STOMP 1.2 WebSocket server listening on ws://0.0.0.0%s", addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server failed: %v", err)
		}
	}()

	// Server, QueueStorage, Authenticator, and HeartBeat are all left at
	// their zero values deliberately: nil QueueStorage defaults to
	// go-stomp's in-memory queue.MemoryQueueStorage, nil Authenticator
	// leaves the connection open (the existing stomp-client sends no
	// login/passcode), and a zero HeartBeat falls back to
	// server.DefaultHeartBeat. Log is left nil so server.Server.Serve
	// installs go-stomp's own default stdlib-based logger.
	srv := &stompserver.Server{}
	if err := srv.Serve(listener); err != nil {
		log.Fatalf("stomp server failed: %v", err)
	}
}
