package main

import (
	"flag"
	"log"
	"net/http"
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

	srv := newServer()
	if err := srv.Serve(listener); err != nil {
		log.Fatalf("stomp server failed: %v", err)
	}
}
