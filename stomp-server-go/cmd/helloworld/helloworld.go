package main

import (
	"log/slog"
	"sync"
	"time"

	"stomp-server-go/internal/stomp"
)

// helloWorldApp is a small application layer built entirely on the
// StompServer interface: it never touches Server's internal session or
// subscription state. The first time any client subscribes to a
// destination, it starts a per-destination timer that publishes a
// "Hello, World" MESSAGE to that destination every helloWorldInterval.
type helloWorldApp struct {
	srv      stomp.StompServer
	log      *slog.Logger
	interval time.Duration

	mu      sync.Mutex
	started map[string]bool // destinations that already have a broadcast loop running
}

const helloWorldInterval = 10 * time.Second

// newHelloWorldApp registers itself as a subscriber to srv's subscription
// events and returns the running app.
func newHelloWorldApp(srv stomp.StompServer, log *slog.Logger) *helloWorldApp {
	app := &helloWorldApp{
		srv:      srv,
		log:      log,
		interval: helloWorldInterval,
		started:  map[string]bool{},
	}
	srv.OnSubscribe(app.onSubscribe)
	return app
}

// onSubscribe starts a broadcast loop the first time a SUBSCRIBE's
// destination is seen; later subscriptions to an already-seen destination
// are no-ops, since a loop for it is already running.
func (a *helloWorldApp) onSubscribe(headers map[string]string) {
	destination := headers["destination"]

	a.mu.Lock()
	if a.started[destination] {
		a.mu.Unlock()
		return
	}
	a.started[destination] = true
	a.mu.Unlock()

	a.log.Info("starting hello-world broadcast", "destination", destination)
	go a.broadcastLoop(destination)
}

func (a *helloWorldApp) broadcastLoop(destination string) {
	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()
	for range ticker.C {
		if err := a.srv.Publish(destination, "Hello, World"); err != nil {
			a.log.Error("failed to publish hello-world message", "destination", destination, "error", err)
		}
	}
}
