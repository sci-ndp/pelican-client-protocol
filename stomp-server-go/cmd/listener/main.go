// Command listener subscribes to a destination on the message-queue server
// and prints every frame it sends or receives, auto-ACKing each MESSAGE.
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"

	"github.com/go-stomp/stomp/v3"
	"github.com/gorilla/websocket"

	"github.com/sci-ndp/pelican-client-protocol/stomp-server-go/client"
)

// loggingConn prints every chunk read from or written to the wrapped conn.
// The WebSocket adapter is message-oriented, so for small frames each call is
// exactly one raw frame (or a lone LF heartbeat).
type loggingConn struct{ net.Conn }

func (c loggingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		fmt.Printf("SERVER -> LISTENER\n%s\n\n", show(p[:n]))
	}
	return n, err
}

func (c loggingConn) Write(p []byte) (int, error) {
	fmt.Printf("LISTENER -> SERVER\n%s\n\n", show(p))
	return c.Conn.Write(p)
}

func show(b []byte) string { return strings.ReplaceAll(string(b), "\x00", "<NUL>") }

func main() {
	url := flag.String("url", "ws://localhost:8080", "server WebSocket URL")
	id := flag.String("subscription", "listener", "subscription id (the client id owning the queue)")
	dest := flag.String("destination", "", "destination to subscribe to, e.g. <subscription>/demo-events (required)")
	flag.Parse()
	if *dest == "" {
		log.Fatal("-destination is required")
	}

	ws, _, err := websocket.DefaultDialer.Dial(*url, nil)
	if err != nil {
		log.Fatalf("websocket dial: %v", err)
	}
	conn, err := client.Connect(loggingConn{client.NetConn(ws)})
	if err != nil {
		log.Fatalf("%v", err)
	}

	sub, err := conn.Subscribe(*dest, stomp.AckClientIndividual,
		stomp.SubscribeOpt.Id(*id),
		stomp.SubscribeOpt.Header("subscription", *id))
	if err != nil {
		log.Fatalf("subscribe: %v", err)
	}

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	for {
		select {
		case msg, ok := <-sub.C:
			if !ok {
				return
			}
			if msg.Err != nil {
				log.Fatalf("server error: %v", msg.Err)
			}
			if err := conn.Ack(msg); err != nil {
				log.Fatalf("ack: %v", err)
			}
		case <-interrupt:
			conn.Disconnect()
			return
		}
	}
}
