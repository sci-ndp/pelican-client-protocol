// Package client connects to a message-queue server's WebSocket endpoint and
// returns a ready-to-use go-stomp connection.
package client

import (
	"context"
	"fmt"
	"net"

	"github.com/go-stomp/stomp/v3"
	"github.com/gorilla/websocket"

	stompserver "github.com/sci-ndp/pelican-client-protocol/stomp-server-go/internal/stomp"
)

// Dial opens a WebSocket to url (e.g. "ws://localhost:8080") and performs the
// STOMP CONNECT handshake over it. Heartbeats are disabled by default; opts
// are applied after the defaults, so they may override them.
//
// Closing the returned connection (Disconnect) also closes the WebSocket.
func Dial(ctx context.Context, url string, opts ...func(*stomp.Conn) error) (*stomp.Conn, error) {
	ws, _, err := websocket.DefaultDialer.DialContext(ctx, url, nil)
	if err != nil {
		return nil, fmt.Errorf("websocket dial: %w", err)
	}
	return Connect(NetConn(ws), opts...)
}

// NetConn adapts a dialed WebSocket to a net.Conn carrying one STOMP frame per
// WebSocket message, as the server expects. Wrap the result (e.g. to log
// traffic) before passing it to Connect.
func NetConn(ws *websocket.Conn) net.Conn { return stompserver.NewWSConn(ws) }

// Connect performs the STOMP CONNECT handshake over nc, which must come from
// NetConn (or wrap one). Heartbeats are disabled by default; opts are applied
// after the defaults, so they may override them. On failure nc is closed.
func Connect(nc net.Conn, opts ...func(*stomp.Conn) error) (*stomp.Conn, error) {
	all := append([]func(*stomp.Conn) error{stomp.ConnOpt.HeartBeat(0, 0)}, opts...)
	conn, err := stomp.Connect(nc, all...)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("stomp connect: %w", err)
	}
	return conn, nil
}
