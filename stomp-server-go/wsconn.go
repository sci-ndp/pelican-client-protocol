package main

import (
	"net"
	"time"

	"github.com/gorilla/websocket"
)

// wsConn adapts a gorilla/websocket connection to the net.Conn interface
// used by the STOMP server below (server.go), so that server logic is
// written against the standard net.Conn interface rather than directly
// against gorilla/websocket. This is the "custom websocket communication
// layer" bridging STOMP-over-WebSocket to that server code.
//
// Each WebSocket message carries exactly one STOMP frame (or the lone LF
// heartbeat), matching how stomp-client and stomp-server already exchange
// frames one-per-message. Read/Write still present a byte-stream interface,
// but readFrame (protocol.go) relies on each Read returning exactly one
// WebSocket message by using a buffer sized larger than any real frame.
type wsConn struct {
	ws   *websocket.Conn
	rbuf []byte // leftover bytes from the most recently read WS message
}

func newWSConn(ws *websocket.Conn) *wsConn {
	return &wsConn{ws: ws}
}

func (c *wsConn) Read(p []byte) (int, error) {
	for len(c.rbuf) == 0 {
		_, data, err := c.ws.ReadMessage()
		if err != nil {
			return 0, err
		}
		c.rbuf = data
	}
	n := copy(p, c.rbuf)
	c.rbuf = c.rbuf[n:]
	return n, nil
}

func (c *wsConn) Write(p []byte) (int, error) {
	if err := c.ws.WriteMessage(websocket.BinaryMessage, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (c *wsConn) Close() error { return c.ws.Close() }

func (c *wsConn) LocalAddr() net.Addr  { return c.ws.LocalAddr() }
func (c *wsConn) RemoteAddr() net.Addr { return c.ws.RemoteAddr() }

func (c *wsConn) SetDeadline(t time.Time) error {
	if err := c.ws.SetReadDeadline(t); err != nil {
		return err
	}
	return c.ws.SetWriteDeadline(t)
}

func (c *wsConn) SetReadDeadline(t time.Time) error  { return c.ws.SetReadDeadline(t) }
func (c *wsConn) SetWriteDeadline(t time.Time) error { return c.ws.SetWriteDeadline(t) }
