package main

import (
	"net"
	"time"

	"github.com/gorilla/websocket"
)

// wsConn adapts a gorilla/websocket connection to the net.Conn interface
// that go-stomp's server/client package expects (it was written for raw
// TCP STOMP and reads/writes frames via frame.Reader/frame.Writer directly
// on top of a net.Conn). This is the "custom websocket communication layer"
// bridging STOMP-over-WebSocket to that library code: everything above the
// net.Conn interface (frame parsing, the connection state machine, topics,
// queues) stays exactly as provided by go-stomp.
//
// Each WebSocket message carries exactly one STOMP frame (or the lone LF
// heartbeat), matching how stomp-client and stomp-server already exchange
// frames one-per-message. Read/Write still present a byte-stream interface
// so that frame.Reader/frame.Writer (which are stream-oriented) work
// unmodified.
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
