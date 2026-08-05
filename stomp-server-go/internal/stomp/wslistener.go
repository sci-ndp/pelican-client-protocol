package stomp

import (
	"errors"
	"net"
	"net/http"

	"github.com/gorilla/websocket"
)

// SimpleAddr is a minimal net.Addr for wsListener.Addr(); Server.Serve never
// actually calls it (it only matters for a ListenAndServe-style helper, which
// callers bypass in favor of an HTTP-upgrade-fed listener), but the
// net.Listener interface requires an implementation.
type SimpleAddr string

func (a SimpleAddr) Network() string { return "ws" }
func (a SimpleAddr) String() string  { return string(a) }

// wsListener is a net.Listener whose connections arrive from an HTTP
// Upgrade to WebSocket rather than from a raw TCP Accept(). This is what
// lets us hand a WebSocket transport to server.Server.Serve(l), which
// otherwise expects to own a real net.Listener (e.g. from net.Listen("tcp", ...)).
type wsListener struct {
	addr     net.Addr
	upgrader websocket.Upgrader
	connCh   chan net.Conn
	closeCh  chan struct{}
}

func NewWSListener(addr net.Addr) *wsListener {
	return &wsListener{
		addr: addr,
		upgrader: websocket.Upgrader{
			// This is a protocol playground reachable from a browser dashboard
			// on a different origin; it performs no authentication of its own.
			CheckOrigin: func(r *http.Request) bool { return true },
		},
		connCh:  make(chan net.Conn),
		closeCh: make(chan struct{}),
	}
}

// ServeHTTP upgrades every incoming request to a WebSocket connection and
// hands it to whatever is calling Accept().
func (l *wsListener) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ws, err := l.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	select {
	case l.connCh <- newWSConn(ws):
	case <-l.closeCh:
		ws.Close()
	}
}

func (l *wsListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.connCh:
		return c, nil
	case <-l.closeCh:
		return nil, errors.New("wslistener: closed")
	}
}

func (l *wsListener) Close() error {
	close(l.closeCh)
	return nil
}

func (l *wsListener) Addr() net.Addr { return l.addr }
