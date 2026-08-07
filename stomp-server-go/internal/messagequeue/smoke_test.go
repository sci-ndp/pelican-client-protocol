package messagequeue

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"stomp-server-go/internal/clientqueue"
	"stomp-server-go/internal/eventsource"
	"stomp-server-go/internal/stomp"
)

// parseHeader does a minimal extraction of one header's value from a raw
// STOMP frame's text, good enough for this smoke test.
func parseHeader(frame, key string) string {
	prefix := "\n" + key + ":"
	idx := strings.Index(frame, prefix)
	if idx < 0 {
		return ""
	}
	rest := frame[idx+len(prefix):]
	end := strings.IndexByte(rest, '\n')
	if end < 0 {
		return rest
	}
	return rest[:end]
}

// TestSmokeEndToEnd exercises the full real stack (real HTTP server, real
// WebSocket upgrade, real Server, real App) with a raw WebSocket client
// speaking STOMP frames directly, as a sanity check beyond the mocked unit
// tests: subscribe, receive an event, ack it, receive the next one.
func TestSmokeEndToEnd(t *testing.T) {
	listener := stomp.NewWSListener(stomp.SimpleAddr("test"))
	mux := http.NewServeMux()
	mux.Handle("/", stomp.RequireAuth(stomp.NoAuth{}, listener))
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	srv := stomp.NewServer(discardLogger())
	go srv.Serve(listener)
	defer listener.Close()

	source := eventsource.NewTickerSource(100 * time.Millisecond)
	New(srv, discardLogger(), source, clientqueue.NewMemoryQueue)

	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http")
	ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer ws.Close()

	send := func(frame string) {
		if err := ws.WriteMessage(websocket.BinaryMessage, []byte(frame+"\x00")); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	recvFrame := func() string {
		_, data, err := ws.ReadMessage()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		return strings.TrimRight(string(data), "\x00")
	}

	send("CONNECT\naccept-version:1.2\n\n")
	connected := recvFrame()
	if !strings.HasPrefix(connected, "CONNECTED") {
		t.Fatalf("expected CONNECTED, got %q", connected)
	}

	// The destination deliberately does not follow the old <subscription>/
	// <event-source-name> single-segment convention, to prove delivery just
	// goes back to whatever was subscribed -- as long as it's still owned by
	// "alice" (its required first path segment).
	send("SUBSCRIBE\nid:sub-0\ndestination:alice/wherever/alice/wants\nack:client-individual\nsubscription:alice\n\n")

	first := recvFrame()
	if !strings.HasPrefix(first, "MESSAGE") {
		t.Fatalf("expected first MESSAGE, got %q", first)
	}
	if got := parseHeader(first, "destination"); got != "alice/wherever/alice/wants" {
		t.Errorf("destination = %q, want %q", got, "alice/wherever/alice/wants")
	}
	ackID := parseHeader(first, "ack")
	if ackID == "" {
		t.Fatal("first MESSAGE has no ack header")
	}

	send(fmt.Sprintf("ACK\nid:%s\n\n", ackID))

	second := recvFrame()
	if !strings.HasPrefix(second, "MESSAGE") {
		t.Fatalf("expected second MESSAGE after ack, got %q", second)
	}
	if got := parseHeader(second, "message-id"); got == parseHeader(first, "message-id") {
		t.Errorf("second MESSAGE reused the first message-id %q", got)
	}
}
