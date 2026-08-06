package stomp

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-stomp/stomp/v3"
	"github.com/gorilla/websocket"
)

// startTestServer brings up a Server (server.go) fed by a wsListener behind
// an httptest.Server, exactly as main.go wires them together, and returns
// the ws:// URL clients should dial along with the Server itself, so tests
// can register callbacks (OnAck, OnSubscribe) or call Disconnect/Connected
// directly. Everything is torn down via t.Cleanup.
func startTestServer(t *testing.T, authenticator Authenticator) (string, *Server) {
	t.Helper()

	listener := NewWSListener(SimpleAddr("test"))
	mux := http.NewServeMux()
	mux.Handle("/", RequireAuth(authenticator, listener))

	httpServer := httptest.NewServer(mux)
	t.Cleanup(httpServer.Close)

	srv := NewServer(discardLogger())
	go srv.Serve(listener)
	t.Cleanup(func() { listener.Close() })

	return "ws" + strings.TrimPrefix(httpServer.URL, "http"), srv
}

// discardStompLogger is a stomp.Logger that drops everything, so a test's
// expected Nack-triggered redelivery (which the client logs as a connection
// event) doesn't clutter `go test -v` output.
type discardStompLogger struct{}

func (discardStompLogger) Debugf(string, ...interface{})   {}
func (discardStompLogger) Infof(string, ...interface{})    {}
func (discardStompLogger) Warningf(string, ...interface{}) {}
func (discardStompLogger) Errorf(string, ...interface{})   {}
func (discardStompLogger) Debug(string)                    {}
func (discardStompLogger) Info(string)                     {}
func (discardStompLogger) Warning(string)                  {}
func (discardStompLogger) Error(string)                    {}

// dialStompClient dials wsURL over WebSocket and performs the STOMP CONNECT
// sequence using the go-stomp/stomp client, our client-under-test's
// counterpart. Heartbeats are disabled since these tests are short-lived.
func dialStompClient(t *testing.T, wsURL string, header http.Header) *stomp.Conn {
	t.Helper()

	ws, _, err := websocket.DefaultDialer.Dial(wsURL, header)
	if err != nil {
		t.Fatalf("websocket dial: %v", err)
	}

	conn, err := stomp.Connect(newWSConn(ws),
		stomp.ConnOpt.HeartBeat(0, 0),
		stomp.ConnOpt.Logger(discardStompLogger{}),
	)
	if err != nil {
		t.Fatalf("stomp connect: %v", err)
	}
	t.Cleanup(func() { conn.Disconnect() })
	return conn
}

func recvMessage(t *testing.T, sub *stomp.Subscription) *stomp.Message {
	t.Helper()
	select {
	case msg := <-sub.C:
		if msg.Err != nil {
			t.Fatalf("received error instead of message: %v", msg.Err)
		}
		return msg
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for message")
		return nil
	}
}

func TestIntegration_ConnectAndDisconnect(t *testing.T) {
	wsURL, _ := startTestServer(t, NoAuth{})
	conn := dialStompClient(t, wsURL, nil)

	if conn.Version() != stomp.V12 {
		t.Errorf("Version() = %v, want %v", conn.Version(), stomp.V12)
	}
	if conn.Server() == "" {
		t.Error("Server() = \"\", want the server's identification string")
	}
	if err := conn.Disconnect(); err != nil {
		t.Errorf("Disconnect: %v", err)
	}
}

func TestIntegration_PublishSubscribeAutoAck(t *testing.T) {
	wsURL, _ := startTestServer(t, NoAuth{})
	conn := dialStompClient(t, wsURL, nil)

	sub, err := conn.Subscribe("/topic/test", stomp.AckAuto)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	const body = `{"greeting":"hi"}`
	if err := conn.Send("/topic/test", "application/json", []byte(body)); err != nil {
		t.Fatalf("Send: %v", err)
	}

	msg := recvMessage(t, sub)
	if msg.Destination != "/topic/test" {
		t.Errorf("Destination = %q, want %q", msg.Destination, "/topic/test")
	}
	if got := string(msg.Body); got != body {
		t.Errorf("Body = %q, want %q", got, body)
	}
	if msg.ShouldAck() {
		t.Error("ShouldAck() = true for an auto-ack subscription")
	}
}

func TestIntegration_ClientIndividualAckAndNackRedelivery(t *testing.T) {
	wsURL, _ := startTestServer(t, NoAuth{})
	conn := dialStompClient(t, wsURL, nil)

	sub, err := conn.Subscribe("/topic/test", stomp.AckClientIndividual)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	const body = `{"n":1}`
	if err := conn.Send("/topic/test", "application/json", []byte(body)); err != nil {
		t.Fatalf("Send: %v", err)
	}

	first := recvMessage(t, sub)
	if !first.ShouldAck() {
		t.Fatal("ShouldAck() = false for a client-individual subscription")
	}
	if err := conn.Nack(first); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	redelivered := recvMessage(t, sub)
	if string(redelivered.Body) != body {
		t.Errorf("redelivered Body = %q, want %q", redelivered.Body, body)
	}
	if redelivered.Header.Get("message-id") != first.Header.Get("message-id") {
		t.Error("redelivered message should keep the original message-id")
	}
	if redelivered.Header.Get("ack") == first.Header.Get("ack") {
		t.Error("redelivered message reused the original ack id")
	}
	if err := conn.Ack(redelivered); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	// The server drops a message after its one redelivery is also nacked,
	// so a second Nack must not produce another redelivery.
	select {
	case msg := <-sub.C:
		t.Fatalf("unexpected extra message after Ack: %+v", msg)
	case <-time.After(500 * time.Millisecond):
	}
}

func TestIntegration_BasicAuthRejectsMissingCredentials(t *testing.T) {
	path := writeHtpasswd(t, map[string]string{"alice": "s3cret"})
	authenticator, err := NewBasicAuth(path, discardLogger())
	if err != nil {
		t.Fatalf("NewBasicAuth: %v", err)
	}
	wsURL, _ := startTestServer(t, authenticator)

	_, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err == nil {
		t.Fatal("expected dial without credentials to fail")
	}
	if resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("response = %+v, want status %d", resp, http.StatusUnauthorized)
	}
}

func TestIntegration_BasicAuthAcceptsCorrectCredentials(t *testing.T) {
	path := writeHtpasswd(t, map[string]string{"alice": "s3cret"})
	authenticator, err := NewBasicAuth(path, discardLogger())
	if err != nil {
		t.Fatalf("NewBasicAuth: %v", err)
	}
	wsURL, _ := startTestServer(t, authenticator)

	header := http.Header{}
	header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("alice:s3cret")))
	conn := dialStompClient(t, wsURL, header)

	if conn.Version() != stomp.V12 {
		t.Errorf("Version() = %v, want %v", conn.Version(), stomp.V12)
	}
}

func TestIntegration_OnAckFiresWithExpectedHeaders(t *testing.T) {
	wsURL, srv := startTestServer(t, NoAuth{})
	conn := dialStompClient(t, wsURL, nil)

	acked := make(chan map[string]string, 1)
	srv.OnAck(func(headers map[string]string) { acked <- headers })

	sub, err := conn.Subscribe("/topic/test", stomp.AckClientIndividual)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := conn.Send("/topic/test", "application/json", []byte(`{"n":1}`)); err != nil {
		t.Fatalf("Send: %v", err)
	}

	msg := recvMessage(t, sub)
	if err := conn.Ack(msg); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	select {
	case headers := <-acked:
		if headers["destination"] != "/topic/test" {
			t.Errorf("destination = %q, want %q", headers["destination"], "/topic/test")
		}
		if headers["message-id"] != msg.Header.Get("message-id") {
			t.Errorf("message-id = %q, want %q", headers["message-id"], msg.Header.Get("message-id"))
		}
		if headers["ack"] != msg.Header.Get("ack") {
			t.Errorf("ack = %q, want %q", headers["ack"], msg.Header.Get("ack"))
		}
		if headers["subscription"] == "" {
			t.Error("subscription header is empty, want the SUBSCRIBE's id")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for OnAck callback")
	}
}

func TestIntegration_OnAckFiresOncePerMessageOnCumulativeAck(t *testing.T) {
	wsURL, srv := startTestServer(t, NoAuth{})
	conn := dialStompClient(t, wsURL, nil)

	var mu sync.Mutex
	var acked []string
	done := make(chan struct{})
	srv.OnAck(func(headers map[string]string) {
		mu.Lock()
		acked = append(acked, headers["message-id"])
		if len(acked) == 2 {
			close(done)
		}
		mu.Unlock()
	})

	sub, err := conn.Subscribe("/topic/test", stomp.AckClient)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := conn.Send("/topic/test", "application/json", []byte(`{"n":1}`)); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := conn.Send("/topic/test", "application/json", []byte(`{"n":2}`)); err != nil {
		t.Fatalf("Send: %v", err)
	}

	first := recvMessage(t, sub)
	second := recvMessage(t, sub)
	if err := conn.Ack(second); err != nil { // cumulative: resolves both first and second
		t.Fatalf("Ack: %v", err)
	}

	select {
	case <-done:
		mu.Lock()
		defer mu.Unlock()
		if len(acked) != 2 {
			t.Fatalf("OnAck fired %d times, want 2: %v", len(acked), acked)
		}
		want := []string{first.Header.Get("message-id"), second.Header.Get("message-id")}
		if acked[0] != want[0] || acked[1] != want[1] {
			t.Errorf("acked message-ids = %v, want %v", acked, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for both OnAck callbacks")
	}
}

func TestIntegration_SendErrorDeliversErrorFrameToSubscribedConnection(t *testing.T) {
	wsURL, srv := startTestServer(t, NoAuth{})
	conn := dialStompClient(t, wsURL, nil)

	sub, err := conn.Subscribe("/topic/test", stomp.AckAuto)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// SUBSCRIBE is processed asynchronously on the server's connection
	// goroutine, so Connected may not reflect it the instant Subscribe returns.
	deadline := time.Now().Add(2 * time.Second)
	for !srv.Connected("/topic/test") {
		if time.Now().After(deadline) {
			t.Fatal("Connected(\"/topic/test\") stayed false after subscribing")
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err := srv.SendError("/topic/test", "misbehaving client"); err != nil {
		t.Fatalf("SendError: %v", err)
	}

	select {
	case msg := <-sub.C:
		if msg.Err == nil {
			t.Fatal("expected an ERROR message, got a normal MESSAGE")
		}
		if msg.Err.Error() != "misbehaving client" {
			t.Errorf("error = %q, want %q", msg.Err.Error(), "misbehaving client")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the ERROR frame")
	}
}

func TestIntegration_SendErrorNoOpWhenNobodySubscribed(t *testing.T) {
	_, srv := startTestServer(t, NoAuth{})
	if err := srv.SendError("/topic/nobody-here", "reason"); err != nil {
		t.Errorf("SendError on an unsubscribed destination: %v, want nil", err)
	}
}

func TestIntegration_DisconnectClosesSubscribedConnection(t *testing.T) {
	wsURL, srv := startTestServer(t, NoAuth{})
	conn := dialStompClient(t, wsURL, nil)

	if _, err := conn.Subscribe("/topic/test", stomp.AckAuto); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := srv.Disconnect("/topic/test"); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !srv.Connected("/topic/test") {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Error("Connected(\"/topic/test\") stayed true after Disconnect")
}

func TestIntegration_DisconnectNoOpWhenNobodySubscribed(t *testing.T) {
	_, srv := startTestServer(t, NoAuth{})
	if err := srv.Disconnect("/topic/nobody-here"); err != nil {
		t.Errorf("Disconnect on an unsubscribed destination: %v, want nil", err)
	}
}

func TestIntegration_ConnectedReflectsSubscriptionState(t *testing.T) {
	wsURL, srv := startTestServer(t, NoAuth{})

	if srv.Connected("/topic/test") {
		t.Fatal("Connected(\"/topic/test\") = true before any subscription")
	}

	conn := dialStompClient(t, wsURL, nil)
	if _, err := conn.Subscribe("/topic/test", stomp.AckAuto); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// SUBSCRIBE is processed asynchronously on the server's connection
	// goroutine, so Connected may not reflect it the instant Subscribe returns.
	deadline := time.Now().Add(2 * time.Second)
	for !srv.Connected("/topic/test") {
		if time.Now().After(deadline) {
			t.Fatal("Connected(\"/topic/test\") stayed false after subscribing")
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err := conn.Disconnect(); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}

	disconnectDeadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(disconnectDeadline) {
		if !srv.Connected("/topic/test") {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Error("Connected(\"/topic/test\") stayed true after the client disconnected")
}
