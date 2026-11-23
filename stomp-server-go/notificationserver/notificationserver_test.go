package notificationserver_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-stomp/stomp/v3"
	"github.com/gorilla/websocket"

	"github.com/sci-ndp/pelican-client-protocol/stomp-server-go/clientqueue"
	"github.com/sci-ndp/pelican-client-protocol/stomp-server-go/eventsource"
	istomp "github.com/sci-ndp/pelican-client-protocol/stomp-server-go/internal/stomp"
	"github.com/sci-ndp/pelican-client-protocol/stomp-server-go/notificationserver"
)

type fakeSource struct {
	eventsource.DefaultNotifier
	ch chan string
}

func (s fakeSource) Events() <-chan string { return s.ch }

func TestNew_RequiresEventSource(t *testing.T) {
	if _, err := notificationserver.New(notificationserver.NotificationServerArgs{}); err == nil {
		t.Error("New() with nil EventSource succeeded, want error")
	}
}

func TestNew_QueueDBAndFactoryMutuallyExclusive(t *testing.T) {
	_, err := notificationserver.New(notificationserver.NotificationServerArgs{
		EventSource:  fakeSource{ch: make(chan string)},
		QueueDB:      "x.db",
		QueueFactory: clientqueue.NewMemoryQueue,
	})
	if err == nil {
		t.Error("New() with both QueueDB and QueueFactory succeeded, want error")
	}
}

func TestNew_ServesMetricsWithCORS(t *testing.T) {
	h, err := notificationserver.New(notificationserver.NotificationServerArgs{
		EventSource: fakeSource{ch: make(chan string)},
	})
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("GET /metrics = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Errorf("Access-Control-Allow-Origin = %q, want *", got)
	}
}

type fixedAuth struct{ name string }

func (a fixedAuth) Authenticate(http.ResponseWriter, *http.Request) (notificationserver.Principal, bool) {
	return notificationserver.Principal{Name: a.name}, true
}

type onlyOwnUser struct{}

func (onlyOwnUser) Authorize(p notificationserver.Principal, destination string) bool {
	return strings.HasPrefix(destination, p.Name+"/")
}

func TestNew_AuthorizerReceivesAuthenticatedPrincipal(t *testing.T) {
	h, err := notificationserver.New(notificationserver.NotificationServerArgs{
		EventSource:   fakeSource{ch: make(chan string)},
		Authenticator: fixedAuth{name: "alice"},
		Authorizer:    onlyOwnUser{},
	})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(h)
	defer ts.Close()

	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(ts.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := stomp.Connect(istomp.NewWSConn(ws), stomp.ConnOpt.HeartBeat(0, 0))
	if err != nil {
		t.Fatal(err)
	}

	sub, err := conn.Subscribe("bob/events", stomp.AckClientIndividual, stomp.SubscribeOpt.Header("subscription", "bob"))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case msg := <-sub.C:
		if msg == nil || msg.Err == nil {
			t.Fatalf("got %+v, want an ERROR frame for the denied destination", msg)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the ERROR frame")
	}
}
