package notificationserver_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sci-ndp/pelican-client-protocol/stomp-server-go/clientqueue"
	"github.com/sci-ndp/pelican-client-protocol/stomp-server-go/eventsource"
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
