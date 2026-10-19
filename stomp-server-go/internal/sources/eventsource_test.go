package sources

import (
	"testing"
	"time"

	"github.com/sci-ndp/pelican-client-protocol/stomp-server-go/eventsource"
)

func TestTickerSource_ImplementsEventSourceViaDefaultNotifier(t *testing.T) {
	var s eventsource.EventSource = NewTickerSource(time.Hour) // never actually ticks during this test
	if !s.ShouldNotify("event", nil) {
		t.Error("TickerSource.ShouldNotify() = false, want true (via embedded DefaultNotifier)")
	}
}
