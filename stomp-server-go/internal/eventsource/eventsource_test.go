package eventsource

import (
	"testing"
	"time"
)

func TestDefaultNotifier_AlwaysReturnsTrue(t *testing.T) {
	var n defaultNotifier
	if !n.ShouldNotify("event", nil) {
		t.Error("defaultNotifier.ShouldNotify() = false, want true")
	}
}

func TestTickerSource_ImplementsEventSourceViaDefaultNotifier(t *testing.T) {
	var s EventSource = NewTickerSource(time.Hour) // never actually ticks during this test
	if !s.ShouldNotify("event", nil) {
		t.Error("TickerSource.ShouldNotify() = false, want true (via embedded defaultNotifier)")
	}
}
