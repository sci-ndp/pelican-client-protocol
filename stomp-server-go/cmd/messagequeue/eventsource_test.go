package main

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

func TestTickerEventSource_ImplementsEventSourceViaDefaultNotifier(t *testing.T) {
	var s EventSource = newTickerEventSource(time.Hour) // never actually ticks during this test
	if !s.ShouldNotify("event", nil) {
		t.Error("tickerEventSource.ShouldNotify() = false, want true (via embedded defaultNotifier)")
	}
}
