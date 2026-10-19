package eventsource

import "testing"

func TestDefaultNotifier_AlwaysReturnsTrue(t *testing.T) {
	var n DefaultNotifier
	if !n.ShouldNotify("event", nil) {
		t.Error("DefaultNotifier.ShouldNotify() = false, want true")
	}
}
