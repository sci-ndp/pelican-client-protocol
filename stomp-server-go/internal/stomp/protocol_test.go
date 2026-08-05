package stomp

import "testing"

func TestFrameEncodeParseRoundTrip(t *testing.T) {
	original := NewFrame("SEND", map[string]string{
		"destination": "/topic/test",
		"content-type": "application/json",
	}, `{"hello":"world"}`)

	parsed, err := parseFrame(original.Encode())
	if err != nil {
		t.Fatalf("parseFrame: %v", err)
	}
	if parsed.Command != original.Command {
		t.Errorf("Command = %q, want %q", parsed.Command, original.Command)
	}
	if parsed.Body != original.Body {
		t.Errorf("Body = %q, want %q", parsed.Body, original.Body)
	}
	for k, v := range original.Headers {
		if parsed.Headers[k] != v {
			t.Errorf("Headers[%q] = %q, want %q", k, parsed.Headers[k], v)
		}
	}
}

func TestFrameEncodeEscapesHeaderValues(t *testing.T) {
	frame := NewFrame("MESSAGE", map[string]string{"message": "a:b\nc\\d"}, "")
	parsed, err := parseFrame(frame.Encode())
	if err != nil {
		t.Fatalf("parseFrame: %v", err)
	}
	if got := parsed.Headers["message"]; got != "a:b\nc\\d" {
		t.Errorf("Headers[message] = %q, want %q", got, "a:b\nc\\d")
	}
}

func TestParseFrameRejectsMissingNulTerminator(t *testing.T) {
	if _, err := parseFrame([]byte("CONNECT\n\n")); err == nil {
		t.Fatal("expected error for frame missing NUL terminator")
	}
}

func TestParseFrameRejectsMissingHeaderBodySeparator(t *testing.T) {
	if _, err := parseFrame([]byte("CONNECT\x00")); err == nil {
		t.Fatal("expected error for frame missing header/body separator")
	}
}

func TestParseFrameRejectsDuplicateHeader(t *testing.T) {
	raw := []byte("SEND\ndestination:/a\ndestination:/b\n\n\x00")
	if _, err := parseFrame(raw); err == nil {
		t.Fatal("expected error for duplicate header")
	}
}

func TestParseFrameRejectsInvalidEscape(t *testing.T) {
	raw := []byte("SEND\ndestination:\\x\n\n\x00")
	if _, err := parseFrame(raw); err == nil {
		t.Fatal("expected error for invalid header escape")
	}
}
