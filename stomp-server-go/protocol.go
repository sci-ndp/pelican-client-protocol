package main

import (
	"fmt"
	"net"
	"sort"
	"strings"
)

// maxFrameSize bounds a single Read() from wsConn. Because each WebSocket
// message carries exactly one STOMP frame (see wsconn.go), a buffer larger
// than any real frame lets one Read() reliably return one whole frame,
// mirroring how the Python server's `await websocket.recv()` returns one
// whole message. This playground only ever exchanges small JSON payloads.
const maxFrameSize = 1 << 20 // 1MB

// ProtocolError is raised when bytes do not form a valid STOMP 1.2 frame.
type ProtocolError struct {
	message string
}

func (e *ProtocolError) Error() string { return e.message }

func protocolErrorf(format string, args ...any) *ProtocolError {
	return &ProtocolError{message: fmt.Sprintf(format, args...)}
}

// Frame is a STOMP command, its headers, and a body.
type Frame struct {
	Command string
	Headers map[string]string
	Body    string
}

func NewFrame(command string, headers map[string]string, body string) *Frame {
	if headers == nil {
		headers = map[string]string{}
	}
	return &Frame{Command: command, Headers: headers, Body: body}
}

// Encode renders the frame using STOMP 1.2's wire format: a command line,
// headers, a blank line, the body, and a NULL terminator. Header values are
// escaped; header keys are not (this server only ever sets keys it controls,
// e.g. "destination", "message-id", so they need no escaping).
func (f *Frame) Encode() []byte {
	keys := make([]string, 0, len(f.Headers))
	for k := range f.Headers {
		keys = append(keys, k)
	}
	sort.Strings(keys) // deterministic, readable output; STOMP doesn't care about header order

	var b strings.Builder
	b.WriteString(f.Command)
	for _, k := range keys {
		b.WriteByte('\n')
		b.WriteString(k)
		b.WriteByte(':')
		b.WriteString(escape(f.Headers[k]))
	}
	b.WriteString("\n\n")
	b.WriteString(f.Body)
	b.WriteByte(0)
	return []byte(b.String())
}

func escape(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	value = strings.ReplaceAll(value, ":", `\c`)
	value = strings.ReplaceAll(value, "\r", `\r`)
	return value
}

func unescape(value string) (string, error) {
	runes := []rune(value)
	var out strings.Builder
	i := 0
	for i < len(runes) {
		if runes[i] == '\\' && i+1 < len(runes) {
			var replacement rune
			switch runes[i+1] {
			case 'n':
				replacement = '\n'
			case 'r':
				replacement = '\r'
			case 'c':
				replacement = ':'
			case '\\':
				replacement = '\\'
			default:
				return "", protocolErrorf("invalid header escape: %s", string(runes[i:i+2]))
			}
			out.WriteRune(replacement)
			i += 2
		} else {
			out.WriteRune(runes[i])
			i++
		}
	}
	return out.String(), nil
}

// parseFrame parses a single NULL-terminated STOMP frame.
func parseFrame(raw []byte) (*Frame, error) {
	if len(raw) == 0 || raw[len(raw)-1] != 0 {
		return nil, protocolErrorf("frame is missing NULL terminator")
	}
	text := string(raw[:len(raw)-1])
	headerText, body, ok := cutOnce(text, "\n\n")
	if !ok {
		return nil, protocolErrorf("frame is missing header/body separator")
	}
	lines := strings.Split(headerText, "\n")
	if len(lines) == 0 || lines[0] == "" {
		return nil, protocolErrorf("frame has no command")
	}

	headers := map[string]string{}
	for _, line := range lines[1:] {
		if line == "" {
			continue
		}
		idx := strings.Index(line, ":")
		if idx < 0 {
			return nil, protocolErrorf("header is missing ':'")
		}
		key, err := unescape(line[:idx])
		if err != nil {
			return nil, err
		}
		if _, exists := headers[key]; exists {
			return nil, protocolErrorf("duplicate header: %s", key)
		}
		value, err := unescape(line[idx+1:])
		if err != nil {
			return nil, err
		}
		headers[key] = value
	}
	return &Frame{Command: lines[0], Headers: headers, Body: body}, nil
}

// cutOnce splits s on the first occurrence of sep, like Python's str.partition.
func cutOnce(s, sep string) (before, after string, found bool) {
	idx := strings.Index(s, sep)
	if idx < 0 {
		return s, "", false
	}
	return s[:idx], s[idx+len(sep):], true
}

// readFrame reads one WebSocket message and parses it as a STOMP frame, or
// recognizes it as a heartbeat (a lone LF or CRLF is not a frame with a
// NULL terminator).
func readFrame(conn net.Conn) (*Frame, error) {
	buf := make([]byte, maxFrameSize)
	n, err := conn.Read(buf)
	if err != nil {
		return nil, err
	}
	data := buf[:n]
	if string(data) == "\n" || string(data) == "\r\n" {
		return &Frame{Command: "HEARTBEAT", Headers: map[string]string{}}, nil
	}
	return parseFrame(data)
}
