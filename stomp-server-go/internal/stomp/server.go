package stomp

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	ackAuto             = "auto"
	ackClient           = "client"
	ackClientIndividual = "client-individual"
)

// readTimeout mirrors the Python server's fixed 90s idle timeout: if no
// frame arrives within this window, send a bare heartbeat and keep waiting,
// rather than negotiating the client's requested heart-beat interval.
const readTimeout = 90 * time.Second

// pendingMessage is a MESSAGE this server delivered under a non-auto ack
// mode and hasn't yet been resolved by an ACK or NACK.
type pendingMessage struct {
	messageID   string
	destination string
	body        string
	attempts    int
}

// subscription is one SUBSCRIBE on a session.
type subscription struct {
	destination string
	ack         string
	pending     map[string]*pendingMessage
	order       []string // ack ids in delivery order, for cumulative (ack:client) resolution
}

// session is the per-connection state the Python server keeps in its
// `state` dict: this connection's subscriptions, and a flat index from
// ack id to the subscription it belongs to.
type session struct {
	conn          net.Conn
	writeMu       sync.Mutex // serializes writes to conn (gorilla/websocket allows only one concurrent writer)
	subscriptions map[string]*subscription
	ackIndex      map[string]string // ack id -> subscription id

	// everSubscribed accumulates every destination this connection has ever
	// held a live subscription to, and -- unlike subscriptions -- is never
	// shrunk by UNSUBSCRIBE. It exists purely so OnDisconnect can still
	// report a destination the client explicitly unsubscribed from earlier
	// in the connection's life: subscriptions alone would have already lost
	// that destination by the time the connection actually closes.
	everSubscribed map[string]bool
}

func newSession(conn net.Conn) *session {
	return &session{
		conn:           conn,
		subscriptions:  map[string]*subscription{},
		ackIndex:       map[string]string{},
		everSubscribed: map[string]bool{},
	}
}

// Server is a STOMP 1.2 server implementing the same subset of the
// protocol as stomp-server/server.py: CONNECT/STOMP, SUBSCRIBE (ack modes
// auto/client/client-individual), SEND, ACK, NACK, DISCONNECT. There is no
// persistence: subscriptions and pending (unacknowledged) messages live
// only in memory for the lifetime of a connection, same as the Python
// implementation.
//
// mu guards all shared state below (sessions, subscriptions, and every
// session's subscriptions/ackIndex/pending maps). It is held across a full
// dispatch/publish operation but never across a network write (see send),
// so one slow client can't stall delivery to everyone else. This trades
// some concurrency for the same simplicity the Python server gets for free
// from being single-threaded asyncio; that's an intentional match to this
// repo's "small, readable components over throughput" design.
type Server struct {
	mu            sync.Mutex
	sessions      map[net.Conn]*session
	subscriptions map[string]map[net.Conn]bool // destination -> set of subscribed conns
	log           *slog.Logger
	onSubscribe   []func(headers map[string]string)
	onUnsubscribe []func(headers map[string]string)
	onAck         []func(headers map[string]string)
	onDisconnect  []func(headers map[string]string)
}

func NewServer(log *slog.Logger) *Server {
	return &Server{
		sessions:      map[net.Conn]*session{},
		subscriptions: map[string]map[net.Conn]bool{},
		log:           log,
	}
}

// StompServer is the surface an application layer built on top of this
// STOMP server is allowed to depend on: publishing to a destination, and
// reacting to subscriptions. It exists so that code like helloworld.go
// never reaches into Server's internal session/subscription bookkeeping.
type StompServer interface {
	// Publish delivers body, JSON-encoded, as a MESSAGE to every session
	// currently subscribed to destination. If messageID is empty, one is
	// generated.
	Publish(destination string, messageID string, body any) error

	// OnSubscribe registers fn to be called after every successful
	// SUBSCRIBE, with the full SUBSCRIBE frame's headers (destination, id,
	// ack, and any custom headers a client sent) so callers aren't limited
	// to whatever fields this server happens to pass through today. fn runs
	// synchronously on the subscribing client's connection goroutine, so
	// it must not block or call back into the server while holding a lock.
	OnSubscribe(fn func(headers map[string]string))

	// OnUnsubscribe registers fn to be called after every successful
	// UNSUBSCRIBE, i.e. one the given id actually matched a live subscription
	// on that connection. fn receives a headers map with "destination" set to
	// that subscription's destination and "id" set to the UNSUBSCRIBE's id
	// header. Unlike OnDisconnect, this fires only for an explicit UNSUBSCRIBE
	// while the connection otherwise stays open; a connection closing (with
	// or without prior UNSUBSCRIBEs) is OnDisconnect's concern, not this
	// one's. fn runs synchronously on the unsubscribing client's connection
	// goroutine, so (like OnSubscribe/OnAck) it must not block or call back
	// into the server while holding a lock.
	OnUnsubscribe(fn func(headers map[string]string))

	// OnAck registers fn to be called once for every pending message this
	// server resolves via a client's ACK. A single cumulative ack:client ACK
	// can resolve more than one message; fn is called once per message
	// resolved. fn receives a headers map mirroring the resolved MESSAGE's
	// fields: "destination", "message-id", "subscription" (the SUBSCRIBE's
	// id header), and "ack" (the ack id the client just acknowledged). fn
	// runs synchronously on the acking client's connection goroutine, so
	// (like OnSubscribe) it must not block or call back into the server
	// while holding a lock.
	OnAck(fn func(headers map[string]string))

	// OnDisconnect registers fn to be called once for every destination the
	// ending connection ever held a live subscription to -- including one it
	// already UNSUBSCRIBEd from earlier in its life, not just what's still
	// subscribed at close -- right as its session is torn down. That end may
	// be a client-sent DISCONNECT, a network error, or the connection being
	// closed via Disconnect/SendError+Disconnect. fn receives a headers map
	// with "destination" set to that destination. A connection that never
	// subscribed to anything produces no calls. fn runs synchronously on
	// that connection's own goroutine, so (like OnSubscribe/OnAck) it must
	// not block or call back into the server while holding a lock.
	OnDisconnect(fn func(headers map[string]string))

	// SendError sends an ERROR frame carrying message to every session
	// currently subscribed to destination, without closing the connection.
	// It is a no-op if none is. Typically paired with a following Disconnect
	// call, so a client being dropped for misbehaving gets a reason first.
	SendError(destination string, message string) error

	// Disconnect closes every session currently subscribed to destination.
	// It is a no-op if none is.
	Disconnect(destination string) error

	// Connected reports whether any session is currently subscribed to
	// destination.
	Connected(destination string) bool
}

var _ StompServer = (*Server)(nil)

// OnSubscribe implements StompServer.
func (s *Server) OnSubscribe(fn func(headers map[string]string)) {
	s.mu.Lock()
	s.onSubscribe = append(s.onSubscribe, fn)
	s.mu.Unlock()
}

// OnUnsubscribe implements StompServer.
func (s *Server) OnUnsubscribe(fn func(headers map[string]string)) {
	s.mu.Lock()
	s.onUnsubscribe = append(s.onUnsubscribe, fn)
	s.mu.Unlock()
}

// OnAck implements StompServer.
func (s *Server) OnAck(fn func(headers map[string]string)) {
	s.mu.Lock()
	s.onAck = append(s.onAck, fn)
	s.mu.Unlock()
}

// OnDisconnect implements StompServer.
func (s *Server) OnDisconnect(fn func(headers map[string]string)) {
	s.mu.Lock()
	s.onDisconnect = append(s.onDisconnect, fn)
	s.mu.Unlock()
}

// SendError implements StompServer.
func (s *Server) SendError(destination string, message string) error {
	s.mu.Lock()
	var sessions []*session
	for conn := range s.subscriptions[destination] {
		if sess, ok := s.sessions[conn]; ok {
			sessions = append(sessions, sess)
		}
	}
	s.mu.Unlock()

	var errs []error
	for _, sess := range sessions {
		if err := s.send(sess, NewFrame("ERROR", map[string]string{"message": message}, message)); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Disconnect implements StompServer. Closing conn unblocks that
// connection's blocked read in handleConn's reader goroutine, which drives
// handleConn's own deferred cleanup (removing it from sessions and
// subscriptions) the same way any other disconnect does.
func (s *Server) Disconnect(destination string) error {
	s.mu.Lock()
	conns := make([]net.Conn, 0, len(s.subscriptions[destination]))
	for conn := range s.subscriptions[destination] {
		conns = append(conns, conn)
	}
	s.mu.Unlock()

	var errs []error
	for _, conn := range conns {
		if err := conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Connected implements StompServer.
func (s *Server) Connected(destination string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.subscriptions[destination]) > 0
}

// frameDump renders a frame's wire format for logging, with the NUL
// terminator spelled out so it's visible in a terminal.
func frameDump(frame *Frame) string {
	return strings.ReplaceAll(string(frame.Encode()), "\x00", "<NUL>")
}

// Serve accepts connections from l and handles each on its own goroutine,
// until l.Accept() returns an error (e.g. the listener was closed).
func (s *Server) Serve(l net.Listener) error {
	for {
		conn, err := l.Accept()
		if err != nil {
			return err
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	s.log.Info("HTTP Upgrade accepted; WebSocket transport established")
	sess := newSession(conn)
	s.mu.Lock()
	s.sessions[conn] = sess
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		for _, sub := range sess.subscriptions {
			if conns, ok := s.subscriptions[sub.destination]; ok {
				delete(conns, conn)
				if len(conns) == 0 {
					delete(s.subscriptions, sub.destination)
				}
			}
		}
		delete(s.sessions, conn)
		destinations := make([]string, 0, len(sess.everSubscribed))
		for d := range sess.everSubscribed {
			destinations = append(destinations, d)
		}
		callbacks := append([]func(map[string]string){}, s.onDisconnect...)
		s.mu.Unlock()

		for _, d := range destinations {
			headers := map[string]string{"destination": d}
			for _, cb := range callbacks {
				cb(headers)
			}
		}

		conn.Close()
	}()

	// gorilla/websocket's Conn treats every error NextReader returns --
	// including a plain read-deadline timeout -- as permanent: once one
	// occurs, every later ReadMessage call replays it without doing any I/O,
	// eventually panicking ("repeated read on failed websocket connection").
	// So the read loop below may never retry a read after an error; the
	// periodic idle heartbeat instead runs on a ticker in a separate
	// goroutine, fed by one single, un-retried blocking read.
	frames := make(chan *Frame, 1)
	readErrs := make(chan error, 1)
	go func() {
		for {
			frame, err := readFrame(conn)
			if err != nil {
				readErrs <- err
				return
			}
			frames <- frame
		}
	}()

	ticker := time.NewTicker(readTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := s.sendBytes(sess, []byte("\n")); err != nil {
				return
			}
			s.log.Debug("SERVER -> CLIENT", "command", "HEARTBEAT")

		case err := <-readErrs:
			var protoErr *ProtocolError
			if errors.As(err, &protoErr) {
				s.send(sess, NewFrame("ERROR", map[string]string{"message": protoErr.Error()}, protoErr.Error()))
			}
			s.log.Debug("CLIENT disconnected", "error", err, "subscriptions", len(sess.subscriptions))
			// otherwise: connection closed, or some other I/O error; clean up via defer
			return

		case frame := <-frames:
			ticker.Reset(readTimeout)
			if frame.Command == "HEARTBEAT" {
				s.log.Debug("CLIENT -> SERVER", "command", "HEARTBEAT")
				continue
			}
			s.log.Info("CLIENT -> SERVER", "command", frame.Command)
			s.log.Debug("CLIENT -> SERVER", "frame", frameDump(frame))
			if err := s.dispatch(sess, frame); err != nil {
				s.log.Error("command failed", "error", err)
				s.send(sess, NewFrame("ERROR", map[string]string{"message": err.Error()}, err.Error()))
			}
		}
	}
}

func (s *Server) sendBytes(sess *session, data []byte) error {
	sess.writeMu.Lock()
	defer sess.writeMu.Unlock()
	_, err := sess.conn.Write(data)
	return err
}

func (s *Server) send(sess *session, frame *Frame) error {
	err := s.sendBytes(sess, frame.Encode())
	if err == nil {
		s.log.Info("SERVER -> CLIENT", "command", frame.Command)
		s.log.Debug("SERVER -> CLIENT", "frame", frameDump(frame))
	}
	return err
}

func (s *Server) dispatch(sess *session, frame *Frame) error {
	switch frame.Command {
	case "CONNECT", "STOMP":
		return s.handleConnect(sess, frame)
	case "SUBSCRIBE":
		return s.handleSubscribe(sess, frame)
	case "UNSUBSCRIBE":
		return s.handleUnsubscribe(sess, frame)
	case "ACK":
		return s.handleAck(sess, frame)
	case "NACK":
		return s.handleNack(sess, frame)
	case "DISCONNECT":
		return s.handleDisconnect(sess, frame)
	case "SEND":
		return s.handleSend(sess, frame)
	default:
		return protocolErrorf("unsupported command: %s", frame.Command)
	}
}

func (s *Server) handleConnect(sess *session, frame *Frame) error {
	return s.send(sess, NewFrame("CONNECTED", map[string]string{
		"version": "1.2",
		"session": uuid.NewString(),
		"server":  "stomp-playground",
	}, ""))
}

func (s *Server) handleSubscribe(sess *session, frame *Frame) error {
	destination := frame.Headers["destination"]
	subID := frame.Headers["id"]
	if destination == "" || subID == "" {
		return protocolErrorf("SUBSCRIBE requires destination and id")
	}
	ackMode := frame.Headers["ack"]
	if ackMode == "" {
		ackMode = ackAuto
	}
	if ackMode != ackAuto && ackMode != ackClient && ackMode != ackClientIndividual {
		return protocolErrorf("unsupported ack mode: %s", ackMode)
	}

	s.mu.Lock()
	sess.subscriptions[subID] = &subscription{destination: destination, ack: ackMode, pending: map[string]*pendingMessage{}}
	sess.everSubscribed[destination] = true
	if s.subscriptions[destination] == nil {
		s.subscriptions[destination] = map[net.Conn]bool{}
	}
	s.subscriptions[destination][sess.conn] = true
	callbacks := append([]func(map[string]string){}, s.onSubscribe...)
	s.mu.Unlock()

	for _, cb := range callbacks {
		cb(frame.Headers)
	}

	if receipt := frame.Headers["receipt"]; receipt != "" {
		return s.send(sess, NewFrame("RECEIPT", map[string]string{"receipt-id": receipt}, ""))
	}
	return nil
}

// handleUnsubscribe removes one subscription from sess, cleaning up both the
// server-wide destination index and any of that subscription's pending
// (unacked) messages from sess.ackIndex -- left behind, a stale ackIndex
// entry would make a later ACK/NACK on it look up a subscription that no
// longer exists and panic on the nil *subscription.
func (s *Server) handleUnsubscribe(sess *session, frame *Frame) error {
	subID := frame.Headers["id"]
	if subID == "" {
		return protocolErrorf("UNSUBSCRIBE requires id")
	}

	s.mu.Lock()
	sub, ok := sess.subscriptions[subID]
	if !ok {
		s.mu.Unlock()
		return protocolErrorf("unknown subscription id: %s", subID)
	}
	destination := sub.destination
	delete(sess.subscriptions, subID)
	for ackID := range sub.pending {
		delete(sess.ackIndex, ackID)
	}

	// A connection can hold more than one subscription to the same
	// destination (different ids); only drop the destination-level entry
	// once none of them remain.
	stillSubscribed := false
	for _, other := range sess.subscriptions {
		if other.destination == destination {
			stillSubscribed = true
			break
		}
	}
	if !stillSubscribed {
		if conns, ok := s.subscriptions[destination]; ok {
			delete(conns, sess.conn)
			if len(conns) == 0 {
				delete(s.subscriptions, destination)
			}
		}
	}
	callbacks := append([]func(map[string]string){}, s.onUnsubscribe...)
	s.mu.Unlock()

	headers := map[string]string{"destination": destination, "id": subID}
	for _, cb := range callbacks {
		cb(headers)
	}

	if receipt := frame.Headers["receipt"]; receipt != "" {
		return s.send(sess, NewFrame("RECEIPT", map[string]string{"receipt-id": receipt}, ""))
	}
	return nil
}

func (s *Server) handleSend(sess *session, frame *Frame) error {
	destination := frame.Headers["destination"]
	if destination == "" {
		return protocolErrorf("SEND requires destination")
	}
	var body any
	if err := json.Unmarshal([]byte(frame.Body), &body); err != nil {
		body = map[string]any{"value": frame.Body}
	}
	return s.Publish(destination, "", body)
}

// delivery is a MESSAGE frame ready to send, prepared while s.mu was held.
type delivery struct {
	sess  *session
	frame *Frame
}

// Publish implements StompServer.
func (s *Server) Publish(destination string, messageID string, body any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	if messageID == "" {
		messageID = uuid.NewString()
	}

	s.mu.Lock()
	var deliveries []delivery
	for conn := range s.subscriptions[destination] {
		sess, ok := s.sessions[conn]
		if !ok {
			continue
		}
		for subID, sub := range sess.subscriptions {
			if sub.destination == destination {
				frame := s.prepareDelivery(subID, sess, sub, messageID, destination, string(payload), 0)
				deliveries = append(deliveries, delivery{sess, frame})
			}
		}
	}
	s.mu.Unlock()

	for _, d := range deliveries {
		if err := s.send(d.sess, d.frame); err != nil {
			s.log.Error("failed to deliver MESSAGE", "error", err)
		}
	}
	return nil
}

// prepareDelivery must be called with s.mu held. It registers the message
// as pending (for non-auto ack modes) and returns the MESSAGE frame to send
// once the lock is released.
func (s *Server) prepareDelivery(subID string, sess *session, sub *subscription, messageID, destination, body string, attempts int) *Frame {
	headers := map[string]string{"subscription": subID, "message-id": messageID, "destination": destination}
	if sub.ack != ackAuto {
		ackID := uuid.NewString()
		headers["ack"] = ackID
		sub.pending[ackID] = &pendingMessage{messageID: messageID, destination: destination, body: body, attempts: attempts}
		sub.order = append(sub.order, ackID)
		sess.ackIndex[ackID] = subID
	}
	return NewFrame("MESSAGE", headers, body)
}

type resolvedMessage struct {
	ackID string
	info  *pendingMessage
}

// resolve removes and returns the pending messages an ACK/NACK on ackID
// resolves, and must be called with s.mu held.
//
// client-individual resolves only ackID; client resolves every message
// delivered at or before ackID on this subscription (cumulative), per
// STOMP 1.2.
func resolve(sess *session, sub *subscription, ackID string) []resolvedMessage {
	var resolved []resolvedMessage
	if sub.ack == ackClient {
		for len(sub.order) > 0 {
			id := sub.order[0]
			sub.order = sub.order[1:]
			info, ok := sub.pending[id]
			delete(sub.pending, id)
			delete(sess.ackIndex, id)
			if ok {
				resolved = append(resolved, resolvedMessage{id, info})
			}
			if id == ackID {
				break
			}
		}
	} else if info, ok := sub.pending[ackID]; ok {
		delete(sub.pending, ackID)
		delete(sess.ackIndex, ackID)
		for i, id := range sub.order {
			if id == ackID {
				sub.order = append(sub.order[:i], sub.order[i+1:]...)
				break
			}
		}
		resolved = append(resolved, resolvedMessage{ackID, info})
	}
	return resolved
}

func (s *Server) handleAck(sess *session, frame *Frame) error {
	ackID := frame.Headers["id"]
	if ackID == "" {
		return protocolErrorf("ACK requires id")
	}

	s.mu.Lock()
	subID, ok := sess.ackIndex[ackID]
	if !ok {
		s.mu.Unlock()
		return protocolErrorf("unknown ack id: %s", ackID)
	}
	resolved := resolve(sess, sess.subscriptions[subID], ackID)
	callbacks := append([]func(map[string]string){}, s.onAck...)
	s.mu.Unlock()

	for _, r := range resolved {
		headers := map[string]string{
			"destination":  r.info.destination,
			"message-id":   r.info.messageID,
			"subscription": subID,
			"ack":          r.ackID,
		}
		for _, cb := range callbacks {
			cb(headers)
		}
	}

	if receipt := frame.Headers["receipt"]; receipt != "" {
		return s.send(sess, NewFrame("RECEIPT", map[string]string{"receipt-id": receipt}, ""))
	}
	return nil
}

func (s *Server) handleNack(sess *session, frame *Frame) error {
	ackID := frame.Headers["id"]
	if ackID == "" {
		return protocolErrorf("NACK requires id")
	}

	s.mu.Lock()
	subID, ok := sess.ackIndex[ackID]
	if !ok {
		s.mu.Unlock()
		return protocolErrorf("unknown ack id: %s", ackID)
	}
	sub := sess.subscriptions[subID]
	resolved := resolve(sess, sub, ackID)

	var redeliveries []*Frame
	for _, r := range resolved {
		if r.info.attempts >= 1 {
			s.log.Warn("dropping message after redelivery was also nacked", "message_id", r.info.messageID)
			continue
		}
		redeliveries = append(redeliveries, s.prepareDelivery(subID, sess, sub, r.info.messageID, r.info.destination, r.info.body, r.info.attempts+1))
	}
	s.mu.Unlock()

	for _, f := range redeliveries {
		if err := s.send(sess, f); err != nil {
			return err
		}
	}

	if receipt := frame.Headers["receipt"]; receipt != "" {
		return s.send(sess, NewFrame("RECEIPT", map[string]string{"receipt-id": receipt}, ""))
	}
	return nil
}

func (s *Server) handleDisconnect(sess *session, frame *Frame) error {
	if receipt := frame.Headers["receipt"]; receipt != "" {
		if err := s.send(sess, NewFrame("RECEIPT", map[string]string{"receipt-id": receipt}, "")); err != nil {
			return err
		}
	}
	return sess.conn.Close()
}
