package main

import (
	"encoding/json"
	"errors"
	"log"
	"net"
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
}

func newSession(conn net.Conn) *session {
	return &session{
		conn:          conn,
		subscriptions: map[string]*subscription{},
		ackIndex:      map[string]string{},
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
}

func newServer() *Server {
	return &Server{
		sessions:      map[net.Conn]*session{},
		subscriptions: map[string]map[net.Conn]bool{},
	}
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
	log.Println("HTTP Upgrade accepted; WebSocket transport established")
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
		s.mu.Unlock()
		conn.Close()
	}()

	// gorilla/websocket permanently poisons a connection after any failed
	// read -- including one that only failed because a SetReadDeadline
	// expired -- so a second read on the same Conn panics with "repeated
	// read on failed websocket connection". That rules out the obvious
	// SetReadDeadline-plus-retry-loop approach to idle-heartbeat timing.
	// Instead a dedicated goroutine reads with no deadline at all, and the
	// loop below waits on either a frame or an idle timer, which is what
	// actually mirrors the Python server's
	// `asyncio.wait_for(read_frame(...), timeout=90)`: that cancels at the
	// coroutine level without ever touching the underlying socket's read
	// state, so it can be retried indefinitely.
	frames := make(chan readResult, 1)
	go func() {
		for {
			frame, err := readFrame(conn)
			frames <- readResult{frame, err}
			if err != nil {
				return
			}
		}
	}()

	for {
		select {
		case res := <-frames:
			if res.err != nil {
				var protoErr *ProtocolError
				if errors.As(res.err, &protoErr) {
					s.send(sess, NewFrame("ERROR", map[string]string{"message": res.err.Error()}, res.err.Error()))
				}
				// connection closed, or some other I/O error: clean up via defer
				return
			}
			frame := res.frame
			if frame.Command == "HEARTBEAT" {
				continue
			}
			log.Printf("CLIENT -> SERVER  %s", frame.Command)
			if err := s.dispatch(sess, frame); err != nil {
				log.Printf("command failed: %v", err)
				s.send(sess, NewFrame("ERROR", map[string]string{"message": err.Error()}, err.Error()))
			}
		case <-time.After(readTimeout):
			if err := s.sendBytes(sess, []byte("\n")); err != nil {
				return
			}
		}
	}
}

type readResult struct {
	frame *Frame
	err   error
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
		log.Printf("SERVER -> CLIENT  %s", frame.Command)
	}
	return err
}

func (s *Server) dispatch(sess *session, frame *Frame) error {
	switch frame.Command {
	case "CONNECT", "STOMP":
		return s.handleConnect(sess, frame)
	case "SUBSCRIBE":
		return s.handleSubscribe(sess, frame)
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
	if s.subscriptions[destination] == nil {
		s.subscriptions[destination] = map[net.Conn]bool{}
	}
	s.subscriptions[destination][sess.conn] = true
	s.mu.Unlock()

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
	return s.publishEvent(destination, body)
}

// delivery is a MESSAGE frame ready to send, prepared while s.mu was held.
type delivery struct {
	sess  *session
	frame *Frame
}

func (s *Server) publishEvent(destination string, body any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	messageID := uuid.NewString()

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
			log.Printf("failed to deliver MESSAGE: %v", err)
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
	resolve(sess, sess.subscriptions[subID], ackID)
	s.mu.Unlock()

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
			log.Printf("dropping message-id=%s after redelivery was also nacked", r.info.messageID)
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
