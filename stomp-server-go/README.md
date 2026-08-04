# stomp-server-go

A STOMP 1.2 server exposed over WebSocket, built almost entirely from
`github.com/go-stomp/stomp/v3`'s own `server` package rather than
reimplementing frame parsing, connection state, or pub/sub dispatch:

- `github.com/go-stomp/stomp/v3/frame` — frame parsing/encoding, header
  escaping, content-length handling, the NULL terminator, and the heartbeat
  LF convention.
- `github.com/go-stomp/stomp/v3/server` (`Server.Serve`) — the entire CONNECT
  negotiation, heartbeat read/write timeout enforcement, SUBSCRIBE/UNSUBSCRIBE,
  SEND, ACK/NACK, transactions (BEGIN/COMMIT/ABORT), and DISCONNECT state
  machine (`server/client.Conn`), plus destination routing into topics
  (`server/topic`, broadcast) or queues (`server/queue`, competing consumers
  backed by the library's own in-memory `MemoryQueueStorage`).

The only code in this repo is `wsconn.go` and `wslistener.go`: a `net.Conn`
adapter around a `gorilla/websocket` connection, and a `net.Listener` whose
`Accept()` is fed by an `http.Handler` doing the WebSocket upgrade. That's
the "custom websocket communication layer" needed because go-stomp's server
was written for raw TCP; everything above the `net.Conn` boundary is
unmodified library code. `main.go` wires an `http.Server` running the
upgrade handler into `stompserver.Server{}.Serve(listener)`.

`Server{}` is constructed with every field at its zero value on purpose:
nil `QueueStorage` defaults to `queue.MemoryQueueStorage` (in-memory, not
persisted to disk), nil `Authenticator` accepts any/no login (the existing
clients send none), a zero `HeartBeat` falls back to
`server.DefaultHeartBeat`, and nil `Log` makes `Server.Serve` install
go-stomp's own default logger.

## Incompatibilities with `stomp-client`

`stomp-client/protocol.py` and `stomp-server/*.py` share a hand-rolled
extension to STOMP 1.2: every event gets a server-assigned integer
`sequence` header (independent of `message-id`), `SUBSCRIBE` carries a
`last-sequence` application header so a reconnecting client replays
everything it missed from a durable SQLite log, and a non-standard `DEMO`
command drives fault-injection for the teaching UI. None of that has an
equivalent in go-stomp's `server` package, and there is no supported way to
bolt it on without forking the library — `server/client.Conn` is a closed
state machine (unexported fields/methods, no hooks), so using it as-is
means accepting its own opinions about message identity, acknowledgement,
and delivery instead. Concretely, verified against this implementation:

1. **No durable replay / no `sequence` header at all.** go-stomp has no
   concept of a persisted per-destination event log or "replay everything
   after sequence N." `server.QueueStorage` only buffers messages that
   haven't been delivered yet (`Enqueue`/`Dequeue`/`Requeue`); once a
   message is delivered it is gone, and topics have no storage whatsoever.
   `SUBSCRIBE`'s `last-sequence` header is simply ignored (there's no hook
   into `handleSubscribe` to read it), and `MESSAGE` frames never carry a
   `sequence` header. **This breaks the existing client outright**:
   `client.py`'s `on_message` does `int(frame.headers["sequence"])`
   unconditionally, which raises `KeyError` on the first message received
   over this server.

2. **Destination routing forks on a hardcoded `/queue` prefix, and the two
   branches are both wrong for this client in different ways.** Anything
   not prefixed `/queue` is a *topic*: broadcast to every current
   subscriber (right fan-out semantics for a destination like
   `/origin/demo`), but `Conn.allocateMessageId` unconditionally strips the
   `ack` header for topic deliveries — **topics are always auto-ack,
   regardless of the subscription's requested `ack` mode.** Confirmed by
   probing this server: a `client-individual` subscription to
   `/origin/demo` receives `MESSAGE` frames with no `ack` header at all.
   `client.py` reads `frame.headers["ack"]` directly when building its
   `ACK` frame, so it would crash the same way `sequence` does. Prefixing
   the destination with `/queue/` does make the `ack` header appear (queues
   track per-message acknowledgement via `SubscriptionList`), but queues are
   **competing-consumer**: each message goes to exactly one subscriber,
   confirmed above by two simultaneous subscribers to `/queue/origin/demo`
   each receiving a different message rather than both receiving both —
   the wrong fan-out for multiple independent clients following the same
   feed.

3. **`message-id` is a per-connection auto-incrementing `uint64`, not a
   UUID**, and `ACK`/`NACK` (`handleAck`/`handleNack` in
   `server/client/conn.go`) require it to parse as `uint64`
   (`strconv.ParseUint(id, 10, 64)`). The existing server assigns a UUID
   `message-id` and a separately-tracked integer `sequence`; nothing in
   go-stomp lets message identity be supplied externally.

4. **The `DEMO` command is rejected outright.** `frame.Reader.Read()`
   validates the command against a fixed whitelist (`CONNECT`, `SEND`,
   `SUBSCRIBE`, `ACK`, `NACK`, ... — see `frame/reader.go`) and returns
   `ErrInvalidCommand` for anything else, so the fault-injection frames the
   browser teaching UI sends (`drop-next`, `reorder-next`,
   `restart-server`) can't reach the server at all through this code path.
   There is also no extension point in `server/client.Conn`'s state
   functions to special-case an unknown command even if the frame layer
   allowed it through.

5. **No per-message fault injection hooks.** The Python server's
   `DROP_EVENT_PERCENT` and `SERVER_RESTART` env vars work by intercepting
   individual `MESSAGE` deliveries in its own dispatch loop. `server/client.Conn`
   owns delivery internally (via unexported `writeChannel`/`subChannel`
   goroutines) with no callback or middleware seam, so equivalent behavior
   isn't reachable without forking the connection type.

6. **`CONNECTED` doesn't echo a `session` header.** The Python server sets
   `session` to the client's `client-id` (a non-standard reuse of that
   header); go-stomp's `handleConnect` only ever sets `version`, `server`,
   and `heart-beat`. Harmless here since `stomp-client` never reads
   `session`, but worth noting as another point where the two servers'
   `CONNECTED` frames differ.

What *does* carry over cleanly: STOMP version negotiation, real bidirectional
heartbeat enforcement (this is stricter than the Python server, which just
fires a blind heartbeat on a timer without ever checking whether the peer
is still alive), and the base frame wire format (escaping, NULL terminator,
content-length).

## Running

```bash
go run . --port 8080
```

or via Docker:

```bash
docker build -t stomp-server-go .
docker run -p 8080:8080 stomp-server-go
```

Point a client at `/queue/<name>` destinations to exercise real
acknowledgement tracking (competing-consumer delivery), or any other
destination to get broadcast delivery with silent auto-ack. Neither matches
`stomp-client` without changes to the client itself.
