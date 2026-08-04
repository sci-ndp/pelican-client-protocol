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

## Compatibility with `stomp-client`

`stomp-client`/`stomp-server` used to share a hand-rolled extension to
STOMP 1.2 (a `sequence` header, a `last-sequence` replay header, a
non-standard `DEMO` fault-injection command) that had no equivalent in
go-stomp's `server` package and would have broken outright against it.
That extension has since been removed from both Python components in favor
of plain STOMP 1.2 semantics, which is what go-stomp already implements, so
`stomp-client` now runs against this server unmodified. One real gap
remains, verified by running the two together (`docker compose up`, see the
top-level `docker-compose.yml`):

- **Topic deliveries are always auto-ack, regardless of the requested `ack`
  mode.** Destination routing forks on a hardcoded `/queue` prefix.
  Anything not prefixed `/queue` is a *topic* — the right fan-out for a
  broadcast destination like `/origin/demo`, since every subscriber gets
  every message — but `Conn.allocateMessageId` unconditionally strips the
  `ack` header for topic deliveries. So a `client-individual` subscription
  to `/origin/demo` still gets `MESSAGE` frames with no `ack` header at
  all, and `stomp-client`'s `on_message` only sends `ACK` when that header
  is present, which is what lets it run against both servers. Prefixing a
  destination with `/queue/` does make `ack` appear (queues track
  per-message acknowledgement via `SubscriptionList`), but queues are
  **competing-consumer**: each message goes to exactly one subscriber, the
  wrong fan-out for multiple independent clients following the same feed.
  There's no supported way to get topic fan-out with per-subscriber ack
  tracking out of `server/client.Conn` — it's a closed state machine
  (unexported fields/methods, no hooks) — so as long as `stomp-client`
  subscribes to a plain topic destination, it will never see real ack/nack
  round-trips against this server; that only works against `stomp-server`
  (the Python implementation), which honors the requested `ack` mode.

- **`message-id` is a per-connection auto-incrementing `uint64`, not a
  UUID**, and `ACK`/`NACK` require it to parse as `uint64`. Harmless for
  `stomp-client`, which treats `message-id` and `ack` as opaque strings and
  never parses them.

- **`CONNECTED` doesn't echo a `session` header** (`handleConnect` only
  ever sets `version`, `server`, and `heart-beat`). Harmless since
  `stomp-client` never reads `session`.

What carries over cleanly: STOMP version negotiation, real bidirectional
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

The top-level `docker-compose.yml` runs this server against the unmodified
`stomp-client` and `event-generator` on the broadcast destination
`/origin/demo` (silent auto-ack, per the topic/queue note above). Point a
client at a `/queue/<name>` destination instead to exercise real
per-subscriber acknowledgement tracking (competing-consumer delivery).
