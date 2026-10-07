# stomp-server-go

A Go implementation of a STOMP-over-WebSockets message broker, plus a small
application layer built on top of it: **messagequeue**, a durable, per-client
event queue with ack-gated, backed-off retry delivery.

## Key design points

- **A custom STOMP-over-WebSockets message broker.** `internal/stomp` is a
  self-contained STOMP 1.2 server (`CONNECT`/`SUBSCRIBE`/`SEND`/`ACK`/`NACK`/
  `DISCONNECT`, ack modes `auto`/`client`/`client-individual`) running over a
  `gorilla/websocket` transport. Applications never reach into its internal
  session/subscription state; they depend only on the narrow `StompServer`
  interface it exposes (`Publish`, `OnSubscribe`, `OnUnsubscribe`, `OnAck`,
  `OnDisconnect`, `SendError`, `Disconnect`, `Connected`).
- **Notifies connected clients of events from a configurable event source.**
  The messagequeue app doesn't generate events itself -- it fans out
  whatever an injected `EventSource` produces to every subscribed client's
  queue. Swapping the event source (a demo ticker, a restart-durable
  sequence counter, or a Pelican federation directory watcher) changes what
  clients are notified about without touching any delivery, retry, or
  persistence logic.
- **File-backed event queue and retry-with-acknowledgement, so delivery
  survives both client and server restarts.** Each client's queue can be
  backed by SQLite instead of memory, so undelivered events, and the record
  of which clients exist at all, survive a server restart. Every delivered
  message is redelivered on a growing backoff until the client sends an
  `ACK`; a client that reconnects with the same `subscription` header resumes
  its queue from wherever it left off, instead of restarting from scratch.

## Package layout

```text
cmd/
  helloworld/       minimal demo app: broadcasts "Hello, World" to every subscriber
  messagequeue/     the real application -- see below. Contains only main.go;
                     all its logic lives in the internal/ packages below.
internal/
  stomp/            the STOMP-over-WebSockets broker (transport, protocol, auth)
  clientqueue/      the Queue interface + in-memory and SQLite implementations
  eventsource/      the EventSource interface + ticker/seq-file/Pelican implementations
  messagequeue/     the App: wires a StompServer, a clientqueue.Factory, and an
                     EventSource together into the actual message-queue behavior
```

Dependencies flow one way, with no cycles: `clientqueue` knows nothing about
`eventsource` or `messagequeue`; `eventsource` depends on `clientqueue` (an
`EventSource` needs to inspect and track `clientqueue.Queue` values); and
`messagequeue` depends on both of those plus `internal/stomp`.

## The messagequeue app

Each client picks a unique `subscription` header when it `SUBSCRIBE`s. The
app creates (or resumes) a durable, ordered event queue for that header,
independent of the destination the client subscribed to. Events from the
configured `EventSource` are fanned out to every registered queue (an
`EventSource` can filter this per client via `ShouldNotify`, based on that
client's subscription parameters -- see the Pelican source below). Each
queue is drained one message at a time:

- A delivered message is redelivered if unacked, starting at a 1 second
  delay and multiplying by 1.2x each retry, up to 16 attempts, after which
  the client is disconnected.
- A queue that grows past 100 undelivered events is trimmed; if the client
  is currently connected when that happens, it's disconnected instead of
  silently losing events it might expect to see.
- A queue survives across reconnects: resubscribing with the same
  `subscription` header resumes delivery from where it left off. Resubscribing
  with **different** subscription parameters, without ever sending
  `UNSUBSCRIBE` first, is treated exactly like an explicit unsubscribe
  followed by a fresh subscribe -- the old queue (and any durable rows
  backing it) is discarded rather than silently kept around under stale
  parameters.

Prometheus metrics (subscriptions, queue depth, retries, acks, ack latency,
disconnect reasons, queue errors, ...) are mounted at `/metrics` on the same
HTTP server as the WebSocket endpoint.

### Queue backends (`internal/clientqueue`)

| Backend | Selected by | Survives restart? |
| --- | --- | --- |
| In-memory (`MemoryQueue`) | default | No |
| SQLite (`sqliteQueue`) | `--queue-db <path>` | Yes |

The SQLite backend keeps two tables: `queue_events` (undelivered events per
client) and `client_metadata` (each client's subscription parameters). On
startup, `ListPersistedClients` + `App.RehydrateQueues` recreate every
durable client's in-process queue *before* serving any connections, so an
event source that needs to know which clients exist (like the Pelican
watcher) doesn't have to wait for each one to reconnect first.

### Event sources (`internal/eventsource`)

| Source | Selected by | Behavior |
| --- | --- | --- |
| `TickerSource` | default | Emits `"Event <n>"` every 5s; not durable. |
| `SeqFileSource` | `TEST_EVENT_SEQ_FILE=<path>` env var | Same, but persists its counter to disk so a restart doesn't repeat/reset it. |
| `PelicanListingSource` | `--pelican-enabled` | See below. |
| `FSNotifySource` | `--watch-dir <path>` | Emits the file path for every file created or modified under the directory tree (via fsnotify). |
| `S3NotifySource` | `--s3-notify-addr <addr>` | Receives Ceph RadosGW bucket notifications; see below. |

Exactly one of `--watch-dir` / `--s3-notify-addr` must be set; the binary
no longer selects the ticker, seq-file or Pelican sources (they remain in the
package).

#### Ceph S3 notifications

`--s3-notify-addr :8082` starts a separate, **unauthenticated** HTTP listener
serving `POST /s3-events`. Keep it off public networks. RGW pushes JSON
`Records` batches there; each record becomes one event, a JSON string:
`{"event","bucket","key","size","etag","version_id","time","sequencer","event_id"}`.
The server replies 200 only after handing every record to the queueing layer,
so an RGW *persistent* topic redelivers while the server is down. Delivery is
at-least-once with no server-side dedup, and an event accepted just before a
crash but before it reaches client queues can still be lost.

The server does not configure Ceph. An admin creates the topic and bucket
notification (the `docker-compose.yml` `ceph-init` service runs exactly this,
see `deploy/ceph/init.sh`):

```sh
aws --endpoint-url http://<rgw>:<port> sns create-topic --name s3-events \
  --attributes '{"push-endpoint":"http://<server>:8082/s3-events","persistent":"true"}'
aws --endpoint-url http://<rgw>:<port> s3api put-bucket-notification-configuration \
  --bucket <bucket> --notification-configuration \
  '{"TopicConfigurations":[{"Id":"all","TopicArn":"<arn from above>","Events":["s3:ObjectCreated:*","s3:ObjectRemoved:*"]}]}'
```

RGW needs `notifications` in `rgw_enable_apis` (on by default) and network
access to the receiver port. The `docker-compose.yml` stack brings up a
single-node dev Ceph (`ceph`), configures it (`ceph-init`) and uploads a
random object every few seconds (`s3-writer`); S3 credentials, bucket and
write interval can be overridden via `S3_ACCESS_KEY`, `S3_SECRET_KEY`,
`S3_BUCKET`, `S3_WRITE_INTERVAL`.

`PelicanListingSource` discovers which Pelican federation directories to
watch dynamically, from each currently-tracked client queue's subscription
parameters (`<protocol>/<object path>`, e.g. `osdf/vdc/public/pelican_protocol`)
rather than one directory fixed at startup. It polls the union of those
directories on a fixed interval via the `pelican` CLI, diffs each
directory's listing against what it last saw (persisted to
`--pelican-state-file` so a restart doesn't re-announce every pre-existing
file as new), and emits one event per newly-appeared file. `ShouldNotify`
then filters delivery so a client only ever sees events for its own
directory. Run with `--debug` to see per-poll directory sets and per-directory
new-file counts.

## Default configuration: SQLite queue + Pelican listing source

The top-level `docker-compose.yml` in the repo root wires the messagequeue
app up with **both** durability features enabled by default: a SQLite-backed
client queue and the Pelican listing event source, rather than the bare
in-memory/demo-ticker defaults you get from running the binary with no flags.
That's the configuration meant to be treated as the "real" deployment target;
the in-memory ticker defaults exist mainly for quick local experimentation
and the test suite.

## Building and running

```bash
go build ./cmd/messagequeue
./messagequeue --port 8080
```

| Flag | Default | Purpose |
| --- | --- | --- |
| `--port` | `8080` | Port to listen on |
| `--debug` | `false` | Log full frame contents (headers + body) for every STOMP frame, plus event-source poll debug logs |
| `--watch-dir` | *(unset)* | Use the filesystem event source on this directory |
| `--s3-notify-addr` | *(unset)* | Use the Ceph bucket-notification event source, listening on this address |
| `--htpasswd` | *(unset)* | Path to an htpasswd file; if unset, connections are unauthenticated |
| `--queue-db` | *(unset)* | Path to a SQLite file for durable client queues; if unset, queues are in-memory only |
| `--pelican-enabled` | `false` | Use the Pelican directory-listing event source instead of the demo ticker |
| `--pelican-poll-interval` | `1m` | How often to poll watched Pelican directories |
| `--pelican-state-file` | *(unset)* | Where to persist each watched directory's last-observed listing (required with `--pelican-enabled`) |
| `--pelican-binary` | `pelican` | Path to the `pelican` CLI used to list watched directories |

`TEST_EVENT_SEQ_FILE=<path>` (an environment variable, not a flag) selects
the persistent sequence-counter demo source instead of the plain ticker, if
`--pelican-enabled` isn't set.

To run the full playground (server + SQLite queue + Pelican source + a
Python client + the server-side and client-side observer UIs) from the repo
root:

```bash
docker compose up -d --build
```

## Testing

```bash
go build ./...
go vet ./...
go test -race ./...
```

Every package's `_test.go` files live in the same (non-`_test`-suffixed)
package as the code under test, so tests can exercise unexported fields and
methods directly rather than being limited to each package's public API.
`internal/messagequeue/smoke_test.go` additionally drives the whole stack
(real HTTP server, real WebSocket upgrade, real `Server`, real `App`) with a
raw WebSocket client speaking STOMP frames directly, as an end-to-end check
beyond the mocked unit tests.
