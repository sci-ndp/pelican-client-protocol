# Plan: Ceph S3 (RGW bucket notification) event source

**Status: implemented per the inline decisions below** (unauthenticated push on
a separate port, no dedup, no ack path, no in-code bootstrapper, `server`
migrated to the S3 source, `s3-writer` added). Deviations: no compose profile
(the default stack now uses Ceph), no `ceph-data` volume (the dev Ceph is
ephemeral), RGW is on container port 8080 (demo image default), no
removal-event flag (the notification's event list controls this), and the
demo image is `quay.io/ceph/demo:latest-squid`. Verified end to end against a
real RGW, including redelivery after a server restart.

Source docs: <https://docs.ceph.com/en/latest/radosgw/notifications/>

## Goal

Add an `EventSource` (alongside `FSNotifySource`) that turns Ceph RadosGW
bucket notifications into events on the STOMP server's fan-out, so object
creates/removes in a Ceph bucket reach subscribed clients.

## How Ceph delivers events (relevant facts)

- RGW **pushes** notifications; we don't poll. Endpoints can be HTTP(S), AMQP
  0.9.1 or Kafka. A topic (`CreateTopic`) holds the endpoint and delivery
  attributes; a bucket notification (`PutBucketNotification`) binds a bucket,
  an event list and optional filters (key prefix/suffix/regex, `x-amz-meta-*`,
  tags) to a topic.
- Payload is JSON: `{"Records":[{eventName, eventTime, s3:{bucket:{name},
  object:{key,size,eTag,versionId,sequencer}, configurationId}, eventId,
  opaqueData, ...}]}`. One POST may carry several records.
- Delivery modes: **synchronous** (sent inline; failure is dropped, the S3
  operation still succeeds) and **persistent** (`persistent=true`; queued in
  RADOS, retried per `time_to_live` / `max_retries` / `retry_sleep_duration`;
  at-least-once). Persistent is the only mode that survives our server being
  down.

## Recommended approach: HTTP push endpoint

The HTTP endpoint needs no extra infrastructure and no new Go dependencies
(Kafka/AMQP would need a broker plus a client library). The server exposes an
HTTP handler; RGW POSTs to it. Kafka/AMQP can be a later alternative
implementation behind the same `EventSource` interface.

### Components

1. **`eventsource/s3notifyeventsource.go`** – `S3NotifySource`
   - `NewS3NotifySource(cfg S3NotifyConfig, log) *S3NotifySource`; embeds
     `defaultNotifier` initially.
   - Implements `http.Handler` (`ServeHTTP`) in addition to `EventSource`.
   - Handler: require `POST`, verify the shared secret (below), cap body size
     (e.g. 1 MiB), decode `Records`, emit one event string per record onto
     `Events()`, then reply `200`. Malformed JSON → `400` (don't retry
     garbage forever); unauthorized → `401`/`403`; shutting down/overloaded →
     `503` so RGW retries.
   - **Ack only after handoff**: respond 2xx only after each record is sent on
     the channel, so a persistent topic keeps retrying while we're down or
     backed up. (Gap: the channel hand-off precedes durable queueing in
     `messagequeue`; an event accepted but not yet appended to client queues
     is lost on a crash. Acceptable for a first cut; closing it requires an
     ack path from `messagequeue`, noted under Open questions.)
   - **Event format**: JSON string per record, normalized so clients don't
     parse AWS structure:
     `{"event":"ObjectCreated:Put","bucket":"…","key":"…","size":N,"etag":"…",
     "version_id":"…","time":"…","sequencer":"…","event_id":"…"}`.
   - **Filtering**: only forward create-type events if we keep parity with
     `FSNotifySource` ("add/modify"); configurable via the notification's
     event list on the Ceph side, so the source forwards whatever it
     receives (optionally dropping `ObjectRemoved:*` via a flag).
   - **Dedup**: at-least-once delivery means duplicates after retries. Keep a
     small bounded LRU of recent `(bucket, key, sequencer)` (or `eventId`
     when non-empty) and drop repeats.
     - No need to deduplicate server-side, the server protocol may already re-send a single event to a client.
   - **Ordering**: no global ordering guarantee; `sequencer` only orders
     changes per object. Don't promise more downstream.
   - Per-client filtering via `ShouldNotify` / `QueueAdded` (e.g. subscription
     params naming a bucket or key prefix) is a follow-up; the first cut fans
     out to every client like `FSNotifySource`.

2. **Wiring in `cmd/messagequeue/main.go`**
   - New flags (below). The `EventSource` interface has no HTTP surface, so
     `main` constructs `*S3NotifySource` and mounts it on the existing `mux`
     (`mux.Handle("/s3-events", s3Source)`), **not** behind the STOMP
     `RequireAuth` wrapper (RGW can't do STOMP basic auth; it uses its own
     shared secret). Sources are mutually exclusive with `--watch-dir`
     (one event source), matching the current "exactly one source" rule.
   - Alternatively listen on a separate internal port so the endpoint is not
     exposed on the public STOMP port; recommended for production.
     - Listen on a separate port in the initial implementation

3. **Optional, phase 2: topic/notification bootstrap**
   - A small `eventsource/cephadmin.go` using the AWS SDK for Go v2 SNS-style
     `CreateTopic` and S3 `PutBucketNotificationConfiguration` against RGW so
     the server configures Ceph itself at startup (idempotent). Needs S3 admin
     credentials at runtime. Default is to **not** do this; the Ceph admin
     creates the topic/notification out of band (a `radosgw-admin`/`aws cli`
     snippet goes in the README).
     - Do not implement the bootstrapper in-code, add a note to the readme.

4. **Tests** (`s3notifyeventsource_test.go`, using `httptest`)
   - Valid single/multi-record POST → one event per record, in order, 200.
   - Wrong/missing secret → 401/403, no event.
   - Non-POST → 405; bad JSON → 400; oversized body → 413.
   - Duplicate record (same sequencer) → emitted once.
   - Channel full / source stopped → 503 (no silent drop).
   - Sample payload from the Ceph docs as a fixture.

5. **docker-compose**: add a Ceph demo as documented below.

## Runtime inputs required

### Server side (our service)

| Input | Purpose |
|---|---|
| `--s3-notify-enabled` (or a `--s3-notify-path`) | Select this source instead of `--watch-dir`. |
| Listen address/port & path for the receiver (e.g. `:8082`, `/s3-events`) | Where RGW POSTs. Must be reachable from the RGW hosts. |
| **Shared secret / auth token** (`S3_NOTIFY_SECRET`, env var, not a flag) | Authenticates RGW to us (see Open questions for the transport). |
| TLS cert + key (or a TLS-terminating proxy) | Needed if RGW pushes over `https`; RGW setting `verify-ssl` must match. |
| Dedup window size, max body size (optional, with defaults) | Tuning. |
| Whether to forward removal events (optional) | Create/modify-only parity with the FS source. |

### Ceph side (must exist before events flow; supplied by the Ceph admin, or by our service in phase 2)

| Input | Purpose |
|---|---|
| RGW with `notifications` in `rgw_enable_apis` | Feature must be enabled. |
| Topic with `push-endpoint=http[s]://<our-host>:<port>/s3-events`, `persistent=true`, and `verify-ssl`, `time_to_live`, `max_retries`, `retry_sleep_duration` | Delivery target and retry behavior. |
| Bucket notification (bucket name, event list e.g. `s3:ObjectCreated:*`, optional prefix/suffix filters) bound to that topic | Which events fire. |
| Network path RGW → server | Firewall/DNS so RGW can reach the receiver. |
| *(Phase 2 only)* RGW S3 endpoint URL, access key, secret key, region/zonegroup, tenant, bucket list | For the server to create the topic and notification itself. |

## Local Ceph in docker-compose (dev/test)

Goal: `docker compose up` brings up a real RGW that pushes notifications to
our server, so the whole path can be exercised end to end. Add these as
services in the top-level `docker-compose.yml`, behind a compose profile
(`profiles: ["ceph"]`) so the default stack doesn't start a heavyweight
Ceph cluster.

### Services

1. **`ceph`** – a single-node, containerized Ceph with RGW enabled.
   - Candidate images: `quay.io/ceph/demo` (single-container mon+mgr+osd+rgw,
     built for exactly this; check it is still published for a current Ceph
     release), or a `vstart.sh`-based dev image built from the Ceph source tree
     (heavy, slow to build). `cephadm`/Rook are not usable inside plain
     compose. **Verify image availability and tags before committing to one**;
     I have not confirmed which of these is currently maintained.
   - Config via env/ceph.conf mounted from `./deploy/ceph/`: set
     `rgw_enable_apis` to include `notifications` (and `s3`, `s3website`,
     etc. as needed), and `rgw_allow_notification_secrets_in_cleartext = true`
     if the dev endpoint is plain `http` with an embedded secret. Persist
     `/var/lib/ceph` on a named volume (`ceph-data`) so the cluster survives
     restarts.
   - Expose RGW on `8000:8000` is already taken by `web-ui`, so map RGW to a
     different host port (e.g. `7480:7480`, RGW's default).
   - Needs enough memory (~2 GB+) and, depending on the image, a healthy
     `healthcheck` on `curl http://localhost:7480` so dependants wait.
   - Dev S3 credentials supplied by env (`CEPH_DEMO_UID`,
     `CEPH_DEMO_ACCESS_KEY`, `CEPH_DEMO_SECRET_KEY`, `CEPH_DEMO_BUCKET` for the
     demo image) – dev-only values, kept in a `.env` example, not real secrets.

2. **`ceph-init`** – a one-shot job (`restart: "no"`, `depends_on: ceph`
   with `condition: service_healthy`) that configures notifications, using
   `aws-cli` (or `boto3`) pointed at `http://ceph:7480`:
   1. `aws sns create-topic --name fs-events --attributes
      push-endpoint=http://server:8082/s3-events?token=$S3_NOTIFY_SECRET,persistent=true`
      (exact attribute/URL syntax and auth form per the open question
      below).
   2. `aws s3 mb s3://$BUCKET`.
   3. `aws s3api put-bucket-notification-configuration` binding the bucket to
      the topic ARN for `s3:ObjectCreated:*` (and optionally
      `s3:ObjectRemoved:*`).
   Idempotent (topic and bucket creation tolerate reruns). This is the same
   flow as the optional "phase 2" bootstrap, kept as a compose script so the
   server needn't hold S3 admin credentials.

3. **`server`** (existing) – add `--s3-notify-*` flags/env, the shared
   `S3_NOTIFY_SECRET`, and `depends_on: ceph` only under the `ceph` profile.
   It listens on the internal receiver port (`8082`), reachable from `ceph`
   over the compose network by service name; do **not** publish that port to
   the host.

4. **`s3-writer`** – a tiny container looping
   `aws s3 cp` / `put-object` of random files into the bucket, replacing the
   idle `generator` for S3 testing. Without it, use `aws s3 cp` by hand
   against `localhost:7480`.
   - Implement this

### Compose-specific notes

- Event source selection is exclusive (`--watch-dir` vs S3), so the `ceph`
  profile needs its own `server` command. Simplest: a second service
  definition `server-s3` (same image, S3 flags, profile `ceph`) rather than
  conditionally editing the default `server` command; point `client`,
  `web-ui` etc. at whichever is running via an env var for the host name.
  - Migrate the existing `server` service off of FS watch and into CEPH watch.
- Use the compose service name (`http://server-s3:8082/...`) as the
  push-endpoint host; RGW resolves it over the compose network.
- Persistent topics: restart the `server-s3` container mid-test and verify
  RGW redelivers (checks the "ack after handoff" behavior); watch
  `persistent_topic_len` via `radosgw-admin`/perf counters.
- Fallback if the Ceph image proves impractical locally: a **stand-in
  sender** (a small script POSTing the Ceph-docs sample payload, or MinIO
  webhook notifications, whose record shape is similar but not identical) can
  test the receiver, but is not a substitute for verifying real RGW behavior
  (retry timing, auth, batching). Keep both: the stand-in in unit/CI tests,
  real Ceph for manual/integration runs.

### Added runtime inputs for this setup

| Input | Value (dev) |
|---|---|
| Compose profile | `ceph` |
| RGW host port | `7480` |
| S3 access/secret key, bucket name | From `.env` (dev-only) |
| `S3_NOTIFY_SECRET` | Random dev value from `.env`, shared by `server-s3` and `ceph-init` |
| Ceph config | `rgw_enable_apis` incl. `notifications`; cleartext-secrets option if using http |
| Ceph resources | ~2 GB RAM, `ceph-data` volume |

## Open questions

1. **Authentication of pushes.** The docs extract I reviewed doesn't describe
   RGW adding custom auth headers to HTTP pushes. Likely options: a secret in
   the endpoint URL (path/query token, or `user:pass@` userinfo if RGW turns it
   into basic auth; RGW also has `rgw_allow_notification_secrets_in_cleartext`
   for non-HTTPS secrets), plus network ACLs/mTLS. Needs verification against
   our RGW version before choosing; the plan assumes a URL token over HTTPS.
   - First pass implementation: Unauthenticated over HTTP for simplicity's sake (if ceph supports this).
2. **Durability gap** between channel hand-off and queue persistence (see
   above): do we need an ack from `messagequeue` back to the source?
   - no ack needed initially.
3. **Multiple RGW instances/zones**: Not in scope
4. **Per-client filtering**: Send all notifications to all clients
5. **Kafka/AMQP**: Support not needed for MVP

## Suggested order of work

1. Receiver + parsing + tests (no auth).
2. Shared-secret auth, body cap, dedup.
3. `main.go` flags and mounting; README section with the Ceph-side
   `CreateTopic` / `PutBucketNotification` commands.
4. Local Ceph compose profile (`ceph`, `ceph-init`, `server-s3`) and an
   end-to-end run, including the restart/redelivery check.
5. (Optional) per-client filtering; server-side topic bootstrap; Kafka/AMQP.
