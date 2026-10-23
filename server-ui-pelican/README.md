# notify-demo

Interactive documentation for `pelican object subscribe`, as a single static page.
Fill in a federation path, a depth and (if needed) a token, and each step of the subscribe workflow shows the exchange the Pelican client performs, with your values.
It is for developers (and their Claude instances) learning how object notifications work.
Not part of Pelican; `local/` is gitignored.

## Run

From the repository root:

```bash
docker build -t pelican-notify-demo local/notify-demo
docker run --rm -p 8099:80 pelican-notify-demo
```

Open <http://localhost:8099>.
Without Docker, any static file server works: `cd local/notify-demo && python3 -m http.server 8099`.

There is no backend.
The page calls the director and the origin directly from the browser, so:

- The browser must trust their TLS certificates.
  For a self-signed development federation, open the director's and the origin's HTTPS URLs in a tab once and accept the certificate.
- The director must allow cross-origin requests.
  It does: `corsHeadersMiddleware` in `director/director.go` allows any origin and exposes `X-Pelican-Namespace`.
- The origin's WebSocket accepts any page origin (the notification server's upgrader does not check it).

## The steps

1. **Director query.**
   The host of the `pelican://` URL is taken to be the director.
   (The client would first read `/.well-known/pelican-configuration` to find it; the director does not allow that route cross-origin, so the page skips it.)
   `GET <director>/<path>?notify`, no token, redirect not followed.
   The director answers with a redirect to the origin and an `X-Pelican-Namespace` header carrying `namespace`, `require-token` and `notify-url`.
2. **Upgrade.**
   `GET <notify-url>` with `Upgrade: websocket` and `Authorization: Bearer <token>`.
3. **Topic and SUBSCRIBE headers.**
   `<client id>/<depth>/<protocol>/<object path>`, e.g. `docs-1a2b3c/2/pelican/ns/dir`.
   The SUBSCRIBE frame needs `destination` (the topic), `id` and `subscription` (both the client id), and `ack: client-individual`.
4. **STOMP.**
   `CONNECT`, then `SUBSCRIBE` (`id` and `subscription` both the client id, `ack:client-individual`), and an `ACK` for each `MESSAGE`.
   A message body is a JSON string holding the object's federation path.

## Where the page departs from the real client

The written requests and responses show what the Pelican client exchanges.
A browser cannot do two of those things, so the page substitutes equivalent requests.
Each step has a collapsed "What this page actually sent" note, and this is the full list:

| Step | Shown | Actually sent |
| --- | --- | --- |
| Director query | `GET <path>?notify` answered by `307` with `Location` | `GET <path>?notify&redirect=false`, answered by `200` with the same headers. Browsers cannot read a redirect's headers. |
| `Location` | The origin that serves the path | Not readable from a `200`; shown as a placeholder. |
| Upgrade | `Authorization: Bearer <token>` header | `?authz=Bearer%20<token>` query parameter. The browser WebSocket API cannot set headers. The origin accepts either (`origin_serve.ExtractTokens`). |
| Upgrade response | `101` with `Upgrade`, `Connection`, `Sec-WebSocket-Accept` | The browser does not expose WebSocket handshake headers. |

The token in the query string can appear in origin and proxy access logs.
Use a short-lived, narrowly scoped token with this page.

## Where this mirrors Pelican

- `client/subscribe.go` (`DoSubscribe`): the `?notify` query, the topic.
- `client/notifications.go` (`Listen`): the upgrade with a bearer token, STOMP subscribe and ack.
- `server_structs/notify.go`: topic format, shared by clients and origins.
- `origin/notifier.go`: the origin's endpoint, authentication and authorization.
- `director/director.go` (`generateRedirectResponse`): the `redirect=false` behavior, and the headers.

## Troubleshooting

- The page reports it "could not reach" a URL: usually an untrusted certificate, mixed content, or missing CORS headers.
- `'aud' claim verification failed` in the origin log: the token's audience is not the origin's URL (or the WLCG wildcard).
- The origin refuses the upgrade: the token is invalid for this origin, or has no `storage.read` scope covering the path.
- No `notify-url` in the director response: no origin exporting the path advertises the Notify capability (the export needs `Notify`, and `Listings`).
- The subscription ends when the token expires.
