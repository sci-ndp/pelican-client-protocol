# Independent VM deployments

The local `docker-compose.yml` is convenient for the all-in-one playground. These three Compose projects are independent deployment units:

```text
deploy/server/     server VM; owns the durable replay history
deploy/client/     client VM; owns the durable last-ACK state
deploy/generator/  optional generator VM; publishes events only
```

They do not use `depends_on`, a shared Docker network, or shared volumes. The only connection between machines is the server's WebSocket port.

## Server VM

Copy the repository (or at least `deploy/server` and `stomp-server`) to the server VM, then:

```bash
cd deploy/server
cp .env.example .env
docker compose up -d --build
docker compose logs -f
```

Allow inbound TCP `8080` in the VM firewall and cloud security group. Confirm the process is listening with `ss -ltn | grep 8080`. The SQLite database is in the named `server-data` volume; do not remove that volume if replay history matters.

The server deployment also serves a server-side observer/publisher UI on port `8001`. Open `http://SERVER_VM_IP:8001` (or `http://localhost:8001` locally) to inspect HTTP Upgrade → `101 Switching Protocols` → WebSocket → STOMP negotiation, view raw frames, and publish a test `SEND` event to subscribed clients.

## Client VM

Set `STOMP_URL` to the server VM's reachable address:

```bash
cd deploy/client
cp .env.example .env
# edit .env: STOMP_URL=ws://SERVER_PUBLIC_IP:8080
docker compose up -d --build
docker compose logs -f
```

The client VM needs outbound TCP access to the server port. Its `client-data` volume preserves the last ACKed sequence across container restarts. Keep the same `CLIENT_ID` when testing recovery; changing it intentionally creates a new logical client.

The client deployment also serves the small browser UI on port `8000`. Open `http://CLIENT_VM_IP:8000`, enter the server URL, choose a heartbeat interval, and click **Open connection**. It currently focuses on connection lifecycle and heartbeat frames; request controls will be added without changing the server address or deployment boundary.

The connection sequence is two layered protocols:

```text
Browser -> Server: HTTP GET / with Upgrade: websocket
Server -> Browser: HTTP/1.1 101 Switching Protocols
Browser -> Server: STOMP CONNECT frame
Server -> Browser: STOMP CONNECTED frame
Both directions: STOMP heartbeat LF frames
```

The browser `WebSocket` API creates the HTTP Upgrade request automatically. There is no separate application-level HTTP CONNECT endpoint in this playground; STOMP `CONNECT` is sent only after the transport upgrade succeeds. The Python client accepts the same public `http://` or `https://` value in `STOMP_URL` and converts it to `ws://` or `wss://` internally.

## Optional generator VM

The generator can run on either VM or a third VM:

```bash
cd deploy/generator
cp .env.example .env
# edit .env: STOMP_URL=ws://SERVER_PUBLIC_IP:8080
docker compose up -d --build
```

Port `8090` exposes pause/resume control for the teaching dashboard. Do not expose this port publicly unless you add authentication or restrict it to a trusted network.

## Internet and TLS

`ws://` is suitable for a direct VM-to-VM protocol experiment. For a browser dashboard or an untrusted network, put the server behind a TLS reverse proxy and use `wss://server.example.com/stomp`; forward WebSocket upgrade headers to the container's port 8080. Open only 80/443 publicly, and keep the raw 8080 port private.

## Suggested two-VM experiment

1. Start the server VM and note its public IP.
2. Start the client VM with `STOMP_URL=ws://SERVER_IP:8080`.
3. Run the generator on the server VM or a third machine.
4. Watch client logs for `CONNECTED`, `MESSAGE`, and `ACK`.
5. Stop the client container, let events accumulate, and start it again. It should replay from the SQLite last ACK.
6. Restart the server container without deleting `server-data`. The client should reconnect and replay retained events.
7. Use `DROP_ACK_PERCENT`, `DROP_EVENT_PERCENT`, and `DISCONNECT_AFTER` to introduce controlled failures.

Troubleshooting begins with connectivity rather than STOMP: verify DNS/IP routing, cloud security-group rules, VM firewall rules, and that `docker compose logs` shows the server listening on `0.0.0.0:8080`.
