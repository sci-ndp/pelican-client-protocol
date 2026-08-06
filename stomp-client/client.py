from __future__ import annotations

import argparse
import asyncio
import base64
import json
import logging
import os
import random
import uuid
from collections import deque
from urllib.parse import urlparse, urlunparse

import websockets
from websockets.exceptions import WebSocketException

from persistence import ClientPersistence
from protocol import Frame, parse, unsubscribe


def basic_auth_header(username: str, password: str) -> str:
    token = base64.b64encode(f"{username}:{password}".encode()).decode()
    return f"Basic {token}"


def websocket_url(value: str) -> str:
    parsed = urlparse(value)
    schemes = {"http": "ws", "https": "wss", "ws": "ws", "wss": "wss"}
    if parsed.scheme not in schemes or not parsed.netloc:
        raise ValueError("STOMP_URL must be an http(s) or ws(s) URL")
    return urlunparse(parsed._replace(scheme=schemes[parsed.scheme]))


def display_frame(raw: bytes) -> str:
    return raw.decode(errors="replace").replace("\x00", "<NUL>").replace("\r", "\\r").replace("\n", "\\n")


class ClientLogBuffer(logging.Handler):
    def __init__(self, limit: int = 1000) -> None:
        super().__init__()
        self.lines = deque(maxlen=limit)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self.lines.append(self.format(record))
        except Exception:
            self.handleError(record)

    def snapshot(self) -> list[str]:
        return list(self.lines)


class StompClient:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.state = ClientPersistence(os.getenv("CLIENT_DB", "client.sqlite3"), args.client_id)
        self.log = logging.getLogger("stomp-client")
        self.log_buffer = next((handler for handler in self.log.handlers if isinstance(handler, ClientLogBuffer)), None)
        self.message_bodies = deque(maxlen=200)
        self.delivery_metrics = {
            "connection_attempts": 0,
            "sessions_established": 0,
            "connection_failures": 0,
            "server_error_frames": 0,
            "messages_received": 0,
            "unique_events_processed": 0,
            "redeliveries_observed": 0,
            "acks_sent": 0,
            "acks_withheld": 0,
            "disconnects_before_ack": 0,
            "graceful_disconnects": 0,
            "disconnect_receipts_received": 0,
            "disconnect_receipt_timeouts": 0,
            "unsubscribe_requests": 0,
            "unsubscribe_receipts_received": 0,
            "unsubscribe_receipt_timeouts": 0,
            "unsubscribe_rejections": 0,
            "immediate_disconnects": 0,
        }
        self.events_seen = 0
        self.drop_ack_percent = float(os.getenv("DROP_ACK_PERCENT", "0"))
        self.disconnect_after = int(os.getenv("DISCONNECT_AFTER", "0"))
        self.drop_next_acks = 0
        self.disconnect_next_messages = 0
        self.username = args.username
        self.password = args.password
        self.active_ws = None
        self.pending_disconnect_receipt = None
        self.disconnect_receipt_waiter = None
        self.pending_unsubscribe_receipt = None
        self.unsubscribe_receipt_waiter = None
        self.subscription_active = False
        self.enabled = True
        self.wake = asyncio.Event()
        self.connection_state = "starting"
        self.last_error = ""
        self.session_headers: dict[str, str] = {}
        self.validate_auth()
        self.log.info(
            "delivery report initialized scope=current client process; repeated deliveries are inferred from duplicate application event IDs",
        )
        self.log.info(
            "configured url=%s destination=%s client_id=%s heartbeat_ms=%s reconnect=%s log_level=%s basic_auth=%s auth_username=%s drop_ack_percent=%s disconnect_after=%s",
            args.url, self.destination(), args.client_id, args.heartbeat, args.reconnect,
            args.log_level, bool(self.username), self.username or "(none)",
            self.drop_ack_percent, self.disconnect_after,
        )

    def validate_auth(self) -> None:
        if bool(self.username) != bool(self.password):
            raise ValueError("STOMP_USERNAME and STOMP_PASSWORD must be provided together")

    def subscription_key(self) -> str:
        return self.args.subscription or f"{self.args.client_id}-sub"

    def destination(self) -> str:
        return f"{self.subscription_key()}/{self.args.event_source}"

    @staticmethod
    def validate_channel_part(name: str, value: str) -> None:
        if not value or "/" in value:
            raise ValueError(f"{name} must be non-empty and must not contain '/'")

    async def run(self) -> None:
        await asyncio.start_server(self.handle_http_request, "0.0.0.0", self.args.log_port)
        self.log.info("application API listening address=0.0.0.0 port=%s paths=/logs,/messages,/report,/status,/config,/connect,/unsubscribe,/disconnect,/disconnect/graceful,/simulation/*", self.args.log_port)
        delay = 1
        while True:
            if not self.enabled:
                self.connection_state = "disconnected"
                await self.wake.wait()
                self.wake.clear()
                delay = 1
                continue
            try:
                await self.connected_session()
                delay = 1
            except (OSError, WebSocketException, RuntimeError) as exc:
                if not self.enabled:
                    self.connection_state = "disconnected"
                    self.last_error = ""
                    self.log.info("connection closed after client-requested disconnect")
                    delay = 1
                    continue
                self.delivery_metrics["connection_failures"] += 1
                self.connection_state = "error"
                self.last_error = f"{type(exc).__name__}: {exc}"
                self.log.warning("connection failure type=%s error=%s; reconnecting in %ss", type(exc).__name__, exc, delay)
                if not self.args.reconnect or not self.enabled:
                    await self.wake.wait()
                    self.wake.clear()
                    delay = 1
                else:
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 30)

    def received_messages(self) -> list[str]:
        """Return recent MESSAGE bodies received by this client, oldest first."""
        return list(self.message_bodies)

    def delivery_report(self) -> dict:
        """Return a transparent account of delivery behavior observed by this client."""
        return {
            "scope": "Current Python client process only; counters reset when the client restarts.",
            "metrics": dict(self.delivery_metrics),
            "notes": {
                "redeliveries_observed": "A repeated application event ID was delivered again. This commonly indicates server retry after an ACK was not received, but the client cannot prove why the server redelivered it.",
                "acks_withheld": "ACKs intentionally not sent by this client because of the configured fault simulations.",
                "unobservable": "Frames lost before reaching this client cannot be counted by the client.",
            },
        }

    def status_payload(self) -> dict:
        return {
            "state": self.connection_state,
            "last_error": self.last_error,
            "session": self.session_headers,
            "subscription": {
                "id": self.subscription_key(),
                "destination": self.destination(),
                "active": self.subscription_active,
            },
            "config": {
                "url": self.args.url,
                "subscription": self.subscription_key(),
                "event_source": self.args.event_source,
                "destination": self.destination(),
                "client_id": self.args.client_id,
                "heartbeat": self.args.heartbeat,
                "log_level": self.args.log_level,
                "basic_auth": bool(self.username),
                "username": self.username,
            },
            "simulation": {
                "drop_next_acks": self.drop_next_acks,
                "disconnect_next_messages": self.disconnect_next_messages,
                "random_ack_loss_percent": self.drop_ack_percent,
            },
        }

    async def apply_config(self, values: dict) -> None:
        url = str(values.get("url", self.args.url)).strip()
        subscription = str(values.get("subscription", self.subscription_key())).strip()
        event_source = str(values.get("event_source", self.args.event_source)).strip()
        client_id = str(values.get("client_id", self.args.client_id)).strip()
        heartbeat = int(values.get("heartbeat", self.args.heartbeat))
        username = str(values.get("username", self.username))
        password = str(values.get("password", self.password))
        log_level = str(values.get("log_level", self.args.log_level)).upper()
        if not client_id or heartbeat < 1000:
            raise ValueError("client_id is required and heartbeat must be at least 1000 ms")
        self.validate_channel_part("subscription", subscription)
        self.validate_channel_part("event_source", event_source)
        websocket_url(url)
        if bool(username) != bool(password):
            raise ValueError("username and password must be provided together")
        if log_level not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
            raise ValueError("invalid log level")

        changed_id = client_id != self.args.client_id
        self.args.url = url
        self.args.subscription = subscription
        self.args.event_source = event_source
        self.args.client_id = client_id
        self.args.heartbeat = heartbeat
        self.args.log_level = log_level
        self.username = username
        self.password = password
        self.log.setLevel(getattr(logging, log_level))
        if changed_id:
            self.state = ClientPersistence(os.getenv("CLIENT_DB", "client.sqlite3"), client_id)
        self.enabled = True
        self.wake.set()
        self.log.info("application configuration applied url=%s subscription=%s destination=%s client_id=%s heartbeat_ms=%s basic_auth=%s auth_username=%s log_level=%s", url, subscription, self.destination(), client_id, heartbeat, bool(username), username or "(none)", log_level)
        if self.active_ws is not None:
            await self.active_ws.close(code=4000, reason="configuration updated")

    async def unsubscribe(self) -> None:
        """Remove the active STOMP subscription and wait for its receipt.

        STOMP 1.2 requires id to exactly match the id sent in SUBSCRIBE. The
        WebSocket remains open; state becomes inactive only on matching RECEIPT.
        """
        if self.active_ws is None or self.connection_state != "connected":
            raise RuntimeError("cannot unsubscribe without an active STOMP session")
        if not self.subscription_active:
            raise RuntimeError("the STOMP subscription is already inactive")
        if self.pending_unsubscribe_receipt:
            raise RuntimeError("an unsubscribe request is already in progress")

        receipt_id = f"unsubscribe-{uuid.uuid4()}"
        waiter = asyncio.get_running_loop().create_future()
        self.pending_unsubscribe_receipt = receipt_id
        self.unsubscribe_receipt_waiter = waiter
        self.delivery_metrics["unsubscribe_requests"] += 1
        received = False
        try:
            await self.send(self.active_ws, unsubscribe(self.subscription_key(), receipt_id))
            self.log.info("STOMP UNSUBSCRIBE sent id=%s receipt=%s; waiting for RECEIPT", self.subscription_key(), receipt_id)
            received = await asyncio.wait_for(asyncio.shield(waiter), timeout=5)
            if received:
                self.log.info("unsubscribe receipt confirmed id=%s receipt=%s", self.subscription_key(), receipt_id)
            else:
                self.delivery_metrics["unsubscribe_receipt_timeouts"] += 1
                self.last_error = f"UNSUBSCRIBE was not confirmed for receipt {receipt_id}"
                self.log.warning("unsubscribe ended before receipt=%s was confirmed", receipt_id)
        except asyncio.TimeoutError:
            self.delivery_metrics["unsubscribe_receipt_timeouts"] += 1
            self.last_error = f"UNSUBSCRIBE timed out waiting for receipt {receipt_id}"
            self.log.warning("unsubscribe timed out waiting for receipt=%s", receipt_id)
        finally:
            if self.pending_unsubscribe_receipt == receipt_id:
                self.pending_unsubscribe_receipt = None
                self.unsubscribe_receipt_waiter = None

    async def graceful_disconnect(self) -> None:
        """Send DISCONNECT with a receipt request, then close after the matching RECEIPT."""
        if self.pending_disconnect_receipt:
            raise RuntimeError("a graceful disconnect is already in progress")
        if self.active_ws is None:
            self.enabled = False
            self.connection_state = "disconnected"
            self.log.info("graceful disconnect requested with no active STOMP session")
            return

        ws = self.active_ws
        receipt_id = f"disconnect-{uuid.uuid4()}"
        waiter = asyncio.get_running_loop().create_future()
        self.pending_disconnect_receipt = receipt_id
        self.disconnect_receipt_waiter = waiter
        self.connection_state = "disconnecting"
        self.last_error = ""
        self.delivery_metrics["graceful_disconnects"] += 1
        received = False
        try:
            await self.send(ws, Frame("DISCONNECT", {"receipt": receipt_id}))
            self.log.info("graceful STOMP DISCONNECT sent receipt=%s; waiting for RECEIPT", receipt_id)
            received = await asyncio.wait_for(asyncio.shield(waiter), timeout=5)
            if received:
                self.log.info("graceful disconnect receipt confirmed receipt=%s", receipt_id)
            else:
                self.delivery_metrics["disconnect_receipt_timeouts"] += 1
                self.log.warning("graceful disconnect ended before receipt=%s was confirmed", receipt_id)
        except asyncio.TimeoutError:
            self.delivery_metrics["disconnect_receipt_timeouts"] += 1
            self.log.warning("graceful disconnect timed out waiting for receipt=%s", receipt_id)
        finally:
            self.enabled = False
            if not received and self.active_ws is ws:
                await ws.close(code=1000, reason="graceful client disconnect")
            if self.pending_disconnect_receipt == receipt_id:
                self.pending_disconnect_receipt = None
                self.disconnect_receipt_waiter = None
            self.session_headers = {}

    def handle_receipt(self, frame: Frame) -> None:
        receipt_id = frame.headers.get("receipt-id", "")
        if receipt_id == self.pending_disconnect_receipt and self.disconnect_receipt_waiter is not None:
            if not self.disconnect_receipt_waiter.done():
                self.disconnect_receipt_waiter.set_result(True)
            self.enabled = False
            self.delivery_metrics["disconnect_receipts_received"] += 1
            self.log.info("server receipt matched graceful disconnect receipt=%s", receipt_id)
        elif receipt_id == self.pending_unsubscribe_receipt and self.unsubscribe_receipt_waiter is not None:
            if not self.unsubscribe_receipt_waiter.done():
                self.unsubscribe_receipt_waiter.set_result(True)
            self.subscription_active = False
            self.delivery_metrics["unsubscribe_receipts_received"] += 1
            self.log.info("server receipt matched unsubscribe id=%s receipt=%s", self.subscription_key(), receipt_id)
        else:
            self.log.info("server receipt received receipt=%s", receipt_id or "(missing)")

    def handle_server_error(self, frame: Frame) -> None:
        """Record server ERROR frames and resolve any rejected unsubscribe promptly."""
        message = frame.body or frame.headers.get("message", "server ERROR")
        self.delivery_metrics["server_error_frames"] += 1
        self.log.error("server ERROR: %s", message)
        if self.pending_unsubscribe_receipt and self.unsubscribe_receipt_waiter is not None:
            self.delivery_metrics["unsubscribe_rejections"] += 1
            self.last_error = f"UNSUBSCRIBE rejected by server: {message}"
            if not self.unsubscribe_receipt_waiter.done():
                self.unsubscribe_receipt_waiter.set_result(False)
            self.log.warning("unsubscribe rejected before receipt=%s", self.pending_unsubscribe_receipt)

    def configure_simulation(self, payload: dict) -> None:
        path = payload.get("path")
        if path == "drop-acks":
            count = int(payload.get("count", 1))
            if count < 1 or count > 1000:
                raise ValueError("drop ACK count must be between 1 and 1000")
            self.drop_next_acks += count
            self.log.warning("simulation armed: dropping next %s ACKs pending_drop_acks=%s", count, self.drop_next_acks)
        elif path == "disconnect-next-message":
            count = int(payload.get("count", 1))
            if count < 1 or count > 100:
                raise ValueError("disconnect count must be between 1 and 100")
            self.disconnect_next_messages += count
            self.log.warning("simulation armed: disconnect before next %s MESSAGEs pending_disconnects=%s", count, self.disconnect_next_messages)
        elif path == "random-ack-loss":
            percent = float(payload.get("percent", 0))
            if not 0 <= percent <= 100:
                raise ValueError("random ACK loss must be between 0 and 100")
            self.drop_ack_percent = percent
            self.log.warning("simulation configured: random ACK loss percent=%s", percent)
        elif path == "reset":
            self.drop_next_acks = 0
            self.disconnect_next_messages = 0
            self.drop_ack_percent = 0
            self.log.info("simulation controls reset")
        else:
            raise ValueError("unknown simulation control")

    async def handle_http_request(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            request = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=2)
            lines = request.decode(errors="replace").split("\r\n")
            method, path, _ = lines[0].split(" ", 2)
            headers = {}
            for line in lines[1:]:
                if ":" in line:
                    key, value = line.split(":", 1)
                    headers[key.lower()] = value.strip()
            length = int(headers.get("content-length", "0"))
            body = await reader.readexactly(length) if length else b""
            payload = json.loads(body) if body else {}

            if method == "OPTIONS":
                response, status = {}, "200 OK"
            elif method == "GET" and path == "/logs":
                response, status = {"logs": self.log_buffer.snapshot() if self.log_buffer else []}, "200 OK"
            elif method == "GET" and path == "/messages":
                response, status = {"messages": self.received_messages()}, "200 OK"
            elif method == "GET" and path == "/report":
                response, status = self.delivery_report(), "200 OK"
            elif method == "GET" and path == "/status":
                response, status = self.status_payload(), "200 OK"
            elif method == "POST" and path == "/config":
                await self.apply_config(payload)
                response, status = self.status_payload(), "200 OK"
            elif method == "POST" and path == "/connect":
                self.enabled = True
                self.wake.set()
                response, status = self.status_payload(), "200 OK"
            elif method == "POST" and path == "/unsubscribe":
                await self.unsubscribe()
                response, status = self.status_payload(), "200 OK"
            elif method == "POST" and path == "/disconnect":
                self.enabled = False
                self.delivery_metrics["immediate_disconnects"] += 1
                if self.active_ws is not None:
                    await self.active_ws.close(code=1000, reason="client disabled by application API")
                response, status = self.status_payload(), "200 OK"
            elif method == "POST" and path == "/disconnect/graceful":
                await self.graceful_disconnect()
                response, status = self.status_payload(), "200 OK"
            elif method == "POST" and path.startswith("/simulation/"):
                action = path.removeprefix("/simulation/")
                if action == "close-connection":
                    if self.active_ws is not None:
                        self.delivery_metrics["immediate_disconnects"] += 1
                        self.log.warning("simulation requested immediate connection close")
                        await self.active_ws.close(code=4001, reason="simulation requested connection close")
                else:
                    payload["path"] = action
                    self.configure_simulation(payload)
                response, status = self.status_payload(), "200 OK"
            else:
                response, status = {"error": "not found"}, "404 Not Found"
        except (asyncio.IncompleteReadError, asyncio.TimeoutError, ValueError, json.JSONDecodeError) as exc:
            response, status = {"error": str(exc) or "bad request"}, "400 Bad Request"
        body = json.dumps(response).encode()
        headers = (
            f"HTTP/1.1 {status}\r\n"
            "Content-Type: application/json\r\n"
            "Access-Control-Allow-Origin: *\r\n"
            "Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n"
            "Access-Control-Allow-Headers: Content-Type\r\n"
            "Cache-Control: no-store\r\n"
            f"Content-Length: {len(body)}\r\n\r\n"
        ).encode()
        writer.write(headers + body)
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    async def connected_session(self) -> None:
        target = websocket_url(self.args.url)
        self.delivery_metrics["connection_attempts"] += 1
        self.connection_state = "connecting"
        self.last_error = ""
        self.log.info("opening WebSocket url=%s", target)
        auth_headers = {"Authorization": basic_auth_header(self.username, self.password)} if self.username else None
        if auth_headers:
            self.log.info("HTTP Basic Auth enabled username=%s", self.username)
        try:
            async with websockets.connect(target, additional_headers=auth_headers, ping_interval=None) as ws:
                self.active_ws = ws
                await self.send(ws, Frame("CONNECT", {"accept-version": "1.2", "host": "playground", "client-id": self.args.client_id, "heart-beat": f"{self.args.heartbeat},{self.args.heartbeat}"}))
                connected = await self.receive(ws)
                if connected.command != "CONNECTED":
                    raise RuntimeError(f"expected CONNECTED, got {connected.command}")
                self.session_headers = connected.headers
                self.delivery_metrics["sessions_established"] += 1
                self.connection_state = "connected"
                self.log.info("STOMP session established headers=%s", connected.headers)
                subscription_id = self.subscription_key()
                destination = self.destination()
                await self.send(ws, Frame("SUBSCRIBE", {"id": subscription_id, "subscription": subscription_id, "destination": destination, "ack": "client-individual"}))
                self.subscription_active = True
                self.log.info("subscription active id=%s subscription=%s destination=%s ack_mode=client-individual", subscription_id, subscription_id, destination)
                while self.enabled:
                    frame = await self.receive(ws)
                    if frame.command == "MESSAGE":
                        await self.on_message(ws, frame)
                    elif frame.command == "RECEIPT":
                        self.handle_receipt(frame)
                    elif frame.command == "ERROR":
                        self.handle_server_error(frame)
        finally:
            self.active_ws = None
            if self.disconnect_receipt_waiter is not None and not self.disconnect_receipt_waiter.done():
                self.disconnect_receipt_waiter.set_result(False)
            if self.unsubscribe_receipt_waiter is not None and not self.unsubscribe_receipt_waiter.done():
                self.unsubscribe_receipt_waiter.set_result(False)
            self.subscription_active = False
            self.connection_state = "disconnected"

    async def send(self, ws, frame: Frame) -> None:
        raw = frame.encode()
        await ws.send(raw)
        self.log.info("CLIENT -> SERVER command=%s headers=%s body_bytes=%s", frame.command, frame.headers, len(frame.body.encode()))
        self.log.debug("CLIENT -> SERVER frame=%s", display_frame(raw))

    async def receive(self, ws) -> Frame:
        data = await ws.recv()
        if isinstance(data, str):
            data = data.encode()
        if data in (b"\n", b"\r\n"):
            self.log.debug("SERVER -> CLIENT HEARTBEAT frame=LF")
            return Frame("HEARTBEAT")
        self.log.debug("SERVER -> CLIENT frame=%s", display_frame(data))
        frame = parse(data)
        self.log.info("SERVER -> CLIENT command=%s headers=%s body_bytes=%s", frame.command, frame.headers, len(frame.body.encode()))
        return frame

    async def on_message(self, ws, frame: Frame) -> None:
        self.events_seen += 1
        self.delivery_metrics["messages_received"] += 1
        self.message_bodies.append(frame.body)
        destination = frame.headers.get("destination", "")
        event_id = self.application_event_id(frame)
        if self.state.record_event(event_id, destination, frame.body):
            self.delivery_metrics["unique_events_processed"] += 1
            self.log.info("application event processed event_id=%s destination=%s body=%s", event_id, destination, frame.body)
        else:
            self.delivery_metrics["redeliveries_observed"] += 1
            self.log.info("application duplicate observed event_id=%s destination=%s", event_id, destination)
        ack_id = frame.headers.get("ack")
        if ack_id:
            if self.disconnect_next_messages:
                self.disconnect_next_messages -= 1
                self.delivery_metrics["disconnects_before_ack"] += 1
                self.log.warning("simulation disconnecting before ACK event_id=%s pending_disconnects=%s", event_id, self.disconnect_next_messages)
                await ws.close(code=4001, reason="simulation disconnect before ACK")
                return
            if self.drop_next_acks:
                self.drop_next_acks -= 1
                self.delivery_metrics["acks_withheld"] += 1
                self.log.warning("simulation dropped ACK event_id=%s pending_drop_acks=%s", event_id, self.drop_next_acks)
            elif random.random() < self.drop_ack_percent / 100:
                self.delivery_metrics["acks_withheld"] += 1
                self.log.warning("random ACK-loss simulation dropped ACK event_id=%s percent=%s", event_id, self.drop_ack_percent)
            else:
                await self.send(ws, Frame("ACK", {"id": ack_id}))
                self.delivery_metrics["acks_sent"] += 1
        if self.disconnect_after and self.events_seen >= self.disconnect_after:
            await ws.close()

    @staticmethod
    def application_event_id(frame: Frame) -> str:
        try:
            payload = json.loads(frame.body)
        except (TypeError, json.JSONDecodeError):
            payload = None
        if isinstance(payload, dict) and payload.get("uuid"):
            return str(payload["uuid"])
        return frame.headers.get("message-id", "unknown-message")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--subscription", default=os.getenv("SUBSCRIPTION", ""), help="unique message-queue subscription key; defaults to <client-id>-sub")
    parser.add_argument("--event-source", default=os.getenv("EVENT_SOURCE", "demo-events"), help="message-queue event source name")
    parser.add_argument("--client-id", default=os.getenv("CLIENT_ID", "playground-client"))
    parser.add_argument("--username", default=os.getenv("STOMP_USERNAME", ""), help="HTTP Basic Auth username")
    parser.add_argument("--password", default=os.getenv("STOMP_PASSWORD", ""), help="HTTP Basic Auth password; never logged")
    parser.add_argument("--url", default=os.getenv("STOMP_URL", "ws://localhost:8080"))
    parser.add_argument("--reconnect", action="store_true")
    parser.add_argument("--heartbeat", type=int, default=10000, help="heartbeat interval in milliseconds")
    parser.add_argument("--log-level", choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"), default=os.getenv("LOG_LEVEL", "INFO").upper(), help="verbosity; DEBUG includes complete STOMP frames")
    parser.add_argument("--log-port", type=int, default=int(os.getenv("LOG_PORT", "8081")), help="HTTP port exposing the application API and recent client logs")
    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(asctime)s.%(msecs)03d %(levelname)s %(name)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%S", force=True)
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    client_logger = logging.getLogger("stomp-client")
    client_logger.propagate = False
    formatter = logging.Formatter("%(asctime)s.%(msecs)03d %(levelname)s %(name)s %(message)s", "%Y-%m-%dT%H:%M:%S")
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    buffer_handler = ClientLogBuffer()
    buffer_handler.setFormatter(formatter)
    client_logger.handlers.clear()
    client_logger.addHandler(stream_handler)
    client_logger.addHandler(buffer_handler)
    try:
        asyncio.run(StompClient(args).run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
