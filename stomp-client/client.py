from __future__ import annotations

import argparse
import asyncio
import base64
import json
import logging
import os
import random
from collections import deque
from urllib.parse import urlparse, urlunparse

import websockets
from websockets.exceptions import WebSocketException

from persistence import ClientPersistence
from protocol import Frame, parse


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
        self.events_seen = 0
        self.drop_ack_percent = float(os.getenv("DROP_ACK_PERCENT", "0"))
        self.disconnect_after = int(os.getenv("DISCONNECT_AFTER", "0"))
        self.drop_next_acks = 0
        self.disconnect_next_messages = 0
        self.username = args.username
        self.password = args.password
        self.active_ws = None
        self.enabled = True
        self.wake = asyncio.Event()
        self.connection_state = "starting"
        self.last_error = ""
        self.session_headers: dict[str, str] = {}
        self.validate_auth()
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
        self.log.info("application API listening address=0.0.0.0 port=%s paths=/logs,/status,/config,/connect,/disconnect,/simulation/*", self.args.log_port)
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

    def status_payload(self) -> dict:
        return {
            "state": self.connection_state,
            "last_error": self.last_error,
            "session": self.session_headers,
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
            elif method == "GET" and path == "/status":
                response, status = self.status_payload(), "200 OK"
            elif method == "POST" and path == "/config":
                await self.apply_config(payload)
                response, status = self.status_payload(), "200 OK"
            elif method == "POST" and path == "/connect":
                self.enabled = True
                self.wake.set()
                response, status = self.status_payload(), "200 OK"
            elif method == "POST" and path == "/disconnect":
                self.enabled = False
                if self.active_ws is not None:
                    await self.active_ws.close(code=1000, reason="client disabled by application API")
                response, status = self.status_payload(), "200 OK"
            elif method == "POST" and path.startswith("/simulation/"):
                action = path.removeprefix("/simulation/")
                if action == "close-connection":
                    if self.active_ws is not None:
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
                self.connection_state = "connected"
                self.log.info("STOMP session established headers=%s", connected.headers)
                subscription_id = self.subscription_key()
                destination = self.destination()
                await self.send(ws, Frame("SUBSCRIBE", {"id": subscription_id, "subscription": subscription_id, "destination": destination, "ack": "client-individual"}))
                self.log.info("subscription active id=%s subscription=%s destination=%s ack_mode=client-individual", subscription_id, subscription_id, destination)
                while self.enabled:
                    frame = await self.receive(ws)
                    if frame.command == "MESSAGE":
                        await self.on_message(ws, frame)
                    elif frame.command == "ERROR":
                        self.log.error("server ERROR: %s", frame.body or frame.headers.get("message"))
        finally:
            self.active_ws = None
            if self.enabled:
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
        destination = frame.headers.get("destination", "")
        event_id = self.application_event_id(frame)
        if self.state.record_event(event_id, destination, frame.body):
            self.log.info("application event processed event_id=%s destination=%s body=%s", event_id, destination, frame.body)
        else:
            self.log.info("application duplicate ignored event_id=%s destination=%s", event_id, destination)
        ack_id = frame.headers.get("ack")
        if ack_id:
            if self.disconnect_next_messages:
                self.disconnect_next_messages -= 1
                self.log.warning("simulation disconnecting before ACK event_id=%s pending_disconnects=%s", event_id, self.disconnect_next_messages)
                await ws.close(code=4001, reason="simulation disconnect before ACK")
                return
            if self.drop_next_acks:
                self.drop_next_acks -= 1
                self.log.warning("simulation dropped ACK event_id=%s pending_drop_acks=%s", event_id, self.drop_next_acks)
            elif random.random() < self.drop_ack_percent / 100:
                self.log.warning("random ACK-loss simulation dropped ACK event_id=%s percent=%s", event_id, self.drop_ack_percent)
            else:
                await self.send(ws, Frame("ACK", {"id": ack_id}))
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
