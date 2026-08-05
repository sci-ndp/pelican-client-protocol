from __future__ import annotations

import argparse
import asyncio
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


def websocket_url(value: str) -> str:
    """Allow operators to configure a normal HTTP(S) public endpoint."""
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
        self.events_seen = 0
        self.drop_ack_percent = float(os.getenv("DROP_ACK_PERCENT", "0"))
        self.disconnect_after = int(os.getenv("DISCONNECT_AFTER", "0"))
        self.log_buffer = next((handler for handler in self.log.handlers if isinstance(handler, ClientLogBuffer)), None)
        self.log.info(
            "configured url=%s destination=%s client_id=%s heartbeat_ms=%s reconnect=%s log_level=%s drop_ack_percent=%s disconnect_after=%s",
            self.args.url, self.args.destination, self.args.client_id, self.args.heartbeat,
            self.args.reconnect, self.args.log_level, self.drop_ack_percent, self.disconnect_after,
        )

    async def run(self) -> None:
        log_server = await asyncio.start_server(self.handle_log_request, "0.0.0.0", self.args.log_port)
        self.log.info("log endpoint listening address=0.0.0.0 port=%s path=/logs", self.args.log_port)
        delay = 1
        while True:
            try:
                await self.connected_session()
                delay = 1
            except (OSError, WebSocketException) as exc:
                self.log.warning("connection failure type=%s error=%s; reconnecting in %ss", type(exc).__name__, exc, delay)
                if not self.args.reconnect: raise
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)

    async def handle_log_request(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            request = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=2)
            path = request.split(b" ", 2)[1].decode(errors="replace") if request.startswith(b"GET ") else ""
            if path != "/logs":
                body = b'{"error":"not found"}'
                status = b"404 Not Found"
            else:
                lines = self.log_buffer.snapshot() if self.log_buffer else []
                body = json.dumps({"logs": lines}).encode()
                status = b"200 OK"
            headers = (
                b"HTTP/1.1 " + status + b"\r\n"
                b"Content-Type: application/json\r\n"
                b"Access-Control-Allow-Origin: *\r\n"
                b"Cache-Control: no-store\r\n"
                b"Content-Length: " + str(len(body)).encode() + b"\r\n\r\n"
            )
            writer.write(headers + body)
            await writer.drain()
        except (asyncio.IncompleteReadError, asyncio.TimeoutError, IndexError):
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    async def connected_session(self) -> None:
        target = websocket_url(self.args.url)
        self.log.info("opening WebSocket url=%s", target)
        async with websockets.connect(target, ping_interval=None) as ws:
            await self.send(ws, Frame("CONNECT", {"accept-version": "1.2", "host": "playground", "client-id": self.args.client_id, "heart-beat": f"{self.args.heartbeat},{self.args.heartbeat}"}))
            connected = await self.receive(ws)
            if connected.command != "CONNECTED":
                raise RuntimeError(f"expected CONNECTED, got {connected.command}")
            self.log.info("STOMP session established headers=%s", connected.headers)
            await self.send(ws, Frame("SUBSCRIBE", {"id": "sub-0", "destination": self.args.destination, "ack": "client-individual"}))
            self.log.info("subscription active id=sub-0 destination=%s ack_mode=client-individual", self.args.destination)
            while True:
                frame = await self.receive(ws)
                if frame.command == "MESSAGE": await self.on_message(ws, frame)
                elif frame.command == "ERROR": self.log.error("server ERROR: %s", frame.body or frame.headers.get("message"))

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
            if random.random() < self.drop_ack_percent / 100:
                self.log.warning("DROP_ACK_PERCENT simulated loss for event %s", event_id)
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
    parser.add_argument("--destination", default="/origin/demo")
    parser.add_argument("--client-id", default=os.getenv("CLIENT_ID", "playground-client"))
    parser.add_argument("--url", default=os.getenv("STOMP_URL", "ws://localhost:8080"))
    parser.add_argument("--reconnect", action="store_true")
    parser.add_argument("--heartbeat", type=int, default=10000, help="heartbeat interval in milliseconds")
    parser.add_argument("--log-level", choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"), default=os.getenv("LOG_LEVEL", "INFO").upper(), help="verbosity; DEBUG includes complete STOMP frames")
    parser.add_argument("--log-port", type=int, default=int(os.getenv("LOG_PORT", "8081")), help="HTTP port exposing recent client logs")
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
    try: asyncio.run(StompClient(args).run())
    except KeyboardInterrupt: pass


if __name__ == "__main__": main()
