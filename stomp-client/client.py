from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import random
from urllib.parse import urlparse, urlunparse

import websockets

from persistence import ClientPersistence
from protocol import Frame, parse


def websocket_url(value: str) -> str:
    """Allow operators to configure a normal HTTP(S) public endpoint."""
    parsed = urlparse(value)
    schemes = {"http": "ws", "https": "wss", "ws": "ws", "wss": "wss"}
    if parsed.scheme not in schemes or not parsed.netloc:
        raise ValueError("STOMP_URL must be an http(s) or ws(s) URL")
    return urlunparse(parsed._replace(scheme=schemes[parsed.scheme]))


class StompClient:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.state = ClientPersistence(os.getenv("CLIENT_DB", "client.sqlite3"), args.client_id)
        self.log = logging.getLogger("stomp-client")
        self.events_seen = 0
        self.drop_ack_percent = float(os.getenv("DROP_ACK_PERCENT", "0"))
        self.disconnect_after = int(os.getenv("DISCONNECT_AFTER", "0"))

    async def run(self) -> None:
        delay = 1
        while True:
            try:
                await self.connected_session()
                delay = 1
            except (OSError, websockets.ConnectionClosed) as exc:
                self.log.warning("connection lost (%s); reconnecting in %ss", exc, delay)
                if not self.args.reconnect: raise
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)

    async def connected_session(self) -> None:
        async with websockets.connect(websocket_url(self.args.url), ping_interval=None) as ws:
            await self.send(ws, Frame("CONNECT", {"accept-version": "1.2", "host": "playground", "client-id": self.args.client_id, "heart-beat": f"{self.args.heartbeat},{self.args.heartbeat}"}))
            connected = await self.receive(ws)
            if connected.command != "CONNECTED": raise RuntimeError(f"expected CONNECTED, got {connected.command}")
            await self.send(ws, Frame("SUBSCRIBE", {"id": "sub-0", "destination": self.args.destination, "ack": "client-individual"}))
            while True:
                frame = await self.receive(ws)
                if frame.command == "MESSAGE": await self.on_message(ws, frame)
                elif frame.command == "ERROR": self.log.error("server ERROR: %s", frame.body or frame.headers.get("message"))

    async def send(self, ws, frame: Frame) -> None:
        await ws.send(frame.encode())
        self.log.info("CLIENT -> SERVER  %s", frame.command)

    async def receive(self, ws) -> Frame:
        data = await ws.recv()
        if isinstance(data, str):
            if data == "\n": return Frame("HEARTBEAT")
            data = data.encode()
        frame = parse(data)
        self.log.info("SERVER -> CLIENT  %s%s", frame.command, f" id={frame.headers.get('message-id')}" if frame.command == "MESSAGE" else "")
        return frame

    async def on_message(self, ws, frame: Frame) -> None:
        self.events_seen += 1
        destination = frame.headers.get("destination", "")
        event_id = self.application_event_id(frame)
        if self.state.record_event(event_id, destination, frame.body):
            self.log.info("processed event id=%s destination=%s body=%s", event_id, destination, frame.body)
        else:
            self.log.info("duplicate event id=%s ignored", event_id)
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
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    try: asyncio.run(StompClient(args).run())
    except KeyboardInterrupt: pass


if __name__ == "__main__": main()
