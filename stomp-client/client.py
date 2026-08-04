from __future__ import annotations

import argparse
import asyncio
import logging
import os
from urllib.parse import urlparse, urlunparse

import websockets

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
        self.log = logging.getLogger("stomp-client")

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
            await self.send(ws, Frame("CONNECT", {"accept-version": "1.2", "host": "playground", "heart-beat": f"{self.args.heartbeat},{self.args.heartbeat}"}))
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
        self.log.info("event destination=%s body=%s", frame.headers.get("destination"), frame.body)
        if frame.headers.get("ack"):
            await self.send(ws, Frame("ACK", {"id": frame.headers["ack"]}))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--destination", default="/origin/demo")
    parser.add_argument("--url", default=os.getenv("STOMP_URL", "ws://localhost:8080"))
    parser.add_argument("--reconnect", action="store_true")
    parser.add_argument("--heartbeat", type=int, default=10000, help="heartbeat interval in milliseconds")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    try: asyncio.run(StompClient(args).run())
    except KeyboardInterrupt: pass


if __name__ == "__main__": main()
