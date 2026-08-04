from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone

import websockets


def frame(destination: str, payload: dict) -> bytes:
    body = json.dumps(payload)
    return (f"SEND\ndestination:{destination}\ncontent-type:application/json\n\n{body}\x00").encode()


async def control_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, state: dict[str, bool]) -> None:
    request = (await reader.readline()).decode(errors="replace")
    path = request.split(" ")[1] if len(request.split(" ")) > 1 else "/status"
    if path == "/pause": state["paused"] = True
    elif path == "/resume": state["paused"] = False
    payload = json.dumps({"paused": state["paused"]}).encode()
    writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: " + str(len(payload)).encode() + b"\r\nAccess-Control-Allow-Origin: *\r\n\r\n" + payload)
    await writer.drain()
    writer.close()
    await writer.wait_closed()


async def run(destination: str, rate: float, url: str, state: dict[str, bool]) -> None:
    log = logging.getLogger("event-generator")
    while True:
        try:
            async with websockets.connect(url, ping_interval=None) as ws:
                await ws.send(b"CONNECT\naccept-version:1.2\nhost:playground\n\n\x00")
                await ws.recv()
                log.info("publishing to %s every %.2fs", destination, rate)
                while True:
                    if state["paused"]:
                        await asyncio.sleep(0.25)
                        continue
                    payload = {"uuid": str(uuid.uuid4()), "timestamp": datetime.now(timezone.utc).isoformat(), "random": random.randint(0, 1_000_000), "sequence": "assigned-by-server"}
                    await ws.send(frame(destination, payload))
                    await asyncio.sleep(rate)
        except (OSError, websockets.ConnectionClosed) as exc:
            log.warning("server unavailable (%s); retrying", exc)
            await asyncio.sleep(2)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--destination", default="/origin/demo")
    parser.add_argument("--rate", type=float, default=2.0, help="seconds between events")
    # In Docker the compose file supplies STOMP_URL=ws://server:8080; locally
    # the same code naturally falls back to localhost.
    parser.add_argument("--url", default=os.getenv("STOMP_URL", "ws://localhost:8080"))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    state = {"paused": False}
    async def serve() -> None:
        control = await asyncio.start_server(lambda r, w: control_handler(r, w, state), "0.0.0.0", int(os.getenv("CONTROL_PORT", "8090")))
        await asyncio.gather(run(args.destination, args.rate, args.url, state), control.serve_forever())
    try: asyncio.run(serve())
    except KeyboardInterrupt: pass


if __name__ == "__main__": main()
