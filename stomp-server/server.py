from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import uuid
from dataclasses import dataclass

import websockets

from logger import configure_logging, frame_log
from protocol import Frame, ProtocolError, read_frame
from storage import Storage


@dataclass
class Subscription:
    subscription_id: str
    destination: str
    ack_mode: str


class StompServer:
    def __init__(self, storage: Storage) -> None:
        self.storage = storage
        self.sessions: dict[object, dict] = {}
        self.subscriptions: dict[str, set[object]] = {}
        self.log = configure_logging()
        self.drop_event_percent = float(os.getenv("DROP_EVENT_PERCENT", "0"))
        self.restart_after = int(os.getenv("SERVER_RESTART", "0"))
        self.event_count = 0

    async def send(self, websocket, frame: Frame) -> None:
        await websocket.send(frame.encode())
        detail = f"id={frame.headers.get('message-id', '')}" if frame.command == "MESSAGE" else ""
        frame_log(self.log, "SERVER -> CLIENT", frame.command, detail)

    async def handler(self, websocket) -> None:
        self.log.info("HTTP Upgrade accepted; WebSocket transport established")
        state = {"client_id": "", "subscriptions": {}, "drop_next": False, "reorder_buffer": None, "reorder_next": False}
        self.sessions[websocket] = state
        try:
            while True:
                try:
                    frame = await asyncio.wait_for(read_frame(websocket), timeout=90)
                except asyncio.TimeoutError:
                    await websocket.send(b"\n")  # STOMP heartbeat is a single LF.
                    continue
                except ProtocolError as exc:
                    await self.send(websocket, Frame("ERROR", {"message": str(exc)}, str(exc)))
                    break
                if frame.command == "HEARTBEAT":
                    self.log.debug("CLIENT -> SERVER  HEARTBEAT")
                    continue
                frame_log(self.log, "CLIENT -> SERVER", frame.command, f"id={frame.headers.get('id', '')}" if frame.headers.get("id") else "")
                try:
                    await self.dispatch(websocket, state, frame)
                except Exception as exc:  # a malformed command should become an ERROR frame, not kill the server
                    self.log.exception("command failed")
                    await self.send(websocket, Frame("ERROR", {"message": str(exc)}, str(exc)))
        except (websockets.ConnectionClosed, ConnectionError):
            pass
        finally:
            for destination in state["subscriptions"].values():
                self.subscriptions.get(destination, set()).discard(websocket)
            self.sessions.pop(websocket, None)

    async def dispatch(self, websocket, state: dict, frame: Frame) -> None:
        handlers = {"CONNECT": self.connect, "STOMP": self.connect, "SUBSCRIBE": self.subscribe, "ACK": self.ack, "NACK": self.nack, "DISCONNECT": self.disconnect, "SEND": self.publish, "DEMO": self.demo}
        handler = handlers.get(frame.command)
        if not handler: raise ProtocolError(f"unsupported command: {frame.command}")
        await handler(websocket, state, frame)

    async def connect(self, websocket, state: dict, frame: Frame) -> None:
        state["client_id"] = frame.headers.get("client-id", frame.headers.get("host", str(uuid.uuid4())))
        await self.send(websocket, Frame("CONNECTED", {"version": "1.2", "session": state["client_id"], "server": "stomp-playground"}))

    async def subscribe(self, websocket, state: dict, frame: Frame) -> None:
        destination = frame.headers.get("destination")
        subscription_id = frame.headers.get("id")
        if not destination or not subscription_id: raise ProtocolError("SUBSCRIBE requires destination and id")
        last = int(frame.headers.get("last-sequence", "0"))
        oldest = self.storage.oldest(destination)
        if last and oldest is not None and last < oldest - 1:
            await self.send(websocket, Frame("ERROR", {"message": "requested sequence is outside replay window", "receipt-id": frame.headers.get("receipt", "")}, "Replay unavailable"))
            return
        state["subscriptions"][subscription_id] = destination
        self.subscriptions.setdefault(destination, set()).add(websocket)
        self.storage.save_subscription(state["client_id"], subscription_id, destination)
        for row in self.storage.replay(destination, last):
            await self.deliver(websocket, subscription_id, row)
        if frame.headers.get("receipt"):
            await self.send(websocket, Frame("RECEIPT", {"receipt-id": frame.headers["receipt"]}))

    async def publish(self, websocket, state: dict, frame: Frame) -> None:
        destination = frame.headers.get("destination")
        if not destination: raise ProtocolError("SEND requires destination")
        try: body = json.loads(frame.body)
        except json.JSONDecodeError: body = {"value": frame.body}
        await self.publish_event(destination, body)

    async def publish_event(self, destination: str, body: dict) -> None:
        message_id = str(uuid.uuid4())
        sequence = self.storage.add_event(destination, message_id, body)
        self.event_count += 1
        for websocket in list(self.subscriptions.get(destination, set())):
            state = self.sessions.get(websocket)
            if state:
                for subscription_id, subscribed_destination in state["subscriptions"].items():
                    if subscribed_destination == destination:
                        row = self.storage.db.execute("SELECT * FROM events WHERE sequence = ?", (sequence,)).fetchone()
                        if state["drop_next"]:
                            state["drop_next"] = False
                            self.log.warning("DEMO dropped MESSAGE sequence=%s for client=%s", sequence, state["client_id"])
                        elif state["reorder_next"]:
                            if state["reorder_buffer"] is None:
                                state["reorder_buffer"] = (subscription_id, row)
                                self.log.warning("DEMO buffered MESSAGE sequence=%s for reordering", sequence)
                            else:
                                buffered_subscription, buffered_row = state["reorder_buffer"]
                                state["reorder_buffer"] = None
                                await self.deliver(websocket, subscription_id, row)
                                await self.deliver(websocket, buffered_subscription, buffered_row)
                                state["reorder_next"] = False
                        else:
                            await self.deliver(websocket, subscription_id, row)
        if self.restart_after and self.event_count >= self.restart_after:
            self.log.warning("SERVER_RESTART reached; closing active connections")
            for websocket in list(self.sessions): await websocket.close()

    async def deliver(self, websocket, subscription_id: str, row) -> None:
        if random.random() < self.drop_event_percent / 100: return
        body = row["body"] if isinstance(row, dict) is False else row["body"]
        await self.send(websocket, Frame("MESSAGE", {"subscription": subscription_id, "message-id": row["message_id"], "destination": row["destination"], "sequence": str(row["sequence"]), "timestamp": row["timestamp"], "ack": row["message_id"]}, body))

    async def ack(self, websocket, state: dict, frame: Frame) -> None:
        sequence = int(frame.headers.get("sequence", "0"))
        self.storage.save_session(state["client_id"], sequence)
        if frame.headers.get("receipt"): await self.send(websocket, Frame("RECEIPT", {"receipt-id": frame.headers["receipt"]}))

    async def nack(self, websocket, state: dict, frame: Frame) -> None:
        await self.send(websocket, Frame("ERROR", {"message": "NACK received; replay by reconnecting with last-sequence"}, "NACK replay is intentionally explicit in this playground."))

    async def demo(self, websocket, state: dict, frame: Frame) -> None:
        """Small, explicit fault-injection commands used by the browser teaching UI."""
        action = frame.headers.get("action", "")
        if action == "drop-next":
            state["drop_next"] = True
        elif action == "reorder-next":
            state["reorder_next"] = True
            state["reorder_buffer"] = None
        elif action == "restart-server":
            self.log.warning("DEMO server restart requested; closing active WebSockets while SQLite remains intact")
            for active in list(self.sessions):
                await active.close(code=1012, reason="demo server restart")
        else:
            raise ProtocolError(f"unknown DEMO action: {action}")
        await self.send(websocket, Frame("RECEIPT", {"receipt-id": frame.headers.get("receipt", action), "demo": action}))

    async def disconnect(self, websocket, state: dict, frame: Frame) -> None:
        if frame.headers.get("receipt"): await self.send(websocket, Frame("RECEIPT", {"receipt-id": frame.headers["receipt"]}))
        await websocket.close()


async def main(port: int, replay_window: int) -> None:
    storage = Storage(os.getenv("SERVER_DB", "server.sqlite3"), replay_window)
    app = StompServer(storage)
    async with websockets.serve(app.handler, "0.0.0.0", port, ping_interval=None):
        app.log.info("STOMP 1.2 WebSocket server listening on ws://0.0.0.0:%s", port)
        await asyncio.Future()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Readable STOMP 1.2 playground server")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--replay-window", type=int, default=1000)
    args = parser.parse_args()
    try: asyncio.run(main(args.port, args.replay_window))
    except KeyboardInterrupt: pass
