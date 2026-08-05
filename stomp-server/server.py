from __future__ import annotations

import argparse
import asyncio
import json
import uuid

import websockets

from logger import configure_logging, frame_log
from protocol import Frame, ProtocolError, read_frame

ACK_MODES = {"auto", "client", "client-individual"}


class StompServer:
    def __init__(self) -> None:
        self.sessions: dict[object, dict] = {}
        self.subscriptions: dict[str, set[object]] = {}
        self.log = configure_logging()

    async def send(self, websocket, frame: Frame) -> None:
        await websocket.send(frame.encode())
        detail = f"id={frame.headers.get('message-id', '')}" if frame.command == "MESSAGE" else ""
        frame_log(self.log, "SERVER -> CLIENT", frame.command, detail)

    async def handler(self, websocket) -> None:
        self.log.info("HTTP Upgrade accepted; WebSocket transport established")
        state = {"subscriptions": {}, "ack_index": {}}
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
            for sub in state["subscriptions"].values():
                self.subscriptions.get(sub["destination"], set()).discard(websocket)
            self.sessions.pop(websocket, None)

    async def dispatch(self, websocket, state: dict, frame: Frame) -> None:
        handlers = {"CONNECT": self.connect, "STOMP": self.connect, "SUBSCRIBE": self.subscribe, "ACK": self.ack, "NACK": self.nack, "DISCONNECT": self.disconnect, "SEND": self.publish}
        handler = handlers.get(frame.command)
        if not handler: raise ProtocolError(f"unsupported command: {frame.command}")
        await handler(websocket, state, frame)

    async def connect(self, websocket, state: dict, frame: Frame) -> None:
        await self.send(websocket, Frame("CONNECTED", {"version": "1.2", "session": str(uuid.uuid4()), "server": "stomp-playground"}))

    async def subscribe(self, websocket, state: dict, frame: Frame) -> None:
        destination = frame.headers.get("destination")
        subscription_id = frame.headers.get("id")
        if not destination or not subscription_id: raise ProtocolError("SUBSCRIBE requires destination and id")
        ack_mode = frame.headers.get("ack", "auto")
        if ack_mode not in ACK_MODES: raise ProtocolError(f"unsupported ack mode: {ack_mode}")
        state["subscriptions"][subscription_id] = {"destination": destination, "ack": ack_mode, "pending": {}}
        self.subscriptions.setdefault(destination, set()).add(websocket)
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
        payload = json.dumps(body)
        for websocket in list(self.subscriptions.get(destination, set())):
            state = self.sessions.get(websocket)
            if not state: continue
            for subscription_id, sub in state["subscriptions"].items():
                if sub["destination"] == destination:
                    await self.deliver(websocket, state, subscription_id, message_id, destination, payload)

    async def deliver(self, websocket, state: dict, subscription_id: str, message_id: str, destination: str, body: str, attempts: int = 0) -> None:
        sub = state["subscriptions"][subscription_id]
        headers = {"subscription": subscription_id, "message-id": message_id, "destination": destination}
        if sub["ack"] != "auto":
            ack_id = str(uuid.uuid4())
            headers["ack"] = ack_id
            sub["pending"][ack_id] = {"message_id": message_id, "destination": destination, "body": body, "attempts": attempts}
            state["ack_index"][ack_id] = subscription_id
        await self.send(websocket, Frame("MESSAGE", headers, body))

    def _resolve(self, state: dict, sub: dict, ack_id: str) -> list[tuple[str, dict]]:
        """Remove and return the (ack_id, pending-message) pairs an ACK/NACK on ack_id resolves.

        client-individual resolves only ack_id; client resolves every message
        delivered at or before ack_id on this subscription (cumulative), per STOMP 1.2.
        """
        resolved = []
        if sub["ack"] == "client":
            for pending_id in list(sub["pending"]):
                info = sub["pending"].pop(pending_id)
                state["ack_index"].pop(pending_id, None)
                resolved.append((pending_id, info))
                if pending_id == ack_id: break
        else:
            info = sub["pending"].pop(ack_id, None)
            state["ack_index"].pop(ack_id, None)
            if info is not None: resolved.append((ack_id, info))
        return resolved

    async def ack(self, websocket, state: dict, frame: Frame) -> None:
        ack_id = frame.headers.get("id")
        if not ack_id: raise ProtocolError("ACK requires id")
        subscription_id = state["ack_index"].get(ack_id)
        if subscription_id is None: raise ProtocolError(f"unknown ack id: {ack_id}")
        self._resolve(state, state["subscriptions"][subscription_id], ack_id)
        if frame.headers.get("receipt"):
            await self.send(websocket, Frame("RECEIPT", {"receipt-id": frame.headers["receipt"]}))

    async def nack(self, websocket, state: dict, frame: Frame) -> None:
        ack_id = frame.headers.get("id")
        if not ack_id: raise ProtocolError("NACK requires id")
        subscription_id = state["ack_index"].get(ack_id)
        if subscription_id is None: raise ProtocolError(f"unknown ack id: {ack_id}")
        sub = state["subscriptions"][subscription_id]
        for _, pending in self._resolve(state, sub, ack_id):
            if pending["attempts"] >= 1:
                self.log.warning("dropping message-id=%s after redelivery was also nacked", pending["message_id"])
            else:
                await self.deliver(websocket, state, subscription_id, pending["message_id"], pending["destination"], pending["body"], attempts=pending["attempts"] + 1)
        if frame.headers.get("receipt"):
            await self.send(websocket, Frame("RECEIPT", {"receipt-id": frame.headers["receipt"]}))

    async def disconnect(self, websocket, state: dict, frame: Frame) -> None:
        if frame.headers.get("receipt"): await self.send(websocket, Frame("RECEIPT", {"receipt-id": frame.headers["receipt"]}))
        await websocket.close()


async def main(port: int) -> None:
    app = StompServer()
    async with websockets.serve(app.handler, "0.0.0.0", port, ping_interval=None):
        app.log.info("STOMP 1.2 WebSocket server listening on ws://0.0.0.0:%s", port)
        await asyncio.Future()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Readable STOMP 1.2 playground server")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    try: asyncio.run(main(args.port))
    except KeyboardInterrupt: pass
