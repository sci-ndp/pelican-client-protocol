from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Session:
    websocket: object
    client_id: str = ""
    subscriptions: dict[str, str] = field(default_factory=dict)
    outgoing: asyncio.Queue | None = None


import asyncio  # kept below the dataclass to make the data shape easy to read
