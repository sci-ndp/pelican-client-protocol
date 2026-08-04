from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Subscription:
    client_id: str
    subscription_id: str
    destination: str
    ack_mode: str = "auto"
