from __future__ import annotations

from .storage import Storage


def replay_events(storage: Storage, destination: str, last_sequence: int) -> list[dict]:
    rows = storage.replay(destination, last_sequence)
    return [dict(row) for row in rows]
