from __future__ import annotations

import sqlite3
from datetime import datetime, timezone


class ClientPersistence:
    """Durable application state for one logical client."""

    def __init__(self, path: str, client_id: str) -> None:
        self.db = sqlite3.connect(path)
        self.db.execute("CREATE TABLE IF NOT EXISTS client_state (client_id TEXT PRIMARY KEY, processed_events INTEGER NOT NULL DEFAULT 0)")
        self.db.execute("CREATE TABLE IF NOT EXISTS processed_events (client_id TEXT NOT NULL, event_id TEXT NOT NULL, destination TEXT NOT NULL, body TEXT NOT NULL, processed_at TEXT NOT NULL, PRIMARY KEY (client_id, event_id))")
        columns = {row[1] for row in self.db.execute("PRAGMA table_info(client_state)")}
        if "processed_events" not in columns:
            self.db.execute("ALTER TABLE client_state ADD COLUMN processed_events INTEGER NOT NULL DEFAULT 0")
        self.db.execute("INSERT OR IGNORE INTO client_state (client_id) VALUES (?)", (client_id,))
        self.client_id = client_id
        self.db.commit()

    def record_event(self, event_id: str, destination: str, body: str) -> bool:
        """Record an event and return True only for its first delivery."""
        with self.db:
            cursor = self.db.execute(
                "INSERT OR IGNORE INTO processed_events (client_id, event_id, destination, body, processed_at) VALUES (?, ?, ?, ?, ?)",
                (self.client_id, event_id, destination, body, datetime.now(timezone.utc).isoformat()),
            )
            if cursor.rowcount:
                self.db.execute("UPDATE client_state SET processed_events = processed_events + 1 WHERE client_id = ?", (self.client_id,))
                return True
        return False

    @property
    def processed_count(self) -> int:
        return int(self.db.execute("SELECT processed_events FROM client_state WHERE client_id = ?", (self.client_id,)).fetchone()[0])

    def close(self) -> None:
        self.db.close()
