from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone


class Storage:
    def __init__(self, path: str, replay_window: int) -> None:
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.lock = threading.Lock()
        self.replay_window = replay_window
        with self.db:
            self.db.executescript("""
            CREATE TABLE IF NOT EXISTS events (sequence INTEGER PRIMARY KEY, destination TEXT NOT NULL, message_id TEXT NOT NULL, timestamp TEXT NOT NULL, body TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS sessions (client_id TEXT PRIMARY KEY, last_sequence INTEGER NOT NULL DEFAULT 0, updated_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS subscriptions (client_id TEXT, subscription_id TEXT, destination TEXT, PRIMARY KEY(client_id, subscription_id));
            """)

    def add_event(self, destination: str, message_id: str, body: dict) -> int:
        with self.lock, self.db:
            row = self.db.execute("SELECT COALESCE(MAX(sequence), 0) + 1 AS next FROM events").fetchone()
            sequence = int(row["next"])
            timestamp = datetime.now(timezone.utc).isoformat()
            self.db.execute("INSERT INTO events VALUES (?, ?, ?, ?, ?)", (sequence, destination, message_id, timestamp, json.dumps(body)))
            self.db.execute("DELETE FROM events WHERE sequence <= (SELECT MAX(sequence) FROM events) - ?", (self.replay_window,))
            return sequence

    def replay(self, destination: str, after: int) -> list[sqlite3.Row]:
        return self.db.execute("SELECT * FROM events WHERE destination = ? AND sequence > ? ORDER BY sequence", (destination, after)).fetchall()

    def oldest(self, destination: str) -> int | None:
        row = self.db.execute("SELECT MIN(sequence) AS value FROM events WHERE destination = ?", (destination,)).fetchone()
        return row["value"]

    def save_session(self, client_id: str, sequence: int) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self.lock, self.db:
            self.db.execute("INSERT INTO sessions(client_id,last_sequence,updated_at) VALUES(?,?,?) ON CONFLICT(client_id) DO UPDATE SET last_sequence=excluded.last_sequence,updated_at=excluded.updated_at", (client_id, sequence, now))

    def save_subscription(self, client_id: str, subscription_id: str, destination: str) -> None:
        with self.lock, self.db:
            self.db.execute("INSERT OR REPLACE INTO subscriptions VALUES(?,?,?)", (client_id, subscription_id, destination))
