from __future__ import annotations

import sqlite3


class ClientPersistence:
    def __init__(self, path: str, client_id: str) -> None:
        self.db = sqlite3.connect(path)
        self.db.execute("CREATE TABLE IF NOT EXISTS client_state (client_id TEXT PRIMARY KEY, last_acked_sequence INTEGER NOT NULL DEFAULT 0)")
        self.db.execute("INSERT OR IGNORE INTO client_state VALUES (?, 0)", (client_id,))
        self.client_id = client_id
        self.db.commit()

    @property
    def last_sequence(self) -> int:
        return int(self.db.execute("SELECT last_acked_sequence FROM client_state WHERE client_id = ?", (self.client_id,)).fetchone()[0])

    def ack(self, sequence: int) -> None:
        with self.db:
            self.db.execute("UPDATE client_state SET last_acked_sequence = MAX(last_acked_sequence, ?) WHERE client_id = ?", (sequence, self.client_id))
