"""SQLite outbox — persistent buffer between read loop and uplink loop.
Same shape as the Phase-0 spike's buffer.py; identical contract so the cloud
side stays unchanged when we move from spike to Satellite to Moxa runtime."""
from __future__ import annotations
import json
import sqlite3
import time
from collections.abc import Iterable
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS outbox (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_json  TEXT NOT NULL,
  created_at  REAL NOT NULL,
  sent        INTEGER NOT NULL DEFAULT 0,
  attempts    INTEGER NOT NULL DEFAULT 0,
  last_error  TEXT
);
CREATE INDEX IF NOT EXISTS outbox_unsent_idx ON outbox(sent, created_at) WHERE sent = 0;
"""


class Outbox:
    def __init__(self, path: str):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
        self.db.executescript(SCHEMA)

    def enqueue(self, batch: dict) -> int:
        cur = self.db.execute(
            "INSERT INTO outbox(batch_json, created_at) VALUES (?, ?)",
            (json.dumps(batch), time.time()),
        )
        return cur.lastrowid or -1

    def peek_unsent(self, limit: int = 50) -> list[tuple[int, str]]:
        return list(self.db.execute(
            "SELECT id, batch_json FROM outbox WHERE sent=0 ORDER BY id LIMIT ?",
            (limit,),
        ))

    def mark_sent(self, ids: Iterable[int]) -> None:
        ids = list(ids)
        if not ids: return
        placeholders = ",".join("?" * len(ids))
        self.db.execute(f"UPDATE outbox SET sent=1 WHERE id IN ({placeholders})", ids)

    def record_failure(self, ids: Iterable[int], err: str) -> None:
        ids = list(ids)
        if not ids: return
        placeholders = ",".join("?" * len(ids))
        self.db.execute(
            f"UPDATE outbox SET attempts = attempts + 1, last_error = ? "
            f"WHERE id IN ({placeholders})", [err, *ids],
        )

    def stats(self) -> dict:
        row = self.db.execute(
            "SELECT SUM(CASE WHEN sent=0 THEN 1 ELSE 0 END), "
            "       SUM(CASE WHEN sent=1 THEN 1 ELSE 0 END) FROM outbox"
        ).fetchone()
        return {"unsent": row[0] or 0, "sent": row[1] or 0}

    def vacuum_sent(self, older_than_sec: int = 86400) -> int:
        """Delete sent rows older than `older_than_sec`. Returns rows removed."""
        cutoff = time.time() - older_than_sec
        cur = self.db.execute(
            "DELETE FROM outbox WHERE sent=1 AND created_at < ?", (cutoff,),
        )
        return cur.rowcount or 0
