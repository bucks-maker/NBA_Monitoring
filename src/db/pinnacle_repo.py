"""CRUD operations for the pinnacle_snapshots table."""
from __future__ import annotations

import sqlite3


class PinnacleRepo:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def insert_snapshot(
        self,
        game_id: str,
        snapshot_time: str,
        total_line: float | None,
        over_price: float | None,
        under_price: float | None,
        over_implied: float | None,
        under_implied: float | None,
    ) -> None:
        """Insert a Pinnacle snapshot (ignore duplicates)."""
        self.conn.execute(
            """INSERT OR IGNORE INTO pinnacle_snapshots
               (game_id, snapshot_time, total_line, over_price, under_price,
                over_implied, under_implied)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (game_id, snapshot_time, total_line, over_price, under_price,
             over_implied, under_implied),
        )

    def get_previous(self, game_id: str) -> tuple | None:
        """Get the second-most-recent snapshot for move detection."""
        return self.conn.execute(
            """SELECT total_line, over_implied, under_implied, snapshot_time
               FROM pinnacle_snapshots
               WHERE game_id = ?
               ORDER BY snapshot_time DESC
               LIMIT 1 OFFSET 1""",
            (game_id,),
        ).fetchone()

    def commit(self) -> None:
        self.conn.commit()
