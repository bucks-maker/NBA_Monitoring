"""CRUD operations for the poly_snapshots table."""
from __future__ import annotations

import sqlite3


class PolyRepo:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def insert_snapshot(
        self,
        game_id: str,
        poly_market_slug: str,
        snapshot_time: str,
        total_line: float | None,
        over_price: float | None,
        under_price: float | None,
        market_type: str = "total",
    ) -> None:
        """Insert a Polymarket snapshot (ignore duplicates)."""
        self.conn.execute(
            """INSERT OR IGNORE INTO poly_snapshots
               (game_id, poly_market_slug, snapshot_time, total_line,
                over_price, under_price,
                over_best_bid, over_best_ask, under_best_bid, under_best_ask,
                market_type)
               VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, ?)""",
            (game_id, poly_market_slug, snapshot_time, total_line,
             over_price, under_price, market_type),
        )

    def get_closest_poly_snap(
        self,
        game_id: str,
        target_line: float,
        market_type: str = "total",
    ) -> tuple | None:
        """Get the poly snapshot closest to a given line."""
        return self.conn.execute(
            """SELECT over_price, under_price, total_line
               FROM poly_snapshots
               WHERE game_id = ? AND market_type = ?
               ORDER BY ABS(total_line - ?), snapshot_time DESC
               LIMIT 1""",
            (game_id, market_type, target_line),
        ).fetchone()

    def commit(self) -> None:
        self.conn.commit()
