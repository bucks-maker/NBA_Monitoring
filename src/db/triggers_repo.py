"""CRUD operations for the triggers table."""
from __future__ import annotations

import sqlite3
from datetime import datetime


class TriggersRepo:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def insert_trigger(
        self,
        game_id: str,
        trigger_time: str,
        trigger_type: str,
        prev_line: float | None,
        prev_over_implied: float | None,
        prev_under_implied: float | None,
        new_line: float | None,
        new_over_implied: float | None,
        new_under_implied: float | None,
        delta_line: float,
        delta_under: float,
        poly_over: float | None,
        poly_under: float | None,
        poly_gap_under: float | None,
        poly_gap_over: float | None,
    ) -> None:
        """Insert a trigger event."""
        self.conn.execute(
            """INSERT INTO triggers
               (game_id, trigger_time, trigger_type,
                prev_line, prev_over_implied, prev_under_implied,
                new_line, new_over_implied, new_under_implied,
                delta_line, delta_under_implied,
                poly_over_price, poly_under_price, poly_gap_under, poly_gap_over)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (game_id, trigger_time, trigger_type,
             prev_line, prev_over_implied, prev_under_implied,
             new_line, new_over_implied, new_under_implied,
             delta_line, delta_under,
             poly_over, poly_under, poly_gap_under, poly_gap_over),
        )

    def get_open_triggers(self) -> list[tuple]:
        """Get triggers where gap hasn't closed yet."""
        return self.conn.execute(
            """SELECT id, game_id, new_line, new_under_implied, new_over_implied, trigger_time
               FROM triggers
               WHERE gap_closed_time IS NULL AND poly_gap_under IS NOT NULL"""
        ).fetchall()

    def update_gap_closed(
        self,
        trigger_id: int,
        closed_time: str,
        lag_seconds: int,
    ) -> None:
        """Mark a trigger's gap as closed."""
        self.conn.execute(
            """UPDATE triggers SET gap_closed_time = ?, lag_seconds = ?
               WHERE id = ?""",
            (closed_time, lag_seconds, trigger_id),
        )

    def commit(self) -> None:
        self.conn.commit()
