"""CRUD operations for move_events_hi_res and gap_series_hi_res tables."""
from __future__ import annotations

import sqlite3
from typing import Optional


class HiResRepo:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def insert_move_event(
        self,
        game_key: str,
        market_type: str,
        move_ts_unix: int,
        oracle_prev_implied: float | None,
        oracle_new_implied: float | None,
        oracle_delta: float | None,
        poly_t0: float | None,
        gap_t0: float | None,
        poly_line: float | None = None,
        oracle_line: float | None = None,
        depth_t0: float | None = None,
        spread_t0: float | None = None,
        trigger_source: str | None = None,
        outcome_name: str | None = None,
    ) -> int | None:
        """Insert a hi-res move event and return its ID."""
        try:
            cur = self.conn.execute(
                """INSERT INTO move_events_hi_res
                   (game_key, market_type, poly_line, oracle_line, move_ts_unix,
                    oracle_prev_implied, oracle_new_implied, oracle_delta,
                    poly_t0, gap_t0, depth_t0, spread_t0,
                    trigger_source, outcome_name)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (game_key, market_type, poly_line, oracle_line, move_ts_unix,
                 oracle_prev_implied, oracle_new_implied, oracle_delta,
                 poly_t0, gap_t0, depth_t0, spread_t0,
                 trigger_source, outcome_name),
            )
            self.conn.commit()
            return cur.lastrowid
        except Exception:
            return None

    def update_capture(
        self,
        move_event_id: int,
        offset_sec: int,
        poly_price: float,
        gap: float,
    ) -> None:
        """Update poly/gap at a specific time offset (3, 10, 30)."""
        col_poly = f"poly_t{offset_sec}s"
        col_gap = f"gap_t{offset_sec}s"
        self.conn.execute(
            f"UPDATE move_events_hi_res SET {col_poly} = ?, {col_gap} = ? WHERE id = ?",
            (poly_price, gap, move_event_id),
        )
        self.conn.commit()

    def insert_gap_series(
        self,
        move_event_id: int,
        ts_offset_sec: int,
        poly_price: float | None,
        gap: float | None,
        bid: float | None = None,
        ask: float | None = None,
        depth: float | None = None,
    ) -> None:
        """Insert a gap series data point."""
        self.conn.execute(
            """INSERT INTO gap_series_hi_res
               (move_event_id, ts_offset_sec, poly_price, gap, bid, ask, depth)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (move_event_id, ts_offset_sec, poly_price, gap, bid, ask, depth),
        )
        self.conn.commit()

    def load_all_events(self) -> list[dict]:
        """Load all hi-res events for analysis."""
        rows = self.conn.execute(
            """SELECT
                id, game_key, market_type, poly_line, oracle_line,
                move_ts_unix, oracle_prev_implied, oracle_new_implied, oracle_delta,
                poly_t0, poly_t3s, poly_t10s, poly_t30s,
                gap_t0, gap_t3s, gap_t10s, gap_t30s,
                depth_t0, spread_t0, trigger_source, outcome_name
               FROM move_events_hi_res
               ORDER BY move_ts_unix"""
        ).fetchall()

        columns = [
            "id", "game_key", "market_type", "poly_line", "oracle_line",
            "move_ts_unix", "oracle_prev_implied", "oracle_new_implied", "oracle_delta",
            "poly_t0", "poly_t3s", "poly_t10s", "poly_t30s",
            "gap_t0", "gap_t3s", "gap_t10s", "gap_t30s",
            "depth_t0", "spread_t0", "trigger_source", "outcome_name",
        ]
        return [dict(zip(columns, row)) for row in rows]

    def commit(self) -> None:
        self.conn.commit()
