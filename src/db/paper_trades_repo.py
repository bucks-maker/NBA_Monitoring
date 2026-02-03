"""Paper trading repository for simulated trades."""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone


class PaperTradesRepo:
    """Repository for paper trading data."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self._ensure_table()

    def _ensure_table(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS paper_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id TEXT NOT NULL,
                market_type TEXT NOT NULL,
                outcome TEXT NOT NULL,
                signal_time TEXT NOT NULL,
                signal_source TEXT,
                gap_at_signal REAL,
                entry_time TEXT,
                entry_price REAL,
                entry_bid REAL,
                exit_time TEXT,
                exit_price REAL,
                exit_ask REAL,
                hold_seconds INTEGER DEFAULT 30,
                pnl_gross REAL,
                pnl_net REAL,
                slippage REAL,
                status TEXT DEFAULT 'open',
                notes TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_paper_trades_status
            ON paper_trades(status, signal_time)
        """)
        self.conn.commit()

    def open_position(
        self,
        game_id: str,
        market_type: str,
        outcome: str,
        signal_source: str,
        gap_at_signal: float,
        entry_price: float,
        entry_bid: float,
    ) -> int:
        """Open a new paper trade position."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        cursor = self.conn.execute(
            """
            INSERT INTO paper_trades
            (game_id, market_type, outcome, signal_time, signal_source,
             gap_at_signal, entry_time, entry_price, entry_bid, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'open')
            """,
            (game_id, market_type, outcome, now, signal_source,
             gap_at_signal, now, entry_price, entry_bid),
        )
        self.conn.commit()
        return cursor.lastrowid

    def close_position(
        self,
        trade_id: int,
        exit_price: float,
        exit_ask: float,
        fee_rate: float = 0.02,
    ) -> dict:
        """Close an open position and calculate PnL."""
        row = self.conn.execute(
            "SELECT entry_price, entry_bid FROM paper_trades WHERE id = ?",
            (trade_id,),
        ).fetchone()

        if not row:
            return None

        entry_price, entry_bid = row
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        # PnL calculation
        pnl_gross = (exit_price - entry_price) / entry_price if entry_price > 0 else 0
        slippage = (entry_price - entry_bid) + (exit_ask - exit_price)  # total spread cost
        pnl_net = pnl_gross - fee_rate

        self.conn.execute(
            """
            UPDATE paper_trades
            SET exit_time = ?, exit_price = ?, exit_ask = ?,
                pnl_gross = ?, pnl_net = ?, slippage = ?, status = 'closed'
            WHERE id = ?
            """,
            (now, exit_price, exit_ask, pnl_gross, pnl_net, slippage, trade_id),
        )
        self.conn.commit()

        return {
            "trade_id": trade_id,
            "entry_price": entry_price,
            "exit_price": exit_price,
            "pnl_gross": pnl_gross,
            "pnl_net": pnl_net,
            "slippage": slippage,
        }

    def get_open_positions(self) -> list[dict]:
        """Get all open positions."""
        rows = self.conn.execute(
            """
            SELECT id, game_id, market_type, outcome, entry_time, entry_price
            FROM paper_trades WHERE status = 'open'
            """
        ).fetchall()
        return [
            {"id": r[0], "game_id": r[1], "market_type": r[2],
             "outcome": r[3], "entry_time": r[4], "entry_price": r[5]}
            for r in rows
        ]

    def get_stats(self, hours: int = 24) -> dict:
        """Get paper trading statistics."""
        row = self.conn.execute(
            f"""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN pnl_net > 0 THEN 1 ELSE 0 END) as wins,
                AVG(pnl_gross) as avg_pnl_gross,
                AVG(pnl_net) as avg_pnl_net,
                AVG(slippage) as avg_slippage,
                SUM(pnl_net) as total_pnl
            FROM paper_trades
            WHERE status = 'closed'
              AND signal_time > datetime('now', '-{hours} hours')
            """
        ).fetchone()

        total = row[0] or 0
        wins = row[1] or 0

        return {
            "total": total,
            "wins": wins,
            "win_rate": wins / total if total > 0 else 0,
            "avg_pnl_gross": row[2] or 0,
            "avg_pnl_net": row[3] or 0,
            "avg_slippage": row[4] or 0,
            "total_pnl": row[5] or 0,
        }

    def get_recent_trades(self, limit: int = 10) -> list[dict]:
        """Get recent closed trades."""
        rows = self.conn.execute(
            """
            SELECT id, game_id, market_type, outcome, signal_source,
                   entry_price, exit_price, pnl_gross, pnl_net, slippage,
                   signal_time
            FROM paper_trades
            WHERE status = 'closed'
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [
            {
                "id": r[0], "game_id": r[1], "market_type": r[2],
                "outcome": r[3], "signal_source": r[4],
                "entry_price": r[5], "exit_price": r[6],
                "pnl_gross": r[7], "pnl_net": r[8], "slippage": r[9],
                "signal_time": r[10],
            }
            for r in rows
        ]

    def commit(self):
        self.conn.commit()
