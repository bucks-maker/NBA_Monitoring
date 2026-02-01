"""CRUD operations for the bot_trades table."""
from __future__ import annotations

import sqlite3


class BotTradesRepo:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def insert_trade(
        self,
        trade_time: str,
        game_id: str | None,
        poly_market_slug: str,
        condition_id: str,
        outcome: str,
        side: str,
        price: float,
        size: float,
        tx_hash: str,
    ) -> None:
        """Insert a bot trade (ignore duplicates by tx_hash)."""
        self.conn.execute(
            """INSERT OR IGNORE INTO bot_trades
               (trade_time, game_id, poly_market_slug, condition_id,
                outcome, side, price, size, tx_hash)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (trade_time, game_id, poly_market_slug, condition_id,
             outcome, side, price, size, tx_hash),
        )

    def commit(self) -> None:
        self.conn.commit()
