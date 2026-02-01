"""CRUD operations for the game_mapping table."""
from __future__ import annotations

import sqlite3

from src.shared.nba import make_poly_slug


class GameMappingRepo:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def upsert(
        self,
        odds_api_id: str,
        home_team: str,
        away_team: str,
        commence_time: str,
    ) -> None:
        """Insert game mapping if not exists, auto-generate poly slug."""
        existing = self.conn.execute(
            "SELECT poly_event_slug FROM game_mapping WHERE odds_api_id = ?",
            (odds_api_id,),
        ).fetchone()

        if existing:
            return

        poly_slug = make_poly_slug(away_team, home_team, commence_time)

        self.conn.execute(
            """INSERT OR IGNORE INTO game_mapping
               (odds_api_id, home_team, away_team, commence_time, poly_event_slug)
               VALUES (?, ?, ?, ?, ?)""",
            (odds_api_id, home_team, away_team, commence_time, poly_slug),
        )

    def get_slug(self, odds_api_id: str) -> str | None:
        """Get poly_event_slug for a game."""
        row = self.conn.execute(
            "SELECT poly_event_slug FROM game_mapping WHERE odds_api_id = ?",
            (odds_api_id,),
        ).fetchone()
        return row[0] if row and row[0] else None

    def mark_found(self, odds_api_id: str) -> None:
        """Mark that the Polymarket event was found."""
        self.conn.execute(
            "UPDATE game_mapping SET poly_event_found = 1 WHERE odds_api_id = ?",
            (odds_api_id,),
        )

    def get_all_slugs(self) -> list[tuple[str, str]]:
        """Return all (odds_api_id, poly_event_slug) pairs with non-empty slugs."""
        return self.conn.execute(
            """SELECT odds_api_id, poly_event_slug
               FROM game_mapping
               WHERE poly_event_slug IS NOT NULL AND poly_event_slug != ''"""
        ).fetchall()

    def get_slug_to_game_id_map(self) -> dict[str, str]:
        """Return {poly_event_slug: odds_api_id} mapping."""
        rows = self.conn.execute(
            "SELECT odds_api_id, poly_event_slug FROM game_mapping WHERE poly_event_slug IS NOT NULL"
        ).fetchall()
        return {row[1]: row[0] for row in rows}

    def commit(self) -> None:
        self.conn.commit()
