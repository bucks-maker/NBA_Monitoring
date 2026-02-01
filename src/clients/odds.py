"""Odds API client for Pinnacle data.

Consolidates Odds API calls from snapshot.py (fetch_pinnacle + fetch_oracle_event_odds).
"""
from __future__ import annotations

import httpx

from src.config import OddsAPIConfig


class OddsClient:
    def __init__(self, config: OddsAPIConfig):
        self.config = config

    def get_odds(
        self,
        markets: str = "totals",
    ) -> tuple[list[dict], dict[str, str]]:
        """Fetch NBA odds from Pinnacle.

        Args:
            markets: Comma-separated market types (e.g. "totals")

        Returns:
            (games_json, credit_info) where credit_info has 'used' and 'remaining'
        """
        url = f"{self.config.base_url}/sports/{self.config.sport}/odds"
        params = {
            "apiKey": self.config.key,
            "regions": "us",
            "markets": markets,
            "bookmakers": self.config.bookmaker,
            "oddsFormat": "decimal",
        }
        resp = httpx.get(url, params=params, timeout=self.config.timeout)
        resp.raise_for_status()

        credits = {
            "used": resp.headers.get("x-requests-used", "?"),
            "remaining": resp.headers.get("x-requests-remaining", "?"),
        }

        return resp.json(), credits

    def get_event_odds(
        self,
        event_id: str,
        markets: str = "h2h,alternate_totals,alternate_spreads",
    ) -> tuple[dict, dict[str, str]]:
        """Fetch odds for a specific event (for alternate line matching).

        Args:
            event_id: Odds API event ID
            markets: Markets to include

        Returns:
            (event_json, credit_info)
        """
        url = (
            f"{self.config.base_url}/sports/{self.config.sport}"
            f"/events/{event_id}/odds"
        )
        params = {
            "apiKey": self.config.key,
            "regions": "us",
            "markets": markets,
            "bookmakers": self.config.bookmaker,
            "oddsFormat": "decimal",
        }
        resp = httpx.get(url, params=params, timeout=self.config.timeout)
        resp.raise_for_status()

        credits = {
            "used": resp.headers.get("x-requests-used", "?"),
            "remaining": resp.headers.get("x-requests-remaining", "?"),
        }

        return resp.json(), credits
