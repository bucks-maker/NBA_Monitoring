"""Data API client for Polymarket bot trade monitoring.

Extracted from snapshot.py check_bot_trades().
"""
from __future__ import annotations

import time

import httpx

from src.config import DataAPIConfig


class DataAPIClient:
    def __init__(self, config: DataAPIConfig | None = None):
        self.config = config or DataAPIConfig()

    def get_recent_activity(
        self,
        user_address: str,
        hours: int = 24,
        limit: int = 100,
    ) -> list[dict]:
        """Fetch recent trade activity for a wallet address.

        Args:
            user_address: Ethereum address
            hours: Lookback period in hours
            limit: Max results

        Returns:
            List of trade activity records.
        """
        now_ts = int(time.time())
        start_ts = now_ts - hours * 3600

        params = {
            "user": user_address,
            "type": "TRADE",
            "start": start_ts,
            "end": now_ts,
            "limit": limit,
            "sortBy": "TIMESTAMP",
            "sortDirection": "DESC",
        }

        try:
            resp = httpx.get(
                f"{self.config.base_url}/activity",
                params=params,
                timeout=self.config.timeout,
            )
            resp.raise_for_status()
            result = resp.json()
            return result if isinstance(result, list) else []
        except Exception:
            return []
