"""CLOB API client for Polymarket orderbook and price data.

Consolidates CLOB calls from rebalance_monitor.py (verify_opportunity, seed_best_asks).
"""
from __future__ import annotations

import httpx

from src.config import CLOBConfig

_DEFAULT_HEADERS = {"User-Agent": "Mozilla/5.0"}


class CLOBClient:
    def __init__(self, config: CLOBConfig | None = None):
        self.config = config or CLOBConfig()

    def get_orderbook(self, token_id: str) -> dict:
        """Fetch the full orderbook for a token.

        Returns:
            {"asks": [...], "bids": [...]}
        """
        resp = httpx.get(
            f"{self.config.base_url}/book",
            params={"token_id": token_id},
            timeout=self.config.timeout,
            headers=_DEFAULT_HEADERS,
        )
        resp.raise_for_status()
        return resp.json()

    def get_price(self, token_id: str, side: str = "sell") -> float | None:
        """Fetch the current price for a token.

        Args:
            token_id: CLOB token ID
            side: "sell" for best ask, "buy" for best bid

        Returns:
            Price as float, or None on failure.
        """
        try:
            resp = httpx.get(
                f"{self.config.base_url}/price",
                params={"token_id": token_id, "side": side},
                timeout=self.config.timeout,
                headers=_DEFAULT_HEADERS,
            )
            resp.raise_for_status()
            price = float(resp.json().get("price", 0))
            return price if price > 0 else None
        except Exception:
            return None
