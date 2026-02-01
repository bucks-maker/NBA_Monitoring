"""Gamma API client for Polymarket event data.

Consolidates Gamma API calls from snapshot.py and rebalance_monitor.py.
"""
from __future__ import annotations

import json
import time
from typing import Any

import httpx

from src.config import GammaConfig


class GammaClient:
    def __init__(self, config: GammaConfig | None = None):
        self.config = config or GammaConfig()
        self._client: httpx.Client | None = None

    @property
    def client(self) -> httpx.Client:
        if self._client is None or self._client.is_closed:
            self._client = httpx.Client(timeout=self.config.timeout)
        return self._client

    def get_event_by_slug(self, slug: str) -> list[dict]:
        """Fetch event(s) by Polymarket slug.

        Used by lag monitor to match Pinnacle games to Polymarket events.
        """
        try:
            resp = self.client.get(
                f"{self.config.base_url}/events",
                params={"slug": slug},
            )
            resp.raise_for_status()
            return resp.json()
        except Exception:
            return []

    def get_all_active_events(self) -> list[dict]:
        """Fetch all active events with pagination.

        Used by rebalance monitor to discover negativeRisk events.
        """
        all_events: list[dict] = []
        offset = 0

        while True:
            params = {
                "closed": "false",
                "active": "true",
                "limit": self.config.fetch_limit,
                "offset": offset,
            }
            try:
                resp = self.client.get(
                    f"{self.config.base_url}/events",
                    params=params,
                )
                resp.raise_for_status()
                events = resp.json()
            except Exception:
                break

            if not events:
                break

            all_events.extend(events)

            if len(events) < self.config.fetch_limit:
                break

            offset += self.config.fetch_limit
            time.sleep(self.config.fetch_delay)

        return all_events

    def get_market_tokens(
        self,
        slug: str,
        classify_fn=None,
    ) -> list[dict]:
        """Extract token IDs from an event's markets.

        Args:
            slug: Polymarket event slug
            classify_fn: Optional function(question, slug) -> market_type

        Returns:
            List of {"token_id", "market_type", "outcome", "market_slug"}
        """
        events = self.get_event_by_slug(slug)
        if not events:
            return []

        event = events[0]
        tokens: list[dict] = []

        for m in event.get("markets", []):
            q = m.get("question") or ""
            market_slug = m.get("slug", "")

            if classify_fn:
                market_type = classify_fn(q, market_slug)
                if market_type in ("player_prop", "other"):
                    continue
            else:
                market_type = "unknown"

            if m.get("closed", False):
                continue

            clob_token_ids = m.get("clobTokenIds")
            if isinstance(clob_token_ids, str):
                clob_token_ids = json.loads(clob_token_ids)
            if not clob_token_ids:
                continue

            outcomes = m.get("outcomes", [])
            if isinstance(outcomes, str):
                outcomes = json.loads(outcomes)

            for i, token_id in enumerate(clob_token_ids):
                outcome = outcomes[i] if i < len(outcomes) else f"outcome_{i}"
                tokens.append({
                    "token_id": token_id,
                    "market_type": market_type,
                    "outcome": outcome,
                    "market_slug": market_slug,
                })

        return tokens

    def close(self) -> None:
        if self._client and not self._client.is_closed:
            self._client.close()
