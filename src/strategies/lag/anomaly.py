"""Real-time anomaly detection for Polymarket price data.

Pinnacle call trigger conditions (strong signals only, no model):
1. Price spike: |delta_price| >= 5%p within 5 minutes
2. Orderbook spread: bid-ask spread >= 5%p (thin book)
3. Yes/No sum deviation: |1 - (yes + no)| >= 3%p

Moved from monitor/anomaly_detector.py (as-is logic, uses config).
"""
from __future__ import annotations

import time
import threading
from typing import Dict, List, Optional, Any, Callable
from collections import defaultdict
from dataclasses import dataclass, field

from src.config import AnomalyConfig


@dataclass
class AnomalyEvent:
    """Detected anomaly event."""
    game_id: str
    market_type: str
    anomaly_type: str  # 'price_change', 'orderbook_spread', 'yes_no_deviation'
    timestamp: float
    details: Dict[str, Any] = field(default_factory=dict)

    def __str__(self):
        return (
            f"[{self.anomaly_type}] game={self.game_id} market={self.market_type} "
            f"details={self.details}"
        )


class AnomalyDetector:
    def __init__(self, config: AnomalyConfig | None = None):
        cfg = config or AnomalyConfig()
        self.price_threshold = cfg.price_change_threshold
        self.price_window = cfg.price_window_seconds
        self.spread_threshold = cfg.bid_ask_spread_threshold
        self.yes_no_threshold = cfg.yes_no_deviation_threshold
        self.cooldown_seconds = cfg.pinnacle_cooldown_seconds

        self._price_history: Dict[tuple, List[tuple]] = defaultdict(list)
        self._orderbook: Dict[tuple, Dict[str, float]] = {}
        self._price_pairs: Dict[tuple, Dict[str, float]] = defaultdict(dict)
        self._pinnacle_cooldown: Dict[str, float] = {}
        self._anomaly_callbacks: List[Callable[[AnomalyEvent], None]] = []

        self._stats = {
            "price_anomalies": 0,
            "spread_anomalies": 0,
            "yes_no_anomalies": 0,
            "pinnacle_triggers": 0,
            "cooldown_blocks": 0,
        }
        self._lock = threading.Lock()

    def on_anomaly(self, callback: Callable[[AnomalyEvent], None]):
        self._anomaly_callbacks.append(callback)
        return self

    def update_price(
        self,
        game_id: str,
        market_type: str,
        outcome: str,
        price: float,
        timestamp: float | None = None,
    ) -> Optional[AnomalyEvent]:
        ts = timestamp or time.time()
        key = (game_id, market_type, outcome)
        pair_key = (game_id, market_type)

        with self._lock:
            self._price_history[key].append((ts, price))
            self._cleanup_history(key, ts)

            normalized = self._normalize_outcome(outcome)
            self._price_pairs[pair_key][normalized] = price

            anomaly = self._check_price_anomaly(game_id, market_type, key, price, ts)
            if anomaly:
                return anomaly

            anomaly = self._check_yes_no_anomaly(game_id, market_type, pair_key, ts)
            if anomaly:
                return anomaly

        return None

    def update_orderbook(
        self,
        game_id: str,
        market_type: str,
        outcome: str,
        best_bid: float,
        best_ask: float,
        timestamp: float | None = None,
    ) -> Optional[AnomalyEvent]:
        ts = timestamp or time.time()
        key = (game_id, market_type, outcome)

        with self._lock:
            self._orderbook[key] = {"bid": best_bid, "ask": best_ask}

            if best_bid <= 0.02 or best_ask >= 0.98:
                return None

            spread = best_ask - best_bid
            if spread >= self.spread_threshold:
                self._stats["spread_anomalies"] += 1
                event = AnomalyEvent(
                    game_id=game_id,
                    market_type=market_type,
                    anomaly_type="orderbook_spread",
                    timestamp=ts,
                    details={
                        "outcome": outcome,
                        "best_bid": best_bid,
                        "best_ask": best_ask,
                        "spread": spread,
                    },
                )
                self._fire_anomaly(event)
                return event

        return None

    def should_call_pinnacle(self, game_id: str) -> bool:
        now = time.time()
        with self._lock:
            last_call = self._pinnacle_cooldown.get(game_id, 0)
            if now - last_call < self.cooldown_seconds:
                self._stats["cooldown_blocks"] += 1
                return False
            return True

    def mark_pinnacle_called(self, game_id: str) -> None:
        with self._lock:
            self._pinnacle_cooldown[game_id] = time.time()
            self._stats["pinnacle_triggers"] += 1

    def get_stats(self) -> Dict[str, Any]:
        return dict(self._stats)

    def _check_price_anomaly(self, game_id, market_type, key, current_price, now):
        history = self._price_history[key]
        if len(history) < 2:
            return None

        cutoff = now - self.price_window
        old_price = None
        for ts, price in history:
            if ts >= cutoff:
                break
            old_price = price

        if old_price is None:
            if len(history) >= 2:
                old_price = history[0][1]
            else:
                return None

        delta = current_price - old_price
        if abs(delta) >= self.price_threshold:
            self._stats["price_anomalies"] += 1
            event = AnomalyEvent(
                game_id=game_id,
                market_type=market_type,
                anomaly_type="price_change",
                timestamp=now,
                details={
                    "outcome": key[2],
                    "old_price": old_price,
                    "new_price": current_price,
                    "delta": delta,
                    "window_seconds": self.price_window,
                },
            )
            self._fire_anomaly(event)
            return event
        return None

    def _check_yes_no_anomaly(self, game_id, market_type, pair_key, now):
        prices = self._price_pairs.get(pair_key, {})
        yes_price = prices.get("yes")
        no_price = prices.get("no")

        if yes_price is None or no_price is None:
            return None

        total = yes_price + no_price
        deviation = abs(1.0 - total)

        if deviation >= self.yes_no_threshold:
            self._stats["yes_no_anomalies"] += 1
            event = AnomalyEvent(
                game_id=game_id,
                market_type=market_type,
                anomaly_type="yes_no_deviation",
                timestamp=now,
                details={
                    "yes_price": yes_price,
                    "no_price": no_price,
                    "total": total,
                    "deviation": deviation,
                    "arbitrage_opportunity": total < 1.0 - 0.01,
                },
            )
            self._fire_anomaly(event)
            return event
        return None

    def _normalize_outcome(self, outcome: str) -> str:
        o = outcome.lower()
        if o in ("yes", "over", "home"):
            return "yes"
        elif o in ("no", "under", "away"):
            return "no"
        return o

    def _cleanup_history(self, key, now):
        cutoff = now - self.price_window - 60
        history = self._price_history[key]
        while history and history[0][0] < cutoff:
            history.pop(0)

    def _fire_anomaly(self, event):
        for cb in self._anomaly_callbacks:
            try:
                cb(event)
            except Exception:
                pass


class TriggerManager:
    """Manages anomaly -> Pinnacle call -> gap recording pipeline."""

    def __init__(
        self,
        detector: AnomalyDetector,
        pinnacle_callback: Callable[[str], None] | None = None,
    ):
        self.detector = detector
        self.pinnacle_callback = pinnacle_callback
        self._pending_triggers: Dict[str, List[AnomalyEvent]] = defaultdict(list)
        self._processed_triggers: List[Dict[str, Any]] = []
        self._lock = threading.Lock()

    def process_anomaly(self, event: AnomalyEvent) -> None:
        game_id = event.game_id
        with self._lock:
            self._pending_triggers[game_id].append(event)

        if self.detector.should_call_pinnacle(game_id):
            self._trigger_pinnacle(game_id)

    def _trigger_pinnacle(self, game_id: str) -> None:
        self.detector.mark_pinnacle_called(game_id)
        if self.pinnacle_callback:
            try:
                self.pinnacle_callback(game_id)
            except Exception:
                pass

        with self._lock:
            events = self._pending_triggers.pop(game_id, [])
            self._processed_triggers.append({
                "game_id": game_id,
                "timestamp": time.time(),
                "events": [str(e) for e in events],
            })

    def get_pending_triggers(self) -> Dict[str, int]:
        with self._lock:
            return {gid: len(events) for gid, events in self._pending_triggers.items()}

    def get_processed_count(self) -> int:
        return len(self._processed_triggers)
