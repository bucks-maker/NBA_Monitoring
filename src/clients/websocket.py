"""Unified Polymarket CLOB WebSocket client.

Merges ws_client.py (PolyWebSocket) and rebalance_monitor.py (RebalanceWebSocket).
Supports all observed message formats:

- Batch price_changes: {"market":"..","price_changes":[{"asset_id":"..","best_ask":"0.5",...}]}
- Legacy price_change: {"event_type":"price_change","asset_id":"..","price":".."}
- Book array: [{"asset_id":"..","asks":[..],"bids":[..]}]
- Book object: {"event_type":"book","asset_id":"..","asks":[..],"bids":[..]}
"""
from __future__ import annotations

import json
import threading
import time
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional

try:
    import websocket
except ImportError:
    raise ImportError("websocket-client required: pip install websocket-client")

from src.config import WebSocketConfig


class PolyWebSocket:
    """Unified WebSocket client for the Polymarket CLOB market channel.

    Usage:
        ws = PolyWebSocket(config)
        ws.on_price_change(my_callback)
        ws.on_book_update(my_book_callback)
        ws.subscribe(["token_id_1", "token_id_2"])
        ws.run_forever(background=True)
    """

    def __init__(self, config: WebSocketConfig | None = None):
        self.config = config or WebSocketConfig()
        self.ws: Optional[websocket.WebSocketApp] = None
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._connected = False
        self._reconnect_delay = self.config.reconnect_initial

        # Subscription management
        self._subscribed_assets: List[str] = []
        self._pending_subscribe: List[str] = []

        # Callbacks
        self._price_callbacks: List[Callable[[str, Dict], None]] = []
        self._book_callbacks: List[Callable[[str, Dict], None]] = []
        self._error_callbacks: List[Callable[[Exception], None]] = []
        self._connect_callbacks: List[Callable[[], None]] = []
        self._disconnect_callbacks: List[Callable[[], None]] = []

        # Price cache (asset_id -> last_price)
        self._price_cache: Dict[str, float] = {}

        # Stats
        self._stats = {
            "messages_received": 0,
            "price_updates": 0,
            "book_updates": 0,
            "reconnects": 0,
            "errors": 0,
            "parse_errors": 0,
            "last_message_time": None,
        }

        self._lock = threading.Lock()

    # ── Callback registration ─────────────────────────────

    def on_price_change(self, callback: Callable[[str, Dict], None]):
        """Register price change callback: fn(asset_id, data)."""
        self._price_callbacks.append(callback)
        return self

    def on_book_update(self, callback: Callable[[str, Dict], None]):
        """Register book update callback: fn(asset_id, data)."""
        self._book_callbacks.append(callback)
        return self

    def on_error(self, callback: Callable[[Exception], None]):
        self._error_callbacks.append(callback)
        return self

    def on_connect(self, callback: Callable[[], None]):
        self._connect_callbacks.append(callback)
        return self

    def on_disconnect(self, callback: Callable[[], None]):
        self._disconnect_callbacks.append(callback)
        return self

    # ── Subscription ──────────────────────────────────────

    def subscribe(self, asset_ids: List[str]) -> None:
        """Subscribe to market data for given token IDs."""
        with self._lock:
            new_assets = [a for a in asset_ids if a not in self._subscribed_assets]
            if not new_assets:
                return

            if self._connected and self.ws:
                self._send_subscribe(new_assets)
                self._subscribed_assets.extend(new_assets)
            else:
                self._pending_subscribe.extend(new_assets)

    def unsubscribe(self, asset_ids: List[str]) -> None:
        """Unsubscribe from market data."""
        with self._lock:
            if self._connected and self.ws:
                msg = {"type": "unsubscribe", "channel": "market", "assets_ids": asset_ids}
                try:
                    self.ws.send(json.dumps(msg))
                except Exception:
                    pass
            for a in asset_ids:
                if a in self._subscribed_assets:
                    self._subscribed_assets.remove(a)

    def _send_subscribe(self, asset_ids: List[str]) -> None:
        """Send subscription in batches."""
        if not asset_ids:
            return
        batch_size = self.config.subscribe_batch_size
        for i in range(0, len(asset_ids), batch_size):
            batch = asset_ids[i : i + batch_size]
            msg = json.dumps({"type": "market", "assets_ids": batch})
            try:
                self.ws.send(msg)
            except Exception as e:
                self._handle_error(e)

    # ── Lifecycle ─────────────────────────────────────────

    def run_forever(self, background: bool = True) -> None:
        self._running = True
        if background:
            self._thread = threading.Thread(target=self._run_loop, daemon=True)
            self._thread.start()
        else:
            self._run_loop()

    def stop(self) -> None:
        self._running = False
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass

    def is_connected(self) -> bool:
        return self._connected

    def get_stats(self) -> Dict[str, Any]:
        return dict(self._stats)

    def get_cached_price(self, asset_id: str) -> Optional[float]:
        return self._price_cache.get(asset_id)

    # ── Internal loop ─────────────────────────────────────

    def _run_loop(self) -> None:
        while self._running:
            try:
                self._connect()
            except Exception as e:
                self._handle_error(e)

            if not self._running:
                break

            time.sleep(self._reconnect_delay)
            self._reconnect_delay = min(
                self._reconnect_delay * self.config.reconnect_multiplier,
                self.config.reconnect_max,
            )
            self._stats["reconnects"] += 1

    def _connect(self) -> None:
        self.ws = websocket.WebSocketApp(
            self.config.url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        self.ws.run_forever(
            ping_interval=self.config.ping_interval,
            ping_timeout=self.config.ping_timeout,
        )

    # ── WebSocket handlers ────────────────────────────────

    def _on_open(self, ws) -> None:
        self._connected = True
        self._reconnect_delay = self.config.reconnect_initial

        with self._lock:
            if self._pending_subscribe:
                self._send_subscribe(self._pending_subscribe)
                self._subscribed_assets.extend(self._pending_subscribe)
                self._pending_subscribe = []
            if self._subscribed_assets:
                self._send_subscribe(self._subscribed_assets)

        for cb in self._connect_callbacks:
            try:
                cb()
            except Exception:
                pass

    def _on_message(self, ws, message: str) -> None:
        self._stats["messages_received"] += 1
        self._stats["last_message_time"] = time.time()

        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            return

        try:
            if isinstance(data, list):
                # Book event (array form)
                for item in data:
                    if isinstance(item, dict):
                        self._handle_book_item(item)
            elif isinstance(data, dict):
                if "price_changes" in data:
                    # Batch price changes (primary format)
                    self._handle_batch_price_changes(data)
                elif data.get("event_type") == "book":
                    self._handle_book_item(data)
                elif data.get("event_type") == "price_change":
                    self._handle_legacy_price_change(data)
                elif data.get("event_type") == "last_trade_price":
                    self._handle_legacy_price_change(data)
                # tick_size_change etc. ignored
        except Exception:
            self._stats["parse_errors"] += 1

    def _on_error(self, ws, error) -> None:
        self._handle_error(
            error if isinstance(error, Exception) else Exception(str(error))
        )

    def _on_close(self, ws, close_status_code, close_msg) -> None:
        self._connected = False
        for cb in self._disconnect_callbacks:
            try:
                cb()
            except Exception:
                pass

    # ── Message parsing ───────────────────────────────────

    def _handle_batch_price_changes(self, data: Dict) -> None:
        """Handle: {"market":"..","price_changes":[{"asset_id":"..","best_ask":"0.5",...}]}"""
        for change in data.get("price_changes", []):
            asset_id = change.get("asset_id")
            if not asset_id:
                continue

            # Update price cache from multiple possible fields
            price = change.get("price") or change.get("best_ask")
            if price is not None:
                self._price_cache[asset_id] = float(price)

            self._stats["price_updates"] += 1

            for cb in self._price_callbacks:
                try:
                    cb(asset_id, change)
                except Exception:
                    pass

    def _handle_legacy_price_change(self, data: Dict) -> None:
        """Handle: {"event_type":"price_change","asset_id":"..","price":".."}"""
        asset_id = data.get("asset_id")
        if not asset_id:
            return

        price = data.get("price")
        if price is not None:
            self._price_cache[asset_id] = float(price)

        self._stats["price_updates"] += 1

        for cb in self._price_callbacks:
            try:
                cb(asset_id, data)
            except Exception:
                pass

    def _handle_book_item(self, item: Dict) -> None:
        """Handle: {"asset_id":"..","asks":[..],"bids":[..]}"""
        asset_id = item.get("asset_id")
        if not asset_id:
            return

        asks = item.get("asks")
        if not asks:
            return

        self._stats["book_updates"] += 1

        for cb in self._book_callbacks:
            try:
                cb(asset_id, item)
            except Exception:
                pass

    def _handle_error(self, error: Exception) -> None:
        self._stats["errors"] += 1
        for cb in self._error_callbacks:
            try:
                cb(error)
            except Exception:
                pass


class AssetPriceTracker:
    """Track per-asset price history over a sliding window.

    Used by the lag monitor for 5-minute price delta detection.
    """

    def __init__(self, window_seconds: int = 300):
        self.window_seconds = window_seconds
        self._history: Dict[str, List[tuple]] = defaultdict(list)
        self._lock = threading.Lock()

    def record(self, asset_id: str, price: float, timestamp: float | None = None) -> None:
        ts = timestamp or time.time()
        with self._lock:
            self._history[asset_id].append((ts, price))
            self._cleanup(asset_id, ts)

    def get_price_delta(
        self,
        asset_id: str,
        lookback_seconds: int | None = None,
    ) -> float | None:
        """Price change over window: current - oldest within window."""
        lookback = lookback_seconds or self.window_seconds
        now = time.time()
        cutoff = now - lookback

        with self._lock:
            history = self._history.get(asset_id, [])
            if len(history) < 2:
                return None

            old_price = None
            for ts, price in history:
                if ts >= cutoff:
                    break
                old_price = price

            if old_price is None:
                old_price = history[0][1]

            return history[-1][1] - old_price

    def get_current_price(self, asset_id: str) -> float | None:
        with self._lock:
            history = self._history.get(asset_id, [])
            return history[-1][1] if history else None

    def _cleanup(self, asset_id: str, now: float) -> None:
        cutoff = now - self.window_seconds - 60
        history = self._history[asset_id]
        while history and history[0][0] < cutoff:
            history.pop(0)
