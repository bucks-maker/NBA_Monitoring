"""Hi-Res gap capture module for Forward Test v2.

Schedules gap measurements at t+3s, t+10s, t+30s after trigger events.
Core question: "After Oracle move + 3s delay, does gap >= 4%p remain executable?"

Moved from monitor/hi_res_capture.py, now uses HiResRepo.
"""
from __future__ import annotations

import threading
import time
from typing import Callable, Dict, List, Optional, Any

from src.config import HiResConfig
from src.db.hi_res_repo import HiResRepo


class HiResCapture:
    """Manages high-resolution gap capture scheduling."""

    def __init__(
        self,
        repo: HiResRepo,
        config: HiResConfig | None = None,
    ):
        self.repo = repo
        self.config = config or HiResConfig()
        self._active_captures: Dict[int, List[threading.Thread]] = {}
        self._price_getter: Optional[Callable] = None
        self._orderbook_getter: Optional[Callable] = None
        self._lock = threading.Lock()
        self._stats = {
            "captures_scheduled": 0,
            "captures_completed": 0,
            "captures_failed": 0,
        }

    def set_price_getter(self, fn: Callable[[str, str, str], Optional[float]]) -> None:
        """Set function to get current Poly price: fn(game_id, market_type, outcome) -> price"""
        self._price_getter = fn

    def set_orderbook_getter(self, fn: Callable) -> None:
        """Set function to get orderbook: fn(game_id, market_type, outcome) -> (bid, ask, depth)"""
        self._orderbook_getter = fn

    def record_move_event(
        self,
        game_key: str,
        market_type: str,
        trigger_source: str,
        oracle_prev_implied: float | None,
        oracle_new_implied: float | None,
        poly_t0: float | None,
        poly_line: float | None = None,
        oracle_line: float | None = None,
        outcome_name: str | None = None,
        depth_t0: float | None = None,
        spread_t0: float | None = None,
    ) -> int | None:
        """Record a hi-res move event at t0."""
        move_ts = int(time.time())

        oracle_delta = None
        if oracle_prev_implied is not None and oracle_new_implied is not None:
            oracle_delta = oracle_new_implied - oracle_prev_implied

        gap_t0 = None
        if oracle_new_implied is not None and poly_t0 is not None:
            gap_t0 = abs(oracle_new_implied - poly_t0)

        event_id = self.repo.insert_move_event(
            game_key=game_key,
            market_type=market_type,
            move_ts_unix=move_ts,
            oracle_prev_implied=oracle_prev_implied,
            oracle_new_implied=oracle_new_implied,
            oracle_delta=oracle_delta,
            poly_t0=poly_t0,
            gap_t0=gap_t0,
            poly_line=poly_line,
            oracle_line=oracle_line,
            depth_t0=depth_t0,
            spread_t0=spread_t0,
            trigger_source=trigger_source,
            outcome_name=outcome_name,
        )

        if event_id is not None:
            self.repo.insert_gap_series(event_id, 0, poly_t0, gap_t0, depth=depth_t0)

        return event_id

    def schedule_captures(
        self,
        move_event_id: int,
        game_key: str,
        market_type: str,
        outcome: str,
        oracle_implied: float,
    ) -> None:
        """Schedule gap captures at configured offsets (3s, 10s, 30s)."""
        if self._price_getter is None:
            return

        threads = []
        for offset in self.config.offsets:
            t = threading.Thread(
                target=self._capture_at_offset,
                args=(move_event_id, game_key, market_type, outcome, oracle_implied, offset),
                daemon=True,
            )
            t.start()
            threads.append(t)

        with self._lock:
            self._active_captures[move_event_id] = threads
            self._stats["captures_scheduled"] += len(self.config.offsets)

    def _capture_at_offset(self, move_event_id, game_key, market_type, outcome, oracle_implied, offset_sec):
        time.sleep(offset_sec)
        try:
            poly_price = self._price_getter(game_key, market_type, outcome)

            bid = ask = depth = None
            if self._orderbook_getter:
                bid, ask, depth = self._orderbook_getter(game_key, market_type, outcome)

            if poly_price is None:
                self._stats["captures_failed"] += 1
                return

            gap = abs(oracle_implied - poly_price)

            self.repo.insert_gap_series(move_event_id, offset_sec, poly_price, gap, bid, ask, depth)
            self.repo.update_capture(move_event_id, offset_sec, poly_price, gap)

            self._stats["captures_completed"] += 1

            if gap >= self.config.actionable_gap:
                print(f"  [HiRes] t+{offset_sec}s: gap={gap*100:.1f}%p (poly={poly_price:.3f}) **ACTIONABLE**")
            else:
                print(f"  [HiRes] t+{offset_sec}s: gap={gap*100:.1f}%p (poly={poly_price:.3f})")

        except Exception as e:
            print(f"[HiResCapture] t+{offset_sec}s capture failed: {e}")
            self._stats["captures_failed"] += 1

    def get_stats(self) -> Dict[str, Any]:
        return dict(self._stats)
