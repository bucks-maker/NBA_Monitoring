"""Paper trading engine for simulated gap trading."""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Callable, Optional

# NBA games typically last ~2.5 hours. Block trades after this window.
MAX_GAME_DURATION_MINUTES = 150

from src.db.paper_trades_repo import PaperTradesRepo
from src.shared.time_utils import now_et_str


@dataclass
class OpenPosition:
    """Represents an open paper trade position."""
    trade_id: int
    game_id: str
    market_type: str
    outcome: str
    entry_time: float  # unix timestamp
    entry_price: float
    entry_bid: float
    hold_seconds: int = 30


class PaperTradingEngine:
    """
    Paper trading engine that simulates trades based on gap signals.

    Entry: Buy at best_ask when gap >= threshold
    Exit: Sell at best_bid after hold_seconds
    """

    def __init__(
        self,
        repo: PaperTradesRepo,
        price_getter: Callable[[str, str, str], Optional[float]],
        book_getter: Callable[[str, str, str], tuple[Optional[float], Optional[float]]],
        token_price_getter: Optional[Callable[[str], Optional[float]]] = None,
        token_book_getter: Optional[Callable[[str], tuple[Optional[float], Optional[float]]]] = None,
        commence_getter: Optional[Callable[[str], Optional[str]]] = None,
        gap_threshold: float = 0.04,
        hold_seconds: int = 30,
        fee_rate: float = 0.02,
        max_positions: int = 10,
        cooldown_seconds: int = 60,
    ):
        """
        Args:
            repo: Database repository for paper trades
            price_getter: (game_id, market_type, outcome) -> price
            book_getter: (game_id, market_type, outcome) -> (bid, ask)
            token_price_getter: (token_id) -> price (direct lookup)
            token_book_getter: (token_id) -> (bid, ask) (direct lookup)
            commence_getter: (game_id) -> commence_time ISO string
            gap_threshold: Minimum gap to enter (default 4%p)
            hold_seconds: Seconds to hold position (default 30)
            fee_rate: Fee rate to deduct from PnL (default 2%)
            max_positions: Maximum concurrent positions
            cooldown_seconds: Cooldown per game/outcome after trade
        """
        self.repo = repo
        self.price_getter = price_getter
        self.book_getter = book_getter
        self.token_price_getter = token_price_getter
        self.token_book_getter = token_book_getter
        self.commence_getter = commence_getter
        self.gap_threshold = gap_threshold
        self.hold_seconds = hold_seconds
        self.fee_rate = fee_rate
        self.max_positions = max_positions
        self.cooldown_seconds = cooldown_seconds

        self.open_positions: dict[int, OpenPosition] = {}
        self.cooldowns: dict[str, float] = {}  # "game_id:outcome" -> last_trade_time
        self._lock = threading.Lock()
        self._running = True
        self._exit_thread = threading.Thread(target=self._exit_loop, daemon=True)
        self._exit_thread.start()

        self.stats = {"signals": 0, "entries": 0, "exits": 0, "skipped": 0}

    def on_signal(
        self,
        game_id: str,
        market_type: str,
        outcome: str,
        oracle_implied: float,
        signal_source: str = "poly_anomaly",
        token_id: Optional[str] = None,
    ) -> Optional[int]:
        """
        Handle a trading signal. Enter position if conditions met.

        Args:
            game_id: Game identifier
            market_type: 'moneyline', 'total', 'spread'
            outcome: Outcome name (e.g., 'Lakers', 'Over')
            oracle_implied: Oracle's implied probability
            signal_source: Signal source for tracking
            token_id: Optional token ID for direct price lookup

        Returns:
            trade_id if position opened, None otherwise
        """
        self.stats["signals"] += 1

        # Only trade moneyline (best performer)
        if market_type != "moneyline":
            self.stats["skipped"] += 1
            return None

        # Check game state: block trades on finished/late-game markets
        if self.commence_getter:
            commence_str = self.commence_getter(game_id)
            if commence_str:
                try:
                    commence_dt = datetime.fromisoformat(commence_str.replace("Z", "+00:00"))
                    now_utc = datetime.now(timezone.utc)
                    elapsed = now_utc - commence_dt
                    if elapsed > timedelta(minutes=MAX_GAME_DURATION_MINUTES):
                        self.stats["skipped"] += 1
                        mins = int(elapsed.total_seconds() / 60)
                        print(f"  [Paper] SKIP {outcome}: game likely over "
                              f"(started {mins}min ago, limit={MAX_GAME_DURATION_MINUTES}min)")
                        return None
                except (ValueError, TypeError):
                    pass  # Unparseable commence_time, allow trade

        # Check cooldown
        key = f"{game_id}:{outcome}"
        now = time.time()
        if key in self.cooldowns and (now - self.cooldowns[key]) < self.cooldown_seconds:
            return None

        # Check max positions
        with self._lock:
            if len(self.open_positions) >= self.max_positions:
                self.stats["skipped"] += 1
                return None

        # Get current price first (most reliable)
        current_price = None
        if token_id and self.token_price_getter:
            current_price = self.token_price_getter(token_id)
        if current_price is None:
            current_price = self.price_getter(game_id, market_type, outcome)

        if current_price is None:
            print(f"  [Paper] SKIP {outcome}: no price (game_id={game_id[:8]}...)")
            return None

        # Try to get orderbook data
        bid, ask = None, None
        book_source = "none"

        # Try direct token lookup first if token_id provided
        if token_id and self.token_book_getter:
            bid, ask = self.token_book_getter(token_id)
            if bid is not None and ask is not None:
                book_source = "token_book"

        # Fallback to game/market/outcome lookup
        if bid is None or ask is None:
            bid, ask = self.book_getter(game_id, market_type, outcome)
            if bid is not None and ask is not None:
                book_source = "book"

        # Sanity check: book data should be close to current price
        # If book ask is too far from price (>20%), use price instead
        if bid is not None and ask is not None:
            if abs(ask - current_price) > 0.20:
                # Book data seems stale/wrong, use price
                bid = current_price - 0.01
                ask = current_price + 0.01
                book_source = "price_fallback"

        # Final fallback to price
        if bid is None or ask is None:
            bid = current_price - 0.01
            ask = current_price + 0.01
            book_source = "price"

        # Calculate gap
        gap = abs(oracle_implied - ask) if oracle_implied else 0

        # Check gap threshold
        if gap < self.gap_threshold:
            print(f"  [Paper] SKIP {outcome}: gap {gap*100:.1f}%p < 4%p "
                  f"(oracle={oracle_implied:.2f}, ask={ask:.2f}, src={book_source})")
            return None

        # Check price range (realistic execution)
        if not (0.15 <= ask <= 0.85):
            self.stats["skipped"] += 1
            print(f"  [Paper] SKIP {outcome}: ask={ask:.2f} out of [0.15,0.85] "
                  f"(oracle={oracle_implied:.2f}, gap={gap*100:.1f}%p, src={book_source})")
            return None

        # Open position
        trade_id = self.repo.open_position(
            game_id=game_id,
            market_type=market_type,
            outcome=outcome,
            signal_source=signal_source,
            gap_at_signal=gap,
            entry_price=ask,
            entry_bid=bid,
        )

        with self._lock:
            self.open_positions[trade_id] = OpenPosition(
                trade_id=trade_id,
                game_id=game_id,
                market_type=market_type,
                outcome=outcome,
                entry_time=now,
                entry_price=ask,
                entry_bid=bid,
                hold_seconds=self.hold_seconds,
            )
            self.cooldowns[key] = now

        self.stats["entries"] += 1
        print(f"  [Paper] ENTRY #{trade_id}: {outcome} @ {ask:.3f} (gap={gap*100:.1f}%p)")

        return trade_id

    def _exit_loop(self):
        """Background thread to close positions after hold period."""
        while self._running:
            try:
                self._check_exits()
            except Exception as e:
                print(f"  [Paper] Exit loop error: {e}")
            time.sleep(1)

    def _check_exits(self):
        """Check and close positions that have reached hold time."""
        now = time.time()
        to_close = []

        with self._lock:
            for trade_id, pos in self.open_positions.items():
                if (now - pos.entry_time) >= pos.hold_seconds:
                    to_close.append((trade_id, pos))

        for trade_id, pos in to_close:
            self._close_position(trade_id, pos)

    def _close_position(self, trade_id: int, pos: OpenPosition):
        """Close a position and record results."""
        # Get current bid/ask
        bid, ask = self.book_getter(pos.game_id, pos.market_type, pos.outcome)
        if bid is None:
            price = self.price_getter(pos.game_id, pos.market_type, pos.outcome)
            bid = price - 0.01 if price else pos.entry_price
            ask = price + 0.01 if price else pos.entry_price

        result = self.repo.close_position(
            trade_id=trade_id,
            exit_price=bid,
            exit_ask=ask or bid + 0.02,
            fee_rate=self.fee_rate,
        )

        with self._lock:
            if trade_id in self.open_positions:
                del self.open_positions[trade_id]

        self.stats["exits"] += 1

        if result:
            pnl_pct = result["pnl_net"] * 100
            emoji = "+" if pnl_pct > 0 else ""
            print(f"  [Paper] EXIT #{trade_id}: {pos.outcome} @ {bid:.3f} -> "
                  f"PnL={emoji}{pnl_pct:.1f}%")

    def get_status(self) -> dict:
        """Get current engine status."""
        with self._lock:
            open_count = len(self.open_positions)

        stats = self.repo.get_stats(hours=24)
        return {
            "open_positions": open_count,
            "stats_24h": stats,
            "engine_stats": self.stats.copy(),
        }

    def print_summary(self):
        """Print trading summary."""
        stats = self.repo.get_stats(hours=24)
        print(f"\n{'='*50}")
        print(f"Paper Trading Summary (24h)")
        print(f"{'='*50}")
        print(f"Trades:    {stats['total']}")
        print(f"Win Rate:  {stats['win_rate']*100:.0f}%")
        print(f"Avg PnL:   {stats['avg_pnl_net']*100:+.2f}%")
        print(f"Total PnL: {stats['total_pnl']*100:+.1f}%")
        print(f"Slippage:  {stats['avg_slippage']*100:.2f}%")
        print(f"{'='*50}\n")

    def stop(self):
        """Stop the engine."""
        self._running = False

        # Close all open positions
        with self._lock:
            positions = list(self.open_positions.items())

        for trade_id, pos in positions:
            self._close_position(trade_id, pos)
