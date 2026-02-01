"""Rebalance arbitrage monitor orchestrator.

Watches multi-outcome events for total best_ask sum < $1.00.
Uses shared WebSocket client and CLOB verification.

Refactored from monitor/rebalance_monitor.py.
"""
from __future__ import annotations

import logging
import signal
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from src.config import AppConfig, load_config
from src.clients.gamma import GammaClient
from src.clients.clob import CLOBClient
from src.clients.websocket import PolyWebSocket
from src.strategies.rebalance.tracker import RebalanceTracker
from src.strategies.rebalance.scanner import scan_and_register
from src.strategies.rebalance.alerts import on_opportunity

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("rebalance")


class RebalanceMonitor:
    def __init__(self, config: AppConfig):
        self.config = config
        self.gamma = GammaClient(config.gamma)
        self.clob = CLOBClient(config.clob)

        self.tracker = RebalanceTracker(
            threshold=config.rebalance.threshold,
            strong_threshold=config.rebalance.strong_threshold,
            min_depth=config.rebalance.min_depth,
            on_opportunity=lambda opp: on_opportunity(
                opp, self.clob, self.tracker, config.alert_file,
            ),
        )

    def seed_best_asks(self) -> None:
        """Fetch actual best_ask from CLOB /price API for all registered tokens."""
        token_ids = self.tracker.registered_token_ids
        n_total = len(token_ids)
        workers = self.config.rebalance.seed_workers
        log.info(f"CLOB best_ask seeding: {n_total} tokens (workers={workers})")

        updated = 0
        failed = 0
        t0 = time.time()

        def fetch_one(tid):
            price = self.clob.get_price(tid, side="sell")
            return (tid, price)

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(fetch_one, tid): tid for tid in token_ids}
            for fut in as_completed(futures):
                tid, best_ask = fut.result()
                if best_ask is not None:
                    self.tracker.update_best_ask(tid, best_ask)
                    updated += 1
                else:
                    failed += 1

                done = updated + failed
                if done % 5000 == 0 and done > 0:
                    elapsed = time.time() - t0
                    log.info(f"  CLOB progress: {done}/{n_total} ({elapsed:.0f}s)")

        elapsed = time.time() - t0
        log.info(f"CLOB seeding complete: {updated} updated, {failed} failed ({elapsed:.0f}s)")

    def print_status(self, ws: PolyWebSocket) -> None:
        sums = self.tracker.get_all_event_sums()
        ws_s = ws.get_stats()
        t_s = self.tracker.stats
        top_n = self.config.rebalance.status_top_n

        now_str = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
        conn = "OK" if ws.is_connected() else "DOWN"

        print(f"\n{'='*72}")
        print(f"[{now_str}] WS:{conn} msgs={ws_s['messages_received']} "
              f"prices={ws_s['price_updates']} books={ws_s['book_updates']} "
              f"reconn={ws_s['reconnects']} err={ws_s['errors']}")
        print(f"Tracker: {self.tracker.n_events} events, {self.tracker.n_tokens} tokens | "
              f"updates={t_s['book_updates']} opps={t_s['opportunities_found']} "
              f"strong={t_s['strong_opportunities']}")

        with_data = [s for s in sums if s["sum"] is not None]
        no_data = [s for s in sums if s["sum"] is None]

        if with_data:
            print(f"\n  TOP {min(top_n, len(with_data))} (lowest ask sum):")
            for s in with_data[:top_n]:
                gap_str = f"{s['gap']*100:+.2f}%" if s["gap"] is not None else "?"
                marker = " <-- OPP (unverified)" if s["sum"] < 1.0 else ""
                print(
                    f"    sum={s['sum']:.4f} gap={gap_str} "
                    f"[{s['n_with_data']}/{s['n_outcomes']}] "
                    f"{s['title'][:50]}{marker}"
                )

        partial = len([s for s in with_data if s["n_with_data"] < s["n_outcomes"]])
        print(f"\n  Data: complete={len(with_data)-partial} partial={partial} "
              f"no_data={len(no_data)}")
        print(f"{'='*72}\n")

    def run(self) -> None:
        log.info("=" * 60)
        log.info("Rebalance Arbitrage Monitor starting")
        log.info("=" * 60)

        # 1. Scan and register events
        token_ids = scan_and_register(self.tracker, self.gamma, self.config.rebalance)
        if not token_ids:
            log.warning("No events to subscribe. Exiting.")
            return

        # 2. Seed best_asks from CLOB
        self.seed_best_asks()

        sums = self.tracker.get_all_event_sums()
        has_data = [s for s in sums if s["sum"] is not None]
        under_1 = [s for s in has_data if s["sum"] < 1.0]
        log.info(f"CLOB init: {len(has_data)} events with sums, {len(under_1)} sum<1.0")

        # 3. WebSocket
        ws = PolyWebSocket(self.config.ws)

        def on_price_change(asset_id, data):
            best_ask = data.get("best_ask") or data.get("price")
            if asset_id and best_ask:
                try:
                    self.tracker.update_best_ask(asset_id, float(best_ask))
                except (TypeError, ValueError):
                    pass

        def on_book_update(asset_id, data):
            if asset_id and data.get("asks"):
                self.tracker.update_book(asset_id, data)

        ws.on_price_change(on_price_change)
        ws.on_book_update(on_book_update)
        ws.subscribe(token_ids)
        log.info(f"WebSocket subscribing: {len(token_ids)} tokens")
        ws.run_forever(background=True)

        # 4. Signal handling
        shutdown = False

        def _signal_handler(sig, frame):
            nonlocal shutdown
            log.info("Shutdown signal received...")
            shutdown = True

        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)

        # 5. Main loop
        cfg = self.config.rebalance
        last_refresh = time.time()
        last_status = time.time()
        log.info("Main loop started (Ctrl+C to stop)")

        while not shutdown:
            now = time.time()

            if now - last_refresh >= cfg.refresh_interval:
                try:
                    new_tokens = scan_and_register(self.tracker, self.gamma, cfg)
                    if new_tokens:
                        log.info(f"New tokens: {len(new_tokens)} subscribed")
                        ws.subscribe(new_tokens)
                except Exception as e:
                    log.error(f"Event refresh failed: {e}")
                last_refresh = now

            if now - last_status >= cfg.status_interval:
                try:
                    self.print_status(ws)
                except Exception as e:
                    log.error(f"Status print failed: {e}")
                last_status = now

            time.sleep(1)

        log.info("Stopping WebSocket...")
        ws.stop()
        log.info("Monitor stopped")
