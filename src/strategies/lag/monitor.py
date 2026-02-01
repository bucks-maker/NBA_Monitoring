"""Pinnacle-Polymarket NBA lag monitor orchestrator.

Two modes:
1. REST polling (default): Pinnacle 1h, Polymarket 30s intervals
2. WebSocket (--ws): Polymarket real-time, Pinnacle on anomaly only

Refactored from monitor/snapshot.py (~1430 lines -> ~400 lines).
API calls, DB, config, and utilities are now in shared modules.
"""
from __future__ import annotations

import json
import re
import signal
import sys
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

from src.config import AppConfig, load_config
from src.clients.gamma import GammaClient
from src.clients.odds import OddsClient
from src.clients.data_api import DataAPIClient
from src.clients.websocket import PolyWebSocket, AssetPriceTracker
from src.db.connection import get_connection
from src.db.game_mapping_repo import GameMappingRepo
from src.db.pinnacle_repo import PinnacleRepo
from src.db.poly_repo import PolyRepo
from src.db.triggers_repo import TriggersRepo
from src.db.bot_trades_repo import BotTradesRepo
from src.db.hi_res_repo import HiResRepo
from src.shared.nba import classify_market, extract_total_line, extract_spread_line
from src.shared.time_utils import now_utc, now_et, now_et_str, is_active_window, seconds_until_active
from src.shared.math_utils import de_vig_implied
from src.strategies.lag.anomaly import AnomalyDetector, AnomalyEvent
from src.strategies.lag.hi_res import HiResCapture

running = True


def _signal_handler(sig, frame):
    global running
    print("\n[STOP] Shutting down...")
    running = False


def _match_team_name(poly_outcome: str, api_outcomes: list) -> int | None:
    poly_lower = poly_outcome.lower().strip()
    for i, o in enumerate(api_outcomes):
        if o.get("name", "").lower().strip() == poly_lower:
            return i
    for i, o in enumerate(api_outcomes):
        api_name = o.get("name", "").lower().strip()
        if poly_lower in api_name or api_name in poly_lower:
            return i
    poly_words = poly_lower.split()
    if poly_words:
        poly_last = poly_words[-1]
        for i, o in enumerate(api_outcomes):
            api_words = o.get("name", "").lower().strip().split()
            if api_words and api_words[-1] == poly_last:
                return i
    return None


class LagMonitor:
    """Main orchestrator for the Pinnacle-Polymarket lag strategy."""

    def __init__(self, config: AppConfig):
        self.config = config
        self.conn = get_connection(config.db_path, thread_safe=True)

        # Repos
        self.game_repo = GameMappingRepo(self.conn)
        self.pin_repo = PinnacleRepo(self.conn)
        self.poly_repo = PolyRepo(self.conn)
        self.triggers_repo = TriggersRepo(self.conn)
        self.bot_repo = BotTradesRepo(self.conn)
        self.hi_res_repo = HiResRepo(self.conn)

        # Clients
        self.odds_client = OddsClient(config.odds)
        self.gamma_client = GammaClient(config.gamma)
        self.data_client = DataAPIClient(config.data_api)

        # State
        self.pinnacle_data: list[dict] = []
        self.pinnacle_data_lock = threading.Lock()

    # ── Pinnacle fetching ─────────────────────────────────

    def fetch_pinnacle(self) -> list[dict]:
        games, credits = self.odds_client.get_odds(markets="totals")
        print(f"  [Odds API] Credits {credits['used']} used / {credits['remaining']} remaining")

        snap_time = now_utc()
        results = []

        for game in games:
            game_id = game["id"]
            home = game["home_team"]
            away = game["away_team"]
            commence = game.get("commence_time", "")

            self.game_repo.upsert(game_id, home, away, commence)

            for bm in game.get("bookmakers", []):
                if bm["key"] != "pinnacle":
                    continue
                for market in bm.get("markets", []):
                    if market["key"] != "totals":
                        continue

                    over_price = under_price = total_line = None
                    for outcome in market["outcomes"]:
                        if outcome["name"] == "Over":
                            over_price = outcome["price"]
                            total_line = outcome["point"]
                        elif outcome["name"] == "Under":
                            under_price = outcome["price"]

                    if total_line is None:
                        continue

                    over_implied = 1 / over_price if over_price else None
                    under_implied = 1 / under_price if under_price else None

                    self.pin_repo.insert_snapshot(
                        game_id, snap_time, total_line,
                        over_price, under_price, over_implied, under_implied,
                    )

                    results.append({
                        "game_id": game_id, "home": home, "away": away,
                        "line": total_line, "over_price": over_price,
                        "under_price": under_price,
                        "over_implied": over_implied, "under_implied": under_implied,
                    })

        self.game_repo.commit()
        self.pin_repo.commit()
        return results

    # ── Polymarket fetching ───────────────────────────────

    def fetch_polymarket(self, games: list[dict]) -> int:
        snap_time = now_utc()
        found = 0

        for game in games:
            game_id = game["game_id"]
            slug = self.game_repo.get_slug(game_id)
            if not slug:
                continue

            events = self.gamma_client.get_event_by_slug(slug)
            if not events:
                continue

            self.game_repo.mark_found(game_id)
            event = events[0]

            for m in event.get("markets", []):
                q = m.get("question") or ""
                market_slug = m.get("slug", "")
                market_type = classify_market(q, market_slug)

                if market_type in ("player_prop", "other"):
                    continue
                if m.get("closed", False):
                    continue

                outcomes = m.get("outcomes", [])
                prices = m.get("outcomePrices", [])
                if isinstance(outcomes, str):
                    outcomes = json.loads(outcomes)
                if isinstance(prices, str):
                    prices = json.loads(prices)

                line = None
                if market_type == "total":
                    line = extract_total_line(q.lower()) or extract_total_line(market_slug)
                    if line is not None and not (170 <= line <= 310):
                        continue
                elif market_type == "spread":
                    line = extract_spread_line(market_slug)

                over_price = under_price = None
                if market_type == "total":
                    for i, name in enumerate(outcomes):
                        p = float(prices[i]) if i < len(prices) else None
                        if p is None:
                            continue
                        if "over" in name.lower():
                            over_price = p
                        else:
                            under_price = p
                else:
                    price1 = float(prices[0]) if len(prices) > 0 else None
                    price2 = float(prices[1]) if len(prices) > 1 else None
                    over_price = price1
                    under_price = price2

                self.poly_repo.insert_snapshot(
                    game_id, market_slug, snap_time, line,
                    over_price, under_price, market_type,
                )
                found += 1

        self.game_repo.commit()
        self.poly_repo.commit()
        return found

    # ── Move detection ────────────────────────────────────

    def detect_moves(self, current: list[dict], hi_res_capture=None) -> list[dict]:
        cfg = self.config.lag
        triggers = []

        for game in current:
            game_id = game["game_id"]
            prev = self.pin_repo.get_previous(game_id)
            if not prev:
                continue

            prev_line, prev_over_imp, prev_under_imp, prev_time = prev
            new_line = game["line"]
            new_over_imp = game["over_implied"]
            new_under_imp = game["under_implied"]

            delta_line = new_line - prev_line if (new_line and prev_line) else 0
            delta_under = (new_under_imp - prev_under_imp) if (new_under_imp and prev_under_imp) else 0
            delta_over = (new_over_imp - prev_over_imp) if (new_over_imp and prev_over_imp) else 0

            trigger_type = None
            if abs(delta_line) >= cfg.line_move_threshold:
                trigger_type = "line_move"
            if abs(delta_under) >= cfg.implied_move_threshold or abs(delta_over) >= cfg.implied_move_threshold:
                trigger_type = "both" if trigger_type else "implied_move"

            if not trigger_type:
                continue

            poly_snap = self.poly_repo.get_closest_poly_snap(game_id, new_line)
            poly_over = poly_snap[0] if poly_snap else None
            poly_under = poly_snap[1] if poly_snap else None
            poly_line = poly_snap[2] if poly_snap else None
            poly_gap_under = (new_under_imp - poly_under) if (new_under_imp and poly_under) else None
            poly_gap_over = (new_over_imp - poly_over) if (new_over_imp and poly_over) else None

            trigger_time = now_utc()

            self.triggers_repo.insert_trigger(
                game_id, trigger_time, trigger_type,
                prev_line, prev_over_imp, prev_under_imp,
                new_line, new_over_imp, new_under_imp,
                delta_line, delta_under,
                poly_over, poly_under, poly_gap_under, poly_gap_over,
            )

            if hi_res_capture and poly_under is not None:
                move_event_id = hi_res_capture.record_move_event(
                    game_key=game_id, market_type="totals",
                    trigger_source="oracle_move",
                    oracle_prev_implied=prev_under_imp,
                    oracle_new_implied=new_under_imp,
                    poly_t0=poly_under, poly_line=poly_line, oracle_line=new_line,
                    outcome_name="Under",
                )
                if move_event_id:
                    gap_t0 = abs(new_under_imp - poly_under) if poly_under else None
                    if gap_t0:
                        print(f"  [HiRes Oracle] Event #{move_event_id}: gap_t0={gap_t0*100:.1f}%p")

            triggers.append({
                "game_id": game_id, "home": game["home"], "away": game["away"],
                "trigger_type": trigger_type, "delta_line": delta_line,
                "delta_under": delta_under, "new_line": new_line,
                "poly_gap_under": poly_gap_under, "poly_gap_over": poly_gap_over,
            })

        self.triggers_repo.commit()
        return triggers

    # ── Bot trades ────────────────────────────────────────

    def check_bot_trades(self) -> int:
        trades = self.data_client.get_recent_activity(self.config.bot_address)
        if not trades:
            return 0

        slug_map = self.game_repo.get_slug_to_game_id_map()
        count = 0

        for t in trades:
            slug = t.get("slug", "")
            ts_raw = t.get("timestamp", "")
            if isinstance(ts_raw, (int, float)) or (isinstance(ts_raw, str) and ts_raw.isdigit()):
                ts_str = datetime.fromtimestamp(int(ts_raw), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            else:
                ts_str = str(ts_raw)

            tx_hash = t.get("transactionHash", "") or f"{slug}_{ts_str}_{t.get('price','')}"

            matched_game_id = None
            for event_slug, game_id in slug_map.items():
                if slug.startswith(event_slug):
                    matched_game_id = game_id
                    break

            self.bot_repo.insert_trade(
                ts_str, matched_game_id, slug,
                t.get("conditionId", ""),
                t.get("outcome", t.get("title", "")),
                t.get("side", ""),
                float(t.get("price", 0) or 0),
                float(t.get("size", 0) or 0),
                tx_hash,
            )
            count += 1

        self.bot_repo.commit()
        return count

    # ── Gap convergence tracking ──────────────────────────

    def track_gap_convergence(self) -> None:
        open_triggers = self.triggers_repo.get_open_triggers()
        for tr in open_triggers:
            tr_id, game_id, tr_line, tr_under_imp, tr_over_imp, tr_time = tr

            poly_snap = self.poly_repo.get_closest_poly_snap(game_id, tr_line)
            if not poly_snap or poly_snap[1] is None:
                continue

            poly_under = poly_snap[1]
            gap_under = abs(tr_under_imp - poly_under) if tr_under_imp else None

            if gap_under is not None and gap_under <= 0.01:
                closed_time = now_utc()
                tr_dt = datetime.fromisoformat(tr_time.replace("Z", "+00:00"))
                closed_dt = datetime.fromisoformat(closed_time.replace("Z", "+00:00"))
                lag = int((closed_dt - tr_dt).total_seconds())
                self.triggers_repo.update_gap_closed(tr_id, closed_time, lag)

        self.triggers_repo.commit()

    # ── Token subscription for WebSocket mode ─────────────

    def fetch_market_tokens(self) -> dict[str, list[dict]]:
        result: dict[str, list[dict]] = {}
        rows = self.game_repo.get_all_slugs()

        for game_id, poly_slug in rows:
            tokens = self.gamma_client.get_market_tokens(poly_slug, classify_fn=classify_market)
            if tokens:
                for t in tokens:
                    t["game_id"] = game_id
                result[game_id] = tokens

        return result

    # ── REST polling mode ─────────────────────────────────

    def run_rest(self) -> None:
        global running
        signal.signal(signal.SIGINT, _signal_handler)
        cfg = self.config.lag

        print(f"Pinnacle-Polymarket NBA Monitor (REST Polling Mode)")
        print(f"DB: {self.config.db_path}")
        print(f"Time: {now_et_str()}")
        print(f"Active window: ET {cfg.active_start_hour:02d}:00 ~ {cfg.active_end_hour:02d}:00")
        print(f"Pinnacle interval: {cfg.normal_interval}s (normal) / {cfg.trigger_interval}s (trigger)")
        print(f"Polymarket interval: {cfg.poly_interval}s")
        print(f"{'='*60}\n")

        pinnacle_interval = cfg.normal_interval
        last_trigger_time = 0
        last_pinnacle_time = 0

        while running:
            if not is_active_window(cfg.active_start_hour, cfg.active_end_hour):
                wait = seconds_until_active(cfg.active_start_hour, cfg.active_end_hour)
                wake_et = (now_et() + timedelta(seconds=wait)).strftime("%H:%M ET")
                print(f"\n[SLEEP] Inactive window. Resuming at {wake_et}")
                for _ in range(wait):
                    if not running:
                        break
                    time.sleep(1)
                continue

            now = time.time()

            if (now - last_trigger_time) > cfg.trigger_cooldown:
                pinnacle_interval = cfg.normal_interval

            if (now - last_pinnacle_time) >= pinnacle_interval:
                try:
                    print(f"\n--- Pinnacle cycle ({now_et_str()}) ---")
                    print("[1/3] Pinnacle collection...")
                    self.pinnacle_data = self.fetch_pinnacle()

                    print("[2/3] Polymarket collection...")
                    poly_count = self.fetch_polymarket(self.pinnacle_data)

                    triggers = self.detect_moves(self.pinnacle_data)
                    if triggers:
                        pinnacle_interval = cfg.trigger_interval
                        last_trigger_time = now

                    print("[3/3] Bot trade check...")
                    bot_count = self.check_bot_trades()

                    self.track_gap_convergence()
                    self._print_status(self.pinnacle_data, poly_count, triggers, bot_count)
                    last_pinnacle_time = now

                except Exception as e:
                    print(f"  [ERROR] {e}")

            elif self.pinnacle_data:
                try:
                    poly_count = self.fetch_polymarket(self.pinnacle_data)
                    self.track_gap_convergence()
                    if poly_count > 0:
                        print(f"  [{now_et_str()}] Poly sub-poll: {poly_count} lines updated")
                except Exception as e:
                    print(f"  [WARN] Poly sub-poll error: {e}")

            for _ in range(cfg.poly_interval):
                if not running:
                    break
                time.sleep(1)

        self.conn.close()
        print("[DONE] Monitor stopped")

    # ── WebSocket mode ────────────────────────────────────

    def run_ws(self) -> None:
        global running
        signal.signal(signal.SIGINT, _signal_handler)
        cfg = self.config.lag

        print(f"Pinnacle-Polymarket NBA Monitor (WebSocket Mode + Forward Test v2)")
        print(f"DB: {self.config.db_path}")
        print(f"Time: {now_et_str()}")
        print(f"Active window: ET {cfg.active_start_hour:02d}:00 ~ {cfg.active_end_hour:02d}:00")
        print(f"{'='*60}\n")

        ws = PolyWebSocket(self.config.ws)
        detector = AnomalyDetector(self.config.anomaly)
        price_tracker = AssetPriceTracker(window_seconds=self.config.anomaly.price_window_seconds)

        token_to_game: dict[str, str] = {}
        token_to_info: dict[str, dict] = {}

        hi_res_capture = HiResCapture(self.hi_res_repo, self.config.hi_res)
        print(f"Forward Test v2: Hi-Res gap capture enabled (t+3s, t+10s, t+30s)")

        ws_stats = {"price_updates": 0, "anomalies_detected": 0, "pinnacle_calls": 0, "hi_res_events": 0}

        def get_poly_price(game_id, market_type, outcome):
            for token_id, info in token_to_info.items():
                if info["game_id"] == game_id and info["market_type"] == market_type and info["outcome"].lower() == outcome.lower():
                    return price_tracker.get_current_price(token_id)
            return None

        hi_res_capture.set_price_getter(get_poly_price)
        hi_res_capture.set_orderbook_getter(lambda *a: (None, None, None))

        def on_price_change(asset_id, data):
            ws_stats["price_updates"] += 1
            info = token_to_info.get(asset_id)
            if not info:
                return
            price = data.get("price")
            if price is None:
                return
            price = float(price)
            price_tracker.record(asset_id, price)
            event = detector.update_price(info["game_id"], info["market_type"], info["outcome"], price)
            if event:
                ws_stats["anomalies_detected"] += 1
                on_anomaly(event)

        def on_book_update(asset_id, data):
            info = token_to_info.get(asset_id)
            if not info:
                return
            bids = data.get("bids", [])
            asks = data.get("asks", [])
            best_bid = float(bids[0]["price"]) if bids else 0.0
            best_ask = float(asks[0]["price"]) if asks else 1.0
            event = detector.update_orderbook(info["game_id"], info["market_type"], info["outcome"], best_bid, best_ask)
            if event:
                ws_stats["anomalies_detected"] += 1
                on_anomaly(event)

        def on_anomaly(event):
            game_id = event.game_id
            print(f"\n[{now_et_str()}] ** ANOMALY: {event}")

            if not detector.should_call_pinnacle(game_id):
                print(f"  (cooldown, skipping Pinnacle)")
                return

            detector.mark_pinnacle_called(game_id)
            ws_stats["pinnacle_calls"] += 1

            try:
                oracle_data, credits = self.odds_client.get_event_odds(game_id)
                print(f"  [Odds API Event] Credits remaining: {credits['remaining']}")

                with self.pinnacle_data_lock:
                    self.pinnacle_data = self.fetch_pinnacle()

                triggers = self.detect_moves(self.pinnacle_data, hi_res_capture)
                for tr in triggers:
                    direction = "UP" if tr["delta_line"] > 0 else "DOWN"
                    gap_str = ""
                    if tr["poly_gap_under"] is not None:
                        gap_str = f" | Poly gap Under={tr['poly_gap_under']:+.1%}"
                    print(f"  ** TRIGGER: {tr['away']}@{tr['home']} "
                          f"line {direction} {abs(tr['delta_line']):.1f}pt "
                          f"(now {tr['new_line']}){gap_str}")

                # Hi-Res capture for anomaly trigger
                self._handle_hi_res_capture(
                    game_id, event.market_type, event, oracle_data,
                    hi_res_capture, token_to_info, price_tracker, ws_stats,
                )
            except Exception as e:
                print(f"  [ERROR] Pinnacle call failed: {e}")

        def initialize():
            nonlocal token_to_game, token_to_info
            print("[Init] Fetching Pinnacle data...")
            try:
                with self.pinnacle_data_lock:
                    self.pinnacle_data = self.fetch_pinnacle()
                print(f"  {len(self.pinnacle_data)} games found")
            except Exception as e:
                print(f"  [ERROR] {e}")
                return

            print("[Init] Fetching Polymarket token IDs...")
            market_tokens = self.fetch_market_tokens()
            all_token_ids = []
            for game_id, tokens in market_tokens.items():
                for t in tokens:
                    token_id = t["token_id"]
                    all_token_ids.append(token_id)
                    token_to_game[token_id] = game_id
                    token_to_info[token_id] = {
                        "game_id": game_id, "market_type": t["market_type"],
                        "outcome": t["outcome"], "market_slug": t["market_slug"],
                    }

            print(f"  {len(all_token_ids)} tokens across {len(market_tokens)} games")
            if all_token_ids:
                ws.subscribe(all_token_ids)
                print(f"[Init] WebSocket subscription complete")

        def refresh():
            nonlocal token_to_game, token_to_info
            market_tokens = self.fetch_market_tokens()
            new_tokens = []
            for game_id, tokens in market_tokens.items():
                for t in tokens:
                    token_id = t["token_id"]
                    if token_id not in token_to_game:
                        new_tokens.append(token_id)
                        token_to_game[token_id] = game_id
                        token_to_info[token_id] = {
                            "game_id": game_id, "market_type": t["market_type"],
                            "outcome": t["outcome"], "market_slug": t["market_slug"],
                        }
            if new_tokens:
                ws.subscribe(new_tokens)
                print(f"[{now_et_str()}] New token subscriptions: {len(new_tokens)}")

        ws.on_connect(lambda: print(f"[{now_et_str()}] WebSocket connected"))
        ws.on_disconnect(lambda: print(f"[{now_et_str()}] WebSocket disconnected, reconnecting..."))
        ws.on_error(lambda e: print(f"[{now_et_str()}] WebSocket error: {e}"))
        ws.on_price_change(on_price_change)
        ws.on_book_update(on_book_update)

        initialize()
        ws.run_forever(background=True)

        last_refresh = time.time()
        last_bot_check = time.time()

        print(f"\n[{now_et_str()}] Main loop started...\n")

        while running:
            if not is_active_window(cfg.active_start_hour, cfg.active_end_hour):
                wait = seconds_until_active(cfg.active_start_hour, cfg.active_end_hour)
                wake_et = (now_et() + timedelta(seconds=wait)).strftime("%H:%M ET")
                print(f"\n[SLEEP] Inactive. Resuming at {wake_et}")
                ws.stop()
                for _ in range(wait):
                    if not running:
                        break
                    time.sleep(1)
                if running:
                    ws.run_forever(background=True)
                    initialize()
                continue

            now_ts = time.time()

            if now_ts - last_refresh >= cfg.refresh_interval:
                try:
                    refresh()
                except Exception as e:
                    print(f"[WARN] Refresh failed: {e}")
                last_refresh = now_ts

            if now_ts - last_bot_check >= cfg.bot_check_interval:
                try:
                    bot_count = self.check_bot_trades()
                    if bot_count > 0:
                        print(f"[{now_et_str()}] Bot trades recorded: {bot_count}")
                except Exception as e:
                    print(f"[WARN] Bot check failed: {e}")
                last_bot_check = now_ts

            with self.pinnacle_data_lock:
                if self.pinnacle_data:
                    self.track_gap_convergence()

            if int(now_ts) % cfg.status_interval == 0:
                ws_st = ws.get_stats()
                hi_res_str = f" | HiRes: {ws_stats.get('hi_res_events', 0)} events"
                print(f"[{now_et_str()}] WS: {ws_st['messages_received']} msgs, "
                      f"{ws_stats['price_updates']} prices | "
                      f"Anomalies: {ws_stats['anomalies_detected']} | "
                      f"Pinnacle: {ws_stats['pinnacle_calls']} calls{hi_res_str}")

            time.sleep(1)

        ws.stop()
        self.conn.close()
        print("[DONE] Monitor stopped")

    # ── Hi-Res capture helper ─────────────────────────────

    def _handle_hi_res_capture(self, game_id, market_type, event, oracle_data,
                               hi_res_capture, token_to_info, price_tracker, ws_stats):
        details = event.details
        outcome = details.get("outcome", "")

        poly_t0 = None
        poly_line = None
        for token_id, info in token_to_info.items():
            if info["game_id"] == game_id and info["market_type"] == market_type:
                slug = info.get("market_slug", "")
                if "total" in slug or "spread" in slug:
                    poly_line = extract_total_line(slug)
                if info["outcome"].lower() == outcome.lower():
                    poly_t0 = price_tracker.get_current_price(token_id)
                    break

        if poly_t0 is None:
            return

        oracle_implied = None

        if market_type == "moneyline":
            for bm in oracle_data.get("bookmakers", []):
                if bm["key"] != "pinnacle":
                    continue
                for mkt in bm.get("markets", []):
                    if mkt["key"] != "h2h":
                        continue
                    api_outcomes = mkt.get("outcomes", [])
                    if len(api_outcomes) < 2:
                        break
                    matched_idx = _match_team_name(outcome, api_outcomes)
                    if matched_idx is None:
                        break
                    other_idx = 1 - matched_idx
                    matched_odds = api_outcomes[matched_idx].get("price", 2.0)
                    other_odds = api_outcomes[other_idx].get("price", 2.0)
                    fair_matched, _ = de_vig_implied(matched_odds, other_odds)
                    oracle_implied = fair_matched
                    break

        elif market_type in ("total", "spread"):
            oracle_mtype = "totals" if market_type == "total" else "spreads"
            if poly_line:
                oracle_implied = self._find_matching_line_implied(
                    oracle_data, oracle_mtype, poly_line, outcome,
                )

        if oracle_implied is None:
            return

        move_event_id = hi_res_capture.record_move_event(
            game_key=game_id, market_type=market_type,
            trigger_source="poly_anomaly",
            oracle_prev_implied=None, oracle_new_implied=oracle_implied,
            poly_t0=poly_t0, poly_line=poly_line,
            outcome_name=outcome,
        )

        if move_event_id is None:
            return

        ws_stats["hi_res_events"] += 1
        gap_t0 = abs(oracle_implied - poly_t0)
        print(f"  [HiRes] Event #{move_event_id}: gap_t0={gap_t0*100:.1f}%p")

        hi_res_capture.schedule_captures(
            move_event_id, game_id, market_type, outcome, oracle_implied,
        )

    def _find_matching_line_implied(self, oracle_data, market_type, poly_line, outcome_name, tolerance=0.5):
        market_key = f"alternate_{market_type}"
        for bm in oracle_data.get("bookmakers", []):
            if bm["key"] != "pinnacle":
                continue
            for market in bm.get("markets", []):
                if market["key"] != market_key:
                    continue

                outcomes_by_line: dict[float, dict] = {}
                for oc in market.get("outcomes", []):
                    line = oc.get("point")
                    if line is None:
                        continue
                    if line not in outcomes_by_line:
                        outcomes_by_line[line] = {}
                    outcomes_by_line[line][oc["name"]] = oc["price"]

                best_line = None
                best_diff = float("inf")
                for line in outcomes_by_line:
                    diff = abs(line - poly_line)
                    if diff <= tolerance and diff < best_diff:
                        best_diff = diff
                        best_line = line

                if best_line is None:
                    continue

                outcomes = outcomes_by_line[best_line]
                if market_type == "totals":
                    over_odds = outcomes.get("Over", 2.0)
                    under_odds = outcomes.get("Under", 2.0)
                else:
                    odds_list = list(outcomes.values())
                    over_odds = odds_list[0] if len(odds_list) > 0 else 2.0
                    under_odds = odds_list[1] if len(odds_list) > 1 else 2.0

                fair_over, fair_under = de_vig_implied(over_odds, under_odds)

                if outcome_name.lower() in ("over", "home"):
                    return fair_over
                else:
                    return fair_under

        return None

    def _print_status(self, pinnacle_data, poly_count, triggers, bot_count):
        t_utc = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
        t_et = now_et_str()
        print(f"[{t_et} / {t_utc}] Pinnacle: {len(pinnacle_data)} games | "
              f"Poly: {poly_count} lines | Triggers: {len(triggers)} | Bot: {bot_count}")

        for tr in triggers:
            direction = "UP" if tr["delta_line"] > 0 else "DOWN"
            gap_str = ""
            if tr["poly_gap_under"] is not None:
                gap_str = f" | Poly gap Under={tr['poly_gap_under']:+.1%} Over={tr['poly_gap_over']:+.1%}"
            print(f"  ** TRIGGER: {tr['away']}@{tr['home']} "
                  f"line {direction} {abs(tr['delta_line']):.1f}pt "
                  f"(now {tr['new_line']}){gap_str}")
