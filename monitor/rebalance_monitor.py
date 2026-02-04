#!/usr/bin/env python3
"""
실시간 리밸런싱 차익거래 모니터

Polymarket의 멀티 아웃컴 이벤트(NBA 챔피언, MVP 등)를 실시간 감시하여,
모든 YES outcome의 best_ask 합이 $1.00 미만이 되는 순간을 포착한다.

실행: python3 monitor/rebalance_monitor.py
"""
from __future__ import annotations

import json
import logging
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

try:
    import websocket
except ImportError:
    raise ImportError("websocket-client 필요: pip install websocket-client")

sys.path.insert(0, str(Path(__file__).resolve().parent))
from rebalance_tracker import RebalanceTracker

# ── 설정 ──────────────────────────────────────────────────────────
GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
FETCH_LIMIT = 100
FETCH_DELAY = 0.3

REFRESH_INTERVAL = 600   # 이벤트 목록 갱신 (10분)
STATUS_INTERVAL = 60     # 상태 출력 (1분)
STATUS_TOP_N = 15

# negativeRisk 판별
MIN_MARKETS = 3

# 알림 파일
DATA_DIR = Path(__file__).resolve().parent / "data"
ALERT_FILE = DATA_DIR / "rebalance_alerts.jsonl"

# WebSocket 재연결
WS_RECONNECT_INITIAL = 1.0
WS_RECONNECT_MAX = 60.0
WS_PING_INTERVAL = 30
WS_PING_TIMEOUT = 10

# ── 로깅 설정 ─────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("rebalance")


# ── Gamma API ─────────────────────────────────────────────────────

def fetch_all_active_events() -> List[Dict]:
    """Gamma API에서 전체 활성 이벤트를 페이지네이션으로 가져오기"""
    all_events = []
    offset = 0
    while True:
        params = {
            "closed": "false",
            "active": "true",
            "limit": FETCH_LIMIT,
            "offset": offset,
        }
        try:
            resp = httpx.get(f"{GAMMA_API}/events", params=params, timeout=30)
            resp.raise_for_status()
            events = resp.json()
        except Exception as e:
            log.error(f"Gamma API 요청 실패 (offset={offset}): {e}")
            break
        if not events:
            break
        all_events.extend(events)
        if len(events) < FETCH_LIMIT:
            break
        offset += FETCH_LIMIT
        time.sleep(FETCH_DELAY)
    return all_events


def is_negative_risk_event(event: Dict) -> bool:
    """negativeRisk 이벤트 여부 판별 (Gamma API 필드만 사용)"""
    if event.get("negativeRisk") is True:
        return True
    markets = event.get("markets", [])
    if markets and any(m.get("negRisk") is True for m in markets):
        return True
    return False


def is_sports_event(event: Dict) -> bool:
    """Sports 태그가 있는 이벤트만 통과"""
    for tag in event.get("tags", []):
        label = tag.get("label", "") if isinstance(tag, dict) else str(tag)
        if label == "Sports":
            return True
    return False


def is_nba_game_event(event: Dict) -> bool:
    """NBA 개별 경기 바이너리 마켓 판별

    negativeRisk(멀티아웃컴)는 기존 로직에서 처리하므로 제외.
    Sports + NBA 태그 또는 타이틀에 NBA 포함된 바이너리 이벤트만 통과.
    """
    if is_negative_risk_event(event):
        return False
    if not is_sports_event(event):
        return False
    # NBA 태그 확인
    for tag in event.get("tags", []):
        label = tag.get("label", "") if isinstance(tag, dict) else str(tag)
        if label == "NBA":
            return True
    # 타이틀 fallback
    if "NBA" in event.get("title", ""):
        return True
    return False


def extract_yes_tokens(event: Dict) -> List[Dict]:
    """이벤트에서 YES 토큰 정보 추출"""
    tokens = []
    for m in event.get("markets", []):
        if m.get("closed"):
            continue
        outcomes = m.get("outcomes", [])
        if isinstance(outcomes, str):
            outcomes = json.loads(outcomes)
        clob_token_ids = m.get("clobTokenIds", [])
        if isinstance(clob_token_ids, str):
            clob_token_ids = json.loads(clob_token_ids)

        if clob_token_ids and outcomes:
            question = m.get("question", "")
            outcome_name = question if question else (outcomes[0] if outcomes else "?")
            tokens.append({
                "token_id": clob_token_ids[0],
                "outcome": outcome_name,
            })
    return tokens


# ── WebSocket 클라이언트 (실제 메시지 형식 대응) ─────────────────

class RebalanceWebSocket:
    """Polymarket WebSocket — 실제 메시지 형식을 올바르게 파싱

    실제 형식:
    - price_change: {"market":"0x..","price_changes":[{"asset_id":"..","best_ask":"0.5",...}]}
    - book (배열):  [{"market":"0x..","asset_id":"..","bids":[..],"asks":[..]}]
    - book (객체):  {"event_type":"book","asset_id":"..","bids":[..],"asks":[..]}
    """

    def __init__(self, tracker: RebalanceTracker):
        self.tracker = tracker
        self._subscribed: List[str] = []
        self._pending: List[str] = []
        self._ws: Optional[websocket.WebSocketApp] = None
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._connected = False
        self._reconnect_delay = WS_RECONNECT_INITIAL
        self._lock = threading.Lock()

        self.stats = {
            "messages": 0,
            "price_updates": 0,
            "book_updates": 0,
            "reconnects": 0,
            "errors": 0,
            "parse_errors": 0,
        }

    def subscribe(self, token_ids: List[str]):
        with self._lock:
            new = [t for t in token_ids if t not in self._subscribed]
            if not new:
                return
            if self._connected and self._ws:
                self._send_subscribe(new)
                self._subscribed.extend(new)
            else:
                self._pending.extend(new)

    def _send_subscribe(self, token_ids: List[str]):
        if not token_ids:
            return
        # 대량 토큰은 배치로 전송 (WebSocket 프레임 크기 제한 대응)
        BATCH = 500
        for i in range(0, len(token_ids), BATCH):
            batch = token_ids[i:i+BATCH]
            msg = json.dumps({"type": "market", "assets_ids": batch})
            try:
                self._ws.send(msg)
            except Exception as e:
                log.error(f"구독 전송 실패: {e}")

    def run_forever(self, background: bool = True):
        self._running = True
        if background:
            self._thread = threading.Thread(target=self._run_loop, daemon=True)
            self._thread.start()
        else:
            self._run_loop()

    def stop(self):
        self._running = False
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass

    def is_connected(self) -> bool:
        return self._connected

    def _run_loop(self):
        while self._running:
            try:
                self._connect()
            except Exception as e:
                self.stats["errors"] += 1
                log.error(f"WS 연결 에러: {e}")
            if not self._running:
                break
            time.sleep(self._reconnect_delay)
            self._reconnect_delay = min(
                self._reconnect_delay * 2.0, WS_RECONNECT_MAX
            )
            self.stats["reconnects"] += 1

    def _connect(self):
        self._ws = websocket.WebSocketApp(
            WS_URL,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        self._ws.run_forever(
            ping_interval=WS_PING_INTERVAL,
            ping_timeout=WS_PING_TIMEOUT,
        )

    def _on_open(self, ws):
        self._connected = True
        self._reconnect_delay = WS_RECONNECT_INITIAL
        log.info("WebSocket 연결됨")
        with self._lock:
            if self._pending:
                self._send_subscribe(self._pending)
                self._subscribed.extend(self._pending)
                self._pending = []
            if self._subscribed:
                self._send_subscribe(self._subscribed)

    def _on_message(self, ws, message: str):
        self.stats["messages"] += 1
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            return

        try:
            if isinstance(data, list):
                # book 이벤트 (배열 형태)
                for item in data:
                    if isinstance(item, dict):
                        self._handle_book_item(item)
            elif isinstance(data, dict):
                if "price_changes" in data:
                    self._handle_price_changes(data)
                elif data.get("event_type") == "book":
                    self._handle_book_item(data)
                elif data.get("event_type") == "price_change":
                    # 구형 형식 fallback
                    self._handle_legacy_price_change(data)
                # tick_size_change 등 무시
        except Exception:
            self.stats["parse_errors"] += 1

    def _handle_price_changes(self, data: Dict):
        """{"market":"..","price_changes":[{"asset_id":"..","best_ask":"0.5",...}]}"""
        for change in data.get("price_changes", []):
            asset_id = change.get("asset_id")
            best_ask = change.get("best_ask")
            if asset_id and best_ask:
                try:
                    self.tracker.update_best_ask(asset_id, float(best_ask))
                    self.stats["price_updates"] += 1
                except (TypeError, ValueError):
                    pass

    def _handle_book_item(self, item: Dict):
        """{"asset_id":"..","bids":[..],"asks":[..]}"""
        asset_id = item.get("asset_id")
        if not asset_id:
            return
        asks = item.get("asks")
        if asks:
            self.tracker.update_book(asset_id, item)
            self.stats["book_updates"] += 1

    def _handle_legacy_price_change(self, data: Dict):
        """구형: {"event_type":"price_change","asset_id":"..","price":".."}"""
        asset_id = data.get("asset_id")
        best_ask = data.get("best_ask")
        if asset_id and best_ask:
            try:
                self.tracker.update_best_ask(asset_id, float(best_ask))
                self.stats["price_updates"] += 1
            except (TypeError, ValueError):
                pass

    def _on_error(self, ws, error):
        self.stats["errors"] += 1

    def _on_close(self, ws, close_status_code, close_msg):
        self._connected = False
        log.warning("WebSocket 연결 끊김 (자동 재연결 예정)")


# ── CLOB 검증 ────────────────────────────────────────────────────

# 모듈 레벨 tracker 참조 (main에서 설정)
_tracker: Optional[RebalanceTracker] = None


def verify_opportunity_with_clob(opportunity: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """CLOB /book API로 실제 오더북 확인하여 opportunity 검증.

    price_change 이벤트의 시차로 인한 false positive를 제거한다.
    검증 통과 시 업데이트된 opportunity 반환, 실패 시 None.
    """
    outcomes = opportunity["outcomes"]
    verified_sum = 0.0
    min_depth = float('inf')

    for oc in outcomes:
        token_id = oc["token_id"]
        try:
            resp = httpx.get(
                f"{CLOB_API}/book",
                params={"token_id": token_id},
                timeout=5,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            resp.raise_for_status()
            book = resp.json()
        except Exception as e:
            log.debug(f"CLOB book 조회 실패 ({token_id[:8]}...): {e}")
            return None

        asks = book.get("asks", [])
        if not asks:
            return None  # 오더북 비어있음

        # best_ask와 depth 계산
        asks_sorted = sorted(asks, key=lambda x: float(x["price"]))
        best_ask = float(asks_sorted[0]["price"])
        best_size = float(asks_sorted[0]["size"])
        depth_dollars = best_ask * best_size

        oc["best_ask"] = best_ask  # 검증된 가격으로 업데이트
        oc["depth"] = depth_dollars
        verified_sum += best_ask
        min_depth = min(min_depth, depth_dollars)

        # tracker에도 반영
        if _tracker is not None:
            _tracker.update_best_ask(token_id, best_ask)

    if verified_sum >= 1.0:
        return None  # 검증 실패: 실제로는 gap 없음

    opportunity["sum"] = verified_sum
    opportunity["gap"] = 1.0 - verified_sum
    opportunity["gap_pct"] = (1.0 - verified_sum) * 100
    opportunity["min_depth"] = min_depth
    opportunity["is_executable"] = min_depth >= 100.0
    opportunity["verified"] = True
    return opportunity


# ── Alert 처리 ────────────────────────────────────────────────────

def on_opportunity(opp: Dict[str, Any]):
    """차익거래 기회 발견 시 호출 — CLOB 검증 후 로깅"""
    # 1. CLOB 검증
    verified = verify_opportunity_with_clob(opp)
    if verified is None:
        log.debug(f"CLOB 검증 실패 (false positive): {opp['title']}")
        return

    # 2. 검증 통과 → 로깅
    strength = ""
    if verified["is_strong"] and verified["is_executable"]:
        strength = " *** EXECUTABLE ***"
    elif verified["is_strong"]:
        strength = " ** STRONG **"

    log.warning(
        f"VERIFIED OPPORTUNITY{strength} | gap={verified['gap_pct']:.2f}% | "
        f"sum={verified['sum']:.4f} | depth>=${verified['min_depth']:.0f} | "
        f"{verified['title']}"
    )
    for o in verified.get("outcomes", []):
        log.info(
            f"  {o['outcome'][:40]:40s} ask={o['best_ask']:.4f} "
            f"depth=${o['depth']:.0f}"
        )
    _write_alert(verified)


def _write_alert(opp: Dict[str, Any]):
    """알림을 JSONL 파일에 기록"""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    record = {
        "timestamp": datetime.fromtimestamp(
            opp["timestamp"], tz=timezone.utc
        ).isoformat(),
        "event_id": opp["event_id"],
        "title": opp["title"],
        "n_outcomes": opp["n_outcomes"],
        "sum": round(opp["sum"], 6),
        "gap": round(opp["gap"], 6),
        "gap_pct": round(opp["gap_pct"], 4),
        "is_strong": opp["is_strong"],
        "is_executable": opp["is_executable"],
        "min_depth": round(opp["min_depth"], 2),
        "verified": opp.get("verified", False),
        "outcomes": [
            {
                "outcome": o["outcome"],
                "best_ask": round(o["best_ask"], 6),
                "depth": round(o["depth"], 2),
            }
            for o in opp.get("outcomes", [])
        ],
    }
    try:
        with open(ALERT_FILE, "a") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as e:
        log.error(f"알림 파일 쓰기 실패: {e}")


# ── 이벤트 스캔 & 등록 ───────────────────────────────────────────

def scan_and_register(tracker: RebalanceTracker) -> List[str]:
    """활성 이벤트를 스캔하고 negativeRisk 이벤트를 tracker에 등록.
    새로 등록된 토큰 ID 목록 반환."""
    log.info("Gamma API 이벤트 스캔 시작...")
    all_events = fetch_all_active_events()
    log.info(f"전체 활성 이벤트: {len(all_events)}개")

    existing_tokens = set(tracker.registered_token_ids)
    new_token_ids = []
    n_new_events = 0

    for event in all_events:
        if not is_negative_risk_event(event):
            continue
        if not is_sports_event(event):
            continue

        event_id = str(event.get("id", ""))
        title = event.get("title", "?")
        tokens = extract_yes_tokens(event)

        if len(tokens) < MIN_MARKETS:
            continue

        # 이미 등록된 이벤트는 건너뛰기
        if any(t["token_id"] in existing_tokens for t in tokens):
            continue

        tracker.register_event(event_id, title, tokens)
        n_new_events += 1
        for t in tokens:
            new_token_ids.append(t["token_id"])

    # ── NBA 개별 경기 바이너리 마켓 (YES+NO 쌍) ──
    existing_tokens = set(tracker.registered_token_ids)
    n_nba_markets = 0

    for event in all_events:
        if not is_nba_game_event(event):
            continue

        event_title = event.get("title", "?")

        for m in event.get("markets", []):
            if m.get("closed"):
                continue

            outcomes = m.get("outcomes", [])
            if isinstance(outcomes, str):
                outcomes = json.loads(outcomes)
            clob_token_ids = m.get("clobTokenIds", [])
            if isinstance(clob_token_ids, str):
                clob_token_ids = json.loads(clob_token_ids)

            if len(clob_token_ids) < 2:
                continue

            yes_tid = clob_token_ids[0]
            no_tid = clob_token_ids[1]

            if yes_tid in existing_tokens or no_tid in existing_tokens:
                continue

            question = m.get("question", "")
            market_title = f"{event_title} | {question}" if question else event_title
            market_id = str(m.get("id", "") or m.get("conditionId", "") or yes_tid)

            tokens = [
                {"token_id": yes_tid, "outcome": outcomes[0] if outcomes else "Yes"},
                {"token_id": no_tid, "outcome": outcomes[1] if len(outcomes) > 1 else "No"},
            ]

            tracker.register_event(market_id, market_title, tokens)
            n_nba_markets += 1
            new_token_ids.append(yes_tid)
            new_token_ids.append(no_tid)
            existing_tokens.add(yes_tid)
            existing_tokens.add(no_tid)

    log.info(
        f"스캔 완료: 멀티아웃컴 {n_new_events}개 + NBA 바이너리 {n_nba_markets}개 | "
        f"총 {tracker.n_events}개 이벤트, {tracker.n_tokens}개 토큰"
    )
    return new_token_ids


def seed_best_asks_from_clob(tracker: RebalanceTracker, workers: int = 50):
    """CLOB /price API에서 실제 best_ask를 동시 조회하여 tracker에 반영."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    token_ids = tracker.registered_token_ids
    n_total = len(token_ids)
    log.info(f"CLOB best_ask 조회 시작: {n_total}개 토큰 (workers={workers})")

    updated = 0
    failed = 0
    t0 = time.time()

    def fetch_one(tid: str) -> tuple:
        """(token_id, best_ask or None)"""
        try:
            resp = httpx.get(
                f"{CLOB_API}/price",
                params={"token_id": tid, "side": "sell"},
                timeout=10,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            resp.raise_for_status()
            price = float(resp.json().get("price", 0))
            return (tid, price if price > 0 else None)
        except Exception:
            return (tid, None)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(fetch_one, tid): tid for tid in token_ids}
        for fut in as_completed(futures):
            tid, best_ask = fut.result()
            if best_ask is not None:
                tracker.update_best_ask(tid, best_ask)
                updated += 1
            else:
                failed += 1

            done = updated + failed
            if done % 5000 == 0 and done > 0:
                elapsed = time.time() - t0
                log.info(f"  CLOB 진행: {done}/{n_total} ({elapsed:.0f}s)")

    elapsed = time.time() - t0
    log.info(f"CLOB best_ask 조회 완료: {updated}개 업데이트, "
             f"{failed}개 실패 ({elapsed:.0f}s)")


# ── 상태 출력 ─────────────────────────────────────────────────────

def print_status(tracker: RebalanceTracker, ws: RebalanceWebSocket):
    """현재 상태 요약 출력"""
    sums = tracker.get_all_event_sums()
    ws_s = ws.stats
    t_s = tracker.stats

    now_str = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    conn = "OK" if ws.is_connected() else "DOWN"

    print(f"\n{'='*72}")
    print(f"[{now_str}] WS:{conn} msgs={ws_s['messages']} "
          f"prices={ws_s['price_updates']} books={ws_s['book_updates']} "
          f"reconn={ws_s['reconnects']} err={ws_s['errors']}")
    print(f"Tracker: {tracker.n_events} events, {tracker.n_tokens} tokens | "
          f"updates={t_s['book_updates']} opps={t_s['opportunities_found']} "
          f"strong={t_s['strong_opportunities']}")

    with_data = [s for s in sums if s["sum"] is not None]
    no_data = [s for s in sums if s["sum"] is None]

    if with_data:
        print(f"\n  TOP {min(STATUS_TOP_N, len(with_data))} (lowest ask sum):")
        for s in with_data[:STATUS_TOP_N]:
            gap_str = f"{s['gap']*100:+.2f}%" if s["gap"] is not None else "?"
            marker = " <-- OPP (unverified)" if s["sum"] < 1.0 else ""
            print(
                f"    sum={s['sum']:.4f} gap={gap_str} "
                f"[{s['n_with_data']}/{s['n_outcomes']}] "
                f"{s['title'][:50]}{marker}"
            )

    partial = len([s for s in with_data if s["n_with_data"] < s["n_outcomes"]])
    print(f"\n  데이터: 완전={len(with_data)-partial} 부분={partial} "
          f"미수신={len(no_data)}")
    print(f"{'='*72}\n")


# ── 메인 ──────────────────────────────────────────────────────────

def main():
    global _tracker

    log.info("=" * 60)
    log.info("실시간 리밸런싱 차익거래 모니터 시작")
    log.info("=" * 60)

    tracker = RebalanceTracker(
        threshold=1.0,
        strong_threshold=0.995,
        min_depth=100.0,
        on_opportunity=on_opportunity,
    )

    # CLOB 검증에서 tracker 참조 필요
    _tracker = tracker

    # 1. 이벤트 스캔 & 등록
    token_ids = scan_and_register(tracker)
    if not token_ids:
        log.warning("구독할 negativeRisk 이벤트가 없습니다. 종료합니다.")
        return

    # 2. CLOB API에서 실제 best_ask 조회
    seed_best_asks_from_clob(tracker)

    sums = tracker.get_all_event_sums()
    has_data = [s for s in sums if s["sum"] is not None]
    under_1 = [s for s in has_data if s["sum"] < 1.0]
    log.info(f"CLOB 초기화: {len(has_data)}개 이벤트 합계 계산됨, "
             f"{len(under_1)}개 sum<1.0")

    # 3. WebSocket 연결
    ws = RebalanceWebSocket(tracker)
    ws.subscribe(token_ids)
    log.info(f"WebSocket 구독: {len(token_ids)}개 토큰")
    ws.run_forever(background=True)

    # 4. 시그널 처리
    shutdown = False

    def _signal_handler(sig, frame):
        nonlocal shutdown
        log.info("종료 시그널 수신...")
        shutdown = True

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    # 5. 메인 루프
    last_refresh = time.time()
    last_status = time.time()
    log.info("메인 루프 시작 (Ctrl+C로 종료)")

    while not shutdown:
        now = time.time()

        if now - last_refresh >= REFRESH_INTERVAL:
            try:
                new_tokens = scan_and_register(tracker)
                if new_tokens:
                    log.info(f"신규 토큰 {len(new_tokens)}개 구독 추가")
                    ws.subscribe(new_tokens)
            except Exception as e:
                log.error(f"이벤트 갱신 실패: {e}")
            last_refresh = now

        if now - last_status >= STATUS_INTERVAL:
            try:
                print_status(tracker, ws)
            except Exception as e:
                log.error(f"상태 출력 실패: {e}")
            last_status = now

        time.sleep(1)

    log.info("WebSocket 종료 중...")
    ws.stop()
    log.info("모니터 종료")


if __name__ == "__main__":
    main()
