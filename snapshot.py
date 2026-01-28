"""
Pinnacle + Polymarket NBA Total 스냅샷 수집기

두 가지 모드 지원:
1. REST 폴링 모드 (기본): Pinnacle 1시간, Polymarket 30초 주기
2. WebSocket 모드 (--ws): Polymarket 실시간, Pinnacle 이상 감지 시에만

WebSocket 모드는 Polymarket을 센서로, Pinnacle을 오라클로 사용하여
Odds API 크레딧을 대폭 절감 (~600/월 → ~100/월)
"""
from __future__ import annotations

import sqlite3
import json
import os
import re
import time
import signal
import sys
import threading
from datetime import datetime, timezone, timedelta
from pathlib import Path
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

import httpx
from dotenv import load_dotenv

# WebSocket 모듈 (선택적)
try:
    from ws_client import PolyWebSocket, AssetPriceTracker
    from anomaly_detector import AnomalyDetector, AnomalyEvent, TriggerManager
    WS_AVAILABLE = True
except ImportError:
    WS_AVAILABLE = False

load_dotenv(Path(__file__).parent / ".env")

DB_PATH = Path(__file__).parent / "data" / "snapshots.db"
SCHEMA_PATH = Path(__file__).parent / "schema.sql"
ODDS_API_KEY = os.environ["ODDS_API_KEY"]
BOT_ADDRESS = os.environ["BOT_ADDRESS"]

# 타임존
ET = ZoneInfo("America/New_York")

# 활성 시간대 (ET 기준) — 봇 활동 06:00~18:00, 경기 19:00~03:00
ACTIVE_START_HOUR = 10   # ET 10:00 부터 수집 시작
ACTIVE_END_HOUR = 3      # ET 03:00 까지 (다음날)

# 크레딧 관리
NORMAL_INTERVAL = 3600       # 1시간 (기본 Pinnacle 폴링)
TRIGGER_INTERVAL = 900       # 15분 (트리거 발생 후 Pinnacle)
TRIGGER_COOLDOWN = 7200      # 2시간 후 기본 간격으로 복귀
POLY_INTERVAL = 30           # Polymarket 스냅샷 간격 (초)

# 트리거 임계값
LINE_MOVE_THRESHOLD = 1.5    # 라인 변동 포인트
IMPLIED_MOVE_THRESHOLD = 0.06  # implied prob 변동 (6%p)

# 팀 약어 매핑
FULL_TO_POLY_ABBR = {
    "Atlanta Hawks": "atl", "Boston Celtics": "bos", "Brooklyn Nets": "bkn",
    "Charlotte Hornets": "cha", "Chicago Bulls": "chi", "Cleveland Cavaliers": "cle",
    "Dallas Mavericks": "dal", "Denver Nuggets": "den", "Detroit Pistons": "det",
    "Golden State Warriors": "gsw", "Houston Rockets": "hou", "Indiana Pacers": "ind",
    "LA Clippers": "lac", "Los Angeles Clippers": "lac",
    "Los Angeles Lakers": "lal", "Memphis Grizzlies": "mem",
    "Miami Heat": "mia", "Milwaukee Bucks": "mil", "Minnesota Timberwolves": "min",
    "New Orleans Pelicans": "nop", "New York Knicks": "nyk",
    "Oklahoma City Thunder": "okc", "Orlando Magic": "orl",
    "Philadelphia 76ers": "phi", "Phoenix Suns": "phx",
    "Portland Trail Blazers": "por", "Sacramento Kings": "sac",
    "San Antonio Spurs": "sas", "Toronto Raptors": "tor",
    "Utah Jazz": "uta", "Washington Wizards": "was",
}

running = True


def signal_handler(sig, frame):
    global running
    print("\n[STOP] 종료 중...")
    running = False


def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    with open(SCHEMA_PATH) as f:
        conn.executescript(f.read())

    # 마이그레이션: poly_snapshots에 market_type 추가
    for col, dtype, default in [
        ("market_type", "TEXT", "'total'"),
    ]:
        try:
            conn.execute(f"ALTER TABLE poly_snapshots ADD COLUMN {col} {dtype} DEFAULT {default}")
        except Exception:
            pass  # 이미 존재

    # 마이그레이션: triggers에 market_type 추가
    for col, dtype, default in [
        ("market_type", "TEXT", "'totals'"),
    ]:
        try:
            conn.execute(f"ALTER TABLE triggers ADD COLUMN {col} {dtype} DEFAULT {default}")
        except Exception:
            pass

    conn.commit()
    return conn


def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def now_et() -> datetime:
    """현재 미국 동부 시간"""
    return datetime.now(ET)


def now_et_str() -> str:
    return now_et().strftime("%H:%M:%S ET")


def is_active_window() -> bool:
    """ET 기준 활성 시간대인지 확인 (10:00~03:00)"""
    hour = now_et().hour
    # ACTIVE_START_HOUR(10) ~ 23:59 또는 00:00 ~ ACTIVE_END_HOUR(3)
    if ACTIVE_START_HOUR <= hour or hour < ACTIVE_END_HOUR:
        return True
    return False


def seconds_until_active() -> int:
    """비활성 시간대일 때, 활성 시작까지 남은 초"""
    et_now = now_et()
    # 다음 활성 시작 시각 계산
    target = et_now.replace(hour=ACTIVE_START_HOUR, minute=0, second=0, microsecond=0)
    if et_now.hour >= ACTIVE_END_HOUR:
        # 이미 03:00 이후 → 오늘 10:00
        pass
    else:
        # 00:00~02:59 사이인데 비활성? (shouldn't happen but handle)
        pass
    diff = (target - et_now).total_seconds()
    if diff <= 0:
        diff += 86400
    return int(diff)


# ── Pinnacle ──────────────────────────────────────────────

def fetch_pinnacle(conn: sqlite3.Connection) -> list[dict]:
    """Pinnacle NBA Total 라인 수집 + DB 저장"""
    url = "https://api.the-odds-api.com/v4/sports/basketball_nba/odds"
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": "us",
        "markets": "totals",
        "bookmakers": "pinnacle",
        "oddsFormat": "decimal",
    }
    resp = httpx.get(url, params=params, timeout=15)
    resp.raise_for_status()

    remaining = resp.headers.get("x-requests-remaining", "?")
    used = resp.headers.get("x-requests-used", "?")
    print(f"  [Odds API] 크레딧 {used} used / {remaining} remaining")

    snap_time = now_utc()
    results = []

    for game in resp.json():
        game_id = game["id"]
        home = game["home_team"]
        away = game["away_team"]
        commence = game.get("commence_time", "")

        # 매핑 테이블 upsert
        _upsert_game_mapping(conn, game_id, home, away, commence)

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

                conn.execute("""
                    INSERT OR IGNORE INTO pinnacle_snapshots
                    (game_id, snapshot_time, total_line, over_price, under_price,
                     over_implied, under_implied)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (game_id, snap_time, total_line, over_price, under_price,
                      over_implied, under_implied))

                results.append({
                    "game_id": game_id,
                    "home": home,
                    "away": away,
                    "line": total_line,
                    "over_price": over_price,
                    "under_price": under_price,
                    "over_implied": over_implied,
                    "under_implied": under_implied,
                })

    conn.commit()
    return results


def _upsert_game_mapping(conn, game_id, home, away, commence):
    """경기 매핑 테이블 upsert + Polymarket slug 자동 생성"""
    existing = conn.execute(
        "SELECT poly_event_slug FROM game_mapping WHERE odds_api_id = ?",
        (game_id,)
    ).fetchone()

    if existing:
        return

    # Polymarket slug 생성: nba-{away_abbr}-{home_abbr}-{date_et}
    away_abbr = FULL_TO_POLY_ABBR.get(away, "")
    home_abbr = FULL_TO_POLY_ABBR.get(home, "")
    poly_slug = ""
    if away_abbr and home_abbr and commence:
        dt_utc = datetime.fromisoformat(commence.replace("Z", "+00:00"))
        dt_et = dt_utc.astimezone(ET)
        date_str = dt_et.strftime("%Y-%m-%d")
        poly_slug = f"nba-{away_abbr}-{home_abbr}-{date_str}"

    conn.execute("""
        INSERT OR IGNORE INTO game_mapping
        (odds_api_id, home_team, away_team, commence_time, poly_event_slug)
        VALUES (?, ?, ?, ?, ?)
    """, (game_id, home, away, commence, poly_slug))


# ── Polymarket ────────────────────────────────────────────

def _classify_market(question: str, slug: str) -> str:
    """Polymarket 마켓 분류: total, spread, moneyline, player_prop, other"""
    q = question.lower()
    s = slug.lower()

    # 선수 프롭 제외
    if any(kw in q for kw in ["points o/u", "rebounds o/u", "assists o/u",
                                "threes o/u", "steals o/u", "blocks o/u"]):
        return "player_prop"

    # 하프/쿼터 제외
    if any(kw in q for kw in ["1h", "1q", "2q", "3q", "4q", "first half", "first quarter"]):
        return "other"

    if "o/u" in q or "total" in s:
        return "total"
    if "spread" in q or "spread" in s:
        return "spread"

    # 남은 건 moneyline (팀 vs 팀)
    if " vs" in q or " vs." in q:
        return "moneyline"

    return "other"


def _extract_spread_line(text: str) -> float:
    """스프레드 라인 추출: 'home-8pt5' → -8.5"""
    m = re.search(r"(\d{1,2})pt(\d)", text)
    if m:
        return float(m.group(1)) + float(m.group(2)) / 10
    m = re.search(r"(\d{1,2}\.\d)", text)
    if m:
        return float(m.group(1))
    return 0.0


def fetch_polymarket(conn: sqlite3.Connection, games: list[dict]):
    """Pinnacle 경기 기반 Polymarket 마켓 스냅샷 (Total + Spread + Moneyline)"""
    client = httpx.Client(timeout=15)
    snap_time = now_utc()
    found = 0

    for game in games:
        game_id = game["game_id"]
        row = conn.execute(
            "SELECT poly_event_slug FROM game_mapping WHERE odds_api_id = ?",
            (game_id,)
        ).fetchone()

        if not row or not row[0]:
            continue

        poly_slug = row[0]

        # Polymarket 이벤트 조회
        try:
            resp = client.get(
                "https://gamma-api.polymarket.com/events",
                params={"slug": poly_slug}
            )
            events = resp.json()
        except Exception:
            continue

        if not events:
            continue

        conn.execute(
            "UPDATE game_mapping SET poly_event_found = 1 WHERE odds_api_id = ?",
            (game_id,)
        )

        event = events[0]
        for m in event.get("markets", []):
            q = m.get("question") or ""
            market_slug = m.get("slug", "")
            market_type = _classify_market(q, market_slug)

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

            price1 = price2 = None
            for i, name in enumerate(outcomes):
                p = float(prices[i]) if i < len(prices) else None
                if p is None:
                    continue
                name_l = name.lower()
                if i == 0:
                    price1 = p
                else:
                    price2 = p

            # 라인 추출
            line = None
            if market_type == "total":
                line = _extract_line(q.lower()) or _extract_line(market_slug)
                if line is not None and not (170 <= line <= 310):
                    continue  # 선수 프롭 필터
            elif market_type == "spread":
                line = _extract_spread_line(market_slug)

            # over/under 또는 outcome1/outcome2로 저장
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
                # spread/moneyline: 첫번째=home/favorite, 두번째=away/underdog
                over_price = price1   # outcome1 price
                under_price = price2  # outcome2 price

            conn.execute("""
                INSERT OR IGNORE INTO poly_snapshots
                (game_id, poly_market_slug, snapshot_time, total_line,
                 over_price, under_price,
                 over_best_bid, over_best_ask, under_best_bid, under_best_ask,
                 market_type)
                VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, ?)
            """, (game_id, market_slug, snap_time, line,
                  over_price, under_price, market_type))
            found += 1

    client.close()
    conn.commit()
    return found


def _extract_line(text: str) -> float | None:
    m = re.search(r"(\d{2,3})pt(\d)", text)
    if m:
        return float(m.group(1)) + float(m.group(2)) / 10
    m = re.search(r"(\d{2,3}\.\d)", text)
    if m:
        return float(m.group(1))
    return None


# ── 변동 감지 ─────────────────────────────────────────────

def detect_moves(conn: sqlite3.Connection, current: list[dict]) -> list[dict]:
    """직전 스냅샷과 비교해서 큰 변동 감지"""
    triggers = []

    for game in current:
        game_id = game["game_id"]

        # 직전 스냅샷 (현재 제외, 가장 최근 1개)
        prev = conn.execute("""
            SELECT total_line, over_implied, under_implied, snapshot_time
            FROM pinnacle_snapshots
            WHERE game_id = ?
            ORDER BY snapshot_time DESC
            LIMIT 1 OFFSET 1
        """, (game_id,)).fetchone()

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
        if abs(delta_line) >= LINE_MOVE_THRESHOLD:
            trigger_type = "line_move"
        if abs(delta_under) >= IMPLIED_MOVE_THRESHOLD or abs(delta_over) >= IMPLIED_MOVE_THRESHOLD:
            trigger_type = "both" if trigger_type else "implied_move"

        if not trigger_type:
            continue

        # 트리거 시점 Polymarket 가격 조회 (가장 가까운 라인)
        poly_snap = conn.execute("""
            SELECT over_price, under_price, total_line
            FROM poly_snapshots
            WHERE game_id = ? AND market_type = 'total'
            ORDER BY ABS(total_line - ?), snapshot_time DESC
            LIMIT 1
        """, (game_id, new_line)).fetchone()

        poly_over = poly_snap[0] if poly_snap else None
        poly_under = poly_snap[1] if poly_snap else None
        poly_gap_under = (new_under_imp - poly_under) if (new_under_imp and poly_under) else None
        poly_gap_over = (new_over_imp - poly_over) if (new_over_imp and poly_over) else None

        trigger_time = now_utc()

        conn.execute("""
            INSERT INTO triggers
            (game_id, trigger_time, trigger_type,
             prev_line, prev_over_implied, prev_under_implied,
             new_line, new_over_implied, new_under_implied,
             delta_line, delta_under_implied,
             poly_over_price, poly_under_price, poly_gap_under, poly_gap_over)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (game_id, trigger_time, trigger_type,
              prev_line, prev_over_imp, prev_under_imp,
              new_line, new_over_imp, new_under_imp,
              delta_line, delta_under,
              poly_over, poly_under, poly_gap_under, poly_gap_over))

        triggers.append({
            "game_id": game_id,
            "home": game["home"],
            "away": game["away"],
            "trigger_type": trigger_type,
            "delta_line": delta_line,
            "delta_under": delta_under,
            "new_line": new_line,
            "poly_gap_under": poly_gap_under,
            "poly_gap_over": poly_gap_over,
        })

    conn.commit()
    return triggers


# ── 봇 거래 모니터링 ──────────────────────────────────────

def check_bot_trades(conn: sqlite3.Connection):
    """봇의 최근 거래를 조회해서 DB에 기록 (data-api /activity endpoint)"""
    url = "https://data-api.polymarket.com/activity"
    now_ts = int(time.time())
    start_ts = now_ts - 24 * 3600
    params = {
        "user": BOT_ADDRESS,
        "type": "TRADE",
        "start": start_ts,
        "end": now_ts,
        "limit": 100,
        "sortBy": "TIMESTAMP",
        "sortDirection": "DESC",
    }

    try:
        resp = httpx.get(url, params=params, timeout=15)
        resp.raise_for_status()
        trades = resp.json()
    except Exception as e:
        print(f"  [WARN] 봇 거래 조회 실패: {e}")
        return 0

    if not isinstance(trades, list):
        return 0

    # poly_event_slug prefix → odds_api_id 매핑 캐시
    event_slug_to_game_id = {}
    for row in conn.execute("SELECT odds_api_id, poly_event_slug FROM game_mapping WHERE poly_event_slug IS NOT NULL"):
        event_slug_to_game_id[row[1]] = row[0]

    count = 0
    for t in trades:
        slug = t.get("slug", "")

        # timestamp → ISO
        ts_raw = t.get("timestamp", "")
        if isinstance(ts_raw, (int, float)) or (isinstance(ts_raw, str) and ts_raw.isdigit()):
            ts_str = datetime.fromtimestamp(int(ts_raw), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            ts_str = str(ts_raw)

        tx_hash = t.get("transactionHash", "") or f"{slug}_{ts_str}_{t.get('price','')}"

        # slug에서 game_id 매칭 (slug: "nba-por-was-2026-01-27-total-233pt5")
        # event_slug: "nba-por-was-2026-01-27" (prefix)
        matched_game_id = None
        for event_slug, game_id in event_slug_to_game_id.items():
            if slug.startswith(event_slug):
                matched_game_id = game_id
                break

        conn.execute("""
            INSERT OR IGNORE INTO bot_trades
            (trade_time, game_id, poly_market_slug, condition_id, outcome, side, price, size, tx_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            ts_str,
            matched_game_id,
            slug,
            t.get("conditionId", ""),
            t.get("outcome", t.get("title", "")),
            t.get("side", ""),
            float(t.get("price", 0) or 0),
            float(t.get("size", 0) or 0),
            tx_hash,
        ))
        count += 1

    conn.commit()
    return count


# ── 메인 루프 ─────────────────────────────────────────────

def print_status(pinnacle_data, poly_count, triggers, bot_count):
    t_utc = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    t_et = now_et_str()
    print(f"[{t_et} / {t_utc}] Pinnacle: {len(pinnacle_data)} games | "
          f"Poly: {poly_count} lines | "
          f"Triggers: {len(triggers)} | "
          f"Bot trades: {bot_count}")

    for tr in triggers:
        direction = "UP" if tr["delta_line"] > 0 else "DOWN"
        gap_str = ""
        if tr["poly_gap_under"] is not None:
            gap_str = f" | Poly gap Under={tr['poly_gap_under']:+.1%} Over={tr['poly_gap_over']:+.1%}"
        print(f"  ** TRIGGER: {tr['away']}@{tr['home']} "
              f"line {direction} {abs(tr['delta_line']):.1f}pt "
              f"(now {tr['new_line']}){gap_str}")


def track_gap_convergence(conn: sqlite3.Connection, pinnacle_data: list[dict]):
    """트리거 발생 후 갭이 1%p 이내로 수렴했는지 추적"""
    open_triggers = conn.execute("""
        SELECT id, game_id, new_line, new_under_implied, new_over_implied, trigger_time
        FROM triggers
        WHERE gap_closed_time IS NULL AND poly_gap_under IS NOT NULL
    """).fetchall()

    for tr in open_triggers:
        tr_id, game_id, tr_line, tr_under_imp, tr_over_imp, tr_time = tr

        # 최신 Polymarket 가격 조회 (가장 가까운 라인)
        poly_snap = conn.execute("""
            SELECT under_price, over_price
            FROM poly_snapshots
            WHERE game_id = ? AND market_type = 'total'
            ORDER BY ABS(total_line - ?), snapshot_time DESC
            LIMIT 1
        """, (game_id, tr_line)).fetchone()

        if not poly_snap or poly_snap[0] is None:
            continue

        poly_under, poly_over = poly_snap
        gap_under = abs(tr_under_imp - poly_under) if tr_under_imp else None

        # 갭이 1%p 이내로 수렴?
        if gap_under is not None and gap_under <= 0.01:
            closed_time = now_utc()
            tr_dt = datetime.fromisoformat(tr_time.replace("Z", "+00:00"))
            closed_dt = datetime.fromisoformat(closed_time.replace("Z", "+00:00"))
            lag = int((closed_dt - tr_dt).total_seconds())

            conn.execute("""
                UPDATE triggers SET gap_closed_time = ?, lag_seconds = ?
                WHERE id = ?
            """, (closed_time, lag, tr_id))

    conn.commit()


# ── WebSocket 모드 ─────────────────────────────────────────

def fetch_market_tokens(conn: sqlite3.Connection) -> dict[str, list[dict]]:
    """
    Gamma API로 당일 NBA 마켓의 token_id(asset_id) 조회

    Returns:
        game_id → [{token_id, market_type, outcome, market_slug}, ...]
    """
    client = httpx.Client(timeout=15)
    result = {}

    # DB에서 poly_event_slug 목록 조회
    rows = conn.execute("""
        SELECT odds_api_id, poly_event_slug
        FROM game_mapping
        WHERE poly_event_slug IS NOT NULL AND poly_event_slug != ''
    """).fetchall()

    for game_id, poly_slug in rows:
        try:
            resp = client.get(
                "https://gamma-api.polymarket.com/events",
                params={"slug": poly_slug}
            )
            events = resp.json()
        except Exception:
            continue

        if not events:
            continue

        event = events[0]
        tokens = []

        for m in event.get("markets", []):
            q = m.get("question") or ""
            market_slug = m.get("slug", "")
            market_type = _classify_market(q, market_slug)

            if market_type in ("player_prop", "other"):
                continue
            if m.get("closed", False):
                continue

            # clobTokenIds 추출
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

        if tokens:
            result[game_id] = tokens

    client.close()
    return result


def main_ws():
    """WebSocket 기반 이벤트 드리븐 메인 루프"""
    global running

    if not WS_AVAILABLE:
        print("[ERROR] WebSocket 모듈 로드 실패. websocket-client 설치 필요:")
        print("  pip install websocket-client")
        sys.exit(1)

    signal.signal(signal.SIGINT, signal_handler)

    conn = init_db()
    et_now = now_et_str()
    print(f"Pinnacle-Polymarket NBA Monitor (WebSocket Mode)")
    print(f"DB: {DB_PATH}")
    print(f"현재 시각: {et_now}")
    print(f"활성 시간대: ET {ACTIVE_START_HOUR:02d}:00 ~ {ACTIVE_END_HOUR:02d}:00")
    print(f"Pinnacle 호출: 이상 감지 시에만 (30분 쿨다운)")
    print(f"트리거 임계값: Δprice>=5%p (5분) | bid-ask>=5%p | Yes+No 불일치>=3%p")
    print(f"{'='*60}\n")

    # WebSocket 클라이언트 초기화
    ws = PolyWebSocket()
    detector = AnomalyDetector()
    price_tracker = AssetPriceTracker(window_seconds=300)

    # token_id → game_id 매핑
    token_to_game: dict[str, str] = {}
    token_to_info: dict[str, dict] = {}

    # 마지막 Pinnacle 데이터 캐시
    pinnacle_data: list[dict] = []
    pinnacle_data_lock = threading.Lock()

    # 통계
    ws_stats = {
        "price_updates": 0,
        "anomalies_detected": 0,
        "pinnacle_calls": 0,
    }

    def on_ws_connect():
        print(f"[{now_et_str()}] WebSocket 연결됨")

    def on_ws_disconnect():
        print(f"[{now_et_str()}] WebSocket 연결 끊김, 재연결 시도...")

    def on_ws_error(e: Exception):
        print(f"[{now_et_str()}] WebSocket 오류: {e}")

    def on_price_change(asset_id: str, data: dict):
        """가격 변동 콜백 → 이상 감지"""
        nonlocal ws_stats

        ws_stats["price_updates"] += 1

        info = token_to_info.get(asset_id)
        if not info:
            return

        game_id = info["game_id"]
        market_type = info["market_type"]
        outcome = info["outcome"]

        price = data.get("price")
        if price is None:
            return

        price = float(price)
        price_tracker.record(asset_id, price)

        # 이상 감지
        event = detector.update_price(game_id, market_type, outcome, price)
        if event:
            ws_stats["anomalies_detected"] += 1
            on_anomaly_detected(event)

    def on_book_update(asset_id: str, data: dict):
        """오더북 업데이트 콜백 → 스프레드 이상 감지"""
        info = token_to_info.get(asset_id)
        if not info:
            return

        game_id = info["game_id"]
        market_type = info["market_type"]
        outcome = info["outcome"]

        # 오더북에서 best bid/ask 추출
        bids = data.get("bids", [])
        asks = data.get("asks", [])

        best_bid = float(bids[0]["price"]) if bids else 0.0
        best_ask = float(asks[0]["price"]) if asks else 1.0

        event = detector.update_orderbook(game_id, market_type, outcome, best_bid, best_ask)
        if event:
            ws_stats["anomalies_detected"] += 1
            on_anomaly_detected(event)

    def on_anomaly_detected(event: AnomalyEvent):
        """이상 감지 시 Pinnacle 호출"""
        nonlocal ws_stats, pinnacle_data

        game_id = event.game_id
        print(f"\n[{now_et_str()}] ** ANOMALY: {event}")

        # Pinnacle 호출 필요 여부 확인
        if not detector.should_call_pinnacle(game_id):
            print(f"  (쿨다운 중, Pinnacle 호출 생략)")
            return

        detector.mark_pinnacle_called(game_id)
        ws_stats["pinnacle_calls"] += 1

        print(f"  [Pinnacle 호출] game_id={game_id}")
        try:
            with pinnacle_data_lock:
                pinnacle_data = fetch_pinnacle(conn)

            # 변동 감지
            triggers = detect_moves(conn, pinnacle_data)
            for tr in triggers:
                direction = "UP" if tr["delta_line"] > 0 else "DOWN"
                gap_str = ""
                if tr["poly_gap_under"] is not None:
                    gap_str = f" | Poly gap Under={tr['poly_gap_under']:+.1%}"
                print(f"  ** TRIGGER: {tr['away']}@{tr['home']} "
                      f"line {direction} {abs(tr['delta_line']):.1f}pt "
                      f"(now {tr['new_line']}){gap_str}")

        except Exception as e:
            print(f"  [ERROR] Pinnacle 호출 실패: {e}")

    def initialize_subscriptions():
        """초기 마켓 토큰 구독"""
        nonlocal token_to_game, token_to_info, pinnacle_data

        print("[초기화] Pinnacle 데이터 수집...")
        try:
            with pinnacle_data_lock:
                pinnacle_data = fetch_pinnacle(conn)
            print(f"  {len(pinnacle_data)} games found")
        except Exception as e:
            print(f"  [ERROR] {e}")
            return

        print("[초기화] Polymarket 토큰 ID 수집...")
        market_tokens = fetch_market_tokens(conn)

        all_token_ids = []
        for game_id, tokens in market_tokens.items():
            for t in tokens:
                token_id = t["token_id"]
                all_token_ids.append(token_id)
                token_to_game[token_id] = game_id
                token_to_info[token_id] = {
                    "game_id": game_id,
                    "market_type": t["market_type"],
                    "outcome": t["outcome"],
                    "market_slug": t["market_slug"],
                }

        print(f"  {len(all_token_ids)} tokens across {len(market_tokens)} games")

        if all_token_ids:
            ws.subscribe(all_token_ids)
            print(f"[초기화] WebSocket 구독 완료")

    def refresh_subscriptions():
        """주기적으로 새 마켓 구독 추가 (10분마다)"""
        nonlocal token_to_game, token_to_info

        market_tokens = fetch_market_tokens(conn)
        new_tokens = []

        for game_id, tokens in market_tokens.items():
            for t in tokens:
                token_id = t["token_id"]
                if token_id not in token_to_game:
                    new_tokens.append(token_id)
                    token_to_game[token_id] = game_id
                    token_to_info[token_id] = {
                        "game_id": game_id,
                        "market_type": t["market_type"],
                        "outcome": t["outcome"],
                        "market_slug": t["market_slug"],
                    }

        if new_tokens:
            ws.subscribe(new_tokens)
            print(f"[{now_et_str()}] 새 토큰 구독: {len(new_tokens)}")

    # 콜백 등록
    ws.on_connect(on_ws_connect)
    ws.on_disconnect(on_ws_disconnect)
    ws.on_error(on_ws_error)
    ws.on_price_change(on_price_change)
    ws.on_book_update(on_book_update)

    # 초기화
    initialize_subscriptions()

    # WebSocket 백그라운드 실행
    ws.run_forever(background=True)

    # 주기적 작업
    last_refresh_time = time.time()
    last_bot_check_time = time.time()
    REFRESH_INTERVAL = 600  # 10분마다 새 마켓 확인
    BOT_CHECK_INTERVAL = 60  # 1분마다 봇 거래 확인

    print(f"\n[{now_et_str()}] 메인 루프 시작...\n")

    while running:
        # ET 활성 시간대 체크
        if not is_active_window():
            wait = seconds_until_active()
            wake_et = (now_et() + timedelta(seconds=wait)).strftime("%H:%M ET")
            print(f"\n[SLEEP] 비활성 시간대. {wake_et}에 재개")
            ws.stop()
            for _ in range(wait):
                if not running:
                    break
                time.sleep(1)
            if running:
                ws.run_forever(background=True)
                initialize_subscriptions()
            continue

        now_ts = time.time()

        # 새 마켓 구독 갱신 (10분마다)
        if now_ts - last_refresh_time >= REFRESH_INTERVAL:
            try:
                refresh_subscriptions()
            except Exception as e:
                print(f"[WARN] 구독 갱신 실패: {e}")
            last_refresh_time = now_ts

        # 봇 거래 체크 (1분마다)
        if now_ts - last_bot_check_time >= BOT_CHECK_INTERVAL:
            try:
                bot_count = check_bot_trades(conn)
                if bot_count > 0:
                    print(f"[{now_et_str()}] 봇 거래 {bot_count}건 기록")
            except Exception as e:
                print(f"[WARN] 봇 거래 체크 실패: {e}")
            last_bot_check_time = now_ts

        # 갭 수렴 추적
        with pinnacle_data_lock:
            if pinnacle_data:
                track_gap_convergence(conn, pinnacle_data)

        # 상태 출력 (5분마다)
        if int(now_ts) % 300 == 0:
            ws_st = ws.get_stats()
            det_st = detector.get_stats()
            print(f"[{now_et_str()}] WS: {ws_st['messages_received']} msgs, "
                  f"{ws_stats['price_updates']} prices | "
                  f"Anomalies: {ws_stats['anomalies_detected']} | "
                  f"Pinnacle: {ws_stats['pinnacle_calls']} calls")

        time.sleep(1)

    ws.stop()
    conn.close()
    print("[DONE] 모니터 종료")


# ── REST 폴링 모드 (기존) ─────────────────────────────────────

def main():
    signal.signal(signal.SIGINT, signal_handler)

    conn = init_db()
    et_now = now_et_str()
    print(f"Pinnacle-Polymarket NBA Monitor (REST Polling Mode)")
    print(f"DB: {DB_PATH}")
    print(f"현재 시각: {et_now}")
    print(f"활성 시간대: ET {ACTIVE_START_HOUR:02d}:00 ~ {ACTIVE_END_HOUR:02d}:00")
    print(f"Pinnacle 간격: {NORMAL_INTERVAL}s (기본) / {TRIGGER_INTERVAL}s (트리거)")
    print(f"Polymarket 간격: {POLY_INTERVAL}s")
    print(f"트리거 임계값: Δline>={LINE_MOVE_THRESHOLD} | Δimplied>={IMPLIED_MOVE_THRESHOLD:.0%}")
    print(f"{'='*60}\n")

    pinnacle_interval = NORMAL_INTERVAL
    last_trigger_time = 0
    last_pinnacle_time = 0
    pinnacle_data = []  # 마지막 Pinnacle 데이터 (Poly 서브루프용)

    while running:
        # ── ET 활성 시간대 체크 ──
        if not is_active_window():
            wait = seconds_until_active()
            wake_et = (now_et() + timedelta(seconds=wait)).strftime("%H:%M ET")
            print(f"\n[SLEEP] 비활성 시간대 (ET {ACTIVE_END_HOUR:02d}:00~{ACTIVE_START_HOUR:02d}:00). "
                  f"{wake_et}에 재개 ({wait//3600}h{(wait%3600)//60}m)")
            for _ in range(wait):
                if not running:
                    break
                time.sleep(1)
            continue

        now = time.time()

        # ── 트리거 쿨다운 체크 ──
        if (now - last_trigger_time) > TRIGGER_COOLDOWN:
            pinnacle_interval = NORMAL_INTERVAL

        # ── Pinnacle 사이클 (1시간 or 15분) ──
        if (now - last_pinnacle_time) >= pinnacle_interval:
            try:
                print(f"\n--- Pinnacle cycle ({now_et_str()}) ---")

                # 1. Pinnacle 스냅샷
                print("[1/3] Pinnacle 수집...")
                pinnacle_data = fetch_pinnacle(conn)

                # 2. Polymarket 스냅샷
                print("[2/3] Polymarket 수집...")
                poly_count = fetch_polymarket(conn, pinnacle_data)

                # 3. 변동 감지
                triggers = detect_moves(conn, pinnacle_data)
                if triggers:
                    pinnacle_interval = TRIGGER_INTERVAL
                    last_trigger_time = now

                # 4. 봇 거래 체크
                print("[3/3] 봇 거래 체크...")
                bot_count = check_bot_trades(conn)

                # 5. 갭 수렴 추적
                track_gap_convergence(conn, pinnacle_data)

                print_status(pinnacle_data, poly_count, triggers, bot_count)
                last_pinnacle_time = now

            except httpx.HTTPStatusError as e:
                print(f"  [ERROR] HTTP {e.response.status_code}: {e}")
            except Exception as e:
                print(f"  [ERROR] {e}")

        # ── Polymarket 서브폴링 (30초 간격) ──
        elif pinnacle_data:
            try:
                poly_count = fetch_polymarket(conn, pinnacle_data)
                track_gap_convergence(conn, pinnacle_data)
                if poly_count > 0:
                    t = now_et_str()
                    print(f"  [{t}] Poly sub-poll: {poly_count} lines updated")
            except Exception as e:
                print(f"  [WARN] Poly sub-poll error: {e}")

        # ── 다음 폴링까지 대기 (POLY_INTERVAL 단위) ──
        for _ in range(POLY_INTERVAL):
            if not running:
                break
            time.sleep(1)

    conn.close()
    print("[DONE] 모니터 종료")


if __name__ == "__main__":
    if "--ws" in sys.argv or "-w" in sys.argv:
        main_ws()
    else:
        main()
