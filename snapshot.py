"""
Pinnacle + Polymarket NBA Total 스냅샷 수집기

Pinnacle 라인/가격을 주기적으로 저장하고,
변동 감지 시 Polymarket 가격과 봇 거래를 교차 기록한다.
"""

import sqlite3
import json
import re
import time
import signal
from datetime import datetime, timezone, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx

DB_PATH = Path(__file__).parent / "data" / "snapshots.db"
SCHEMA_PATH = Path(__file__).parent / "schema.sql"
ODDS_API_KEY = "5700da6b9fe3d555aa4dbb4ec2d00a60"
BOT_ADDRESS = "0x6e82b93eb57b01a63027bd0c6d2f3f04934a752c"

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

def fetch_polymarket(conn: sqlite3.Connection, games: list[dict]):
    """Pinnacle 경기 목록 기반으로 Polymarket Total 마켓 스냅샷"""
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
        resp = client.get(
            "https://gamma-api.polymarket.com/events",
            params={"slug": poly_slug}
        )
        events = resp.json()
        if not events:
            continue

        conn.execute(
            "UPDATE game_mapping SET poly_event_found = 1 WHERE odds_api_id = ?",
            (game_id,)
        )

        event = events[0]
        for m in event.get("markets", []):
            q = (m.get("question") or "").lower()
            # 풀게임 O/U만 (선수 프롭/1H 제외)
            is_player_prop = any(kw in q for kw in ["points o/u", "rebounds o/u", "assists o/u"])
            if "o/u" not in q or "1h" in q or "1q" in q or is_player_prop:
                continue

            line = _extract_line(q) or _extract_line(m.get("slug", ""))
            if line is None or not (170 <= line <= 310):
                continue

            outcomes = m.get("outcomes", [])
            prices = m.get("outcomePrices", [])
            if isinstance(outcomes, str):
                outcomes = json.loads(outcomes)
            if isinstance(prices, str):
                prices = json.loads(prices)

            over_price = under_price = None
            for i, name in enumerate(outcomes):
                p = float(prices[i]) if i < len(prices) else None
                if p is None:
                    continue
                if "over" in name.lower():
                    over_price = p
                else:
                    under_price = p

            market_slug = m.get("slug", "")

            # TODO: CLOB 오더북 (추후 추가)
            conn.execute("""
                INSERT OR IGNORE INTO poly_snapshots
                (game_id, poly_market_slug, snapshot_time, total_line,
                 over_price, under_price,
                 over_best_bid, over_best_ask, under_best_bid, under_best_ask)
                VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL)
            """, (game_id, market_slug, snap_time, line, over_price, under_price))
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

        # 트리거 시점 Polymarket 가격 조회
        poly_snap = conn.execute("""
            SELECT over_price, under_price
            FROM poly_snapshots
            WHERE game_id = ? AND total_line = ?
            ORDER BY snapshot_time DESC LIMIT 1
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

        # 최신 Polymarket 가격 조회
        poly_snap = conn.execute("""
            SELECT under_price, over_price
            FROM poly_snapshots
            WHERE game_id = ? AND total_line = ?
            ORDER BY snapshot_time DESC LIMIT 1
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


def main():
    signal.signal(signal.SIGINT, signal_handler)

    conn = init_db()
    et_now = now_et_str()
    print(f"Pinnacle-Polymarket NBA Monitor")
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
    main()
