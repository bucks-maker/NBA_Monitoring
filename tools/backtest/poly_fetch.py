"""
Polymarket 과거 가격/마켓 데이터 수집기

1. Gamma API: 마켓 매핑 (event → markets → clobTokenIds)
2. CLOB prices-history: 분 단위 가격 히스토리
3. data-api: 봇/지갑 거래 내역
"""
from __future__ import annotations

import json
import re
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"
DATA_API = "https://data-api.polymarket.com"

# 팀 약어 매핑 (snapshot.py와 동일)
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

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")


def _classify_market(question: str, slug: str) -> str:
    """Polymarket 마켓 분류"""
    q = question.lower()
    s = slug.lower()

    if any(kw in q for kw in ["points o/u", "rebounds o/u", "assists o/u",
                                "threes o/u", "steals o/u", "blocks o/u"]):
        return "player_prop"
    if any(kw in q for kw in ["1h", "1q", "2q", "3q", "4q", "first half", "first quarter"]):
        return "other"
    if "o/u" in q or "total" in s:
        return "total"
    if "spread" in q or "spread" in s:
        return "spread"
    if " vs" in q or " vs." in q:
        return "moneyline"
    return "other"


def _make_poly_slug(home: str, away: str, commence: str) -> str:
    """Polymarket event slug 생성: nba-{away_abbr}-{home_abbr}-{date_et}"""
    away_abbr = FULL_TO_POLY_ABBR.get(away, "")
    home_abbr = FULL_TO_POLY_ABBR.get(home, "")
    if not (away_abbr and home_abbr and commence):
        return ""
    dt_utc = datetime.fromisoformat(commence.replace("Z", "+00:00"))
    dt_et = dt_utc.astimezone(ET)
    return f"nba-{away_abbr}-{home_abbr}-{dt_et.strftime('%Y-%m-%d')}"


def discover_markets(
    conn: sqlite3.Connection,
    game_key: str,
    home: str,
    away: str,
    commence: str,
) -> list[dict]:
    """
    Gamma API로 경기의 Polymarket 마켓 매핑 수집

    Returns:
        [{market_type, poly_market_slug, token_id_1, token_id_2,
          outcome1_name, outcome2_name}, ...]
    """
    poly_slug = _make_poly_slug(home, away, commence)
    if not poly_slug:
        return []

    client = httpx.Client(timeout=15)
    try:
        resp = client.get(f"{GAMMA_API}/events", params={"slug": poly_slug})
        events = resp.json()
    except Exception:
        client.close()
        return []

    if not events:
        client.close()
        return []

    results = []
    event = events[0]

    for m in event.get("markets", []):
        q = m.get("question") or ""
        market_slug = m.get("slug", "")
        market_type = _classify_market(q, market_slug)

        if market_type in ("player_prop", "other"):
            continue

        clob_token_ids = m.get("clobTokenIds")
        if isinstance(clob_token_ids, str):
            clob_token_ids = json.loads(clob_token_ids)
        if not clob_token_ids or len(clob_token_ids) < 2:
            continue

        outcomes = m.get("outcomes", [])
        if isinstance(outcomes, str):
            outcomes = json.loads(outcomes)

        token_1 = clob_token_ids[0]
        token_2 = clob_token_ids[1] if len(clob_token_ids) > 1 else ""
        name_1 = outcomes[0] if outcomes else "outcome1"
        name_2 = outcomes[1] if len(outcomes) > 1 else "outcome2"

        # DB 저장
        try:
            conn.execute("""
                INSERT OR REPLACE INTO market_mapping
                (game_key, home_team, away_team, commence_time,
                 poly_event_slug, market_type, poly_market_slug,
                 token_id_1, token_id_2, outcome1_name, outcome2_name)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                game_key, home, away, commence,
                poly_slug, market_type, market_slug,
                token_1, token_2, name_1, name_2,
            ))
        except Exception:
            pass

        results.append({
            "market_type": market_type,
            "poly_market_slug": market_slug,
            "token_id_1": token_1,
            "token_id_2": token_2,
            "outcome1_name": name_1,
            "outcome2_name": name_2,
        })

    client.close()
    conn.commit()
    return results


def fetch_price_history(
    token_id: str,
    start_ts: int,
    end_ts: int,
    fidelity: int = 1,  # 1분 단위
) -> list[dict]:
    """
    CLOB prices-history에서 가격 히스토리 조회

    Args:
        token_id: clobTokenId
        start_ts: unix timestamp (UTC)
        end_ts: unix timestamp (UTC)
        fidelity: 해상도 (분)

    Returns:
        [{"t": unix_ts, "p": price}, ...]
    """
    params = {
        "market": token_id,
        "startTs": start_ts,
        "endTs": end_ts,
        "fidelity": fidelity,
    }

    try:
        resp = httpx.get(f"{CLOB_API}/prices-history", params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data.get("history", [])
    except Exception as e:
        print(f"  [WARN] prices-history failed for {token_id}: {e}")
        return []


def store_price_history(
    conn: sqlite3.Connection,
    game_key: str,
    market_type: str,
    token_id: str,
    outcome: str,
    history: list[dict],
) -> int:
    """가격 히스토리 DB 저장"""
    count = 0
    for point in history:
        ts = point.get("t")
        price = point.get("p")
        if ts is None or price is None:
            continue
        try:
            conn.execute("""
                INSERT OR IGNORE INTO poly_prices
                (game_key, market_type, token_id, outcome, ts_unix, price)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (game_key, market_type, token_id, outcome, int(ts), float(price)))
            count += 1
        except Exception:
            pass
    conn.commit()
    return count


def collect_poly_prices_for_game(
    conn: sqlite3.Connection,
    game_key: str,
    start_ts: int,
    end_ts: int,
    fidelity: int = 1,
    delay: float = 0.5,
) -> int:
    """
    한 경기의 모든 마켓 가격 히스토리 수집

    Args:
        game_key: Odds API event ID
        start_ts: 수집 시작 (move 시점 - 30분 등)
        end_ts: 수집 종료 (move 시점 + 60분 등)

    Returns:
        총 저장된 데이터 포인트 수
    """
    mappings = conn.execute("""
        SELECT market_type, token_id_1, token_id_2,
               outcome1_name, outcome2_name
        FROM market_mapping
        WHERE game_key = ?
    """, (game_key,)).fetchall()

    if not mappings:
        return 0

    total = 0
    for mtype, tid1, tid2, name1, name2 in mappings:
        for token_id, outcome in [(tid1, name1), (tid2, name2)]:
            if not token_id:
                continue
            history = fetch_price_history(token_id, start_ts, end_ts, fidelity)
            stored = store_price_history(conn, game_key, mtype, token_id, outcome, history)
            total += stored
            time.sleep(delay)

    return total
