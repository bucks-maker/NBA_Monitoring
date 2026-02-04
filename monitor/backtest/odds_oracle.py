"""
Odds API Historical 데이터 수집기

The Odds API v4 Historical endpoint를 사용하여
Pinnacle NBA 라인/가격 스냅샷을 수집한다.

- 5분 간격 스냅샷 (2022.09 이후)
- 10 크레딧/리전/마켓
- 1회 요청 = 해당 시점 전체 NBA 경기 반환
"""
from __future__ import annotations

import sqlite3
import time
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path

import httpx
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

ODDS_API_KEY = os.environ["ODDS_API_KEY"]
BASE_URL = "https://api.the-odds-api.com/v4"
SPORT = "basketball_nba"
BOOKMAKER = "pinnacle"
MARKETS = "totals,spreads,h2h"
REGION = "us"

# 크레딧: 3 마켓 × 1 리전 × 10 = 30 크레딧/요청
CREDITS_PER_REQUEST = 30


def fetch_historical_snapshot(
    date_iso: str,
    sport: str = SPORT,
    markets: str = MARKETS,
    bookmaker: str = BOOKMAKER,
) -> tuple[list[dict], dict]:
    """
    특정 시점의 Historical odds 스냅샷 조회

    Args:
        date_iso: ISO 8601 형식 (e.g. "2026-01-15T19:00:00Z")

    Returns:
        (games_list, meta_dict)
        meta_dict: {timestamp, remaining_credits, used_credits, snapshot_time}
    """
    url = f"{BASE_URL}/historical/sports/{sport}/odds"
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": REGION,
        "markets": markets,
        "bookmakers": bookmaker,
        "oddsFormat": "decimal",
        "date": date_iso,
    }

    resp = httpx.get(url, params=params, timeout=30)
    resp.raise_for_status()

    body = resp.json()
    # Historical endpoint returns {data: [...], ...} wrapper
    games = body.get("data", body) if isinstance(body, dict) else body

    meta = {
        "requested_date": date_iso,
        "snapshot_time": body.get("timestamp") if isinstance(body, dict) else date_iso,
        "remaining_credits": resp.headers.get("x-requests-remaining", "?"),
        "used_credits": resp.headers.get("x-requests-used", "?"),
    }

    return games if isinstance(games, list) else [], meta


def fetch_historical_event(
    event_id: str,
    date_iso: str,
    sport: str = SPORT,
    markets: str = MARKETS,
) -> tuple[dict | None, dict]:
    """
    특정 경기의 Historical odds 조회

    Args:
        event_id: Odds API event ID
        date_iso: ISO 8601 형식

    Returns:
        (event_dict, meta_dict)
    """
    url = f"{BASE_URL}/historical/sports/{sport}/events/{event_id}/odds"
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": REGION,
        "markets": markets,
        "oddsFormat": "decimal",
        "date": date_iso,
    }

    resp = httpx.get(url, params=params, timeout=30)
    resp.raise_for_status()

    body = resp.json()
    data = body.get("data") if isinstance(body, dict) else body
    meta = {
        "requested_date": date_iso,
        "snapshot_time": body.get("timestamp") if isinstance(body, dict) else date_iso,
        "remaining_credits": resp.headers.get("x-requests-remaining", "?"),
        "used_credits": resp.headers.get("x-requests-used", "?"),
    }

    return data, meta


def store_snapshot(conn: sqlite3.Connection, games: list[dict], snapshot_ts: str):
    """
    스냅샷 데이터를 DB에 저장

    Args:
        games: Odds API response games list
        snapshot_ts: 실제 스냅샷 시점 (ISO8601)
    """
    ts_unix = int(datetime.fromisoformat(
        snapshot_ts.replace("Z", "+00:00")
    ).timestamp())

    count = 0
    for game in games:
        game_key = game.get("id", "")
        home = game.get("home_team", "")
        away = game.get("away_team", "")
        commence = game.get("commence_time", "")

        for bm in game.get("bookmakers", []):
            if bm["key"] != BOOKMAKER:
                continue

            for market in bm.get("markets", []):
                market_key = market["key"]  # 'totals', 'spreads', 'h2h'
                outcomes = market.get("outcomes", [])

                if len(outcomes) < 2:
                    continue

                o1 = outcomes[0]
                o2 = outcomes[1]
                line = o1.get("point") or o2.get("point")

                o1_odds = o1.get("price")
                o2_odds = o2.get("price")
                o1_implied = 1.0 / o1_odds if o1_odds and o1_odds > 0 else None
                o2_implied = 1.0 / o2_odds if o2_odds and o2_odds > 0 else None

                try:
                    conn.execute("""
                        INSERT OR IGNORE INTO oracle_snapshots
                        (game_key, market_type, ts, ts_unix, line,
                         outcome1_name, outcome2_name,
                         outcome1_odds, outcome2_odds,
                         outcome1_implied, outcome2_implied,
                         bookmaker)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        game_key, market_key, snapshot_ts, ts_unix, line,
                        o1["name"], o2["name"],
                        o1_odds, o2_odds,
                        o1_implied, o2_implied,
                        BOOKMAKER,
                    ))
                    count += 1
                except Exception:
                    pass

    conn.commit()
    return count


def collect_date_range(
    conn: sqlite3.Connection,
    start_date: datetime,
    end_date: datetime,
    interval_minutes: int = 30,
    active_start_hour: int = 17,  # UTC 17 = ET 12
    active_end_hour: int = 8,     # UTC 08 = ET 03
    delay: float = 1.0,
) -> dict:
    """
    날짜 범위의 Historical 스냅샷 수집

    Args:
        start_date: 시작일 (UTC)
        end_date: 종료일 (UTC)
        interval_minutes: 스냅샷 간격 (분)
        active_start_hour: 수집 시작 시간 (UTC)
        active_end_hour: 수집 종료 시간 (UTC, 다음날)
        delay: API 호출 간 대기 (초)

    Returns:
        {total_requests, total_credits, games_stored, errors}
    """
    stats = {
        "total_requests": 0,
        "total_credits_estimated": 0,
        "games_stored": 0,
        "errors": 0,
        "remaining_credits": "?",
    }

    current = start_date
    while current <= end_date:
        hour = current.hour

        # 활성 시간대만 수집 (UTC 17:00~08:00 = ET 12:00~03:00)
        is_active = (hour >= active_start_hour) or (hour < active_end_hour)
        if not is_active:
            current += timedelta(hours=1)
            continue

        date_iso = current.strftime("%Y-%m-%dT%H:%M:%SZ")

        try:
            games, meta = fetch_historical_snapshot(date_iso)
            stored = store_snapshot(conn, games, meta.get("snapshot_time", date_iso))

            stats["total_requests"] += 1
            stats["total_credits_estimated"] += CREDITS_PER_REQUEST
            stats["games_stored"] += stored
            stats["remaining_credits"] = meta.get("remaining_credits", "?")

            print(f"  [{date_iso}] {stored} lines stored | "
                  f"Credits: {meta.get('used_credits', '?')} used / "
                  f"{meta.get('remaining_credits', '?')} remaining")

        except httpx.HTTPStatusError as e:
            stats["errors"] += 1
            print(f"  [{date_iso}] ERROR {e.response.status_code}: {e}")
            if e.response.status_code == 422:
                # 데이터 없는 날짜 → 건너뛰기
                current += timedelta(days=1)
                current = current.replace(hour=active_start_hour, minute=0, second=0)
                continue
            if e.response.status_code == 429:
                print("  Rate limited, waiting 60s...")
                time.sleep(60)
                continue
        except Exception as e:
            stats["errors"] += 1
            print(f"  [{date_iso}] ERROR: {e}")

        time.sleep(delay)
        current += timedelta(minutes=interval_minutes)

    return stats
