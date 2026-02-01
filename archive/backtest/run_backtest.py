#!/usr/bin/env python3
"""
백테스트 메인 실행 스크립트

Oracle Move → Polymarket Gap 가설 검증

사용법:
    # 최근 2주 데이터 수집 + 분석
    python3 run_backtest.py

    # 특정 날짜 범위
    python3 run_backtest.py --start 2026-01-14 --end 2026-01-28

    # 단계별 실행
    python3 run_backtest.py --step collect_odds    # 1. Odds API 수집만
    python3 run_backtest.py --step discover_poly   # 2. Poly 마켓 매핑만
    python3 run_backtest.py --step collect_poly    # 3. Poly 가격 수집만
    python3 run_backtest.py --step analyze         # 4. 분석만
    python3 run_backtest.py --step report          # 5. 리포트만

환경 변수:
    ODDS_API_KEY: The Odds API 키 (.env 파일)
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

# 프로젝트 경로 추가
sys.path.insert(0, str(Path(__file__).parent))

from odds_oracle import collect_date_range, fetch_historical_snapshot, store_snapshot
from poly_fetch import discover_markets, collect_poly_prices_for_game
from triggers import extract_move_events, get_move_events
from gap_metrics import compute_all_gaps
from report import print_report, generate_report

DB_PATH = Path(__file__).parent / "data" / "backtest.db"
SCHEMA_PATH = Path(__file__).parent / "schema.sql"


def init_db() -> sqlite3.Connection:
    """DB 초기화"""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    with open(SCHEMA_PATH) as f:
        conn.executescript(f.read())
    conn.commit()
    return conn


def step_collect_odds(conn: sqlite3.Connection, start: datetime, end: datetime, interval: int):
    """Step 1: Odds API Historical 데이터 수집"""
    print(f"\n{'='*60}")
    print(f"STEP 1: Odds API Historical 수집")
    print(f"  기간: {start.date()} ~ {end.date()}")
    print(f"  간격: {interval}분")
    print(f"  예상 크레딧: ~{estimate_credits(start, end, interval)} credits")
    print(f"{'='*60}\n")

    stats = collect_date_range(
        conn, start, end,
        interval_minutes=interval,
        delay=1.0,
    )

    print(f"\n[완료] {stats['total_requests']}건 요청 | "
          f"{stats['games_stored']}개 라인 저장 | "
          f"~{stats['total_credits_estimated']} 크레딧 사용 | "
          f"{stats['errors']}건 에러 | "
          f"잔여: {stats['remaining_credits']}")

    return stats


def step_discover_poly(conn: sqlite3.Connection):
    """Step 2: Polymarket 마켓 매핑 수집"""
    print(f"\n{'='*60}")
    print(f"STEP 2: Polymarket 마켓 매핑 수집")
    print(f"{'='*60}\n")

    # oracle_snapshots에서 유니크 게임 목록
    games = conn.execute("""
        SELECT DISTINCT game_key
        FROM oracle_snapshots
    """).fetchall()

    print(f"  총 {len(games)}개 게임")

    total_markets = 0
    for (game_key,) in games:
        # 경기 정보 조회 (Odds API 데이터에서)
        info = conn.execute("""
            SELECT DISTINCT outcome1_name, outcome2_name
            FROM oracle_snapshots
            WHERE game_key = ? AND market_type = 'h2h'
            LIMIT 1
        """, (game_key,)).fetchone()

        if not info:
            continue

        # h2h outcome names = team names
        home = info[0] or ""
        away = info[1] or ""

        # commence_time 추정 (가장 이른 스냅샷 기준은 부정확 → Odds API에서 직접)
        # 일단 스냅샷에서 추정
        first_ts = conn.execute("""
            SELECT MIN(ts) FROM oracle_snapshots WHERE game_key = ?
        """, (game_key,)).fetchone()

        commence = first_ts[0] if first_ts else ""

        markets = discover_markets(conn, game_key, home, away, commence)
        total_markets += len(markets)

        if markets:
            mtypes = [m["market_type"] for m in markets]
            print(f"  {away} @ {home}: {mtypes}")
        else:
            print(f"  {away} @ {home}: (Poly 마켓 없음)")

        time.sleep(0.3)

    print(f"\n[완료] {total_markets}개 마켓 매핑됨")
    return total_markets


def step_collect_poly(conn: sqlite3.Connection):
    """Step 3: Polymarket 가격 히스토리 수집"""
    print(f"\n{'='*60}")
    print(f"STEP 3: Polymarket 가격 히스토리 수집")
    print(f"{'='*60}\n")

    # 매핑된 게임의 시간 범위 조회
    games = conn.execute("""
        SELECT DISTINCT mm.game_key,
               MIN(os.ts_unix) as min_ts,
               MAX(os.ts_unix) as max_ts
        FROM market_mapping mm
        JOIN oracle_snapshots os ON os.game_key = mm.game_key
        GROUP BY mm.game_key
    """).fetchall()

    print(f"  총 {len(games)}개 게임 (Poly 매핑 있음)")

    total_points = 0
    for game_key, min_ts, max_ts in games:
        # 수집 범위: 첫 스냅샷 30분 전 ~ 마지막 스냅샷 60분 후
        start_ts = min_ts - 1800
        end_ts = max_ts + 3600

        points = collect_poly_prices_for_game(
            conn, game_key, start_ts, end_ts,
            fidelity=1,  # 1분 단위
            delay=0.5,
        )
        total_points += points

        info = conn.execute("""
            SELECT home_team, away_team FROM market_mapping WHERE game_key = ? LIMIT 1
        """, (game_key,)).fetchone()
        game_label = f"{info[1]} @ {info[0]}" if info else game_key

        print(f"  {game_label}: {points} price points")

    print(f"\n[완료] {total_points}개 가격 포인트 수집")
    return total_points


def step_analyze(conn: sqlite3.Connection):
    """Step 4: Move 감지 + Gap 계산"""
    print(f"\n{'='*60}")
    print(f"STEP 4: Move 감지 + Gap 계산")
    print(f"{'='*60}\n")

    # Move events 추출
    print("  Move events 추출 중...")
    move_count = extract_move_events(conn)
    print(f"  → {move_count}개 move events 감지됨")

    events = get_move_events(conn)
    for evt in events[:10]:  # 처음 10개만 출력
        print(f"    [{evt['move_ts']}] {evt['market_type']} "
              f"Δ={evt['delta_value']:+.3f} "
              f"({evt['home_team'] or '?'} vs {evt['away_team'] or '?'})")
    if len(events) > 10:
        print(f"    ... +{len(events)-10}개")

    # Gap 계산
    print(f"\n  Gap 계산 중...")
    gap_stats = compute_all_gaps(conn)
    print(f"  → {gap_stats['total_events']}개 이벤트 중 "
          f"{gap_stats['events_with_poly']}개에 Poly 데이터 존재")
    print(f"  → 라인 불일치로 건너뜀: {gap_stats.get('skipped_line_mismatch', 0)}개")
    print(f"  → Actionable (gap_5m >= 4%p): {gap_stats['actionable_count']}개")

    return gap_stats


def step_report(conn: sqlite3.Connection):
    """Step 5: 리포트 생성"""
    report = print_report(conn)

    # 파일로도 저장
    report_path = Path(__file__).parent / "data" / "report.txt"
    report_path.write_text(report)
    print(f"\n리포트 저장: {report_path}")

    return report


def estimate_credits(start: datetime, end: datetime, interval_minutes: int) -> int:
    """크레딧 사용량 예상"""
    total_hours = (end - start).total_seconds() / 3600
    # 활성 시간 비율 (~63%: 15시간/24시간)
    active_hours = total_hours * 0.63
    requests = active_hours * (60 / interval_minutes)
    return int(requests * 30)  # 30 credits per request


def main():
    parser = argparse.ArgumentParser(description="NBA Oracle-Poly Gap 백테스트")
    parser.add_argument("--start", type=str, help="시작일 (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="종료일 (YYYY-MM-DD)")
    parser.add_argument("--interval", type=int, default=30,
                        help="Odds API 호출 간격 (분, 기본 30)")
    parser.add_argument("--step", type=str, default="all",
                        choices=["all", "collect_odds", "discover_poly",
                                 "collect_poly", "analyze", "report"],
                        help="실행 단계")

    args = parser.parse_args()

    # 기본값: 최근 2주
    if args.end:
        end = datetime.strptime(args.end, "%Y-%m-%d").replace(
            tzinfo=timezone.utc, hour=8)  # UTC 08 = ET 03
    else:
        end = datetime.now(timezone.utc)

    if args.start:
        start = datetime.strptime(args.start, "%Y-%m-%d").replace(
            tzinfo=timezone.utc, hour=17)  # UTC 17 = ET 12
    else:
        start = end - timedelta(days=14)
        start = start.replace(hour=17)

    conn = init_db()

    print(f"NBA Oracle-Poly Gap Backtest")
    print(f"DB: {DB_PATH}")
    print(f"기간: {start.date()} ~ {end.date()}")
    print(f"Odds API 간격: {args.interval}분")
    print(f"예상 크레딧: ~{estimate_credits(start, end, args.interval)}")

    try:
        if args.step in ("all", "collect_odds"):
            step_collect_odds(conn, start, end, args.interval)

        if args.step in ("all", "discover_poly"):
            step_discover_poly(conn)

        if args.step in ("all", "collect_poly"):
            step_collect_poly(conn)

        if args.step in ("all", "analyze"):
            step_analyze(conn)

        if args.step in ("all", "report"):
            step_report(conn)

    except KeyboardInterrupt:
        print("\n[중단됨]")
    finally:
        conn.close()

    print(f"\n[완료] DB: {DB_PATH}")


if __name__ == "__main__":
    main()
