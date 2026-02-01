"""
Oracle Move Event 추출

oracle_snapshots 테이블에서 연속 스냅샷 간 큰 변동을 감지하여
move_events 테이블에 기록한다.

트리거 조건:
- |Δimplied_prob| >= 6%p (기본)
- |Δline| >= 1.5 (보조)
"""
from __future__ import annotations

import sqlite3
from typing import Optional

# 기본 임계값
IMPLIED_THRESHOLD = 0.06  # 6%p
LINE_THRESHOLD = 1.5      # 1.5 포인트


def extract_move_events(
    conn: sqlite3.Connection,
    implied_threshold: float = IMPLIED_THRESHOLD,
    line_threshold: float = LINE_THRESHOLD,
) -> int:
    """
    oracle_snapshots에서 연속 스냅샷 간 변동 감지 → move_events 생성

    Returns:
        감지된 이벤트 수
    """
    # 게임+마켓별 스냅샷을 시간순으로 조회
    games = conn.execute("""
        SELECT DISTINCT game_key, market_type
        FROM oracle_snapshots
        ORDER BY game_key, market_type
    """).fetchall()

    count = 0

    for game_key, market_type in games:
        snapshots = conn.execute("""
            SELECT ts, ts_unix, line,
                   outcome1_implied, outcome2_implied,
                   outcome1_name, outcome2_name
            FROM oracle_snapshots
            WHERE game_key = ? AND market_type = ?
            ORDER BY ts_unix ASC
        """, (game_key, market_type)).fetchall()

        if len(snapshots) < 2:
            continue

        for i in range(1, len(snapshots)):
            prev = snapshots[i - 1]
            curr = snapshots[i]

            prev_ts, prev_ts_unix, prev_line, prev_imp1, prev_imp2, _, _ = prev
            curr_ts, curr_ts_unix, curr_line, curr_imp1, curr_imp2, name1, name2 = curr

            # implied prob 변동 체크
            triggered = False
            metric = None
            delta = None
            prev_val = None
            new_val = None

            if prev_imp1 is not None and curr_imp1 is not None:
                d1 = curr_imp1 - prev_imp1
                if abs(d1) >= implied_threshold:
                    triggered = True
                    metric = "implied_prob"
                    delta = d1
                    prev_val = prev_imp1
                    new_val = curr_imp1

            if prev_imp2 is not None and curr_imp2 is not None:
                d2 = curr_imp2 - prev_imp2
                if abs(d2) >= implied_threshold and (not triggered or abs(d2) > abs(delta)):
                    triggered = True
                    metric = "implied_prob"
                    delta = d2
                    prev_val = prev_imp2
                    new_val = curr_imp2

            # line 변동 체크 (보조)
            if prev_line is not None and curr_line is not None:
                dl = curr_line - prev_line
                if abs(dl) >= line_threshold:
                    if not triggered:
                        triggered = True
                        metric = "line"
                        delta = dl
                        prev_val = prev_line
                        new_val = curr_line

            if not triggered:
                continue

            # 이미 등록된 이벤트 중복 체크
            existing = conn.execute("""
                SELECT id FROM move_events
                WHERE game_key = ? AND market_type = ? AND move_ts_unix = ?
            """, (game_key, market_type, curr_ts_unix)).fetchone()

            if existing:
                continue

            # 경기 정보 조회
            game_info = conn.execute("""
                SELECT home_team, away_team, commence_time
                FROM market_mapping
                WHERE game_key = ?
                LIMIT 1
            """, (game_key,)).fetchone()

            home = game_info[0] if game_info else ""
            away = game_info[1] if game_info else ""
            commence = game_info[2] if game_info else ""

            conn.execute("""
                INSERT INTO move_events
                (game_key, market_type, move_ts, move_ts_unix,
                 metric, prev_value, new_value, delta_value, prev_ts,
                 home_team, away_team, commence_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                game_key, market_type, curr_ts, curr_ts_unix,
                metric, prev_val, new_val, delta,
                prev_ts,
                home, away, commence,
            ))
            count += 1

    conn.commit()
    return count


def get_move_events(
    conn: sqlite3.Connection,
    market_type: Optional[str] = None,
    min_delta: Optional[float] = None,
) -> list[dict]:
    """
    저장된 move events 조회

    Args:
        market_type: 필터 (None=전체)
        min_delta: 최소 |delta| 필터

    Returns:
        [{id, game_key, market_type, move_ts_unix, metric, delta_value, ...}]
    """
    query = "SELECT * FROM move_events WHERE 1=1"
    params = []

    if market_type:
        query += " AND market_type = ?"
        params.append(market_type)

    if min_delta is not None:
        query += " AND ABS(delta_value) >= ?"
        params.append(min_delta)

    query += " ORDER BY move_ts_unix ASC"

    rows = conn.execute(query, params).fetchall()
    columns = [d[0] for d in conn.execute(query, params).description] if rows else []

    # re-execute for description (cursor is consumed)
    cursor = conn.execute(query, params)
    columns = [d[0] for d in cursor.description]
    rows = cursor.fetchall()

    return [dict(zip(columns, row)) for row in rows]
