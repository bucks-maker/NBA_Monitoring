"""
갭 측정 및 분석

각 move_event에 대해:
1. oracle implied prob 시계열 (oracle_snapshots)
2. Polymarket 가격 시계열 (poly_prices)
를 결합하여 gap decay를 계산한다.

출력 지표:
- gap_0m: move 시점 갭
- gap_5m: +5분 갭
- gap_10m: +10분 갭
- gap_30m, gap_60m
- half_life: 갭이 50% 줄어든 시간 (초)
- actionable: gap_5m >= threshold
"""
from __future__ import annotations

import re
import sqlite3
from typing import Optional


# actionable 판정 기준
ACTIONABLE_GAP_THRESHOLD = 0.04  # 4%p at t+5m

# 라인 근접 허용 범위 (totals/spreads에서 Oracle vs Poly 라인 비교)
LINE_PROXIMITY_THRESHOLD = 1.5  # ±1.5 points


def _extract_poly_line(slug: str) -> Optional[float]:
    """Poly market slug에서 라인 추출.
    예: 'nba-uta-dal-2026-01-15-total-235pt5' → 235.5
        'nba-hou-det-2026-01-23-spread-home-4pt5' → 4.5
    """
    if not slug:
        return None
    m = re.search(r"(\d+)pt(\d+)", slug)
    if m:
        return float(f"{m.group(1)}.{m.group(2)}")
    # 정수 라인 (pt 없이)
    m = re.search(r"(?:total|spread-(?:home|away))-(\d+)$", slug)
    if m:
        return float(m.group(1))
    return None


def _check_line_proximity(
    conn: sqlite3.Connection,
    game_key: str,
    market_type: str,
    move_ts: int,
) -> bool:
    """totals/spreads에서 Oracle 라인이 Poly 라인과 충분히 가까운지 확인.
    h2h는 항상 True 반환.
    """
    if market_type == "h2h":
        return True

    poly_mtype = {"totals": "total", "spreads": "spread"}.get(market_type, market_type)

    mapping = conn.execute("""
        SELECT poly_market_slug
        FROM market_mapping
        WHERE game_key = ? AND market_type = ?
        LIMIT 1
    """, (game_key, poly_mtype)).fetchone()

    if not mapping or not mapping[0]:
        return False

    poly_line = _extract_poly_line(mapping[0])
    if poly_line is None:
        return False

    # Oracle 라인 조회 (move 시점)
    orow = conn.execute("""
        SELECT line FROM oracle_snapshots
        WHERE game_key = ? AND market_type = ?
          AND ts_unix <= ?
        ORDER BY ts_unix DESC
        LIMIT 1
    """, (game_key, market_type, move_ts)).fetchone()

    if not orow or orow[0] is None:
        return False

    oracle_line = abs(orow[0])  # spreads는 음수일 수 있음
    return abs(oracle_line - poly_line) <= LINE_PROXIMITY_THRESHOLD


def _find_closest_poly_price(
    conn: sqlite3.Connection,
    game_key: str,
    market_type: str,
    target_ts: int,
    oracle_outcome1_name: str = "",
    window_sec: int = 600,
) -> Optional[float]:
    """
    target_ts에 가장 가까운 Poly 가격 조회
    Oracle outcome1과 동일한 팀/outcome의 Poly 가격을 반환

    Args:
        game_key: 경기 ID
        market_type: 'totals', 'spreads', 'h2h'
        target_ts: 목표 unix timestamp
        oracle_outcome1_name: Oracle의 outcome1 이름 (팀명/Over 등)
        window_sec: 검색 윈도우 (±초)
    """
    poly_mtype = {
        "totals": "total",
        "spreads": "spread",
        "h2h": "moneyline",
    }.get(market_type, market_type)

    mapping = conn.execute("""
        SELECT token_id_1, token_id_2, outcome1_name, outcome2_name
        FROM market_mapping
        WHERE game_key = ? AND market_type = ?
        LIMIT 1
    """, (game_key, poly_mtype)).fetchone()

    if not mapping:
        return None

    tid1, tid2, poly_name1, poly_name2 = mapping

    # Oracle outcome1 이름과 Poly outcome 이름 매칭
    token_id = tid1  # 기본값
    if oracle_outcome1_name:
        oracle_lower = oracle_outcome1_name.lower()
        poly_lower1 = (poly_name1 or "").lower()
        poly_lower2 = (poly_name2 or "").lower()

        # 정확 매칭 시도
        if oracle_lower in poly_lower1 or poly_lower1 in oracle_lower:
            token_id = tid1
        elif oracle_lower in poly_lower2 or poly_lower2 in oracle_lower:
            token_id = tid2
        else:
            # 키워드 매칭: Over/Under, Home/Away 팀 약어
            if oracle_lower == "over":
                # Poly에서 "Over" 찾기
                token_id = tid1 if "over" in poly_lower1 else tid2
            elif oracle_lower == "under":
                token_id = tid1 if "under" in poly_lower1 else tid2
            else:
                # 팀명 부분 매칭 (마지막 단어 비교 등)
                oracle_parts = oracle_lower.split()
                if any(p in poly_lower1 for p in oracle_parts if len(p) > 3):
                    token_id = tid1
                elif any(p in poly_lower2 for p in oracle_parts if len(p) > 3):
                    token_id = tid2

    if not token_id:
        return None

    row = conn.execute("""
        SELECT price, ts_unix
        FROM poly_prices
        WHERE token_id = ?
          AND ts_unix BETWEEN ? AND ?
        ORDER BY ABS(ts_unix - ?)
        LIMIT 1
    """, (token_id, target_ts - window_sec, target_ts + window_sec, target_ts)).fetchone()

    return row[0] if row else None


def _get_devigged_oracle(
    conn: sqlite3.Connection,
    game_key: str,
    market_type: str,
    target_ts: int,
) -> tuple[float | None, float | None, str]:
    """
    특정 시점의 de-vigged oracle implied 조회
    Returns: (outcome1_fair, outcome2_fair, outcome1_name)
    """
    row = conn.execute("""
        SELECT outcome1_implied, outcome2_implied, outcome1_name
        FROM oracle_snapshots
        WHERE game_key = ? AND market_type = ?
          AND ts_unix <= ?
        ORDER BY ts_unix DESC
        LIMIT 1
    """, (game_key, market_type, target_ts)).fetchone()

    if not row or row[0] is None or row[1] is None:
        return None, None, ""

    raw1, raw2 = row[0], row[1]
    total = raw1 + raw2
    if total <= 0:
        return None, None, ""

    return raw1 / total, raw2 / total, row[2] or ""


def compute_gap_series(
    conn: sqlite3.Connection,
    move_event_id: int,
    game_key: str,
    market_type: str,
    move_ts_unix: int,
    oracle_implied: float,  # t0에서의 oracle implied (참조용)
    offsets: list[int] | None = None,
) -> list[dict]:
    """
    move_event에 대한 갭 시계열 계산

    핵심: 각 offset에서 oracle AND poly를 둘 다 조회하여
    gap(t) = |oracle(t) - poly(t)| 계산. oracle도 시간에 따라 변함.

    Args:
        offsets: 측정할 오프셋 (초). 기본: [0, 60, 120, 300, 600, 1800, 3600]

    Returns:
        [{ts_offset_sec, oracle_implied, poly_price, gap, gap_abs}]
    """
    if offsets is None:
        offsets = [0, 60, 120, 300, 600, 1800, 3600]

    results = []

    for offset in offsets:
        target_ts = move_ts_unix + offset

        # Oracle de-vigged implied at target_ts
        oracle_fair1, oracle_fair2, outcome1_name = _get_devigged_oracle(
            conn, game_key, market_type, target_ts
        )
        # outcome1 (Over/Home) 사용
        oracle_at_t = oracle_fair1

        # Poly 가격 조회 (outcome1_name으로 올바른 토큰 매칭)
        window = max(120, offset // 2 + 60)
        poly_price = _find_closest_poly_price(
            conn, game_key, market_type, target_ts,
            oracle_outcome1_name=outcome1_name,
            window_sec=window,
        )

        gap = (oracle_at_t - poly_price) if (oracle_at_t and poly_price) else None
        gap_abs = abs(gap) if gap is not None else None

        entry = {
            "ts_offset_sec": offset,
            "oracle_implied": oracle_at_t,
            "poly_price": poly_price,
            "gap": gap,
            "gap_abs": gap_abs,
        }
        results.append(entry)

        # DB 저장
        try:
            conn.execute("""
                INSERT OR REPLACE INTO gap_series
                (move_event_id, ts_offset_sec, oracle_implied, poly_price, gap, gap_abs)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                move_event_id, offset, oracle_at_t,
                poly_price, gap, gap_abs,
            ))
        except Exception:
            pass

    conn.commit()
    return results


def compute_gap_summary(
    conn: sqlite3.Connection,
    move_event_id: int,
    actionable_threshold: float = ACTIONABLE_GAP_THRESHOLD,
) -> dict:
    """
    move_event의 갭 요약 지표 계산

    Returns:
        {gap_0m, gap_5m, gap_10m, gap_30m, gap_60m, half_life_sec,
         max_gap, max_gap_offset_sec, actionable}
    """
    rows = conn.execute("""
        SELECT ts_offset_sec, gap_abs, gap
        FROM gap_series
        WHERE move_event_id = ?
        ORDER BY ts_offset_sec ASC
    """, (move_event_id,)).fetchall()

    if not rows:
        return {}

    # 오프셋별 갭 매핑
    gap_by_offset = {}
    for offset, gap_abs, gap in rows:
        gap_by_offset[offset] = gap_abs

    gap_0m = gap_by_offset.get(0)
    gap_5m = gap_by_offset.get(300)
    gap_10m = gap_by_offset.get(600)
    gap_30m = gap_by_offset.get(1800)
    gap_60m = gap_by_offset.get(3600)

    # 최대 갭
    max_gap = None
    max_offset = None
    for offset, gap_abs, gap in rows:
        if gap_abs is not None and (max_gap is None or gap_abs > max_gap):
            max_gap = gap_abs
            max_offset = offset

    # half-life 계산: gap_0m의 절반 이하로 떨어지는 첫 시점
    half_life = None
    if gap_0m and gap_0m > 0:
        half_target = gap_0m / 2
        for offset, gap_abs, gap in rows:
            if offset > 0 and gap_abs is not None and gap_abs <= half_target:
                half_life = offset
                break

    # actionable 판정: gap_5m >= threshold
    actionable = 1 if (gap_5m is not None and gap_5m >= actionable_threshold) else 0

    summary = {
        "gap_0m": gap_0m,
        "gap_5m": gap_5m,
        "gap_10m": gap_10m,
        "gap_30m": gap_30m,
        "gap_60m": gap_60m,
        "half_life_sec": half_life,
        "max_gap": max_gap,
        "max_gap_offset_sec": max_offset,
        "actionable": actionable,
    }

    # DB 저장
    try:
        conn.execute("""
            INSERT OR REPLACE INTO gap_summary
            (move_event_id, gap_0m, gap_5m, gap_10m, gap_30m, gap_60m,
             half_life_sec, max_gap, max_gap_offset_sec, actionable)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            move_event_id,
            gap_0m, gap_5m, gap_10m, gap_30m, gap_60m,
            half_life, max_gap, max_offset, actionable,
        ))
    except Exception:
        pass

    conn.commit()
    return summary


def _get_oracle_implied_at(
    conn: sqlite3.Connection,
    game_key: str,
    market_type: str,
    ts_unix: int,
) -> float | None:
    """
    특정 시점의 oracle implied probability 조회
    outcome1_implied (Over/Home/Yes 쪽) 반환
    """
    row = conn.execute("""
        SELECT outcome1_implied, outcome2_implied
        FROM oracle_snapshots
        WHERE game_key = ? AND market_type = ?
          AND ts_unix <= ?
        ORDER BY ts_unix DESC
        LIMIT 1
    """, (game_key, market_type, ts_unix)).fetchone()

    if not row:
        return None
    # outcome1 (Over/Home) 반환
    return row[0]


def compute_all_gaps(
    conn: sqlite3.Connection,
    actionable_threshold: float = ACTIONABLE_GAP_THRESHOLD,
) -> dict:
    """
    모든 move_events에 대해 갭 계산 실행

    핵심: oracle의 implied probability를 직접 조회하여
    Poly 가격과 비교. new_value가 라인인 경우 무시.

    Returns:
        {total_events, events_with_poly, actionable_count, summary_stats}
    """
    events = conn.execute("""
        SELECT id, game_key, market_type, move_ts_unix, new_value, metric
        FROM move_events
        ORDER BY move_ts_unix
    """).fetchall()

    stats = {
        "total_events": len(events),
        "events_with_poly": 0,
        "actionable_count": 0,
        "summaries": [],
    }

    skipped_line = 0

    for evt_id, game_key, market_type, move_ts, new_value, metric in events:
        # totals/spreads: Oracle 라인이 Poly 라인과 가까운지 확인
        if not _check_line_proximity(conn, game_key, market_type, move_ts):
            skipped_line += 1
            continue

        # De-vigged oracle implied at move time
        oracle_fair1, _, outcome1_name = _get_devigged_oracle(conn, game_key, market_type, move_ts)

        if oracle_fair1 is None or oracle_fair1 > 1.0 or oracle_fair1 < 0.0:
            continue

        # gap series 계산 (각 offset에서 oracle + poly 동시 조회)
        series = compute_gap_series(
            conn, evt_id, game_key, market_type, move_ts, oracle_fair1
        )

        # Poly 데이터 존재 여부
        has_poly = any(s["poly_price"] is not None for s in series)
        if has_poly:
            stats["events_with_poly"] += 1

        # gap summary
        summary = compute_gap_summary(conn, evt_id, actionable_threshold)
        if summary.get("actionable"):
            stats["actionable_count"] += 1

        summary["move_event_id"] = evt_id
        summary["game_key"] = game_key
        summary["market_type"] = market_type
        stats["summaries"].append(summary)

    stats["skipped_line_mismatch"] = skipped_line
    return stats
