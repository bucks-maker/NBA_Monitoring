"""
백테스트 리포트 생성

gap_summary 데이터를 분석하여:
1. 트리거 이벤트 수
2. gap_0m/gap_5m/gap_10m 분포
3. half-life 분포
4. actionable 이벤트 빈도
5. 최종 결론 (가능/불가/불확실)
"""
from __future__ import annotations

import sqlite3
import statistics
from typing import Optional


def _percentile(data: list[float], p: float) -> float:
    """p-percentile (0~100)"""
    if not data:
        return 0.0
    sorted_data = sorted(data)
    k = (len(sorted_data) - 1) * p / 100
    f = int(k)
    c = f + 1
    if c >= len(sorted_data):
        return sorted_data[f]
    return sorted_data[f] + (k - f) * (sorted_data[c] - sorted_data[f])


def _format_pct(v: Optional[float]) -> str:
    """값을 %p 형식으로 포맷"""
    if v is None:
        return "N/A"
    return f"{v * 100:.1f}%p"


def generate_report(conn: sqlite3.Connection) -> str:
    """
    백테스트 리포트 문자열 생성

    Returns:
        마크다운 형식 리포트
    """
    lines = []
    lines.append("=" * 70)
    lines.append("BACKTEST REPORT: Oracle Move → Polymarket Gap Analysis")
    lines.append("=" * 70)

    # ── 1. 데이터 개요 ──
    oracle_count = conn.execute("SELECT COUNT(*) FROM oracle_snapshots").fetchone()[0]
    move_count = conn.execute("SELECT COUNT(*) FROM move_events").fetchone()[0]
    poly_count = conn.execute("SELECT COUNT(*) FROM poly_prices").fetchone()[0]
    mapping_count = conn.execute("SELECT COUNT(*) FROM market_mapping").fetchone()[0]
    gap_count = conn.execute("SELECT COUNT(*) FROM gap_summary").fetchone()[0]

    lines.append(f"\n## 1. 데이터 개요")
    lines.append(f"  Oracle 스냅샷: {oracle_count}")
    lines.append(f"  Move 이벤트:   {move_count}")
    lines.append(f"  Poly 가격:     {poly_count}")
    lines.append(f"  마켓 매핑:     {mapping_count}")
    lines.append(f"  갭 분석:       {gap_count}")

    if move_count == 0:
        lines.append(f"\n⚠️  Move 이벤트가 0건입니다. 임계값을 낮추거나 데이터 기간을 늘리세요.")
        return "\n".join(lines)

    # ── 2. 트리거 이벤트 분포 ──
    lines.append(f"\n## 2. 트리거 이벤트 분포")

    by_market = conn.execute("""
        SELECT market_type, COUNT(*), AVG(ABS(delta_value))
        FROM move_events
        GROUP BY market_type
    """).fetchall()

    lines.append(f"  {'Market':<12} {'Count':>6} {'Avg |Δ|':>10}")
    lines.append(f"  {'-'*12} {'-'*6} {'-'*10}")
    for mtype, cnt, avg_d in by_market:
        lines.append(f"  {mtype:<12} {cnt:>6} {_format_pct(avg_d):>10}")

    by_metric = conn.execute("""
        SELECT metric, COUNT(*)
        FROM move_events
        GROUP BY metric
    """).fetchall()

    lines.append(f"\n  By metric: {dict(by_metric)}")

    # ── 3. 갭 분포 (마켓별) ──
    lines.append(f"\n## 3. 갭 분포 (확률 gap, %p)")
    lines.append(f"  ※ totals/spreads: Oracle 라인 ±1.5pt 이내만 포함")

    summaries_all = conn.execute("""
        SELECT gs.gap_0m, gs.gap_5m, gs.gap_10m, gs.gap_30m, gs.gap_60m,
               gs.half_life_sec, gs.actionable, me.market_type
        FROM gap_summary gs
        JOIN move_events me ON me.id = gs.move_event_id
    """).fetchall()

    if not summaries_all:
        lines.append(f"  (갭 데이터 없음)")
        return "\n".join(lines)

    # 마켓별 + 전체 분석
    market_groups = {"ALL": summaries_all}
    for s in summaries_all:
        mt = s[7]
        if mt not in market_groups:
            market_groups[mt] = []
        market_groups[mt].append(s)

    for group_name in ["ALL", "h2h", "totals", "spreads"]:
        sums = market_groups.get(group_name, [])
        if not sums:
            continue

        label = f"[{group_name}]" if group_name != "ALL" else "[전체]"
        lines.append(f"\n  --- {label} (N={len(sums)}) ---")

        gap_metrics = {
            "gap_0m": [s[0] for s in sums if s[0] is not None],
            "gap_5m": [s[1] for s in sums if s[1] is not None],
            "gap_10m": [s[2] for s in sums if s[2] is not None],
            "gap_30m": [s[3] for s in sums if s[3] is not None],
            "gap_60m": [s[4] for s in sums if s[4] is not None],
        }

        lines.append(f"  {'Metric':<10} {'N':>4} {'Mean':>8} {'Median':>8} {'P75':>8} {'P90':>8} {'Max':>8}")
        lines.append(f"  {'-'*10} {'-'*4} {'-'*8} {'-'*8} {'-'*8} {'-'*8} {'-'*8}")

        for name, values in gap_metrics.items():
            if not values:
                lines.append(f"  {name:<10} {0:>4} {'N/A':>8} {'N/A':>8} {'N/A':>8} {'N/A':>8} {'N/A':>8}")
                continue
            mean = statistics.mean(values)
            median = statistics.median(values)
            p75 = _percentile(values, 75)
            p90 = _percentile(values, 90)
            mx = max(values)
            lines.append(
                f"  {name:<10} {len(values):>4} "
                f"{_format_pct(mean):>8} {_format_pct(median):>8} "
                f"{_format_pct(p75):>8} {_format_pct(p90):>8} {_format_pct(mx):>8}"
            )

    # ── 4. Gap Decay 분석 (t=0 vs t=30m, Oracle 스냅샷 경계) ──
    lines.append(f"\n## 4. Gap Decay 분석")
    lines.append(f"  Oracle 스냅샷 간격: 30분 → t=5m/t=10m은 Oracle 갱신 없음 (stale)")
    lines.append(f"  유의미한 비교: t=0 vs t=30m (다음 Oracle 스냅샷)")

    decay_rows = conn.execute("""
        SELECT gs0.gap_abs, gs30.gap_abs,
               gs0.poly_price, gs30.poly_price,
               gs0.oracle_implied, gs30.oracle_implied
        FROM gap_series gs0
        JOIN gap_series gs30 ON gs30.move_event_id = gs0.move_event_id AND gs30.ts_offset_sec = 1800
        JOIN move_events me ON me.id = gs0.move_event_id
        WHERE gs0.ts_offset_sec = 0
          AND me.market_type = 'h2h'
          AND gs0.poly_price IS NOT NULL AND gs30.poly_price IS NOT NULL
          AND gs0.poly_price >= 0.05 AND gs0.poly_price <= 0.95
          AND gs0.oracle_implied IS NOT NULL AND gs30.oracle_implied IS NOT NULL
    """).fetchall()

    if decay_rows:
        converged = sum(1 for r in decay_rows if r[1] < r[0])
        diverged = sum(1 for r in decay_rows if r[1] > r[0])
        n = len(decay_rows)
        lines.append(f"\n  h2h 비해소 경기 (Poly 5-95%): {n}건")
        lines.append(f"  Gap 수렴 (t30m < t0m): {converged} ({converged/n:.0%})")
        lines.append(f"  Gap 발산 (t30m > t0m): {diverged} ({diverged/n:.0%})")

        # Poly convergence toward Oracle@t0
        toward = sum(1 for r in decay_rows if abs(r[3] - r[4]) < abs(r[2] - r[4]))
        lines.append(f"  Poly→Oracle@t0 수렴:   {toward} ({toward/n:.0%})")
        lines.append(f"  Poly→Oracle@t0 발산:   {n-toward} ({(n-toward)/n:.0%})")
    else:
        lines.append(f"  (decay 데이터 없음)")

    # ── 5. Half-life 분포 ──
    lines.append(f"\n## 5. Gap Half-life (초)")
    lines.append(f"  ⚠️ Oracle 30분 간격 → t=5m/t=10m half-life는 비신뢰")

    half_lives = [s[5] for s in summaries_all if s[5] is not None]
    if half_lives:
        lines.append(f"  N:      {len(half_lives)}")
        lines.append(f"  Mean:   {statistics.mean(half_lives):.0f}s")
        lines.append(f"  Median: {statistics.median(half_lives):.0f}s")
    else:
        lines.append(f"  (half-life 측정 불가 — 갭이 측정 윈도우 내에서 줄지 않음)")

    # ── 6. Actionable 이벤트 ──
    lines.append(f"\n## 6. Actionable 이벤트 (gap_5m >= 4%p)")
    lines.append(f"  ⚠️ gap_5m은 stale Oracle 기준 → 해석 주의")

    actionable_count = sum(1 for s in summaries_all if s[6] == 1)
    total_with_gap5m = len([s for s in summaries_all if s[1] is not None])

    lines.append(f"  총 move events:         {move_count}")
    lines.append(f"  gap_5m 측정 가능:       {total_with_gap5m}")
    lines.append(f"  Actionable (>=4%p):     {actionable_count}")
    if total_with_gap5m > 0:
        lines.append(f"  Actionable 비율:        {actionable_count/total_with_gap5m:.1%}")

    # 기간으로 빈도 추정
    ts_range = conn.execute("""
        SELECT MIN(move_ts_unix), MAX(move_ts_unix)
        FROM move_events
    """).fetchone()

    if ts_range[0] and ts_range[1]:
        days = max(1, (ts_range[1] - ts_range[0]) / 86400)
        lines.append(f"\n  데이터 기간:            {days:.1f}일")
        lines.append(f"  하루 평균 move:         {move_count / days:.1f}")

    # ── 7. 결론 ──
    lines.append(f"\n## 7. 결론")
    lines.append(f"{'='*70}")

    # Use decay analysis for conclusion
    if not decay_rows or len(decay_rows) < 5:
        lines.append(f"  판정: ⚠️  데이터 부족 (Poly 매핑 경기 수 부족)")
        lines.append(f"  → 더 많은 경기 데이터 필요 (Poly 마켓 커버리지 확대).")
    else:
        converge_pct = converged / len(decay_rows) if decay_rows else 0
        toward_pct = toward / len(decay_rows) if decay_rows else 0

        lines.append(f"  1. 갭 존재 확인: h2h gap_0m median = {_format_pct(statistics.median([r[0] for r in decay_rows]))}")
        lines.append(f"  2. 갭 수렴 여부: {converge_pct:.0%} 수렴 / {1-converge_pct:.0%} 발산")
        lines.append(f"  3. Poly→Oracle 추종: {toward_pct:.0%} 수렴 / {1-toward_pct:.0%} 발산")
        lines.append(f"")

        if toward_pct >= 0.6:
            lines.append(f"  판정: ✅ 유망 — Poly가 Oracle을 체계적으로 추종")
            lines.append(f"  → 실시간 모니터링(WebSocket)으로 gap_3s 전방 측정 권장.")
        elif toward_pct >= 0.4:
            lines.append(f"  판정: ⚠️  불확실 — 수렴/발산 비율이 유의미한 차이 없음")
            lines.append(f"  → Oracle 30분 간격으로는 결론 불가.")
            lines.append(f"  → 한계: 라인 무브의 실제 시점을 알 수 없음 (±30분 오차).")
            lines.append(f"  → 실시간 모니터링으로 정밀 측정 필요.")
        else:
            lines.append(f"  판정: ❌ 가설 불성립 — Poly가 Oracle을 추종하지 않음")
            lines.append(f"  → 갭은 존재하나 '지연(lag)'이 아닌 구조적 차이일 가능성.")
            lines.append(f"  → 인게임 동학이 지배적 → 양 시장이 독립적으로 반응.")

        lines.append(f"")
        lines.append(f"  핵심 한계:")
        lines.append(f"  → Oracle 30분 간격: 실제 무브 시점 ±30분 오차")
        lines.append(f"  → Poly 1분 간격: 초 단위 반응 측정 불가")
        lines.append(f"  → 데이터 대부분 인게임: 프리게임 무브 거의 없음")
        lines.append(f"  → 가설 검증에는 초 단위 실시간 데이터 필요")

    lines.append(f"\n{'='*70}")
    return "\n".join(lines)


def print_report(conn: sqlite3.Connection):
    """리포트 출력"""
    report = generate_report(conn)
    print(report)
    return report
