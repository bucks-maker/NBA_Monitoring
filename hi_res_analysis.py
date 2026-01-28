"""
Forward Test v2 분석 스크립트

move_events_hi_res 테이블의 gap_t3s 데이터를 분석하여
"3초 딜레이 이후에도 gap >= 4%p가 남아 체결 가능한가?" 결론 도출
"""
import sqlite3
import statistics
from pathlib import Path
from datetime import datetime

DB_PATH = Path(__file__).parent / "data" / "snapshots.db"


def load_hi_res_events(conn: sqlite3.Connection) -> list[dict]:
    """move_events_hi_res 데이터 로드"""
    rows = conn.execute("""
        SELECT
            id, game_key, market_type, poly_line, oracle_line,
            move_ts_unix, oracle_prev_implied, oracle_new_implied, oracle_delta,
            poly_t0, poly_t3s, poly_t10s, poly_t30s,
            gap_t0, gap_t3s, gap_t10s, gap_t30s,
            depth_t0, spread_t0, trigger_source, outcome_name
        FROM move_events_hi_res
        ORDER BY move_ts_unix
    """).fetchall()

    columns = [
        "id", "game_key", "market_type", "poly_line", "oracle_line",
        "move_ts_unix", "oracle_prev_implied", "oracle_new_implied", "oracle_delta",
        "poly_t0", "poly_t3s", "poly_t10s", "poly_t30s",
        "gap_t0", "gap_t3s", "gap_t10s", "gap_t30s",
        "depth_t0", "spread_t0", "trigger_source", "outcome_name"
    ]

    return [dict(zip(columns, row)) for row in rows]


def analyze_gap_t3s(events: list[dict]) -> dict:
    """gap_t3s 분석 (핵심 지표)"""
    # gap_t3s가 있는 이벤트만
    with_gap = [e for e in events if e["gap_t3s"] is not None]

    if not with_gap:
        return {
            "n": 0,
            "mean": None,
            "median": None,
            "actionable_rate": None,
            "verdict": "데이터 부족",
        }

    gaps = [e["gap_t3s"] for e in with_gap]

    # 4%p 이상 비율 (actionable)
    actionable = [g for g in gaps if g >= 0.04]
    actionable_rate = len(actionable) / len(gaps)

    # 판정
    if actionable_rate >= 0.30:
        verdict = "A: 유망 - gap_t3s >= 4%p가 30% 이상"
    elif actionable_rate >= 0.10:
        verdict = "C: 불확실 - 약한 신호 (10-30%)"
    else:
        verdict = "B: 전략 불가 - gap_t3s >= 4%p가 10% 미만"

    return {
        "n": len(gaps),
        "mean": statistics.mean(gaps),
        "median": statistics.median(gaps),
        "std": statistics.stdev(gaps) if len(gaps) > 1 else 0,
        "min": min(gaps),
        "max": max(gaps),
        "actionable_count": len(actionable),
        "actionable_rate": actionable_rate,
        "verdict": verdict,
    }


def analyze_by_market(events: list[dict]) -> dict:
    """마켓 타입별 분석"""
    results = {}

    for mtype in ["h2h", "totals", "spreads", "moneyline", "total", "spread"]:
        mtype_events = [e for e in events if e["market_type"] == mtype]
        if mtype_events:
            results[mtype] = analyze_gap_t3s(mtype_events)

    return results


def analyze_by_trigger(events: list[dict]) -> dict:
    """트리거 소스별 분석"""
    results = {}

    for source in ["oracle_move", "poly_anomaly"]:
        source_events = [e for e in events if e["trigger_source"] == source]
        if source_events:
            results[source] = analyze_gap_t3s(source_events)

    return results


def analyze_gap_decay(events: list[dict]) -> dict:
    """gap decay 분석 (t0 → t3s → t10s → t30s)"""
    # 모든 시점의 gap이 있는 이벤트만
    complete = [
        e for e in events
        if all(e[f"gap_t{t}"] is not None for t in ["0", "3s", "10s", "30s"])
    ]

    if not complete:
        return {"n": 0, "decay_rates": None}

    def mean_gap(events, field):
        return statistics.mean(e[field] for e in events)

    t0 = mean_gap(complete, "gap_t0")
    t3 = mean_gap(complete, "gap_t3s")
    t10 = mean_gap(complete, "gap_t10s")
    t30 = mean_gap(complete, "gap_t30s")

    return {
        "n": len(complete),
        "mean_gap_t0": t0,
        "mean_gap_t3s": t3,
        "mean_gap_t10s": t10,
        "mean_gap_t30s": t30,
        "decay_t0_to_t3s": (t0 - t3) / t0 if t0 > 0 else 0,
        "decay_t3s_to_t10s": (t3 - t10) / t3 if t3 > 0 else 0,
        "decay_t10s_to_t30s": (t10 - t30) / t10 if t10 > 0 else 0,
    }


def print_report(events: list[dict]):
    """분석 리포트 출력"""
    print("=" * 70)
    print("Forward Test v2 분석 리포트")
    print("=" * 70)
    print(f"생성 시각: {datetime.now().isoformat()}")
    print()

    # 1. 데이터 요약
    print("## 1. 데이터 요약")
    print("-" * 50)
    print(f"  총 이벤트:     {len(events)}")

    by_trigger = {}
    for e in events:
        src = e["trigger_source"] or "unknown"
        by_trigger[src] = by_trigger.get(src, 0) + 1
    for src, cnt in by_trigger.items():
        print(f"    {src}: {cnt}")

    by_market = {}
    for e in events:
        mt = e["market_type"] or "unknown"
        by_market[mt] = by_market.get(mt, 0) + 1
    for mt, cnt in by_market.items():
        print(f"    {mt}: {cnt}")
    print()

    # 2. 핵심 분석: gap_t3s
    print("## 2. gap_t3s 분석 (핵심 지표)")
    print("-" * 50)
    analysis = analyze_gap_t3s(events)

    if analysis["n"] == 0:
        print("  데이터 부족")
    else:
        print(f"  표본 수:       {analysis['n']}")
        print(f"  Mean gap:      {analysis['mean']*100:.1f}%p")
        print(f"  Median gap:    {analysis['median']*100:.1f}%p")
        print(f"  Std:           {analysis['std']*100:.1f}%p")
        print(f"  Min/Max:       {analysis['min']*100:.1f}%p / {analysis['max']*100:.1f}%p")
        print()
        print(f"  Actionable (>=4%p): {analysis['actionable_count']}/{analysis['n']} "
              f"({analysis['actionable_rate']*100:.1f}%)")
        print()
        print(f"  ** 판정: {analysis['verdict']} **")
    print()

    # 3. 마켓별 분석
    print("## 3. 마켓별 gap_t3s")
    print("-" * 50)
    by_market_analysis = analyze_by_market(events)
    for mtype, a in by_market_analysis.items():
        if a["n"] > 0:
            print(f"  [{mtype}] N={a['n']}, Mean={a['mean']*100:.1f}%p, "
                  f"Actionable={a['actionable_rate']*100:.1f}%")
    print()

    # 4. 트리거별 분석
    print("## 4. 트리거별 gap_t3s")
    print("-" * 50)
    by_trigger_analysis = analyze_by_trigger(events)
    for src, a in by_trigger_analysis.items():
        if a["n"] > 0:
            print(f"  [{src}] N={a['n']}, Mean={a['mean']*100:.1f}%p, "
                  f"Actionable={a['actionable_rate']*100:.1f}%")
    print()

    # 5. Gap Decay 분석
    print("## 5. Gap Decay (t0 → t30s)")
    print("-" * 50)
    decay = analyze_gap_decay(events)
    if decay["n"] > 0:
        print(f"  완전 데이터 수: {decay['n']}")
        print(f"  Mean gap_t0:   {decay['mean_gap_t0']*100:.1f}%p")
        print(f"  Mean gap_t3s:  {decay['mean_gap_t3s']*100:.1f}%p")
        print(f"  Mean gap_t10s: {decay['mean_gap_t10s']*100:.1f}%p")
        print(f"  Mean gap_t30s: {decay['mean_gap_t30s']*100:.1f}%p")
        print()
        print(f"  Decay t0→t3s:  {decay['decay_t0_to_t3s']*100:.1f}%")
        print(f"  Decay t3s→t10s: {decay['decay_t3s_to_t10s']*100:.1f}%")
    else:
        print("  완전 데이터 부족")
    print()

    # 6. 최종 결론
    print("=" * 70)
    print("## 6. 최종 결론")
    print("=" * 70)
    if analysis["n"] < 5:
        print("  ▶ 데이터 부족 - 테스트 연장 필요")
    else:
        print(f"  ▶ {analysis['verdict']}")
        print()
        if analysis["actionable_rate"] >= 0.30:
            print("  다음 단계: 봇 개발 및 실거래 테스트")
        elif analysis["actionable_rate"] >= 0.10:
            print("  다음 단계: 추가 데이터 수집 또는 임계값 조정")
        else:
            print("  다음 단계: 전략 재검토 또는 다른 마켓 탐색")
    print()


def main():
    if not DB_PATH.exists():
        print(f"DB not found: {DB_PATH}")
        return

    conn = sqlite3.connect(str(DB_PATH))

    # 테이블 존재 확인
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='move_events_hi_res'"
    ).fetchone()

    if not tables:
        print("move_events_hi_res 테이블이 없습니다. Forward Test v2를 먼저 실행하세요.")
        conn.close()
        return

    events = load_hi_res_events(conn)

    if not events:
        print("move_events_hi_res에 데이터가 없습니다.")
        conn.close()
        return

    print_report(events)

    conn.close()


if __name__ == "__main__":
    main()
