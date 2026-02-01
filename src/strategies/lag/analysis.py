"""Forward Test v2 analysis script.

Analyzes move_events_hi_res gap_t3s data to determine:
"After 3s delay, does gap >= 4%p remain executable?"

Moved from monitor/hi_res_analysis.py, now uses HiResRepo.
"""
from __future__ import annotations

import statistics
from datetime import datetime
from pathlib import Path

from src.db.connection import get_connection
from src.db.hi_res_repo import HiResRepo


def analyze_gap_t3s(events: list[dict]) -> dict:
    with_gap = [e for e in events if e["gap_t3s"] is not None]

    if not with_gap:
        return {"n": 0, "mean": None, "median": None, "actionable_rate": None, "verdict": "Insufficient data"}

    gaps = [e["gap_t3s"] for e in with_gap]
    actionable = [g for g in gaps if g >= 0.04]
    actionable_rate = len(actionable) / len(gaps)

    if actionable_rate >= 0.30:
        verdict = "A: Promising - gap_t3s >= 4%p in 30%+ of cases"
    elif actionable_rate >= 0.10:
        verdict = "C: Uncertain - weak signal (10-30%)"
    else:
        verdict = "B: Not viable - gap_t3s >= 4%p in <10% of cases"

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
    results = {}
    for mtype in ["h2h", "totals", "spreads", "moneyline", "total", "spread"]:
        mtype_events = [e for e in events if e["market_type"] == mtype]
        if mtype_events:
            results[mtype] = analyze_gap_t3s(mtype_events)
    return results


def analyze_by_trigger(events: list[dict]) -> dict:
    results = {}
    for source in ["oracle_move", "poly_anomaly"]:
        source_events = [e for e in events if e["trigger_source"] == source]
        if source_events:
            results[source] = analyze_gap_t3s(source_events)
    return results


def analyze_gap_decay(events: list[dict]) -> dict:
    complete = [
        e for e in events
        if all(e.get(f"gap_t{t}") is not None for t in ["0", "3s", "10s", "30s"])
    ]

    if not complete:
        return {"n": 0, "decay_rates": None}

    def mean_gap(evts, field):
        return statistics.mean(e[field] for e in evts)

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


def print_report(events: list[dict]) -> None:
    print("=" * 70)
    print("Forward Test v2 Analysis Report")
    print("=" * 70)
    print(f"Generated: {datetime.now().isoformat()}")
    print()

    print("## 1. Data Summary")
    print("-" * 50)
    print(f"  Total events:  {len(events)}")

    by_trigger: dict[str, int] = {}
    for e in events:
        src = e["trigger_source"] or "unknown"
        by_trigger[src] = by_trigger.get(src, 0) + 1
    for src, cnt in by_trigger.items():
        print(f"    {src}: {cnt}")

    by_market: dict[str, int] = {}
    for e in events:
        mt = e["market_type"] or "unknown"
        by_market[mt] = by_market.get(mt, 0) + 1
    for mt, cnt in by_market.items():
        print(f"    {mt}: {cnt}")
    print()

    print("## 2. gap_t3s Analysis (Key Metric)")
    print("-" * 50)
    analysis = analyze_gap_t3s(events)

    if analysis["n"] == 0:
        print("  Insufficient data")
    else:
        print(f"  Samples:       {analysis['n']}")
        print(f"  Mean gap:      {analysis['mean']*100:.1f}%p")
        print(f"  Median gap:    {analysis['median']*100:.1f}%p")
        print(f"  Std:           {analysis['std']*100:.1f}%p")
        print(f"  Min/Max:       {analysis['min']*100:.1f}%p / {analysis['max']*100:.1f}%p")
        print()
        print(f"  Actionable (>=4%p): {analysis['actionable_count']}/{analysis['n']} "
              f"({analysis['actionable_rate']*100:.1f}%)")
        print()
        print(f"  ** Verdict: {analysis['verdict']} **")
    print()

    print("## 3. By Market Type")
    print("-" * 50)
    for mtype, a in analyze_by_market(events).items():
        if a["n"] > 0:
            print(f"  [{mtype}] N={a['n']}, Mean={a['mean']*100:.1f}%p, "
                  f"Actionable={a['actionable_rate']*100:.1f}%")
    print()

    print("## 4. By Trigger Source")
    print("-" * 50)
    for src, a in analyze_by_trigger(events).items():
        if a["n"] > 0:
            print(f"  [{src}] N={a['n']}, Mean={a['mean']*100:.1f}%p, "
                  f"Actionable={a['actionable_rate']*100:.1f}%")
    print()

    print("## 5. Gap Decay (t0 -> t30s)")
    print("-" * 50)
    decay = analyze_gap_decay(events)
    if decay["n"] > 0:
        print(f"  Complete data:  {decay['n']}")
        print(f"  Mean gap_t0:   {decay['mean_gap_t0']*100:.1f}%p")
        print(f"  Mean gap_t3s:  {decay['mean_gap_t3s']*100:.1f}%p")
        print(f"  Mean gap_t10s: {decay['mean_gap_t10s']*100:.1f}%p")
        print(f"  Mean gap_t30s: {decay['mean_gap_t30s']*100:.1f}%p")
        print()
        print(f"  Decay t0->t3s:  {decay['decay_t0_to_t3s']*100:.1f}%")
        print(f"  Decay t3s->t10s: {decay['decay_t3s_to_t10s']*100:.1f}%")
    else:
        print("  Insufficient complete data")
    print()

    print("=" * 70)
    print("## 6. Conclusion")
    print("=" * 70)
    if analysis["n"] < 5:
        print("  >> Insufficient data - extend test period")
    else:
        print(f"  >> {analysis['verdict']}")
        print()
        if analysis["actionable_rate"] >= 0.30:
            print("  Next: Bot development + live trading test")
        elif analysis["actionable_rate"] >= 0.10:
            print("  Next: More data collection or threshold adjustment")
        else:
            print("  Next: Strategy review or explore other markets")
    print()


def main(db_path: Path) -> None:
    if not db_path.exists():
        print(f"DB not found: {db_path}")
        return

    conn = get_connection(db_path)
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='move_events_hi_res'"
    ).fetchone()

    if not tables:
        print("move_events_hi_res table not found. Run Forward Test v2 first.")
        conn.close()
        return

    repo = HiResRepo(conn)
    events = repo.load_all_events()

    if not events:
        print("No data in move_events_hi_res.")
        conn.close()
        return

    print_report(events)
    conn.close()
