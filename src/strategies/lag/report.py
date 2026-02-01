"""Monitoring data report generator.

Moved from monitor/report.py, now uses db repos and config.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from datetime import datetime, timezone

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

from src.db.connection import get_row_connection


def report(db_path: Path) -> None:
    """Print analysis report from collected snapshots/triggers/bot trades."""
    conn = get_row_connection(db_path)
    et = ZoneInfo("America/New_York")
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    now_et = datetime.now(et).strftime("%Y-%m-%d %H:%M ET")
    print(f"Pinnacle-Polymarket Monitor Report")
    print(f"Time: {now_et} ({now_utc})")
    print(f"{'='*70}\n")

    # 1. Collection stats
    pin_count = conn.execute("SELECT COUNT(*) FROM pinnacle_snapshots").fetchone()[0]
    poly_count = conn.execute("SELECT COUNT(*) FROM poly_snapshots").fetchone()[0]
    game_count = conn.execute("SELECT COUNT(*) FROM game_mapping").fetchone()[0]
    trigger_count = conn.execute("SELECT COUNT(*) FROM triggers").fetchone()[0]
    bot_count = conn.execute("SELECT COUNT(*) FROM bot_trades").fetchone()[0]

    poly_by_type = {}
    try:
        for row in conn.execute(
            "SELECT COALESCE(market_type, 'total') as mt, COUNT(*) FROM poly_snapshots GROUP BY mt"
        ).fetchall():
            poly_by_type[row[0]] = row[1]
    except Exception:
        poly_by_type["total"] = poly_count
    poly_type_str = " | ".join(f"{k}: {v}" for k, v in sorted(poly_by_type.items()))

    print(f"[Collection Stats]")
    print(f"  Games: {game_count} | Pinnacle snaps: {pin_count} | Poly snaps: {poly_count}")
    print(f"  Poly markets: {poly_type_str}")
    print(f"  Triggers: {trigger_count} | Bot trades: {bot_count}")

    # 2. Game mapping
    mapped = conn.execute(
        "SELECT COUNT(*) FROM game_mapping WHERE poly_event_found = 1"
    ).fetchone()[0]
    print(f"\n[Game Mapping]")
    print(f"  Total: {game_count} | Polymarket matched: {mapped}")

    games = conn.execute("""
        SELECT away_team, home_team, commence_time, poly_event_slug, poly_event_found
        FROM game_mapping ORDER BY commence_time
    """).fetchall()
    for g in games:
        status = "OK" if g["poly_event_found"] else "NO MATCH"
        print(f"  {g['away_team'][:3].upper()} @ {g['home_team'][:3].upper()} "
              f"({g['commence_time'][:16]}) -> {g['poly_event_slug'] or 'N/A'} [{status}]")

    # 3. Pinnacle line history
    print(f"\n[Pinnacle Line Moves]")
    games_with_snaps = conn.execute("""
        SELECT DISTINCT p.game_id, g.away_team, g.home_team
        FROM pinnacle_snapshots p
        JOIN game_mapping g ON g.odds_api_id = p.game_id
    """).fetchall()

    for gs in games_with_snaps:
        snaps = conn.execute("""
            SELECT snapshot_time, total_line, over_implied, under_implied
            FROM pinnacle_snapshots WHERE game_id = ? ORDER BY snapshot_time
        """, (gs["game_id"],)).fetchall()

        if len(snaps) < 2:
            continue

        away = gs["away_team"][:3].upper()
        home = gs["home_team"][:3].upper()
        first, last = snaps[0], snaps[-1]
        delta = (last["total_line"] or 0) - (first["total_line"] or 0)

        if abs(delta) >= 0.5:
            print(f"  {away}@{home}: {first['total_line']} -> {last['total_line']} "
                  f"({delta:+.1f}) [{len(snaps)} snaps]")
            for s in snaps:
                t = s["snapshot_time"][11:16]
                imp_u = f"{s['under_implied']:.1%}" if s["under_implied"] else "N/A"
                print(f"    {t} UTC: O/U {s['total_line']}  Under imp={imp_u}")

    # 4. Trigger details
    print(f"\n[Trigger Events] ({trigger_count})")
    triggers = conn.execute("""
        SELECT t.*, g.away_team, g.home_team
        FROM triggers t
        JOIN game_mapping g ON g.odds_api_id = t.game_id
        ORDER BY t.trigger_time
    """).fetchall()

    for tr in triggers:
        away = tr["away_team"][:3].upper()
        home = tr["home_team"][:3].upper()
        time_str = tr["trigger_time"][11:19]
        print(f"\n  {away}@{home} ({time_str} UTC) [{tr['trigger_type']}]")
        print(f"    Line: {tr['prev_line']} -> {tr['new_line']} (d{tr['delta_line']:+.1f})")
        print(f"    Under implied: {tr['prev_under_implied']:.1%} -> "
              f"{tr['new_under_implied']:.1%} (d{tr['delta_under_implied']:+.1%})")

        if tr["poly_under_price"] is not None:
            gap_str = f" | Gap: {tr['poly_gap_under']:+.1%}" if tr["poly_gap_under"] else ""
            print(f"    Poly Under: {tr['poly_under_price']:.3f}{gap_str}")

        if tr["bot_entered"]:
            print(f"    ** Bot entry: {tr['bot_entry_side']} @ {tr['bot_entry_price']:.3f} "
                  f"({tr['bot_entry_time']})")

        if tr["lag_seconds"]:
            print(f"    Gap convergence: {tr['lag_seconds']}s")

    # 5. Bot trade summary
    if bot_count > 0:
        print(f"\n[Bot Trade Analysis]")
        bot_slugs = conn.execute("SELECT poly_market_slug, side, size FROM bot_trades").fetchall()
        type_stats = {"total": 0, "spread": 0, "moneyline": 0, "other": 0}
        type_volume = {"total": 0.0, "spread": 0.0, "moneyline": 0.0, "other": 0.0}
        for bs in bot_slugs:
            slug = bs["poly_market_slug"] or ""
            if "total" in slug or "o-u" in slug:
                t = "total"
            elif "spread" in slug:
                t = "spread"
            elif slug.count("-") <= 4 and "nba-" in slug:
                t = "moneyline"
            else:
                t = "other"
            type_stats[t] += 1
            type_volume[t] += bs["size"] or 0

        for t in ["total", "spread", "moneyline", "other"]:
            if type_stats[t] > 0:
                print(f"  {t:>10}: {type_stats[t]:>4} trades  ${type_volume[t]:>12,.2f}")

        print(f"\n  Last 20 trades:")
        bot_trades = conn.execute(
            "SELECT * FROM bot_trades ORDER BY trade_time DESC LIMIT 20"
        ).fetchall()
        for bt in bot_trades:
            slug = bt["poly_market_slug"] or ""
            if "spread" in slug:
                mtype = "[SPR]"
            elif "total" in slug or "o-u" in slug:
                mtype = "[TOT]"
            else:
                mtype = "[ML] "
            print(f"  {bt['trade_time'][:19]} {mtype} {slug[:38]:<38} "
                  f"{bt['side']:<4} ${bt['size']:>10,.2f} @ {bt['price']:.3f}")

    # 6. Hypothesis summary
    print(f"\n{'='*70}")
    print(f"[Hypothesis Summary]")
    if trigger_count == 0:
        print(f"  No triggers yet. Collecting data...")
        print(f"  Thresholds: |dline| >= 1.5pt or |dimplied| >= 6%p")
    else:
        bot_entered = conn.execute(
            "SELECT COUNT(*) FROM triggers WHERE bot_entered = 1"
        ).fetchone()[0]
        print(f"  Triggers: {trigger_count}, bot entries: {bot_entered} "
              f"({bot_entered/trigger_count*100:.0f}%)")

        avg_gap = conn.execute(
            "SELECT AVG(ABS(poly_gap_under)) FROM triggers WHERE poly_gap_under IS NOT NULL"
        ).fetchone()[0]
        if avg_gap:
            print(f"  Avg under gap: {avg_gap:.1%}")

        avg_lag = conn.execute(
            "SELECT AVG(lag_seconds) FROM triggers WHERE lag_seconds IS NOT NULL"
        ).fetchone()[0]
        if avg_lag:
            print(f"  Avg gap convergence: {avg_lag:.0f}s")

    conn.close()
