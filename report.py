"""
모니터링 데이터 리포트

수집된 스냅샷/트리거/봇 거래를 분석해서
가설 검증 결과를 출력한다.
"""

import sqlite3
from pathlib import Path
from datetime import datetime, timezone
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

DB_PATH = Path(__file__).parent / "data" / "snapshots.db"


def report():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    et = ZoneInfo("America/New_York")
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    now_et = datetime.now(et).strftime("%Y-%m-%d %H:%M ET")
    print(f"Pinnacle-Polymarket Monitor Report")
    print(f"시간: {now_et} ({now_utc})")
    print(f"{'='*70}\n")

    # 1. 수집 현황
    pin_count = conn.execute("SELECT COUNT(*) FROM pinnacle_snapshots").fetchone()[0]
    poly_count = conn.execute("SELECT COUNT(*) FROM poly_snapshots").fetchone()[0]
    game_count = conn.execute("SELECT COUNT(*) FROM game_mapping").fetchone()[0]
    trigger_count = conn.execute("SELECT COUNT(*) FROM triggers").fetchone()[0]
    bot_count = conn.execute("SELECT COUNT(*) FROM bot_trades").fetchone()[0]

    print(f"[수집 현황]")
    print(f"  경기: {game_count} | Pinnacle 스냅샷: {pin_count} | "
          f"Poly 스냅샷: {poly_count}")
    print(f"  트리거: {trigger_count} | 봇 거래: {bot_count}")

    # 2. 경기 매핑 현황
    mapped = conn.execute(
        "SELECT COUNT(*) FROM game_mapping WHERE poly_event_found = 1"
    ).fetchone()[0]
    print(f"\n[경기 매핑]")
    print(f"  전체: {game_count} | Polymarket 매칭: {mapped}")

    games = conn.execute("""
        SELECT away_team, home_team, commence_time, poly_event_slug, poly_event_found
        FROM game_mapping ORDER BY commence_time
    """).fetchall()
    for g in games:
        status = "OK" if g["poly_event_found"] else "NO MATCH"
        print(f"  {g['away_team'][:3].upper()} @ {g['home_team'][:3].upper()} "
              f"({g['commence_time'][:16]}) → {g['poly_event_slug'] or 'N/A'} [{status}]")

    # 3. Pinnacle 라인 변동 이력
    print(f"\n[Pinnacle 라인 변동]")
    games_with_snaps = conn.execute("""
        SELECT DISTINCT p.game_id, g.away_team, g.home_team
        FROM pinnacle_snapshots p
        JOIN game_mapping g ON g.odds_api_id = p.game_id
    """).fetchall()

    for gs in games_with_snaps:
        snaps = conn.execute("""
            SELECT snapshot_time, total_line, over_implied, under_implied
            FROM pinnacle_snapshots
            WHERE game_id = ?
            ORDER BY snapshot_time
        """, (gs["game_id"],)).fetchall()

        if len(snaps) < 2:
            continue

        away = gs["away_team"][:3].upper()
        home = gs["home_team"][:3].upper()
        first = snaps[0]
        last = snaps[-1]
        delta = (last["total_line"] or 0) - (first["total_line"] or 0)

        if abs(delta) >= 0.5:
            print(f"  {away}@{home}: {first['total_line']} → {last['total_line']} "
                  f"({delta:+.1f}) [{len(snaps)} snaps]")
            for s in snaps:
                t = s["snapshot_time"][11:16]
                imp_u = f"{s['under_implied']:.1%}" if s["under_implied"] else "N/A"
                print(f"    {t} UTC: O/U {s['total_line']}  Under imp={imp_u}")

    # 4. 트리거 이벤트 상세
    print(f"\n[트리거 이벤트] ({trigger_count}건)")
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
        print(f"    라인: {tr['prev_line']} → {tr['new_line']} (Δ{tr['delta_line']:+.1f})")
        print(f"    Under implied: {tr['prev_under_implied']:.1%} → "
              f"{tr['new_under_implied']:.1%} (Δ{tr['delta_under_implied']:+.1%})")

        if tr["poly_under_price"] is not None:
            gap_str = f" | Gap: {tr['poly_gap_under']:+.1%}" if tr["poly_gap_under"] else ""
            print(f"    Poly Under: {tr['poly_under_price']:.3f}{gap_str}")

        if tr["bot_entered"]:
            print(f"    ** 봇 진입: {tr['bot_entry_side']} @ {tr['bot_entry_price']:.3f} "
                  f"({tr['bot_entry_time']})")

        if tr["lag_seconds"]:
            print(f"    갭 수렴 시간: {tr['lag_seconds']}초")

    # 5. 봇 거래 요약
    if bot_count > 0:
        print(f"\n[봇 거래] (최근 20건)")
        bot_trades = conn.execute("""
            SELECT * FROM bot_trades ORDER BY trade_time DESC LIMIT 20
        """).fetchall()
        for bt in bot_trades:
            print(f"  {bt['trade_time'][:19]} {bt['poly_market_slug'][:40]:<40} "
                  f"{bt['outcome']:<10} {bt['side']:<4} "
                  f"${bt['size']:>10,.2f} @ {bt['price']:.3f}")

    # 6. 가설 검증 요약
    print(f"\n{'='*70}")
    print(f"[가설 검증 요약]")
    if trigger_count == 0:
        print(f"  아직 트리거 없음. 데이터 수집 중...")
        print(f"  임계값: |Δline| >= 1.5pt 또는 |Δimplied| >= 6%p")
    else:
        # 트리거 후 봇 진입 비율
        bot_entered = conn.execute(
            "SELECT COUNT(*) FROM triggers WHERE bot_entered = 1"
        ).fetchone()[0]
        print(f"  트리거 {trigger_count}건 중 봇 진입: {bot_entered}건 "
              f"({bot_entered/trigger_count*100:.0f}%)")

        # 평균 갭
        avg_gap = conn.execute(
            "SELECT AVG(ABS(poly_gap_under)) FROM triggers WHERE poly_gap_under IS NOT NULL"
        ).fetchone()[0]
        if avg_gap:
            print(f"  평균 Under 갭: {avg_gap:.1%}")

        # 평균 지연시간
        avg_lag = conn.execute(
            "SELECT AVG(lag_seconds) FROM triggers WHERE lag_seconds IS NOT NULL"
        ).fetchone()[0]
        if avg_lag:
            print(f"  평균 갭 수렴 시간: {avg_lag:.0f}초")

    conn.close()


if __name__ == "__main__":
    report()
