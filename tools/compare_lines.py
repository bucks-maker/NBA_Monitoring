"""
Pinnacle vs Polymarket NBA O/U 라인 비교기

Pinnacle Total 라인 (The Odds API) vs Polymarket NBA 개별 경기 Total 마켓 가격 비교
"""

import argparse
import json
import re
import httpx
from datetime import datetime, timezone, timedelta

# NBA 팀 풀네임 → Polymarket slug 약어
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

DISPLAY_ABBR = {v: v.upper() for v in FULL_TO_POLY_ABBR.values()}


def fetch_odds_api_totals(api_key: str) -> list[dict]:
    """The Odds API에서 Pinnacle NBA Total 라인 수집"""
    url = "https://api.the-odds-api.com/v4/sports/basketball_nba/odds"
    params = {
        "apiKey": api_key,
        "regions": "us",
        "markets": "totals",
        "bookmakers": "pinnacle",
        "oddsFormat": "decimal",
    }
    resp = httpx.get(url, params=params, timeout=15)
    resp.raise_for_status()

    remaining = resp.headers.get("x-requests-remaining", "?")
    used = resp.headers.get("x-requests-used", "?")
    print(f"[Odds API] 크레딧: {used} used / {remaining} remaining")

    results = []
    for game in resp.json():
        home = game["home_team"]
        away = game["away_team"]
        commence = game.get("commence_time", "")

        # UTC commence time → 미국 동부(ET) 날짜로 변환 (Polymarket slug 기준)
        game_date_utc = ""
        game_date_et = ""
        if commence:
            dt_utc = datetime.fromisoformat(commence.replace("Z", "+00:00"))
            game_date_utc = dt_utc.strftime("%Y-%m-%d")
            dt_et = dt_utc - timedelta(hours=5)  # EST (rough)
            game_date_et = dt_et.strftime("%Y-%m-%d")

        for bm in game.get("bookmakers", []):
            if bm["key"] != "pinnacle":
                continue
            for market in bm.get("markets", []):
                if market["key"] != "totals":
                    continue
                over_price = under_price = total_line = None
                for outcome in market["outcomes"]:
                    if outcome["name"] == "Over":
                        over_price = outcome["price"]
                        total_line = outcome["point"]
                    elif outcome["name"] == "Under":
                        under_price = outcome["price"]
                if total_line is not None:
                    results.append({
                        "home": home,
                        "away": away,
                        "date_utc": game_date_utc,
                        "date_et": game_date_et,
                        "line": total_line,
                        "over_price": over_price,
                        "under_price": under_price,
                    })
    return results


def fetch_polymarket_game_markets(odds_data: list[dict]) -> list[dict]:
    """Odds API 결과를 기반으로 Polymarket 개별 경기 이벤트와 Total 마켓 조회"""
    client = httpx.Client(timeout=15)
    results = []

    for game in odds_data:
        away_abbr = FULL_TO_POLY_ABBR.get(game["away"])
        home_abbr = FULL_TO_POLY_ABBR.get(game["home"])
        if not away_abbr or not home_abbr:
            print(f"  [WARN] 팀 매핑 실패: {game['away']} / {game['home']}")
            continue

        # Polymarket slug는 미국 날짜 기준
        # UTC와 ET 날짜 모두 시도
        dates_to_try = list(dict.fromkeys([game["date_et"], game["date_utc"]]))

        event_found = None
        for date_str in dates_to_try:
            slug = f"nba-{away_abbr}-{home_abbr}-{date_str}"
            resp = client.get("https://gamma-api.polymarket.com/events", params={"slug": slug})
            events = resp.json()
            if events:
                event_found = events[0]
                break

        if not event_found:
            results.append({
                "game": game,
                "poly_event": None,
                "total_markets": [],
            })
            continue

        # 이벤트 내 Total O/U 마켓 추출
        total_markets = []
        for m in event_found.get("markets", []):
            q = (m.get("question") or "").lower()
            # 선수 프롭 제외: "Points O/U", "Rebounds O/U", "Assists O/U"
            is_player_prop = any(kw in q for kw in ["points o/u", "rebounds o/u", "assists o/u"])
            if "o/u" in q and "1h" not in q and "1q" not in q and not is_player_prop:
                # 풀게임 O/U 마켓만
                outcomes = m.get("outcomes", [])
                prices = m.get("outcomePrices", [])
                if isinstance(outcomes, str):
                    outcomes = json.loads(outcomes)
                if isinstance(prices, str):
                    prices = json.loads(prices)

                line = _extract_line(q) or _extract_line(m.get("slug", ""))
                over_price = under_price = None
                for i, name in enumerate(outcomes):
                    p = float(prices[i]) if i < len(prices) else None
                    if p is None:
                        continue
                    if "over" in name.lower() or "yes" in name.lower():
                        over_price = p
                    else:
                        under_price = p

                # 게임 토탈은 180~300 범위, 그 외는 선수 프롭
                if line is not None and not (170 <= line <= 310):
                    continue

                total_markets.append({
                    "question": m.get("question", ""),
                    "slug": m.get("slug", ""),
                    "line": line,
                    "over_price": over_price,
                    "under_price": under_price,
                    "closed": m.get("closed", False),
                })

        results.append({
            "game": game,
            "poly_event": event_found.get("title"),
            "total_markets": total_markets,
        })

    client.close()
    return results


def _extract_line(text: str) -> float | None:
    """텍스트에서 O/U 라인 숫자 추출"""
    # "225pt5" 패턴
    m = re.search(r"(\d{2,3})pt(\d)", text)
    if m:
        return float(m.group(1)) + float(m.group(2)) / 10
    # "225.5" 패턴
    m = re.search(r"(\d{2,3}\.\d)", text)
    if m:
        return float(m.group(1))
    return None


def display_results(data: list[dict]):
    """결과 출력"""
    print(f"\n{'='*90}")
    print("  NBA Total Lines: Pinnacle vs Polymarket")
    print(f"{'='*90}")

    matched_count = 0
    for item in data:
        g = item["game"]
        away_a = FULL_TO_POLY_ABBR.get(g["away"], "???").upper()
        home_a = FULL_TO_POLY_ABBR.get(g["home"], "???").upper()
        game_label = f"{away_a} @ {home_a}"
        date_label = g["date_et"][-5:].replace("-", "/")

        pin_line = g["line"]
        pin_over = g["over_price"]
        pin_under = g["under_price"]
        pin_under_implied = 1 / pin_under if pin_under else None

        print(f"\n  {game_label} ({date_label})")
        print(f"  Pinnacle: O/U {pin_line}  [Over {pin_over:.3f}  Under {pin_under:.3f}]")

        if not item["poly_event"]:
            print(f"  Polymarket: 마켓 없음 (이벤트 미생성)")
            continue

        totals = item["total_markets"]
        if not totals:
            print(f"  Polymarket: 이벤트 있음 ({item['poly_event']}) 하지만 Total O/U 마켓 없음")
            continue

        # 가장 가까운 라인으로 비교
        best = None
        for t in totals:
            if t["line"] is None or t["closed"]:
                continue
            diff = abs(t["line"] - pin_line) if t["line"] else 999
            if best is None or diff < abs(best["line"] - pin_line):
                best = t

        for t in totals:
            if t["closed"]:
                continue
            poly_line = t["line"]
            poly_over = t["over_price"]
            poly_under = t["under_price"]

            if poly_line is None:
                continue

            line_diff = poly_line - pin_line
            is_best = (t == best)

            # Edge 계산
            edge_str = ""
            if poly_under is not None and pin_under_implied is not None:
                edge = pin_under_implied - poly_under
                if abs(edge) > 0.01:
                    side = "Under cheap" if edge > 0 else "Over cheap"
                    edge_str = f"  → {side} by {abs(edge):.1%}"

            marker = " ◀ closest" if is_best and len(totals) > 1 else ""
            print(f"  Polymarket: O/U {poly_line}  "
                  f"[Over {poly_over:.3f}  Under {poly_under:.3f}]  "
                  f"Diff: {line_diff:+.1f}{edge_str}{marker}")
            matched_count += 1

    # 요약
    print(f"\n{'='*90}")
    big_diffs = []
    for item in data:
        g = item["game"]
        pin_line = g["line"]
        for t in item["total_markets"]:
            if t["closed"] or t["line"] is None:
                continue
            diff = t["line"] - pin_line
            if abs(diff) >= 3:
                away_a = FULL_TO_POLY_ABBR.get(g["away"], "???").upper()
                home_a = FULL_TO_POLY_ABBR.get(g["home"], "???").upper()
                big_diffs.append((f"{away_a}@{home_a}", pin_line, t["line"], diff))

    print(f"  총 {len(data)}개 경기, {matched_count}개 Polymarket Total 라인 매칭")

    if big_diffs:
        print(f"\n  ** 라인 괴리 >= 3점 ({len(big_diffs)}개) **")
        for label, pin, poly, diff in big_diffs:
            direction = "Poly Higher (Under value?)" if diff > 0 else "Poly Lower (Over value?)"
            print(f"    {label}: Pinnacle {pin} vs Poly {poly} ({diff:+.1f}) → {direction}")
    else:
        print("  라인 괴리 >= 3점 없음")


def main():
    parser = argparse.ArgumentParser(description="Pinnacle vs Polymarket NBA O/U 라인 비교기")
    parser.add_argument("--api-key", required=True, help="The Odds API key")
    args = parser.parse_args()

    print("Pinnacle vs Polymarket NBA Total 비교기")
    print(f"시간: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n")

    print("[1/2] Pinnacle NBA Total 라인 수집 중...")
    odds_data = fetch_odds_api_totals(args.api_key)
    print(f"  → {len(odds_data)}개 경기\n")

    print("[2/2] Polymarket 개별 경기 마켓 조회 중...")
    results = fetch_polymarket_game_markets(odds_data)

    display_results(results)


if __name__ == "__main__":
    main()
