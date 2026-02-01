#!/usr/bin/env python3
"""
Polymarket 리밸런싱 차익거래 스캐너
- 모든 활성 이벤트를 스캔
- 같은 이벤트 내 모든 outcome 가격 합 < 1.0 이면 차익거래 기회
"""
import httpx
import json
import time

GAMMA_API = "https://gamma-api.polymarket.com"

def fetch_active_events(tag=None, limit=100, offset=0):
    """활성 이벤트 목록 가져오기"""
    params = {
        "closed": "false",
        "active": "true",
        "limit": limit,
        "offset": offset,
    }
    if tag:
        params["tag"] = tag

    resp = httpx.get(f"{GAMMA_API}/events", params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_all_active_events(tag=None):
    """페이지네이션으로 전체 활성 이벤트 가져오기"""
    all_events = []
    offset = 0
    limit = 100

    while True:
        events = fetch_active_events(tag=tag, limit=limit, offset=offset)
        if not events:
            break
        all_events.extend(events)
        if len(events) < limit:
            break
        offset += limit
        time.sleep(0.3)

    return all_events


def analyze_event(event):
    """이벤트 내 모든 마켓의 outcome 가격 합산 체크"""
    results = []
    markets = event.get("markets", [])

    for market in markets:
        if market.get("closed"):
            continue

        question = market.get("question", "")
        outcomes = market.get("outcomes", [])
        prices_raw = market.get("outcomePrices", [])

        if isinstance(outcomes, str):
            outcomes = json.loads(outcomes)
        if isinstance(prices_raw, str):
            prices_raw = json.loads(prices_raw)

        if not prices_raw or not outcomes:
            continue

        prices = [float(p) for p in prices_raw]
        price_sum = sum(prices)

        results.append({
            "question": question,
            "slug": market.get("slug", ""),
            "outcomes": outcomes,
            "prices": prices,
            "sum": price_sum,
            "gap": 1.0 - price_sum,  # 양수면 차익거래 기회
        })

    return results


def scan_negative_risk_events():
    """negativeRisk 이벤트 스캔 (멀티 아웃컴 마켓)
    예: NBA 챔피언, 대통령 선거 등 - 여러 후보 중 하나만 당선
    이런 마켓에서 모든 YES 가격의 합 < 1 이면 차익거래
    """
    # negativeRisk 마켓 직접 검색
    params = {
        "closed": "false",
        "active": "true",
        "limit": 100,
        "offset": 0,
    }
    resp = httpx.get(f"{GAMMA_API}/events", params=params, timeout=30)
    events = resp.json()

    multi_outcome_events = []
    for event in events:
        markets = event.get("markets", [])
        # negativeRisk 이벤트: 마켓이 여러 개이고 각 마켓이 같은 이벤트의 다른 outcome
        if len(markets) >= 3:
            multi_outcome_events.append(event)

    return multi_outcome_events


def main():
    print("=" * 80)
    print("Polymarket 리밸런싱 차익거래 스캐너")
    print("=" * 80)

    # 1단계: NBA 이벤트 스캔
    print("\n[1] NBA 이벤트 스캔 중...")
    nba_events = fetch_all_active_events(tag="nba")
    print(f"    활성 NBA 이벤트: {len(nba_events)}개")

    nba_opportunities = []
    for event in nba_events:
        title = event.get("title", "")
        markets = analyze_event(event)

        for m in markets:
            if m["gap"] > 0.001:  # 0.1% 이상 갭
                m["event_title"] = title
                m["event_id"] = event.get("id", "")
                nba_opportunities.append(m)

    print(f"    차익거래 기회 (gap > 0.1%): {len(nba_opportunities)}개")

    if nba_opportunities:
        # 갭 크기순 정렬
        nba_opportunities.sort(key=lambda x: -x["gap"])
        print("\n    --- NBA 차익거래 기회 ---")
        for opp in nba_opportunities[:20]:
            print(f"    [{opp['gap']*100:.2f}% gap] {opp['event_title']}")
            print(f"      마켓: {opp['question']}")
            for i, (out, price) in enumerate(zip(opp['outcomes'], opp['prices'])):
                print(f"        {out}: {price:.4f}")
            print(f"      합계: {opp['sum']:.4f} | 갭: {opp['gap']:.4f}")
            print()
    else:
        print("    NBA에서 차익거래 기회 없음")

    # 1-1단계: NBA 바이너리 마켓 (합 > 1 포함) 현황
    print("\n[1-1] NBA 바이너리 마켓 가격 합 분포")
    all_nba_markets = []
    for event in nba_events:
        title = event.get("title", "")
        for m in analyze_event(event):
            m["event_title"] = title
            all_nba_markets.append(m)

    if all_nba_markets:
        sums = [m["sum"] for m in all_nba_markets]
        under_1 = [s for s in sums if s < 1.0]
        over_1 = [s for s in sums if s >= 1.0]
        print(f"    전체 마켓: {len(sums)}개")
        print(f"    합 < 1.0 (차익거래 가능): {len(under_1)}개")
        print(f"    합 >= 1.0 (차익거래 불가): {len(over_1)}개")
        if sums:
            print(f"    합 범위: {min(sums):.4f} ~ {max(sums):.4f}")
            print(f"    평균: {sum(sums)/len(sums):.4f}")

        # 합 < 1 인 마켓 상세
        if under_1:
            print(f"\n    --- 합 < 1.0 마켓 상세 ---")
            under_markets = sorted([m for m in all_nba_markets if m["sum"] < 1.0],
                                   key=lambda x: x["sum"])
            for m in under_markets[:10]:
                print(f"    합={m['sum']:.4f} | gap={m['gap']*100:.2f}% | {m['event_title']}")
                print(f"      {m['question']}: {dict(zip(m['outcomes'], m['prices']))}")

    # 2단계: 스포츠 전체 스캔
    print("\n\n[2] 전체 스포츠 이벤트 스캔 중...")
    sports_tags = ["nba", "nfl", "mlb", "nhl", "soccer", "tennis", "mma"]
    all_sports_opps = []

    for tag in sports_tags:
        events = fetch_all_active_events(tag=tag)
        count = 0
        for event in events:
            title = event.get("title", "")
            markets = analyze_event(event)
            for m in markets:
                if m["gap"] > 0.001:
                    m["event_title"] = title
                    m["tag"] = tag
                    all_sports_opps.append(m)
                    count += 1
        print(f"    {tag:10s}: {len(events):4d} 이벤트, {count:3d} 기회")
        time.sleep(0.3)

    if all_sports_opps:
        all_sports_opps.sort(key=lambda x: -x["gap"])
        print(f"\n    --- 전체 스포츠 차익거래 기회 TOP 20 ---")
        for opp in all_sports_opps[:20]:
            print(f"    [{opp['gap']*100:.2f}%] [{opp['tag']}] {opp['event_title']}")
            print(f"      {opp['question']}: {dict(zip(opp['outcomes'], opp['prices']))}")
            print(f"      합: {opp['sum']:.4f}")
            print()

    # 3단계: 멀티 아웃컴 이벤트 (negativeRisk)
    print("\n[3] 멀티 아웃컴 이벤트 스캔 (3개+ 마켓)...")
    all_events = fetch_all_active_events()
    multi_events = [e for e in all_events if len(e.get("markets", [])) >= 3]
    print(f"    전체 활성 이벤트: {len(all_events)}개")
    print(f"    멀티 아웃컴 이벤트 (3+ 마켓): {len(multi_events)}개")

    multi_opps = []
    for event in multi_events:
        title = event.get("title", "")
        markets = event.get("markets", [])

        # 각 마켓의 YES(첫번째 outcome) 가격 합산
        yes_prices = []
        details = []
        for m in markets:
            if m.get("closed"):
                continue
            prices_raw = m.get("outcomePrices", [])
            outcomes = m.get("outcomes", [])
            if isinstance(prices_raw, str):
                prices_raw = json.loads(prices_raw)
            if isinstance(outcomes, str):
                outcomes = json.loads(outcomes)
            if prices_raw:
                yes_price = float(prices_raw[0])
                yes_prices.append(yes_price)
                q = m.get("question", "")
                details.append({"question": q, "outcome": outcomes[0] if outcomes else "?",
                                "price": yes_price})

        if len(yes_prices) >= 3:
            total = sum(yes_prices)
            gap = 1.0 - total
            if gap > 0.001:
                multi_opps.append({
                    "title": title,
                    "n_outcomes": len(yes_prices),
                    "sum": total,
                    "gap": gap,
                    "details": details,
                })

    print(f"    멀티 아웃컴 차익거래 기회: {len(multi_opps)}개")

    if multi_opps:
        multi_opps.sort(key=lambda x: -x["gap"])
        print(f"\n    --- 멀티 아웃컴 차익거래 기회 ---")
        for opp in multi_opps[:10]:
            print(f"    [{opp['gap']*100:.2f}% gap] {opp['title']}")
            print(f"      아웃컴 {opp['n_outcomes']}개, YES 합: {opp['sum']:.4f}")
            # 가격 높은 순으로 5개만 표시
            sorted_details = sorted(opp["details"], key=lambda x: -x["price"])
            for d in sorted_details[:5]:
                print(f"        {d['outcome']:30s} @ {d['price']:.4f}")
            if len(sorted_details) > 5:
                print(f"        ... +{len(sorted_details)-5}개")
            print()

    # 최종 요약
    print("=" * 80)
    print("요약")
    print("=" * 80)
    total_opps = len(nba_opportunities) + len(all_sports_opps) + len(multi_opps)
    print(f"  NBA 바이너리 기회: {len(nba_opportunities)}")
    print(f"  전체 스포츠 바이너리 기회: {len(all_sports_opps)}")
    print(f"  멀티 아웃컴 기회: {len(multi_opps)}")
    print(f"  총 기회: {total_opps}")


if __name__ == "__main__":
    main()
