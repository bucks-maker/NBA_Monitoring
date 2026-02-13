#!/usr/bin/env python3
"""
Bot 0x6e82b93e 전체 P&L 분석 및 전략 분류
"""
import json
from collections import defaultdict

DATA_DIR = "/Users/parkgeonwoo/poly/data/bot_0x6e82/0x6e82b93e"

# ============================================================
# 1. 데이터 로드
# ============================================================
with open(f"{DATA_DIR}/positions_raw.json") as f:
    open_positions = json.load(f)

closed_positions = []
with open(f"{DATA_DIR}/closed_positions_raw.jsonl") as f:
    for line in f:
        line = line.strip()
        if line:
            closed_positions.append(json.loads(line))

print(f"=== 데이터 로드 ===")
print(f"  오픈 포지션: {len(open_positions)}개")
print(f"  클로즈 포지션: {len(closed_positions)}개")
print(f"  전체 포지션: {len(open_positions) + len(closed_positions)}개")
print()

# ============================================================
# 2. 클로즈 포지션 분석 (이미 종료 + 정산 완료)
# ============================================================
print("=" * 70)
print("=== 클로즈 포지션 (종료된 이벤트, 정산 완료) ===")
print("=" * 70)

closed_total_bought = 0
closed_total_pnl = 0
closed_wins = 0
closed_losses = 0

for p in closed_positions:
    pnl = float(p.get("realizedPnl", 0))
    bought = float(p.get("totalBought", 0))
    closed_total_bought += bought
    closed_total_pnl += pnl
    if pnl > 0:
        closed_wins += 1
    else:
        closed_losses += 1

print(f"  승: {closed_wins}, 패: {closed_losses}")
print(f"  총 투자금: ${closed_total_bought:,.2f}")
print(f"  총 실현 수익: ${closed_total_pnl:,.2f}")
print(f"  수익률: {closed_total_pnl / closed_total_bought * 100:.1f}%" if closed_total_bought > 0 else "")
print()

# ============================================================
# 3. 오픈 포지션 분석
# ============================================================
print("=" * 70)
print("=== 오픈 포지션 분석 ===")
print("=" * 70)

# 분류: resolved(curPrice=0 or 1, redeemable=true) vs unresolved(진행중)
resolved_open = []  # 이벤트 종료됐지만 아직 open positions에 있는것
unresolved_open = []  # 아직 진행중인 이벤트

for p in open_positions:
    cur_price = p.get("curPrice", None)
    redeemable = p.get("redeemable", False)

    # curPrice가 0 또는 1이고 redeemable이면 이미 결과가 나온 것
    if redeemable and cur_price is not None and (cur_price == 0 or cur_price == 1):
        resolved_open.append(p)
    elif cur_price is not None and (cur_price == 0 or cur_price == 1):
        resolved_open.append(p)
    else:
        unresolved_open.append(p)

print(f"  결과 확정된 오픈 포지션: {len(resolved_open)}개")
print(f"  진행중인 오픈 포지션: {len(unresolved_open)}개")
print()

# 3a. 결과 확정된 오픈 포지션 (이미 결과가 나왔지만 클레임 안한 것)
resolved_wins = 0
resolved_losses = 0
resolved_pnl = 0
resolved_bought = 0

for p in resolved_open:
    cash_pnl = p.get("cashPnl", 0)
    initial = p.get("initialValue", 0)
    current = p.get("currentValue", 0)
    cur_price = p.get("curPrice", 0)
    size = float(p.get("size", 0))
    avg_price = p.get("avgPrice", 0)

    resolved_bought += initial
    resolved_pnl += cash_pnl

    if cur_price == 1:
        resolved_wins += 1
    else:
        resolved_losses += 1

print(f"--- 결과 확정 오픈 포지션 ---")
print(f"  승(curPrice=1): {resolved_wins}, 패(curPrice=0): {resolved_losses}")
print(f"  총 투자금: ${resolved_bought:,.2f}")
print(f"  총 P&L: ${resolved_pnl:,.2f}")
if resolved_bought > 0:
    print(f"  수익률: {resolved_pnl / resolved_bought * 100:.1f}%")
print()

# 큰 손실/수익 TOP 10
print("--- 결과 확정 - 큰 손실 TOP 10 ---")
resolved_sorted = sorted(resolved_open, key=lambda x: x.get("cashPnl", 0))
for p in resolved_sorted[:10]:
    print(f"  {p['title']:50s} | outcome={p['outcome']:12s} | "
          f"avgPrice={p.get('avgPrice',0):.3f} | curPrice={p.get('curPrice',0)} | "
          f"cashPnl=${p.get('cashPnl',0):>12,.2f} | invested=${p.get('initialValue',0):>10,.2f}")

print()
print("--- 결과 확정 - 큰 수익 TOP 10 ---")
for p in resolved_sorted[-10:]:
    print(f"  {p['title']:50s} | outcome={p['outcome']:12s} | "
          f"avgPrice={p.get('avgPrice',0):.3f} | curPrice={p.get('curPrice',0)} | "
          f"cashPnl=${p.get('cashPnl',0):>12,.2f} | invested=${p.get('initialValue',0):>10,.2f}")

print()

# 3b. 진행중 오픈 포지션
unresolved_pnl = 0
unresolved_bought = 0
for p in unresolved_open:
    unresolved_pnl += p.get("cashPnl", 0)
    unresolved_bought += p.get("initialValue", 0)

print(f"--- 진행중 오픈 포지션 ---")
print(f"  개수: {len(unresolved_open)}개")
print(f"  총 투자금: ${unresolved_bought:,.2f}")
print(f"  현재 미실현 P&L: ${unresolved_pnl:,.2f}")
print()

# ============================================================
# 4. 전체 P&L 종합
# ============================================================
print("=" * 70)
print("=== 전체 P&L 종합 ===")
print("=" * 70)

total_invested = closed_total_bought + resolved_bought + unresolved_bought
total_realized = closed_total_pnl + resolved_pnl
total_unrealized = unresolved_pnl
total_pnl = total_realized + total_unrealized

print(f"  [A] 클로즈 포지션 실현 P&L:    ${closed_total_pnl:>14,.2f}")
print(f"  [B] 확정 오픈 포지션 P&L:       ${resolved_pnl:>14,.2f}")
print(f"  [C] 진행중 포지션 미실현 P&L:   ${unresolved_pnl:>14,.2f}")
print(f"  -------------------------------------------")
print(f"  [A+B] 확정 P&L:                 ${closed_total_pnl + resolved_pnl:>14,.2f}")
print(f"  [A+B+C] 총 P&L (미실현 포함):   ${total_pnl:>14,.2f}")
print(f"  총 투자금:                       ${total_invested:>14,.2f}")
if total_invested > 0:
    print(f"  총 수익률:                       {total_pnl / total_invested * 100:>13.1f}%")
print()

# ============================================================
# 5. 전략 분류: 이벤트별 그룹핑
# ============================================================
print("=" * 70)
print("=== 전략 분류: 이벤트별 분석 ===")
print("=" * 70)

# 모든 포지션 (open + closed) 이벤트별로 그룹핑
all_positions = []

for p in open_positions:
    p["_source"] = "open"
    all_positions.append(p)

for p in closed_positions:
    p["_source"] = "closed"
    all_positions.append(p)

# eventSlug 기준 그룹핑
events = defaultdict(list)
for p in all_positions:
    event_slug = p.get("eventSlug", p.get("slug", "unknown"))
    events[event_slug].append(p)

# 이벤트 분류
multi_outcome_events = {}  # 같은 이벤트에 2개 이상 포지션 = 잠재적 차익거래
single_outcome_events = {}  # 1개 포지션 = 방향성 베팅

for event_slug, positions in events.items():
    if len(positions) >= 2:
        multi_outcome_events[event_slug] = positions
    else:
        single_outcome_events[event_slug] = positions

print(f"  고유 이벤트 수: {len(events)}")
print(f"  멀티 아웃컴 이벤트 (2+ 포지션): {len(multi_outcome_events)}")
print(f"  싱글 아웃컴 이벤트 (1 포지션): {len(single_outcome_events)}")
print()

# 멀티 아웃컴 이벤트 상세 분석
print("--- 멀티 아웃컴 이벤트 상세 (잠재적 차익거래) ---")
arb_total_invested = 0
arb_total_pnl = 0
arb_count = 0

for event_slug, positions in sorted(multi_outcome_events.items()):
    arb_count += 1
    total_avg = sum(p.get("avgPrice", 0) for p in positions)
    invested = sum(float(p.get("initialValue", 0) or 0) or float(p.get("totalBought", 0) or 0) * float(p.get("avgPrice", 0) or 0)
                   for p in positions)
    pnl = sum(float(p.get("cashPnl", 0) or 0) or float(p.get("realizedPnl", 0) or 0) for p in positions)

    arb_total_invested += invested
    arb_total_pnl += pnl

    outcomes = [f"{p['outcome']}@{p.get('avgPrice',0):.3f}" for p in positions]
    title = positions[0].get("title", event_slug)

    # 차익거래 판별: 모든 outcome의 avgPrice 합 < 1 이면 리밸런싱 차익거래
    is_arb = total_avg < 1.0
    arb_label = "ARB" if is_arb else "NOT-ARB"

    if arb_count <= 30:  # 처음 30개만 출력
        print(f"  [{arb_label}] {event_slug}")
        print(f"    포지션수: {len(positions)} | 가격합: {total_avg:.4f} | "
              f"투자: ${invested:,.0f} | P&L: ${pnl:,.0f}")
        for p in positions:
            src = p.get("_source", "?")
            cp = p.get("curPrice", p.get("curPrice", "?"))
            print(f"      {p['outcome']:15s} @{p.get('avgPrice',0):.4f} | "
                  f"curPrice={cp} | size={float(p.get('size', 0) or 0):,.0f} | "
                  f"source={src}")
        print()

print(f"\n  멀티 아웃컴 총 투자: ${arb_total_invested:,.2f}")
print(f"  멀티 아웃컴 총 P&L: ${arb_total_pnl:,.2f}")
print()

# 싱글 아웃컴 이벤트 분석
print("--- 싱글 아웃컴 이벤트 (방향성 베팅) ---")
dir_total_invested = 0
dir_total_pnl = 0
dir_wins = 0
dir_losses = 0
dir_pending = 0

# 마켓 타입별 분류
market_types = defaultdict(lambda: {"count": 0, "invested": 0, "pnl": 0, "wins": 0, "losses": 0})

for event_slug, positions in single_outcome_events.items():
    p = positions[0]
    invested = float(p.get("initialValue", 0) or 0) or float(p.get("totalBought", 0) or 0) * float(p.get("avgPrice", 0) or 0)
    pnl = float(p.get("cashPnl", 0) or 0) or float(p.get("realizedPnl", 0) or 0)
    cur_price = p.get("curPrice", None)

    dir_total_invested += invested
    dir_total_pnl += pnl

    if cur_price == 1:
        dir_wins += 1
    elif cur_price == 0:
        dir_losses += 1
    else:
        dir_pending += 1

    # 마켓 타입 분류
    slug = p.get("slug", "")
    title = p.get("title", "")
    if "spread" in slug or "Spread" in title:
        mtype = "스프레드"
    elif "total" in slug or "O/U" in title:
        mtype = "오버/언더"
    elif "nba" in slug and "playoff" not in slug:
        mtype = "NBA 머니라인"
    elif "wnba" in slug:
        mtype = "WNBA"
    elif "playoff" in slug:
        mtype = "NBA 플레이오프"
    elif "nba-champion" in slug or "nba-finals" in slug:
        mtype = "NBA 챔피언"
    else:
        mtype = "기타"

    mt = market_types[mtype]
    mt["count"] += 1
    mt["invested"] += invested
    mt["pnl"] += pnl
    if cur_price == 1:
        mt["wins"] += 1
    elif cur_price == 0:
        mt["losses"] += 1

print(f"  총 싱글 이벤트: {len(single_outcome_events)}")
print(f"  승: {dir_wins}, 패: {dir_losses}, 진행중: {dir_pending}")
print(f"  총 투자: ${dir_total_invested:,.2f}")
print(f"  총 P&L: ${dir_total_pnl:,.2f}")
if dir_total_invested > 0:
    print(f"  수익률: {dir_total_pnl / dir_total_invested * 100:.1f}%")
print()

print("--- 마켓 타입별 분류 ---")
for mtype, mt in sorted(market_types.items(), key=lambda x: -x[1]["invested"]):
    roi = mt["pnl"] / mt["invested"] * 100 if mt["invested"] > 0 else 0
    print(f"  {mtype:15s} | 건수: {mt['count']:3d} | "
          f"투자: ${mt['invested']:>12,.0f} | P&L: ${mt['pnl']:>12,.0f} | "
          f"수익률: {roi:>6.1f}% | 승: {mt['wins']} 패: {mt['losses']}")
print()

# ============================================================
# 6. avgPrice 분포 분석 (방향성 베팅 - 어떤 확률에서 진입?)
# ============================================================
print("=" * 70)
print("=== 진입 가격 분포 (방향성 베팅의 핵심) ===")
print("=" * 70)

underdog_bets = []  # avgPrice < 0.5 = 언더독 베팅
favorite_bets = []   # avgPrice >= 0.5 = 페이버릿 베팅

for event_slug, positions in single_outcome_events.items():
    p = positions[0]
    avg = p.get("avgPrice", 0)
    if avg and avg > 0:
        entry = {
            "title": p.get("title", ""),
            "outcome": p.get("outcome", ""),
            "avgPrice": avg,
            "cashPnl": float(p.get("cashPnl", 0) or 0) or float(p.get("realizedPnl", 0) or 0),
            "invested": p.get("initialValue", 0) or float(p.get("totalBought", 0)) * avg,
            "curPrice": p.get("curPrice", None),
            "slug": p.get("slug", "")
        }
        if avg < 0.5:
            underdog_bets.append(entry)
        else:
            favorite_bets.append(entry)

ug_invested = sum(b["invested"] for b in underdog_bets)
ug_pnl = sum(b["cashPnl"] for b in underdog_bets)
fav_invested = sum(b["invested"] for b in favorite_bets)
fav_pnl = sum(b["cashPnl"] for b in favorite_bets)

print(f"  언더독 베팅 (avgPrice < 0.5): {len(underdog_bets)}건")
print(f"    투자: ${ug_invested:,.0f} | P&L: ${ug_pnl:,.0f} | "
      f"수익률: {ug_pnl/ug_invested*100:.1f}%" if ug_invested > 0 else "")
print(f"  페이버릿 베팅 (avgPrice >= 0.5): {len(favorite_bets)}건")
print(f"    투자: ${fav_invested:,.0f} | P&L: ${fav_pnl:,.0f} | "
      f"수익률: {fav_pnl/fav_invested*100:.1f}%" if fav_invested > 0 else "")
print()

# 가격대별 세분화
price_buckets = defaultdict(lambda: {"count": 0, "invested": 0, "pnl": 0, "wins": 0, "losses": 0})
for event_slug, positions in single_outcome_events.items():
    p = positions[0]
    avg = p.get("avgPrice", 0)
    cur_price = p.get("curPrice", None)
    if avg and avg > 0:
        bucket = f"{int(avg*10)*10:2d}-{int(avg*10)*10+10:2d}%"
        pb = price_buckets[bucket]
        pb["count"] += 1
        pb["invested"] += p.get("initialValue", 0) or float(p.get("totalBought", 0)) * avg
        pb["pnl"] += float(p.get("cashPnl", 0) or 0) or float(p.get("realizedPnl", 0) or 0)
        if cur_price == 1:
            pb["wins"] += 1
        elif cur_price == 0:
            pb["losses"] += 1

print("--- 가격대별 수익률 ---")
for bucket in sorted(price_buckets.keys()):
    pb = price_buckets[bucket]
    roi = pb["pnl"] / pb["invested"] * 100 if pb["invested"] > 0 else 0
    winrate = pb["wins"] / (pb["wins"] + pb["losses"]) * 100 if (pb["wins"] + pb["losses"]) > 0 else 0
    print(f"  {bucket} | 건수: {pb['count']:3d} | 투자: ${pb['invested']:>10,.0f} | "
          f"P&L: ${pb['pnl']:>10,.0f} | ROI: {roi:>6.1f}% | "
          f"승률: {winrate:.0f}% ({pb['wins']}/{pb['wins']+pb['losses']})")
print()

# ============================================================
# 7. 핵심 결론
# ============================================================
print("=" * 70)
print("=== 핵심 결론 ===")
print("=" * 70)

all_arb_events = len(multi_outcome_events)
all_dir_events = len(single_outcome_events)
total_events = all_arb_events + all_dir_events

print(f"\n  1. 전략 비율:")
print(f"     - 방향성 베팅: {all_dir_events}건 ({all_dir_events/total_events*100:.1f}%)")
print(f"     - 멀티아웃컴(잠재 차익거래): {all_arb_events}건 ({all_arb_events/total_events*100:.1f}%)")

print(f"\n  2. 확정 P&L (결과 나온 모든 포지션):")
confirmed_pnl = closed_total_pnl + resolved_pnl
confirmed_invested = closed_total_bought + resolved_bought
print(f"     - 확정 수익: ${confirmed_pnl:,.2f}")
print(f"     - 확정 투자금: ${confirmed_invested:,.2f}")
if confirmed_invested > 0:
    print(f"     - 확정 수익률: {confirmed_pnl/confirmed_invested*100:.1f}%")

print(f"\n  3. 전체 P&L (미실현 포함):")
print(f"     - 총 P&L: ${total_pnl:,.2f}")
print(f"     - 총 투자: ${total_invested:,.2f}")
if total_invested > 0:
    print(f"     - 총 수익률: {total_pnl/total_invested*100:.1f}%")

# 봇의 핵심 패턴
print(f"\n  4. 봇의 핵심 패턴:")
print(f"     - 주로 언더독 베팅 (낮은 확률 매수)")
print(f"     - 언더독: {len(underdog_bets)}건 vs 페이버릿: {len(favorite_bets)}건")
avg_entry = sum(p.get("avgPrice",0) for es,ps in single_outcome_events.items() for p in ps if p.get("avgPrice",0) > 0) / max(1, sum(1 for es,ps in single_outcome_events.items() for p in ps if p.get("avgPrice",0) > 0))
print(f"     - 평균 진입 가격: {avg_entry:.3f} (={avg_entry*100:.1f}%)")
print()
