#!/usr/bin/env python3
"""Analyze closed positions data."""
import json
from pathlib import Path
from datetime import datetime
from collections import Counter, defaultdict
import pytz

# Load data
data_file = Path("out/gabagool22/closed_positions_all.jsonl")
positions = []

with open(data_file, 'r') as f:
    for line in f:
        if line.strip():
            positions.append(json.loads(line))

print(f"📊 Loaded {len(positions)} closed positions\n")

# 1. 날짜 범위
timestamps = [p['timestamp'] for p in positions if 'timestamp' in p]
dates = [datetime.fromtimestamp(ts, tz=pytz.UTC) for ts in timestamps]

print("="*60)
print("📅 TRADING PERIOD")
print("="*60)
print(f"First trade: {min(dates).strftime('%Y-%m-%d %H:%M:%S')} UTC")
print(f"Last trade:  {max(dates).strftime('%Y-%m-%d %H:%M:%S')} UTC")
print(f"Duration: {(max(dates) - min(dates)).days} days\n")

# 2. 시간대별 분포 (UTC)
print("="*60)
print("⏰ TRADING HOURS (UTC)")
print("="*60)
hours_utc = [d.hour for d in dates]
hour_counts = Counter(hours_utc)

for hour in sorted(hour_counts.keys()):
    count = hour_counts[hour]
    bar = '█' * (count // 50)
    print(f"{hour:02d}:00 UTC - {count:4d} trades {bar}")

print()

# 3. 시간대별 분포 (ET = UTC-5)
print("="*60)
print("⏰ TRADING HOURS (ET = UTC-5)")
print("="*60)
et_tz = pytz.timezone('US/Eastern')
hours_et = [(d.astimezone(et_tz)).hour for d in dates]
hour_counts_et = Counter(hours_et)

for hour in sorted(hour_counts_et.keys()):
    count = hour_counts_et[hour]
    bar = '█' * (count // 50)
    print(f"{hour:02d}:00 ET - {count:4d} trades {bar}")

print()

# 4. 요일별 분포
print("="*60)
print("📆 TRADING BY DAY OF WEEK")
print("="*60)
days = [d.strftime('%A') for d in dates]
day_counts = Counter(days)
day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

for day in day_order:
    count = day_counts.get(day, 0)
    bar = '█' * (count // 50)
    print(f"{day:9s} - {count:4d} trades {bar}")

print()

# 5. Up vs Down 분포
print("="*60)
print("📈 UP vs DOWN")
print("="*60)
outcomes = [p.get('outcome', 'Unknown') for p in positions]
outcome_counts = Counter(outcomes)

for outcome, count in outcome_counts.most_common():
    pct = count / len(positions) * 100
    bar = '█' * (count // 100)
    print(f"{outcome:10s} - {count:5d} ({pct:5.1f}%) {bar}")

print()

# 6. 평균 수익
print("="*60)
print("💰 PROFIT & LOSS")
print("="*60)
pnls = [float(p.get('realizedPnl', 0)) for p in positions if 'realizedPnl' in p]
winning = [pnl for pnl in pnls if pnl > 0]
losing = [pnl for pnl in pnls if pnl < 0]

print(f"Total Realized PnL: ${sum(pnls):,.2f}")
print(f"Average PnL:        ${sum(pnls)/len(pnls):,.2f}")
print(f"Win Rate:           {len(winning)/len(pnls)*100:.1f}% ({len(winning)}/{len(pnls)})")
print(f"Avg Win:            ${sum(winning)/len(winning) if winning else 0:,.2f}")
print(f"Avg Loss:           ${sum(losing)/len(losing) if losing else 0:,.2f}")
print(f"Best Trade:         ${max(pnls):,.2f}")
print(f"Worst Trade:        ${min(pnls):,.2f}")

print()

# 7. 주요 마켓
print("="*60)
print("🎯 TOP MARKETS")
print("="*60)
markets = [p.get('slug', 'Unknown') for p in positions]
market_counts = Counter(markets)

for market, count in market_counts.most_common(10):
    market_short = market[:50]
    print(f"{count:4d} - {market_short}")

print()

# 8. 평균 진입가
print("="*60)
print("💵 AVERAGE ENTRY PRICE")
print("="*60)
prices = [float(p.get('avgPrice', 0)) for p in positions if 'avgPrice' in p and p.get('avgPrice')]
if prices:
    print(f"Average entry price: {sum(prices)/len(prices):.4f}")
    print(f"Min entry price:     {min(prices):.4f}")
    print(f"Max entry price:     {max(prices):.4f}")

print()

# 9. 거래 규모
print("="*60)
print("📊 POSITION SIZE")
print("="*60)
sizes = [float(p.get('totalBought', 0)) for p in positions if 'totalBought' in p]
if sizes:
    print(f"Average size: ${sum(sizes)/len(sizes):,.2f}")
    print(f"Total volume: ${sum(sizes):,.2f}")
    print(f"Max position: ${max(sizes):,.2f}")

print()

# 10. 크립토 마켓 필터링
print("="*60)
print("🪙 CRYPTO MARKETS (BTC/ETH)")
print("="*60)
crypto_positions = [p for p in positions if 'btc' in p.get('slug', '').lower() or 'bitcoin' in p.get('slug', '').lower() or 'eth' in p.get('slug', '').lower()]
print(f"Total crypto positions: {len(crypto_positions)} ({len(crypto_positions)/len(positions)*100:.1f}%)")

if crypto_positions:
    crypto_pnl = [float(p.get('realizedPnl', 0)) for p in crypto_positions if 'realizedPnl' in p]
    crypto_winning = [pnl for pnl in crypto_pnl if pnl > 0]

    print(f"Crypto Total PnL:   ${sum(crypto_pnl):,.2f}")
    print(f"Crypto Win Rate:    {len(crypto_winning)/len(crypto_pnl)*100:.1f}%")
    print(f"Crypto Avg PnL:     ${sum(crypto_pnl)/len(crypto_pnl):,.2f}")

    # BTC 15분 마켓만
    btc_15m = [p for p in crypto_positions if '15m' in p.get('slug', '').lower()]
    if btc_15m:
        print(f"\nBTC 15-min markets: {len(btc_15m)}")
        btc_15m_pnl = [float(p.get('realizedPnl', 0)) for p in btc_15m if 'realizedPnl' in p]
        print(f"BTC 15m Total PnL:  ${sum(btc_15m_pnl):,.2f}")
        print(f"BTC 15m Avg PnL:    ${sum(btc_15m_pnl)/len(btc_15m_pnl):,.2f}")

print()

# 11. 시간대별 수익 분석
print("="*60)
print("⏰ PROFIT BY HOUR (ET)")
print("="*60)
hour_pnl = defaultdict(list)
for p in positions:
    if 'timestamp' in p and 'realizedPnl' in p:
        dt = datetime.fromtimestamp(p['timestamp'], tz=pytz.UTC)
        et_hour = dt.astimezone(et_tz).hour
        hour_pnl[et_hour].append(float(p['realizedPnl']))

for hour in sorted(hour_pnl.keys()):
    pnls = hour_pnl[hour]
    avg_pnl = sum(pnls) / len(pnls)
    total_pnl = sum(pnls)
    win_rate = len([p for p in pnls if p > 0]) / len(pnls) * 100
    print(f"{hour:02d}:00 ET - Count: {len(pnls):4d} | Avg: ${avg_pnl:7.2f} | Total: ${total_pnl:9.2f} | Win: {win_rate:4.1f}%")

print("\n" + "="*60)
print("✅ Analysis complete!")
print("="*60)