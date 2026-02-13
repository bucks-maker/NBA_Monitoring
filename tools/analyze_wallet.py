#!/usr/bin/env python3
"""Comprehensive wallet analysis script."""
import json
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta

DATA_DIR = '/Users/parkgeonwoo/poly/out/0x6e82b93e'

# Load data
activity = []
with open(f'{DATA_DIR}/activity_trades_all.jsonl') as f:
    for line in f:
        activity.append(json.loads(line))

closed = []
with open(f'{DATA_DIR}/closed_positions_all.jsonl') as f:
    for line in f:
        closed.append(json.loads(line))

trades = []
with open(f'{DATA_DIR}/trades_all.jsonl') as f:
    for line in f:
        trades.append(json.loads(line))

print('=' * 80)
print('WALLET 0x6e82b93eb57b01a63027bd0c6d2f3f04934a752c ANALYSIS')
print('=' * 80)

# === 1. BASIC STATS ===
print('\n## 1) BASIC STATS')
print(f'Activity trades: {len(activity)}')
print(f'Closed positions: {len(closed)}')
print(f'Raw trades: {len(trades)}')

buy_count = sum(1 for t in activity if t.get('side') == 'BUY')
sell_count = sum(1 for t in activity if t.get('side') == 'SELL')
print(f'BUY: {buy_count}, SELL: {sell_count}')
if sell_count > 0:
    print(f'Buy/Sell ratio: {buy_count / sell_count:.1f}:1')
else:
    print(f'Buy/Sell ratio: {buy_count}:0 (100% BUY)')

# Timestamps
ts_list = sorted([t['timestamp'] for t in activity])
et_offset = timedelta(hours=-5)
if ts_list:
    dt_min = datetime.fromtimestamp(ts_list[0], tz=timezone.utc)
    dt_max = datetime.fromtimestamp(ts_list[-1], tz=timezone.utc)
    days = (dt_max - dt_min).days
    print(f'Date range: {dt_min.strftime("%Y-%m-%d")} to {dt_max.strftime("%Y-%m-%d")} ({days} days)')
    print(f'Trades per day: {len(activity) / max(days, 1):.1f}')

total_vol = sum(float(t.get('usdcSize', 0) or 0) for t in activity)
print(f'Total volume (activity): ${total_vol:,.2f}')

prices = [float(t['price']) for t in activity if t.get('price')]
avg_price = sum(prices) / len(prices) if prices else 0
print(f'Average entry price: {avg_price:.4f}')

# Underdog vs favorite
under = sum(1 for p in prices if p < 0.5)
favor = sum(1 for p in prices if p >= 0.5)
print(f'Underdog (<0.5): {under} ({under/len(prices)*100:.1f}%)')
print(f'Favorite (>=0.5): {favor} ({favor/len(prices)*100:.1f}%)')

# === 2. PRICE DISTRIBUTION ===
print('\n## 2) PRICE DISTRIBUTION')
price_buckets = Counter()
for t in activity:
    p = float(t.get('price', 0))
    lo = int(p * 10) / 10
    bucket = f'{lo:.1f}-{lo + 0.1:.1f}'
    price_buckets[bucket] += 1
for b in sorted(price_buckets.keys()):
    pct = price_buckets[b] / len(activity) * 100
    bar = '#' * int(pct)
    print(f'  {b}: {price_buckets[b]:>5} ({pct:>5.1f}%) {bar}')

# === 3. LEAGUE DETECTION ===
print('\n## 3) LEAGUE/SPORT DISTRIBUTION')
league_map = {
    'nba': 'NBA', 'nfl': 'NFL', 'nhl': 'NHL', 'mlb': 'MLB',
    'cs2': 'CS2', 'lol': 'LoL', 'dota': 'Dota2', 'val': 'Valorant',
    'epl': 'EPL', 'lal': 'LaLiga', 'ser': 'SerieA', 'bun': 'Bundesliga',
    'lig': 'Ligue1', 'ucl': 'UCL', 'uel': 'UEL',
    'atp': 'ATP', 'wta': 'WTA',
    'ufc': 'UFC', 'boxing': 'Boxing',
    'ncaa': 'NCAA', 'mls': 'MLS',
    'cbb': 'CBB',
}

def detect_league(slug):
    if not slug:
        return 'Other'
    slug_lower = slug.lower()
    for prefix, name in league_map.items():
        if slug_lower.startswith(prefix + '-') or ('-' + prefix + '-') in slug_lower:
            return name
    # Check title-based
    if 'crypto' in slug_lower or 'btc' in slug_lower or 'eth' in slug_lower:
        return 'Crypto'
    if 'politics' in slug_lower or 'trump' in slug_lower or 'biden' in slug_lower:
        return 'Politics'
    return 'Other'

league_counts = Counter()
league_vol = defaultdict(float)
for t in activity:
    league = detect_league(t.get('slug', '') or t.get('eventSlug', ''))
    league_counts[league] += 1
    league_vol[league] += float(t.get('usdcSize', 0) or 0)

for league, count in league_counts.most_common(20):
    vol = league_vol[league]
    pct = count / len(activity) * 100
    print(f'  {league:12s}: {count:>5} trades ({pct:>5.1f}%), ${vol:>12,.2f} volume')

# === 4. CLOSED POSITION PnL ===
print('\n## 4) CLOSED POSITION PnL')
pnl_values = []
for c in closed:
    pnl = float(c.get('realizedPnl', 0) or 0)
    pnl_values.append(pnl)

total_pnl = sum(pnl_values)
wins = [p for p in pnl_values if p > 0]
losses = [p for p in pnl_values if p < 0]
breakeven = [p for p in pnl_values if p == 0]
print(f'Total realized PnL: ${total_pnl:,.2f}')
print(f'Wins: {len(wins)}, Losses: {len(losses)}, Breakeven: {len(breakeven)}')
if pnl_values:
    win_rate = len(wins) / (len(wins) + len(losses)) * 100 if (len(wins) + len(losses)) > 0 else 0
    print(f'Win rate: {win_rate:.1f}%')
if wins:
    print(f'Avg win: ${sum(wins) / len(wins):,.2f}')
    print(f'Max win: ${max(wins):,.2f}')
if losses:
    print(f'Avg loss: ${sum(losses) / len(losses):,.2f}')
    print(f'Max loss: ${min(losses):,.2f}')
if wins and losses:
    gross_win = sum(wins)
    gross_loss = abs(sum(losses))
    pf = gross_win / gross_loss if gross_loss > 0 else float('inf')
    print(f'Profit Factor: {pf:.2f}')
    print(f'Gross Win: ${gross_win:,.2f}')
    print(f'Gross Loss: -${gross_loss:,.2f}')

# === 5. PnL BY LEAGUE ===
print('\n## 5) PnL BY LEAGUE')
league_pnl = defaultdict(lambda: {'pnl': 0, 'count': 0, 'wins': 0, 'losses': 0, 'vol': 0})
for c in closed:
    league = detect_league(c.get('slug', '') or c.get('eventSlug', ''))
    pnl = float(c.get('realizedPnl', 0) or 0)
    tb = float(c.get('totalBought', 0) or 0)
    league_pnl[league]['pnl'] += pnl
    league_pnl[league]['count'] += 1
    league_pnl[league]['vol'] += tb
    if pnl > 0:
        league_pnl[league]['wins'] += 1
    elif pnl < 0:
        league_pnl[league]['losses'] += 1

sorted_leagues = sorted(league_pnl.items(), key=lambda x: x[1]['pnl'], reverse=True)
print(f'  {"League":12s} {"Pos":>6s} {"Wins":>6s} {"Loss":>6s} {"WR":>7s} {"Volume":>14s} {"PnL":>14s}')
for league, data in sorted_leagues:
    total = data['wins'] + data['losses']
    wr = data['wins'] / total * 100 if total > 0 else 0
    print(f'  {league:12s} {data["count"]:>6d} {data["wins"]:>6d} {data["losses"]:>6d} {wr:>6.1f}% ${data["vol"]:>13,.0f} ${data["pnl"]:>13,.2f}')

# === 6. PnL BY PRICE BUCKET ===
print('\n## 6) PnL BY ENTRY PRICE BUCKET')
price_pnl = defaultdict(lambda: {'pnl': 0, 'count': 0, 'wins': 0, 'losses': 0})
for c in closed:
    avg_p = float(c.get('avgPrice', 0) or 0)
    if avg_p == 0:
        continue
    lo = int(avg_p * 10) / 10
    bucket = f'{lo:.1f}-{lo + 0.1:.1f}'
    pnl = float(c.get('realizedPnl', 0) or 0)
    price_pnl[bucket]['pnl'] += pnl
    price_pnl[bucket]['count'] += 1
    if pnl > 0:
        price_pnl[bucket]['wins'] += 1
    elif pnl < 0:
        price_pnl[bucket]['losses'] += 1

print(f'  {"Bucket":10s} {"Pos":>6s} {"Wins":>6s} {"Loss":>6s} {"WR":>7s} {"PnL":>14s}')
for b in sorted(price_pnl.keys()):
    d = price_pnl[b]
    total = d['wins'] + d['losses']
    wr = d['wins'] / total * 100 if total > 0 else 0
    print(f'  {b:10s} {d["count"]:>6d} {d["wins"]:>6d} {d["losses"]:>6d} {wr:>6.1f}% ${d["pnl"]:>13,.2f}')

# === 7. TIME ANALYSIS ===
print('\n## 7) TRADING HOURS (ET)')
hour_counts = Counter()
for t in activity:
    ts = t['timestamp']
    dt = datetime.fromtimestamp(ts, tz=timezone.utc) + et_offset
    hour_counts[dt.hour] += 1

for h in range(24):
    cnt = hour_counts.get(h, 0)
    pct = cnt / len(activity) * 100
    bar = '#' * int(pct * 2)
    print(f'  {h:02d}:00 ET: {cnt:>5} ({pct:>5.1f}%) {bar}')

# === 8. DAY OF WEEK ===
print('\n## 8) TRADING DAYS')
dow_names = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
dow_counts = Counter()
for t in activity:
    dt = datetime.fromtimestamp(t['timestamp'], tz=timezone.utc) + et_offset
    dow_counts[dt.weekday()] += 1
for d in range(7):
    print(f'  {dow_names[d]}: {dow_counts.get(d, 0):>5}')

# === 9. TRADE INTERVAL ANALYSIS ===
print('\n## 9) TRADE INTERVALS')
intervals = []
for i in range(1, len(ts_list)):
    diff = ts_list[i] - ts_list[i - 1]
    intervals.append(diff)

if intervals:
    int_counter = Counter()
    for iv in intervals:
        if iv == 0:
            int_counter['0s (same second)'] += 1
        elif iv <= 10:
            int_counter['1-10s'] += 1
        elif iv <= 60:
            int_counter['10-60s'] += 1
        elif iv <= 300:
            int_counter['1-5min'] += 1
        elif iv <= 900:
            int_counter['5-15min'] += 1
        elif iv <= 3600:
            int_counter['15-60min'] += 1
        elif iv <= 21600:
            int_counter['1-6h'] += 1
        else:
            int_counter['6h+'] += 1

    ordered = ['0s (same second)', '1-10s', '10-60s', '1-5min', '5-15min', '15-60min', '1-6h', '6h+']
    for label in ordered:
        cnt = int_counter.get(label, 0)
        pct = cnt / len(intervals) * 100
        print(f'  {label:20s}: {cnt:>5} ({pct:.1f}%)')

    exact_intervals = Counter(intervals)
    print(f'\n  Most common intervals:')
    for iv, cnt in exact_intervals.most_common(10):
        print(f'    {iv}s: {cnt} times')

    non_zero = [i for i in intervals if i > 0]
    if non_zero:
        print(f'  Median interval (non-zero): {statistics.median(non_zero):.0f}s')
        mean_iv = statistics.mean(non_zero)
        std_iv = statistics.stdev(non_zero) if len(non_zero) > 1 else 0
        cov = std_iv / mean_iv if mean_iv > 0 else 0
        print(f'  Mean: {mean_iv:.1f}s, StdDev: {std_iv:.1f}s, CoV: {cov:.2f}')

# === 10. SIZE ANALYSIS ===
print('\n## 10) TRADE SIZE')
sizes = [float(t.get('usdcSize', 0) or 0) for t in activity]
sizes_nonzero = [s for s in sizes if s > 0]
if sizes_nonzero:
    print(f'  Mean: ${statistics.mean(sizes_nonzero):,.2f}')
    print(f'  Median: ${statistics.median(sizes_nonzero):,.2f}')
    print(f'  Max: ${max(sizes_nonzero):,.2f}')
    print(f'  Min: ${min(sizes_nonzero):,.2f}')

    size_buckets = Counter()
    for s in sizes_nonzero:
        if s < 10:
            size_buckets['$0-10'] += 1
        elif s < 50:
            size_buckets['$10-50'] += 1
        elif s < 100:
            size_buckets['$50-100'] += 1
        elif s < 500:
            size_buckets['$100-500'] += 1
        elif s < 1000:
            size_buckets['$500-1K'] += 1
        elif s < 5000:
            size_buckets['$1K-5K'] += 1
        elif s < 10000:
            size_buckets['$5K-10K'] += 1
        else:
            size_buckets['$10K+'] += 1

    ordered_size = ['$0-10', '$10-50', '$50-100', '$100-500', '$500-1K', '$1K-5K', '$5K-10K', '$10K+']
    for label in ordered_size:
        cnt = size_buckets.get(label, 0)
        pct = cnt / len(sizes_nonzero) * 100
        print(f'  {label:12s}: {cnt:>5} ({pct:.1f}%)')

# === 11. TOP MARKETS ===
print('\n## 11) TOP 10 MARKETS BY TRADE COUNT')
market_data = defaultdict(lambda: {'count': 0, 'vol': 0, 'outcomes': set(), 'prices': [], 'sides': Counter(), 'title': ''})
for t in activity:
    slug = t.get('slug', 'unknown')
    market_data[slug]['count'] += 1
    market_data[slug]['vol'] += float(t.get('usdcSize', 0) or 0)
    market_data[slug]['outcomes'].add(t.get('outcome', ''))
    market_data[slug]['prices'].append(float(t.get('price', 0)))
    market_data[slug]['sides'][t.get('side', '')] += 1
    market_data[slug]['title'] = t.get('title', '')

top_markets = sorted(market_data.items(), key=lambda x: x[1]['count'], reverse=True)[:10]
for slug, d in top_markets:
    avg_p = sum(d['prices']) / len(d['prices']) if d['prices'] else 0
    outcomes = ', '.join(d['outcomes'])
    print(f'  {slug}: {d["count"]} trades, ${d["vol"]:,.0f} vol, avg_price={avg_p:.3f}')
    print(f'    Title: {d["title"]}')
    print(f'    Outcomes: {outcomes}')
    print(f'    Sides: {dict(d["sides"])}')

# === 12. POSITION SIZE (CLOSED) ===
print('\n## 12) POSITION SIZE (CLOSED)')
total_bought = [float(c.get('totalBought', 0) or 0) for c in closed]
total_bought_nz = [t for t in total_bought if t > 0]
if total_bought_nz:
    print(f'  Mean position: ${statistics.mean(total_bought_nz):,.2f}')
    print(f'  Median position: ${statistics.median(total_bought_nz):,.2f}')
    print(f'  Max position: ${max(total_bought_nz):,.2f}')
    print(f'  Total invested: ${sum(total_bought_nz):,.2f}')

# === 13. BURST DETECTION ===
print('\n## 13) BURST DETECTION (3+ trades in 2min window)')
bursts = []
i = 0
while i < len(ts_list):
    j = i + 1
    while j < len(ts_list) and ts_list[j] - ts_list[i] <= 120:
        j += 1
    burst_size = j - i
    if burst_size >= 3:
        bursts.append(burst_size)
        i = j
    else:
        i += 1

print(f'  Total bursts: {len(bursts)}')
if bursts:
    print(f'  Avg burst size: {sum(bursts) / len(bursts):.1f}')
    print(f'  Max burst size: {max(bursts)}')
    print(f'  Median burst: {sorted(bursts)[len(bursts) // 2]}')
    burst_sizes = Counter()
    for b in bursts:
        if b <= 5:
            burst_sizes['3-5'] += 1
        elif b <= 10:
            burst_sizes['5-10'] += 1
        elif b <= 20:
            burst_sizes['10-20'] += 1
        elif b <= 50:
            burst_sizes['20-50'] += 1
        else:
            burst_sizes['50+'] += 1
    for label in ['3-5', '5-10', '10-20', '20-50', '50+']:
        print(f'  {label} trades: {burst_sizes.get(label, 0)}')

# === 14. COMPLEMENTARY OUTCOME CHECK ===
print('\n## 14) COMPLEMENTARY OUTCOME ANALYSIS')
event_outcomes = defaultdict(set)
for t in activity:
    event = t.get('eventSlug', '') or t.get('slug', '')
    outcome = t.get('outcome', '')
    if event and outcome:
        event_outcomes[event].add(outcome)

multi_outcome = {e: o for e, o in event_outcomes.items() if len(o) > 1}
print(f'  Total unique events: {len(event_outcomes)}')
print(f'  Events with multiple outcomes bought: {len(multi_outcome)} ({len(multi_outcome) / max(len(event_outcomes), 1) * 100:.1f}%)')
print(f'  Events with single outcome: {len(event_outcomes) - len(multi_outcome)}')

# === 15. PnL BY SETTLEMENT HOUR (ET) ===
print('\n## 15) PnL BY SETTLEMENT HOUR (ET)')
hour_pnl = defaultdict(lambda: {'pnl': 0, 'count': 0, 'wins': 0})
for c in closed:
    ts = c.get('timestamp', 0)
    pnl = float(c.get('realizedPnl', 0) or 0)
    if ts:
        dt = datetime.fromtimestamp(ts, tz=timezone.utc) + et_offset
        h = dt.hour
        hour_pnl[h]['pnl'] += pnl
        hour_pnl[h]['count'] += 1
        if pnl > 0:
            hour_pnl[h]['wins'] += 1

print(f'  {"Hour":>6s} {"Pos":>5s} {"WR":>7s} {"PnL":>14s}')
for h in range(24):
    d = hour_pnl.get(h, {'pnl': 0, 'count': 0, 'wins': 0})
    if d['count'] > 0:
        wr = d['wins'] / d['count'] * 100
        print(f'  {h:02d}:00  {d["count"]:>5} {wr:>6.1f}% ${d["pnl"]:>13,.2f}')

# === 16. UNIQUE MARKETS COUNT ===
print('\n## 16) MARKET DIVERSITY')
unique_slugs = set(t.get('slug', '') for t in activity)
print(f'  Unique markets: {len(unique_slugs)}')
total_trades = len(activity)
top_10_share = sum(d['count'] for _, d in top_markets)
print(f'  Top 10 market share: {top_10_share / total_trades * 100:.1f}%')
# HHI
shares = [(d['count'] / total_trades * 100) for _, d in market_data.items()]
hhi = sum(s ** 2 for s in shares)
print(f'  HHI (concentration): {hhi:.1f}')

print('\n' + '=' * 80)
print('ANALYSIS COMPLETE')
print('=' * 80)
