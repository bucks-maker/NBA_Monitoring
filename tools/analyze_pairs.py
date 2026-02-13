#!/usr/bin/env python3
"""Analyze Up/Down pairs to find the real edge."""
import json
from pathlib import Path
from collections import defaultdict
from datetime import datetime

# Load data
data_file = Path("out/gabagool22/closed_positions_all.jsonl")
positions = []

with open(data_file, 'r') as f:
    for line in f:
        if line.strip():
            positions.append(json.loads(line))

print(f"📊 Loaded {len(positions)} positions\n")

# Group by event
events = defaultdict(list)
for p in positions:
    event_slug = p.get('eventSlug', p.get('slug', ''))
    events[event_slug].append(p)

print(f"Total unique events: {len(events)}\n")

# Find events with both Up and Down
paired_events = []
for event_slug, positions_list in events.items():
    outcomes = set(p.get('outcome') for p in positions_list)
    if 'Up' in outcomes and 'Down' in outcomes:
        paired_events.append((event_slug, positions_list))

print(f"Events with both Up and Down: {len(paired_events)}\n")

print("="*70)
print("SAMPLE PAIRED TRADES")
print("="*70)

# Analyze first 20 pairs
for i, (event_slug, positions_list) in enumerate(paired_events[:20]):
    up_pos = [p for p in positions_list if p.get('outcome') == 'Up']
    down_pos = [p for p in positions_list if p.get('outcome') == 'Down']

    if not up_pos or not down_pos:
        continue

    # Take first of each
    up = up_pos[0]
    down = down_pos[0]

    up_pnl = float(up.get('realizedPnl', 0))
    down_pnl = float(down.get('realizedPnl', 0))
    net_pnl = up_pnl + down_pnl

    up_bought = float(up.get('totalBought', 0))
    down_bought = float(down.get('totalBought', 0))
    total_invested = up_bought + down_bought

    up_price = up.get('avgPrice', 0)
    down_price = down.get('avgPrice', 0)

    timestamp = up.get('timestamp', 0)
    dt = datetime.fromtimestamp(timestamp) if timestamp else None

    print(f"\n{i+1}. {event_slug[:40]}...")
    if dt:
        print(f"   Time: {dt.strftime('%Y-%m-%d %H:%M')}")
    print(f"   Up:   ${up_bought:7.2f} @ {up_price:.4f} → PnL: ${up_pnl:7.2f}")
    print(f"   Down: ${down_bought:7.2f} @ {down_price:.4f} → PnL: ${down_pnl:7.2f}")
    print(f"   Total invested: ${total_invested:,.2f}")
    print(f"   NET PnL: ${net_pnl:,.2f} ({net_pnl/total_invested*100:.2f}% ROI)")

# Summary stats
print("\n" + "="*70)
print("OVERALL PAIR STATISTICS")
print("="*70)

all_net_pnls = []
all_rois = []

for event_slug, positions_list in paired_events:
    up_pos = [p for p in positions_list if p.get('outcome') == 'Up']
    down_pos = [p for p in positions_list if p.get('outcome') == 'Down']

    for up in up_pos:
        for down in down_pos:
            up_pnl = float(up.get('realizedPnl', 0))
            down_pnl = float(down.get('realizedPnl', 0))
            net_pnl = up_pnl + down_pnl

            up_bought = float(up.get('totalBought', 0))
            down_bought = float(down.get('totalBought', 0))
            total_invested = up_bought + down_bought

            if total_invested > 0:
                roi = net_pnl / total_invested * 100
                all_net_pnls.append(net_pnl)
                all_rois.append(roi)

if all_net_pnls:
    print(f"Total pairs analyzed: {len(all_net_pnls)}")
    print(f"Average NET PnL per pair: ${sum(all_net_pnls)/len(all_net_pnls):,.2f}")
    print(f"Average ROI per pair: {sum(all_rois)/len(all_rois):.2f}%")
    print(f"Total NET PnL: ${sum(all_net_pnls):,.2f}")
    print(f"Best pair: ${max(all_net_pnls):,.2f}")
    print(f"Worst pair: ${min(all_net_pnls):,.2f}")

    winning_pairs = [p for p in all_net_pnls if p > 0]
    print(f"\nWinning pairs: {len(winning_pairs)}/{len(all_net_pnls)} ({len(winning_pairs)/len(all_net_pnls)*100:.1f}%)")

print("\n" + "="*70)