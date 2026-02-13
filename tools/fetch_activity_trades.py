#!/usr/bin/env python3
"""Fetch trades using /activity API with time slicing to bypass offset limits."""
import asyncio
import json
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from polymarket_collector.api import GammaAPIClient
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = "https://data-api.polymarket.com/activity"


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
)
async def fetch_activity(
    client: httpx.AsyncClient,
    user: str,
    start: int,
    end: int,
    limit: int = 500,
):
    """Fetch activity (TRADE type only) for a time window."""
    params = {
        "user": user,
        "type": "TRADE",
        "start": start,
        "end": end,
        "limit": limit,
        "sortBy": "TIMESTAMP",
        "sortDirection": "ASC",  # Important: ascending order for cursor-based collection
    }

    logger.info(f"  API call: start={start} ({datetime.fromtimestamp(start).date()}) end={end} ({datetime.fromtimestamp(end).date()})")

    r = await client.get(BASE_URL, params=params)
    r.raise_for_status()
    return r.json()


async def collect_all_trades(user: str, start_date: datetime, end_date: datetime):
    """
    Collect ALL trades using time-windowing strategy.

    Strategy:
    1. Start with 6-hour windows
    2. If a window returns 500 (limit), shrink it (too dense)
    3. Advance cursor by last timestamp + 1
    4. Dedupe by transaction hash
    """
    all_trades = []
    seen_hashes = set()

    start = int(start_date.timestamp())
    end = int(end_date.timestamp())

    current = start
    window = 6 * 3600  # Start with 6 hours
    min_window = 60  # Minimum 1 minute

    logger.info("="*70)
    logger.info(f"Collecting trades from {start_date.date()} to {end_date.date()}")
    logger.info("="*70)

    async with httpx.AsyncClient(timeout=30) as client:
        while current < end:
            # Calculate window end
            window_end = min(current + window, end)

            try:
                chunk = await fetch_activity(client, user, current, window_end)

                # If we hit the limit, window is too dense - shrink it
                if len(chunk) >= 500:
                    logger.info(f"  ⚠️  Window too dense ({len(chunk)} trades), shrinking window")
                    window = max(min_window, window // 2)
                    continue

                # Process and dedupe
                new_count = 0
                for trade in chunk:
                    tx_hash = trade.get("transactionHash")
                    if tx_hash and tx_hash not in seen_hashes:
                        seen_hashes.add(tx_hash)
                        all_trades.append(trade)
                        new_count += 1

                logger.info(f"  ✅ Got {new_count} new trades (total: {len(all_trades)})")

                # Advance cursor
                if chunk:
                    last_ts = chunk[-1].get("timestamp")
                    if last_ts:
                        current = int(last_ts) + 1
                    else:
                        current = window_end

                    # If window wasn't full, we can try a bigger window next time
                    if len(chunk) < 250 and window < 24 * 3600:
                        window = min(24 * 3600, window * 2)
                else:
                    # Empty window, jump forward
                    current = window_end

                # Rate limiting
                await asyncio.sleep(0.2)

            except Exception as e:
                logger.error(f"  ❌ Error: {e}")
                # On error, advance conservatively
                current = current + min_window
                await asyncio.sleep(2.0)
                continue

    return all_trades


async def main():
    import sys

    handle = None
    wallet = None

    # Parse arguments: --wallet <addr> or --handle <name> or default
    args = sys.argv[1:]
    if "--wallet" in args:
        idx = args.index("--wallet")
        wallet = args[idx + 1]
    elif "--handle" in args:
        idx = args.index("--handle")
        handle = args[idx + 1]
    else:
        handle = "gabagool22"

    # Resolve wallet from handle if needed
    if not wallet:
        async with GammaAPIClient() as gamma:
            wallet = await gamma.resolve_handle_to_wallet(handle)
        if not wallet:
            logger.error(f"Could not resolve {handle}")
            return

    logger.info(f"Wallet: {wallet}\n")

    # Define collection period
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=60)

    logger.info(f"Collection period: {start_date.date()} to {end_date.date()}\n")

    # Collect all trades
    trades = await collect_all_trades(wallet, start_date, end_date)

    # Save
    dir_name = handle if handle else wallet[:10]
    output_dir = Path(f"out/{dir_name}")
    output_dir.mkdir(parents=True, exist_ok=True)

    trades_file = output_dir / "activity_trades_all.jsonl"
    with open(trades_file, 'w') as f:
        for trade in trades:
            f.write(json.dumps(trade) + '\n')

    logger.info(f"\n💾 Saved {len(trades)} trades to {trades_file}")

    # Summary
    logger.info("\n" + "="*70)
    logger.info("SUMMARY")
    logger.info("="*70)
    logger.info(f"Total trades collected: {len(trades)}")

    if trades:
        timestamps = [t.get('timestamp') for t in trades if t.get('timestamp')]
        if timestamps:
            dates = [datetime.fromtimestamp(ts) for ts in timestamps]
            logger.info(f"Date range: {min(dates).date()} to {max(dates).date()}")

        # Count BUY vs SELL
        sides = {}
        for t in trades:
            side = t.get('side', 'UNKNOWN')
            sides[side] = sides.get(side, 0) + 1

        logger.info(f"BUY: {sides.get('BUY', 0)}")
        logger.info(f"SELL: {sides.get('SELL', 0)}")

    logger.info("="*70)


if __name__ == "__main__":
    asyncio.run(main())
