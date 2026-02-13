#!/usr/bin/env python3
"""Fetch one week of data for a user."""
import asyncio
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta
from polymarket_collector.api import GammaAPIClient, DataAPIClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def fetch_week_trades(user: str, output_dir: Path, days: int = 7):
    """Fetch trades from last N days."""
    output_dir.mkdir(parents=True, exist_ok=True)

    # Calculate cutoff time
    from datetime import timezone
    cutoff_time = datetime.now(timezone.utc) - timedelta(days=days)
    logger.info(f"Fetching trades since: {cutoff_time} (last {days} days)")

    all_trades = []
    offset = 0
    batch_size = 500  # API max is 500
    reached_cutoff = False

    async with DataAPIClient() as client:
        while not reached_cutoff:
            logger.info(f"Fetching batch at offset {offset}...")
            trades = await client.fetch_trades(
                user=user,
                limit=batch_size,
                offset=offset,
                taker_only=False
            )

            if not trades:
                logger.info("No more trades available from API")
                break

            # Check timestamps
            oldest_in_batch = min(trade.timestamp for trade in trades)
            newest_in_batch = max(trade.timestamp for trade in trades)

            logger.info(f"  Batch range: {oldest_in_batch} to {newest_in_batch}")

            # Filter trades within time window
            recent_trades = [t for t in trades if t.timestamp >= cutoff_time]
            all_trades.extend(recent_trades)

            logger.info(f"  Added {len(recent_trades)}/{len(trades)} trades (total: {len(all_trades)})")

            # Check if we've gone past the cutoff
            if oldest_in_batch < cutoff_time:
                logger.info(f"Reached trades older than {days} days, stopping")
                reached_cutoff = True
                break

            # If we got less than requested, we've reached the end
            if len(trades) < batch_size:
                logger.info("Reached end of available data")
                break

            offset += batch_size
            await asyncio.sleep(1.0)  # Rate limiting - be conservative

    # Save all trades
    trades_file = output_dir / "trades_week.jsonl"
    with open(trades_file, 'w') as f:
        for trade in all_trades:
            f.write(trade.model_dump_json(by_alias=True) + '\n')

    logger.info(f"Saved {len(all_trades)} trades to {trades_file}")
    return all_trades


async def fetch_all_closed_positions(user: str, output_dir: Path):
    """Fetch ALL closed positions (no time limit) - keep going until no more data."""
    all_closed = []
    offset = 0
    batch_size = 500  # Max API limit
    consecutive_empty = 0
    max_offset = 100000  # Safety limit to prevent infinite loop

    logger.info("="*60)
    logger.info("Starting to fetch ALL closed positions...")
    logger.info("This may take a while if there are many positions")
    logger.info("="*60)

    async with DataAPIClient() as client:
        while offset < max_offset:
            logger.info(f"📥 Fetching closed positions at offset {offset}...")

            try:
                closed = await client.fetch_closed_positions(
                    user=user,
                    limit=batch_size,
                    offset=offset
                )

                if not closed or len(closed) == 0:
                    consecutive_empty += 1
                    logger.info(f"  ⚠️  Empty batch (consecutive: {consecutive_empty})")

                    if consecutive_empty >= 3:
                        logger.info("No more closed positions after 3 empty batches")
                        break
                else:
                    consecutive_empty = 0
                    all_closed.extend(closed)
                    logger.info(f"  ✅ Got {len(closed)} positions (total: {len(all_closed)})")

                # Always continue to next offset
                offset += batch_size
                await asyncio.sleep(1.0)  # Rate limiting

            except Exception as e:
                logger.error(f"Error at offset {offset}: {e}")
                offset += batch_size
                await asyncio.sleep(2.0)
                continue

    # Save all closed positions
    closed_file = output_dir / "closed_positions_all.jsonl"
    with open(closed_file, 'w') as f:
        for pos in all_closed:
            f.write(pos.model_dump_json(by_alias=True) + '\n')

    logger.info(f"Saved {len(all_closed)} closed positions to {closed_file}")
    return all_closed


async def main():
    # Resolve gabagool22
    handle = "gabagool22"

    async with GammaAPIClient() as gamma:
        wallet = await gamma.resolve_handle_to_wallet(handle)

    if not wallet:
        logger.error(f"Could not resolve {handle}")
        return

    logger.info(f"Resolved {handle} -> {wallet}")

    output_dir = Path(f"out/{handle}")

    # Fetch trades (last 7 days)
    trades = await fetch_week_trades(wallet, output_dir)

    # Fetch ALL closed positions
    closed = await fetch_all_closed_positions(wallet, output_dir)

    # Summary
    logger.info("\n" + "="*60)
    logger.info(f"SUMMARY:")
    logger.info(f"  Trades (last 7 days): {len(trades)}")
    logger.info(f"  Closed Positions (all time): {len(closed)}")
    logger.info("="*60)


if __name__ == "__main__":
    asyncio.run(main())