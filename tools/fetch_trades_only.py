#!/usr/bin/env python3
"""Fetch only trades (all historical trades)."""
import asyncio
import json
import logging
from pathlib import Path
from datetime import datetime
from polymarket_collector.api import GammaAPIClient, DataAPIClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)


async def fetch_all_trades(user: str, output_dir: Path):
    """Fetch ALL trades with pagination."""
    all_trades = []
    offset = 0
    batch_size = 500  # API max
    consecutive_empty = 0
    seen_ids = set()  # Track duplicates

    logger.info("="*60)
    logger.info("Fetching ALL trades...")
    logger.info("="*60)

    async with DataAPIClient() as client:
        while offset < 20000:  # Safety limit
            logger.info(f"📥 offset {offset}...")

            try:
                trades = await client.fetch_trades(
                    user=user,
                    limit=batch_size,
                    offset=offset,
                    taker_only=False
                )

                if not trades or len(trades) == 0:
                    consecutive_empty += 1
                    logger.info(f"  Empty (consecutive: {consecutive_empty})")

                    if consecutive_empty >= 3:
                        logger.info("✅ Done - no more data")
                        break
                else:
                    consecutive_empty = 0

                    # Check for duplicates by transaction hash
                    new_ids = set(t.transaction_hash for t in trades if t.transaction_hash)
                    duplicates = new_ids & seen_ids

                    # Filter out duplicates
                    unique_trades = [t for t in trades if t.transaction_hash not in seen_ids]

                    if len(duplicates) == len(trades):
                        logger.info(f"  ⚠️ All {len(trades)} trades are duplicates! Stopping.")
                        break

                    if duplicates:
                        logger.info(f"  ⚠️ Found {len(duplicates)} duplicates, keeping {len(unique_trades)} new trades")

                    # Add unique trades
                    all_trades.extend(unique_trades)
                    seen_ids.update(new_ids)

                    logger.info(f"  ✅ {len(unique_trades)} new trades (total: {len(all_trades)})")

                    # Show date range
                    if trades[0].timestamp:
                        dates = [t.timestamp for t in trades if t.timestamp]
                        if dates:
                            logger.info(f"     Range: {min(dates).date()} to {max(dates).date()}")

                offset += batch_size
                await asyncio.sleep(1.0)

            except Exception as e:
                logger.error(f"❌ Error: {e}")
                break

    logger.info(f"\n📊 Total: {len(all_trades)} unique trades")

    # Save
    output_dir.mkdir(parents=True, exist_ok=True)
    trades_file = output_dir / "trades_all.jsonl"
    with open(trades_file, 'w') as f:
        for trade in all_trades:
            f.write(trade.model_dump_json(by_alias=True) + '\n')

    logger.info(f"💾 Saved to {trades_file}")
    return all_trades


async def main():
    import sys

    handle = None
    wallet = None

    args = sys.argv[1:]
    if "--wallet" in args:
        idx = args.index("--wallet")
        wallet = args[idx + 1]
    elif "--handle" in args:
        idx = args.index("--handle")
        handle = args[idx + 1]
    else:
        handle = "gabagool22"

    if not wallet:
        async with GammaAPIClient() as gamma:
            wallet = await gamma.resolve_handle_to_wallet(handle)
        if not wallet:
            logger.error(f"Could not resolve {handle}")
            return

    logger.info(f"Wallet: {wallet}\n")

    dir_name = handle if handle else wallet[:10]
    output_dir = Path(f"out/{dir_name}")

    # Fetch all trades
    trades = await fetch_all_trades(wallet, output_dir)

    # Summary
    logger.info("\n" + "="*60)
    logger.info(f"DONE: {len(trades)} trades")
    logger.info("="*60)


if __name__ == "__main__":
    asyncio.run(main())
