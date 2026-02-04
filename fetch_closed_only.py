#!/usr/bin/env python3
"""Fetch only closed positions (completed trades)."""
import asyncio
import json
import logging
from pathlib import Path
from datetime import datetime
from polymarket_collector.api import GammaAPIClient, DataAPIClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)


async def fetch_all_closed_positions(user: str, output_dir: Path):
    """Fetch ALL closed positions."""
    all_closed = []
    offset = 0
    batch_size = 50  # Start small to test
    consecutive_empty = 0

    logger.info("="*60)
    logger.info("Fetching ALL closed positions...")
    logger.info("="*60)

    async with DataAPIClient() as client:
        while offset < 10000:  # Safety limit
            logger.info(f"📥 offset {offset}...")

            try:
                closed = await client.fetch_closed_positions(
                    user=user,
                    limit=batch_size,
                    offset=offset
                )

                if not closed or len(closed) == 0:
                    consecutive_empty += 1
                    logger.info(f"  Empty (consecutive: {consecutive_empty})")

                    if consecutive_empty >= 3:
                        logger.info("✅ Done - no more data")
                        break
                else:
                    consecutive_empty = 0

                    # Check for duplicates
                    new_ids = set(c.condition_id for c in closed)
                    existing_ids = set(c.condition_id for c in all_closed)
                    duplicates = new_ids & existing_ids

                    if duplicates and len(duplicates) == len(closed):
                        logger.info(f"  ⚠️ All duplicates! Stopping.")
                        break

                    all_closed.extend(closed)
                    logger.info(f"  ✅ {len(closed)} positions (total: {len(all_closed)})")

                    # Show date range
                    if closed[0].timestamp:
                        dates = [datetime.fromtimestamp(c.timestamp) for c in closed if c.timestamp]
                        if dates:
                            logger.info(f"     Range: {min(dates).date()} to {max(dates).date()}")

                offset += batch_size
                await asyncio.sleep(1.0)

            except Exception as e:
                logger.error(f"❌ Error: {e}")
                break

    # Remove duplicates
    unique_closed = []
    seen_ids = set()
    for pos in all_closed:
        key = (pos.condition_id, pos.outcome_index)
        if key not in seen_ids:
            seen_ids.add(key)
            unique_closed.append(pos)

    logger.info(f"\n📊 Total: {len(unique_closed)} unique positions (removed {len(all_closed) - len(unique_closed)} duplicates)")

    # Save
    output_dir.mkdir(parents=True, exist_ok=True)
    closed_file = output_dir / "closed_positions_all.jsonl"
    with open(closed_file, 'w') as f:
        for pos in unique_closed:
            f.write(pos.model_dump_json(by_alias=True) + '\n')

    logger.info(f"💾 Saved to {closed_file}")
    return unique_closed


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

    # Fetch closed positions only
    closed = await fetch_all_closed_positions(wallet, output_dir)

    # Summary
    logger.info("\n" + "="*60)
    logger.info(f"DONE: {len(closed)} closed positions")
    logger.info("="*60)


if __name__ == "__main__":
    asyncio.run(main())