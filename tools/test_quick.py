#!/usr/bin/env python3
"""Quick test script to verify API access."""
import asyncio
import logging
from polymarket_collector.api import GammaAPIClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_profile_resolution():
    """Test resolving a handle to wallet."""
    handle = "gabagool22"

    async with GammaAPIClient() as client:
        try:
            logger.info(f"Testing profile resolution for: {handle}")
            wallet = await client.resolve_handle_to_wallet(handle)

            if wallet:
                logger.info(f"✅ Successfully resolved {handle} to wallet: {wallet}")
                return True
            else:
                logger.error(f"❌ Could not resolve handle: {handle}")
                return False

        except Exception as e:
            logger.error(f"❌ Error during resolution: {e}")
            return False


if __name__ == "__main__":
    result = asyncio.run(test_profile_resolution())
    exit(0 if result else 1)