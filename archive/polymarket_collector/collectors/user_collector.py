"""User data collector for fetching all Polymarket data."""
import asyncio
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List
import re

from ..api import GammaAPIClient, DataAPIClient
from ..models import Trade, Activity, Position, ClosedPosition
from ..utils.storage import DataStorage

logger = logging.getLogger(__name__)


class UserDataCollector:
    """Collects all data for a Polymarket user."""

    def __init__(self, output_dir: Path):
        self.output_dir = Path(output_dir)
        self.storage = DataStorage(self.output_dir)
        self.metadata: Dict[str, Any] = {
            "collection_started": None,
            "collection_completed": None,
            "total_trades": 0,
            "total_activities": 0,
            "total_positions": 0,
            "total_closed_positions": 0,
            "errors": [],
        }

    def extract_handle_from_url(self, url: str) -> Optional[str]:
        """Extract handle from Polymarket profile URL."""
        # Pattern: https://polymarket.com/@handle or @handle?tab=...
        pattern = r"@([^/?]+)"
        match = re.search(pattern, url)
        if match:
            return match.group(1)
        return None

    async def resolve_user(
        self,
        profile_url: Optional[str] = None,
        handle: Optional[str] = None,
        wallet: Optional[str] = None,
    ) -> str:
        """Resolve user handle or wallet address to proxy wallet address."""
        # If wallet address is provided directly, skip Gamma API resolution
        if wallet:
            wallet = wallet.lower().strip()
            dir_name = wallet[:10]  # Use first 10 chars as directory name
            logger.info(f"Using wallet address directly: {wallet}")

            profile_info = {
                "handle": None,
                "profile_url": None,
                "proxy_wallet": wallet,
                "resolved_at": datetime.utcnow().isoformat(),
            }

            # Create output directory for this wallet
            self.output_dir = self.output_dir / dir_name
            self.output_dir.mkdir(parents=True, exist_ok=True)

            # Update storage with new directory
            self.storage = DataStorage(self.output_dir)

            # Save profile info
            with open(self.output_dir / "resolved_profile.json", "w") as f:
                json.dump(profile_info, f, indent=2)

            return wallet

        # Extract handle from URL if provided
        if profile_url and not handle:
            handle = self.extract_handle_from_url(profile_url)
            if not handle:
                raise ValueError(f"Could not extract handle from URL: {profile_url}")

        if not handle:
            raise ValueError("Either profile_url, handle, or wallet must be provided")

        logger.info(f"Resolving handle: {handle}")

        # Use Gamma API to resolve handle to wallet
        async with GammaAPIClient() as gamma_client:
            proxy_wallet = await gamma_client.resolve_handle_to_wallet(handle)

            if not proxy_wallet:
                raise ValueError(f"Could not resolve handle '{handle}' to wallet address")

            # Save resolved profile info
            profile_info = {
                "handle": handle,
                "profile_url": profile_url or f"https://polymarket.com/@{handle}",
                "proxy_wallet": proxy_wallet,
                "resolved_at": datetime.utcnow().isoformat(),
            }

            # Create output directory for this user
            self.output_dir = self.output_dir / handle
            self.output_dir.mkdir(parents=True, exist_ok=True)

            # Update storage with new directory
            self.storage = DataStorage(self.output_dir)

            # Save profile info
            with open(self.output_dir / "resolved_profile.json", "w") as f:
                json.dump(profile_info, f, indent=2)

            logger.info(f"Resolved to wallet: {proxy_wallet}")
            return proxy_wallet

    async def fetch_trades(self, user: str) -> int:
        """Fetch all trades for a user."""
        logger.info(f"Fetching trades for user: {user}")
        total_trades = 0

        async with DataAPIClient() as data_client:
            # Open file for appending JSONL
            trades_file = self.output_dir / "trades_raw.jsonl"

            with open(trades_file, "w") as f:
                async for trades_batch in data_client.fetch_all_trades(user):
                    # Save raw data
                    for trade in trades_batch:
                        f.write(trade.model_dump_json(by_alias=True) + "\n")

                    total_trades += len(trades_batch)
                    logger.info(f"Saved {len(trades_batch)} trades (total: {total_trades})")

        self.metadata["total_trades"] = total_trades
        return total_trades

    async def fetch_activity(self, user: str) -> int:
        """Fetch all activity for a user."""
        logger.info(f"Fetching activity for user: {user}")
        total_activities = 0

        async with DataAPIClient() as data_client:
            # Open file for appending JSONL
            activity_file = self.output_dir / "activity_raw.jsonl"

            with open(activity_file, "w") as f:
                async for activity_batch in data_client.fetch_all_activity(user):
                    # Save raw data
                    for activity in activity_batch:
                        f.write(activity.model_dump_json(by_alias=True) + "\n")

                    total_activities += len(activity_batch)
                    logger.info(f"Saved {len(activity_batch)} activities (total: {total_activities})")

        self.metadata["total_activities"] = total_activities
        return total_activities

    async def fetch_positions(self, user: str) -> int:
        """Fetch all open positions for a user."""
        logger.info(f"Fetching positions for user: {user}")
        all_positions = []

        async with DataAPIClient() as data_client:
            async for positions_batch in data_client.fetch_all_positions(user):
                all_positions.extend(positions_batch)
                logger.info(f"Fetched {len(positions_batch)} positions (total: {len(all_positions)})")

        # Save all positions to a single JSON file
        positions_file = self.output_dir / "positions_raw.json"
        with open(positions_file, "w") as f:
            json.dump(
                [pos.model_dump(by_alias=True) for pos in all_positions],
                f,
                indent=2,
                default=str,
            )

        self.metadata["total_positions"] = len(all_positions)
        return len(all_positions)

    async def fetch_closed_positions(self, user: str) -> int:
        """Fetch all closed positions for a user."""
        logger.info(f"Fetching closed positions for user: {user}")
        total_closed = 0

        async with DataAPIClient() as data_client:
            # Open file for appending JSONL
            closed_file = self.output_dir / "closed_positions_raw.jsonl"

            with open(closed_file, "w") as f:
                async for closed_batch in data_client.fetch_all_closed_positions(user):
                    # Save raw data
                    for closed_pos in closed_batch:
                        f.write(closed_pos.model_dump_json(by_alias=True) + "\n")

                    total_closed += len(closed_batch)
                    logger.info(f"Saved {len(closed_batch)} closed positions (total: {total_closed})")

        self.metadata["total_closed_positions"] = total_closed
        return total_closed

    async def collect_all_data(
        self,
        profile_url: Optional[str] = None,
        handle: Optional[str] = None,
        wallet: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Collect all data for a user."""
        self.metadata["collection_started"] = datetime.utcnow().isoformat()

        try:
            # Resolve user to wallet address
            proxy_wallet = await self.resolve_user(profile_url, handle, wallet)
            self.metadata["proxy_wallet"] = proxy_wallet
            self.metadata["handle"] = self.output_dir.name

            # Fetch all data types concurrently with small delays
            logger.info("Starting data collection...")

            # Run fetches with small delays between them to avoid rate limits
            trades_task = asyncio.create_task(self.fetch_trades(proxy_wallet))
            await asyncio.sleep(1)  # Small delay

            activity_task = asyncio.create_task(self.fetch_activity(proxy_wallet))
            await asyncio.sleep(1)  # Small delay

            positions_task = asyncio.create_task(self.fetch_positions(proxy_wallet))
            await asyncio.sleep(1)  # Small delay

            closed_task = asyncio.create_task(self.fetch_closed_positions(proxy_wallet))

            # Wait for all tasks to complete
            results = await asyncio.gather(
                trades_task,
                activity_task,
                positions_task,
                closed_task,
                return_exceptions=True,
            )

            # Check for errors
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    error_msg = f"Task {i} failed: {str(result)}"
                    logger.error(error_msg)
                    self.metadata["errors"].append(error_msg)

            logger.info("Data collection completed")

        except Exception as e:
            error_msg = f"Collection failed: {str(e)}"
            logger.error(error_msg)
            self.metadata["errors"].append(error_msg)
            raise

        finally:
            # Save metadata
            self.metadata["collection_completed"] = datetime.utcnow().isoformat()
            metadata_file = self.output_dir / "metadata.json"
            with open(metadata_file, "w") as f:
                json.dump(self.metadata, f, indent=2)

        # Normalize and save data in various formats
        logger.info("Normalizing and saving data...")
        await self.storage.normalize_and_save()

        return self.metadata