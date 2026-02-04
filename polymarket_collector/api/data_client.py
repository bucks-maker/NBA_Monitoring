"""Data API client for trades, activities, and positions."""
import logging
from typing import List, Optional, Dict, Any, AsyncGenerator
from datetime import datetime

from .base import BaseAPIClient
from ..models import Trade, Activity, Position, ClosedPosition

logger = logging.getLogger(__name__)


class DataAPIClient(BaseAPIClient):
    """Client for Data API (trades, activities, positions)."""

    def __init__(self, **kwargs):
        super().__init__(
            base_url="https://data-api.polymarket.com",
            **kwargs
        )

    async def fetch_trades(
        self,
        user: str,
        limit: int = 1000,
        offset: int = 0,
        taker_only: bool = False,
    ) -> List[Trade]:
        """Fetch trades for a user with pagination."""
        logger.info(f"Fetching trades for user {user} (offset={offset}, limit={limit})")

        response = await self.get(
            "/trades",
            params={
                "user": user,
                "limit": limit,
                "offset": offset,
                "takerOnly": str(taker_only).lower(),
            },
        )

        # Response is typically a list of trades
        trades = []
        if isinstance(response, list):
            for trade_data in response:
                try:
                    # Add proxy wallet if not present
                    if "proxyWallet" not in trade_data:
                        trade_data["proxyWallet"] = user
                    trades.append(Trade(**trade_data))
                except Exception as e:
                    logger.error(f"Failed to parse trade: {e}")
                    logger.debug(f"Trade data: {trade_data}")

        logger.info(f"Fetched {len(trades)} trades")
        return trades

    async def fetch_all_trades(
        self,
        user: str,
        batch_size: int = 1000,
        taker_only: bool = False,
    ) -> AsyncGenerator[List[Trade], None]:
        """Fetch all trades for a user with pagination."""
        offset = 0
        total_fetched = 0

        while True:
            trades = await self.fetch_trades(
                user=user,
                limit=batch_size,
                offset=offset,
                taker_only=taker_only,
            )

            if not trades:
                logger.info(f"No more trades. Total fetched: {total_fetched}")
                break

            total_fetched += len(trades)
            logger.info(f"Progress: fetched {total_fetched} trades total")

            yield trades

            # Check if we got less than requested (last page)
            if len(trades) < batch_size:
                logger.info(f"Last page reached. Total fetched: {total_fetched}")
                break

            offset += batch_size

    async def fetch_activity(
        self,
        user: str,
        limit: int = 1000,
        offset: int = 0,
        activity_types: Optional[List[str]] = None,
    ) -> List[Activity]:
        """Fetch activity for a user."""
        if activity_types is None:
            activity_types = ["TRADE", "SPLIT", "MERGE", "REDEEM", "REWARD", "CONVERSION"]

        logger.info(f"Fetching activity for user {user} (offset={offset}, limit={limit})")

        params = {
            "user": user,
            "limit": limit,
            "offset": offset,
        }

        # Add type filter if specified
        if activity_types:
            params["type"] = ",".join(activity_types)

        response = await self.get("/activity", params=params)

        # Parse activities
        activities = []
        if isinstance(response, list):
            for activity_data in response:
                try:
                    activities.append(Activity(**activity_data))
                except Exception as e:
                    logger.error(f"Failed to parse activity: {e}")
                    logger.debug(f"Activity data: {activity_data}")

        logger.info(f"Fetched {len(activities)} activities")
        return activities

    async def fetch_all_activity(
        self,
        user: str,
        batch_size: int = 1000,
        activity_types: Optional[List[str]] = None,
    ) -> AsyncGenerator[List[Activity], None]:
        """Fetch all activity for a user with pagination."""
        offset = 0
        total_fetched = 0

        while True:
            activities = await self.fetch_activity(
                user=user,
                limit=batch_size,
                offset=offset,
                activity_types=activity_types,
            )

            if not activities:
                logger.info(f"No more activities. Total fetched: {total_fetched}")
                break

            total_fetched += len(activities)
            logger.info(f"Progress: fetched {total_fetched} activities total")

            yield activities

            # Check if we got less than requested (last page)
            if len(activities) < batch_size:
                logger.info(f"Last page reached. Total fetched: {total_fetched}")
                break

            offset += batch_size

    async def fetch_positions(
        self,
        user: str,
        limit: int = 500,
        offset: int = 0,
    ) -> List[Position]:
        """Fetch open positions for a user."""
        logger.info(f"Fetching positions for user {user} (offset={offset}, limit={limit})")

        response = await self.get(
            "/positions",
            params={
                "user": user,
                "limit": limit,
                "offset": offset,
            },
        )

        # Parse positions
        positions = []
        if isinstance(response, list):
            for position_data in response:
                try:
                    positions.append(Position(**position_data))
                except Exception as e:
                    logger.error(f"Failed to parse position: {e}")
                    logger.debug(f"Position data: {position_data}")

        logger.info(f"Fetched {len(positions)} positions")
        return positions

    async def fetch_all_positions(
        self,
        user: str,
        batch_size: int = 500,
    ) -> AsyncGenerator[List[Position], None]:
        """Fetch all open positions for a user with pagination."""
        offset = 0
        total_fetched = 0

        while True:
            positions = await self.fetch_positions(
                user=user,
                limit=batch_size,
                offset=offset,
            )

            if not positions:
                logger.info(f"No more positions. Total fetched: {total_fetched}")
                break

            total_fetched += len(positions)
            logger.info(f"Progress: fetched {total_fetched} positions total")

            yield positions

            # Check if we got less than requested (last page)
            if len(positions) < batch_size:
                logger.info(f"Last page reached. Total fetched: {total_fetched}")
                break

            offset += batch_size

    async def fetch_closed_positions(
        self,
        user: str,
        limit: int = 500,
        offset: int = 0,
    ) -> List[ClosedPosition]:
        """Fetch closed positions for a user."""
        logger.info(f"Fetching closed positions for user {user} (offset={offset}, limit={limit})")

        response = await self.get(
            "/closed-positions",
            params={
                "user": user,
                "limit": limit,
                "offset": offset,
            },
        )

        # Parse closed positions
        closed_positions = []
        if isinstance(response, list):
            for position_data in response:
                try:
                    closed_positions.append(ClosedPosition(**position_data))
                except Exception as e:
                    logger.error(f"Failed to parse closed position: {e}")
                    logger.debug(f"Closed position data: {position_data}")

        logger.info(f"Fetched {len(closed_positions)} closed positions")
        return closed_positions

    async def fetch_all_closed_positions(
        self,
        user: str,
        batch_size: int = 500,
    ) -> AsyncGenerator[List[ClosedPosition], None]:
        """Fetch all closed positions for a user with pagination."""
        offset = 0
        total_fetched = 0

        while True:
            closed_positions = await self.fetch_closed_positions(
                user=user,
                limit=batch_size,
                offset=offset,
            )

            if not closed_positions:
                logger.info(f"No more closed positions. Total fetched: {total_fetched}")
                break

            total_fetched += len(closed_positions)
            logger.info(f"Progress: fetched {total_fetched} closed positions total")

            yield closed_positions

            # Check if we got less than requested (last page)
            if len(closed_positions) < batch_size:
                logger.info(f"Last page reached. Total fetched: {total_fetched}")
                break

            offset += batch_size