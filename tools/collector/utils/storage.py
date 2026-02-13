"""Data storage and normalization utilities."""
import json
import logging
from pathlib import Path
from typing import List, Dict, Any
import pandas as pd
from datetime import datetime

from ..models import (
    Trade,
    Activity,
    Position,
    ClosedPosition,
    NormalizedTrade,
    NormalizedActivity,
    NormalizedPosition,
)

logger = logging.getLogger(__name__)


class DataStorage:
    """Handles data normalization and storage in multiple formats."""

    def __init__(self, output_dir: Path):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def load_jsonl(self, filename: str) -> List[Dict[str, Any]]:
        """Load data from JSONL file."""
        filepath = self.output_dir / filename
        if not filepath.exists():
            logger.warning(f"File not found: {filepath}")
            return []

        data = []
        with open(filepath, "r") as f:
            for line in f:
                if line.strip():
                    data.append(json.loads(line))
        return data

    def load_json(self, filename: str) -> List[Dict[str, Any]]:
        """Load data from JSON file."""
        filepath = self.output_dir / filename
        if not filepath.exists():
            logger.warning(f"File not found: {filepath}")
            return []

        with open(filepath, "r") as f:
            data = json.load(f)
            if not isinstance(data, list):
                data = [data]
        return data

    def normalize_trades(self) -> pd.DataFrame:
        """Normalize trades data into DataFrame."""
        logger.info("Normalizing trades...")

        raw_trades = self.load_jsonl("trades_raw.jsonl")
        if not raw_trades:
            logger.warning("No trades to normalize")
            return pd.DataFrame()

        normalized = []
        for trade_data in raw_trades:
            try:
                # Parse with model for validation
                trade = Trade(**trade_data)

                # Convert to normalized format
                norm = NormalizedTrade(
                    timestamp=trade.timestamp,
                    transaction_hash=trade.transaction_hash,
                    condition_id=trade.condition_id,
                    slug=trade.slug,
                    event_slug=trade.event_slug,
                    outcome=trade.outcome,
                    outcome_index=trade.outcome_index,
                    side=trade.side,
                    size=float(trade.size) if trade.size else 0.0,
                    price=float(trade.price) if trade.price else 0.0,
                    usdc_size=float(trade.usdc_size) if trade.usdc_size else None,
                    proxy_wallet=trade.proxy_wallet or "",
                )
                normalized.append(norm.model_dump())
            except Exception as e:
                logger.error(f"Failed to normalize trade: {e}")
                continue

        df = pd.DataFrame(normalized)
        if not df.empty:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df = df.sort_values("timestamp")

        logger.info(f"Normalized {len(df)} trades")
        return df

    def normalize_activity(self) -> pd.DataFrame:
        """Normalize activity data into DataFrame."""
        logger.info("Normalizing activity...")

        raw_activity = self.load_jsonl("activity_raw.jsonl")
        if not raw_activity:
            logger.warning("No activity to normalize")
            return pd.DataFrame()

        normalized = []
        for activity_data in raw_activity:
            try:
                # Parse with model for validation
                activity = Activity(**activity_data)

                # Get proxy wallet from metadata if available
                metadata = self.load_json("metadata.json")
                proxy_wallet = ""
                if metadata and isinstance(metadata, list):
                    proxy_wallet = metadata[0].get("proxy_wallet", "")

                # Convert to normalized format
                norm = NormalizedActivity(
                    timestamp=activity.timestamp,
                    type=activity.type,
                    condition_id=activity.condition_id,
                    transaction_hash=activity.transaction_hash,
                    side=activity.side,
                    size=float(activity.size) if activity.size else None,
                    usdc_size=float(activity.usdc_size) if activity.usdc_size else None,
                    proxy_wallet=proxy_wallet,
                )
                normalized.append(norm.model_dump())
            except Exception as e:
                logger.error(f"Failed to normalize activity: {e}")
                continue

        df = pd.DataFrame(normalized)
        if not df.empty:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df = df.sort_values("timestamp")

        logger.info(f"Normalized {len(df)} activities")
        return df

    def normalize_positions(self) -> pd.DataFrame:
        """Normalize positions (open and closed) into DataFrame."""
        logger.info("Normalizing positions...")

        # Get proxy wallet from metadata
        metadata = self.load_json("metadata.json")
        proxy_wallet = ""
        if metadata and isinstance(metadata, list):
            proxy_wallet = metadata[0].get("proxy_wallet", "")

        normalized = []

        # Process open positions
        open_positions = self.load_json("positions_raw.json")
        for pos_data in open_positions:
            try:
                pos = Position(**pos_data)
                norm = NormalizedPosition(
                    condition_id=pos.condition_id,
                    slug=pos.slug,
                    event_slug=pos.event_slug,
                    outcome=pos.outcome,
                    outcome_index=pos.outcome_index,
                    size=float(pos.size) if pos.size else 0.0,
                    average_price=float(pos.average_price) if pos.average_price else None,
                    usdc_value=float(pos.usdc_value) if pos.usdc_value else None,
                    unrealized_pnl=float(pos.unrealized_pnl) if pos.unrealized_pnl else None,
                    realized_pnl=None,
                    is_closed=False,
                    close_timestamp=None,
                    proxy_wallet=proxy_wallet,
                )
                normalized.append(norm.model_dump())
            except Exception as e:
                logger.error(f"Failed to normalize position: {e}")

        # Process closed positions
        closed_positions = self.load_jsonl("closed_positions_raw.jsonl")
        for pos_data in closed_positions:
            try:
                pos = ClosedPosition(**pos_data)
                norm = NormalizedPosition(
                    condition_id=pos.condition_id,
                    slug=pos.slug,
                    event_slug=pos.event_slug,
                    outcome=pos.outcome,
                    outcome_index=pos.outcome_index,
                    size=float(pos.size) if pos.size else 0.0,
                    average_price=float(pos.average_price) if pos.average_price else None,
                    usdc_value=None,
                    unrealized_pnl=None,
                    realized_pnl=float(pos.realized_pnl) if pos.realized_pnl else None,
                    is_closed=True,
                    close_timestamp=pos.close_timestamp,
                    proxy_wallet=proxy_wallet,
                )
                normalized.append(norm.model_dump())
            except Exception as e:
                logger.error(f"Failed to normalize closed position: {e}")

        df = pd.DataFrame(normalized)
        if not df.empty and "close_timestamp" in df.columns:
            df["close_timestamp"] = pd.to_datetime(df["close_timestamp"])

        logger.info(f"Normalized {len(df)} positions")
        return df

    async def normalize_and_save(self):
        """Normalize all data and save in multiple formats."""
        logger.info("Starting normalization and saving...")

        # Normalize trades
        trades_df = self.normalize_trades()
        if not trades_df.empty:
            # Save as CSV
            trades_df.to_csv(self.output_dir / "trades.csv", index=False)
            # Save as Parquet
            trades_df.to_parquet(self.output_dir / "trades.parquet", index=False)
            logger.info(f"Saved {len(trades_df)} trades")

        # Normalize activity
        activity_df = self.normalize_activity()
        if not activity_df.empty:
            # Save as CSV
            activity_df.to_csv(self.output_dir / "activity.csv", index=False)
            # Save as Parquet
            activity_df.to_parquet(self.output_dir / "activity.parquet", index=False)
            logger.info(f"Saved {len(activity_df)} activities")

        # Normalize positions
        positions_df = self.normalize_positions()
        if not positions_df.empty:
            # Save as CSV
            positions_df.to_csv(self.output_dir / "positions.csv", index=False)
            # Save as Parquet
            positions_df.to_parquet(self.output_dir / "positions.parquet", index=False)
            logger.info(f"Saved {len(positions_df)} positions")

        logger.info("Normalization and saving completed")