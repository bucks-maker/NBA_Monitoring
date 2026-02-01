"""Report analyzer for generating trading analysis reports."""
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple, Optional
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class ReportAnalyzer:
    """Generates analysis reports from collected data."""

    def __init__(self, data_dir: Path):
        self.data_dir = Path(data_dir)
        self.trades_df = None
        self.activity_df = None
        self.positions_df = None
        self.metadata = None

    def load_data(self):
        """Load all data files."""
        logger.info(f"Loading data from {self.data_dir}")

        # Load metadata
        metadata_file = self.data_dir / "metadata.json"
        if metadata_file.exists():
            with open(metadata_file, "r") as f:
                self.metadata = json.load(f)

        # Load trades
        trades_file = self.data_dir / "trades.parquet"
        if trades_file.exists():
            self.trades_df = pd.read_parquet(trades_file)
        elif (self.data_dir / "trades.csv").exists():
            self.trades_df = pd.read_csv(self.data_dir / "trades.csv")
            if "timestamp" in self.trades_df.columns:
                self.trades_df["timestamp"] = pd.to_datetime(self.trades_df["timestamp"])

        # Load activity
        activity_file = self.data_dir / "activity.parquet"
        if activity_file.exists():
            self.activity_df = pd.read_parquet(activity_file)
        elif (self.data_dir / "activity.csv").exists():
            self.activity_df = pd.read_csv(self.data_dir / "activity.csv")
            if "timestamp" in self.activity_df.columns:
                self.activity_df["timestamp"] = pd.to_datetime(self.activity_df["timestamp"])

        # Load positions
        positions_file = self.data_dir / "positions.parquet"
        if positions_file.exists():
            self.positions_df = pd.read_parquet(positions_file)
        elif (self.data_dir / "positions.csv").exists():
            self.positions_df = pd.read_csv(self.data_dir / "positions.csv")

        logger.info("Data loaded successfully")

    def analyze_basic_stats(self) -> Dict[str, Any]:
        """Calculate basic statistics."""
        stats = {
            "total_trades": 0,
            "total_activities": 0,
            "total_positions": 0,
            "total_closed_positions": 0,
            "first_trade_time": None,
            "last_trade_time": None,
            "unique_markets": 0,
            "unique_events": 0,
            "buy_trades": 0,
            "sell_trades": 0,
            "buy_sell_ratio": 0,
            "total_volume_usdc": 0,
        }

        if self.trades_df is not None and not self.trades_df.empty:
            stats["total_trades"] = len(self.trades_df)
            stats["first_trade_time"] = self.trades_df["timestamp"].min()
            stats["last_trade_time"] = self.trades_df["timestamp"].max()
            stats["unique_markets"] = self.trades_df["condition_id"].nunique()
            stats["unique_events"] = self.trades_df["event_slug"].nunique()

            # Buy/Sell analysis
            stats["buy_trades"] = len(self.trades_df[self.trades_df["side"] == "BUY"])
            stats["sell_trades"] = len(self.trades_df[self.trades_df["side"] == "SELL"])
            if stats["sell_trades"] > 0:
                stats["buy_sell_ratio"] = stats["buy_trades"] / stats["sell_trades"]

            # Volume
            if "usdc_size" in self.trades_df.columns:
                stats["total_volume_usdc"] = self.trades_df["usdc_size"].sum()

        if self.activity_df is not None:
            stats["total_activities"] = len(self.activity_df)

        if self.positions_df is not None:
            stats["total_positions"] = len(self.positions_df[~self.positions_df["is_closed"]])
            stats["total_closed_positions"] = len(self.positions_df[self.positions_df["is_closed"]])

        return stats

    def analyze_top_markets(self, n: int = 10) -> pd.DataFrame:
        """Analyze top markets by trade count and volume."""
        if self.trades_df is None or self.trades_df.empty:
            return pd.DataFrame()

        market_stats = self.trades_df.groupby(["condition_id", "slug", "event_slug"]).agg({
            "transaction_hash": "count",  # Trade count
            "usdc_size": "sum",  # Total volume
            "price": "mean",  # Average price
            "side": lambda x: (x == "BUY").sum(),  # Buy count
        }).round(2)

        market_stats.columns = ["trade_count", "total_volume", "avg_price", "buy_count"]
        market_stats["sell_count"] = market_stats["trade_count"] - market_stats["buy_count"]

        # Sort by trade count
        top_markets = market_stats.nlargest(n, "trade_count")
        return top_markets

    def detect_scalping_patterns(self) -> List[Dict[str, Any]]:
        """Detect potential scalping/momentum trading patterns."""
        patterns = []

        if self.trades_df is None or self.trades_df.empty:
            return patterns

        # Group by market
        for condition_id, market_trades in self.trades_df.groupby("condition_id"):
            market_trades = market_trades.sort_values("timestamp")

            # Look for rapid consecutive trades (within 1 hour)
            if len(market_trades) < 2:
                continue

            # Calculate time differences
            time_diffs = market_trades["timestamp"].diff()

            # Find rapid trade sequences
            rapid_trades_mask = time_diffs <= timedelta(hours=1)
            rapid_sequences = []

            current_sequence = []
            for idx, is_rapid in enumerate(rapid_trades_mask):
                if is_rapid:
                    if not current_sequence:
                        current_sequence.append(idx - 1)  # Include previous trade
                    current_sequence.append(idx)
                else:
                    if len(current_sequence) >= 3:  # At least 3 trades in sequence
                        rapid_sequences.append(current_sequence)
                    current_sequence = []

            if len(current_sequence) >= 3:
                rapid_sequences.append(current_sequence)

            # Analyze each rapid sequence
            for sequence in rapid_sequences:
                sequence_trades = market_trades.iloc[sequence]
                buy_count = (sequence_trades["side"] == "BUY").sum()
                sell_count = (sequence_trades["side"] == "SELL").sum()

                if buy_count > 0 and sell_count > 0:  # Both buy and sell in sequence
                    pattern = {
                        "type": "scalping_candidate",
                        "condition_id": condition_id,
                        "slug": sequence_trades.iloc[0]["slug"],
                        "trade_count": len(sequence_trades),
                        "duration_minutes": (
                            sequence_trades["timestamp"].max() - sequence_trades["timestamp"].min()
                        ).total_seconds() / 60,
                        "buy_count": buy_count,
                        "sell_count": sell_count,
                        "avg_price": sequence_trades["price"].mean(),
                        "price_range": sequence_trades["price"].max() - sequence_trades["price"].min(),
                        "start_time": sequence_trades["timestamp"].min(),
                        "end_time": sequence_trades["timestamp"].max(),
                    }
                    patterns.append(pattern)

        return patterns

    def detect_delta_neutral_patterns(self) -> List[Dict[str, Any]]:
        """Detect potential delta-neutral/hedge trading patterns."""
        patterns = []

        if self.trades_df is None or self.trades_df.empty:
            return patterns

        # Group by event to find opposite outcome trading
        for event_slug, event_trades in self.trades_df.groupby("event_slug"):
            if pd.isna(event_slug):
                continue

            # Get unique outcomes
            unique_outcomes = event_trades["outcome_index"].dropna().unique()

            if len(unique_outcomes) >= 2:  # Trading multiple outcomes
                # Check time proximity of trades
                time_window = timedelta(hours=24)  # Within 24 hours

                # Group trades by time windows
                event_trades = event_trades.sort_values("timestamp")

                for i in range(len(event_trades)):
                    window_start = event_trades.iloc[i]["timestamp"]
                    window_end = window_start + time_window

                    window_trades = event_trades[
                        (event_trades["timestamp"] >= window_start) &
                        (event_trades["timestamp"] <= window_end)
                    ]

                    unique_outcomes_in_window = window_trades["outcome_index"].dropna().unique()

                    if len(unique_outcomes_in_window) >= 2:
                        # Calculate positions per outcome
                        outcome_positions = {}
                        for outcome in unique_outcomes_in_window:
                            outcome_trades = window_trades[window_trades["outcome_index"] == outcome]
                            buy_size = outcome_trades[outcome_trades["side"] == "BUY"]["size"].sum()
                            sell_size = outcome_trades[outcome_trades["side"] == "SELL"]["size"].sum()
                            net_position = buy_size - sell_size
                            outcome_positions[int(outcome)] = net_position

                        # Check if positions are balanced (potential hedge)
                        positions = list(outcome_positions.values())
                        if len(positions) >= 2 and all(p > 0 for p in positions):
                            pattern = {
                                "type": "delta_neutral_candidate",
                                "event_slug": event_slug,
                                "outcomes_traded": len(unique_outcomes_in_window),
                                "outcome_positions": outcome_positions,
                                "total_trades": len(window_trades),
                                "time_window_start": window_start,
                                "time_window_end": window_trades["timestamp"].max(),
                                "total_volume": window_trades["usdc_size"].sum() if "usdc_size" in window_trades else 0,
                            }
                            patterns.append(pattern)
                            break  # Avoid duplicate detection

        # Deduplicate patterns
        unique_patterns = []
        seen = set()
        for pattern in patterns:
            key = (pattern["event_slug"], pattern["time_window_start"].isoformat())
            if key not in seen:
                seen.add(key)
                unique_patterns.append(pattern)

        return unique_patterns

    def analyze_pnl(self) -> Dict[str, Any]:
        """Analyze profit and loss from closed positions."""
        pnl_stats = {
            "total_realized_pnl": 0,
            "winning_positions": 0,
            "losing_positions": 0,
            "win_rate": 0,
            "best_trade": None,
            "worst_trade": None,
            "avg_win": 0,
            "avg_loss": 0,
        }

        if self.positions_df is None or self.positions_df.empty:
            return pnl_stats

        closed = self.positions_df[self.positions_df["is_closed"]]

        if closed.empty or "realized_pnl" not in closed.columns:
            return pnl_stats

        # Filter valid PnL values
        valid_pnl = closed.dropna(subset=["realized_pnl"])

        if not valid_pnl.empty:
            pnl_stats["total_realized_pnl"] = valid_pnl["realized_pnl"].sum()
            pnl_stats["winning_positions"] = (valid_pnl["realized_pnl"] > 0).sum()
            pnl_stats["losing_positions"] = (valid_pnl["realized_pnl"] < 0).sum()

            total_positions = pnl_stats["winning_positions"] + pnl_stats["losing_positions"]
            if total_positions > 0:
                pnl_stats["win_rate"] = pnl_stats["winning_positions"] / total_positions

            # Best and worst trades
            if len(valid_pnl) > 0:
                best_idx = valid_pnl["realized_pnl"].idxmax()
                worst_idx = valid_pnl["realized_pnl"].idxmin()

                pnl_stats["best_trade"] = {
                    "slug": valid_pnl.loc[best_idx, "slug"],
                    "outcome": valid_pnl.loc[best_idx, "outcome"],
                    "realized_pnl": valid_pnl.loc[best_idx, "realized_pnl"],
                }

                pnl_stats["worst_trade"] = {
                    "slug": valid_pnl.loc[worst_idx, "slug"],
                    "outcome": valid_pnl.loc[worst_idx, "outcome"],
                    "realized_pnl": valid_pnl.loc[worst_idx, "realized_pnl"],
                }

            # Average win/loss
            wins = valid_pnl[valid_pnl["realized_pnl"] > 0]["realized_pnl"]
            losses = valid_pnl[valid_pnl["realized_pnl"] < 0]["realized_pnl"]

            if len(wins) > 0:
                pnl_stats["avg_win"] = wins.mean()
            if len(losses) > 0:
                pnl_stats["avg_loss"] = losses.mean()

        return pnl_stats

    def generate_report(self) -> str:
        """Generate comprehensive markdown report."""
        self.load_data()

        # Get handle from directory name
        handle = self.data_dir.name

        report = f"# Polymarket Trading Analysis Report\n\n"
        report += f"## Profile: @{handle}\n\n"

        # Add metadata
        if self.metadata:
            report += f"- **Proxy Wallet**: `{self.metadata.get('proxy_wallet', 'N/A')}`\n"
            report += f"- **Data Collected**: {self.metadata.get('collection_completed', 'N/A')}\n\n"

        # Basic statistics
        report += "## Summary Statistics\n\n"
        basic_stats = self.analyze_basic_stats()

        report += f"- **Total Trades**: {basic_stats['total_trades']:,}\n"
        report += f"- **Total Activities**: {basic_stats['total_activities']:,}\n"
        report += f"- **Open Positions**: {basic_stats['total_positions']:,}\n"
        report += f"- **Closed Positions**: {basic_stats['total_closed_positions']:,}\n"

        if basic_stats["first_trade_time"]:
            report += f"- **First Trade**: {basic_stats['first_trade_time'].strftime('%Y-%m-%d %H:%M:%S')} UTC\n"
            report += f"- **Last Trade**: {basic_stats['last_trade_time'].strftime('%Y-%m-%d %H:%M:%S')} UTC\n"
            duration = basic_stats["last_trade_time"] - basic_stats["first_trade_time"]
            report += f"- **Trading Period**: {duration.days} days\n"

        report += f"- **Unique Markets Traded**: {basic_stats['unique_markets']:,}\n"
        report += f"- **Unique Events Traded**: {basic_stats['unique_events']:,}\n\n"

        report += "### Trade Distribution\n\n"
        report += f"- **Buy Trades**: {basic_stats['buy_trades']:,}\n"
        report += f"- **Sell Trades**: {basic_stats['sell_trades']:,}\n"
        report += f"- **Buy/Sell Ratio**: {basic_stats['buy_sell_ratio']:.2f}\n"
        report += f"- **Total Volume (USDC)**: ${basic_stats['total_volume_usdc']:,.2f}\n\n"

        # Top markets
        report += "## Top Markets by Trade Count\n\n"
        top_markets = self.analyze_top_markets(10)

        if not top_markets.empty:
            report += "| Market | Event | Trades | Volume (USDC) | Avg Price | Buys | Sells |\n"
            report += "|--------|-------|--------|---------------|-----------|------|-------|\n"

            for (condition_id, slug, event_slug), row in top_markets.iterrows():
                slug_short = slug[:30] + "..." if len(slug) > 30 else slug
                event_short = event_slug[:20] + "..." if len(event_slug) > 20 else event_slug
                report += f"| {slug_short} | {event_short} | "
                report += f"{int(row['trade_count'])} | "
                report += f"${row['total_volume']:,.2f} | "
                report += f"{row['avg_price']:.4f} | "
                report += f"{int(row['buy_count'])} | "
                report += f"{int(row['sell_count'])} |\n"
        else:
            report += "*No market data available*\n"

        report += "\n"

        # PnL Analysis
        report += "## Profit & Loss Analysis\n\n"
        pnl_stats = self.analyze_pnl()

        report += f"- **Total Realized PnL**: ${pnl_stats['total_realized_pnl']:,.2f}\n"
        report += f"- **Win Rate**: {pnl_stats['win_rate']*100:.1f}%\n"
        report += f"- **Winning Positions**: {pnl_stats['winning_positions']}\n"
        report += f"- **Losing Positions**: {pnl_stats['losing_positions']}\n"
        report += f"- **Average Win**: ${pnl_stats['avg_win']:,.2f}\n"
        report += f"- **Average Loss**: ${pnl_stats['avg_loss']:,.2f}\n\n"

        if pnl_stats["best_trade"]:
            report += f"**Best Trade**: {pnl_stats['best_trade']['slug']} "
            report += f"(+${pnl_stats['best_trade']['realized_pnl']:,.2f})\n"

        if pnl_stats["worst_trade"]:
            report += f"**Worst Trade**: {pnl_stats['worst_trade']['slug']} "
            report += f"(${pnl_stats['worst_trade']['realized_pnl']:,.2f})\n"

        report += "\n"

        # Strategy patterns
        report += "## Detected Trading Patterns\n\n"

        # Scalping patterns
        report += "### Potential Scalping/Momentum Trading\n\n"
        scalping_patterns = self.detect_scalping_patterns()

        if scalping_patterns:
            report += f"Found {len(scalping_patterns)} potential scalping sequences:\n\n"
            for i, pattern in enumerate(scalping_patterns[:5], 1):  # Show top 5
                report += f"{i}. **{pattern['slug']}**\n"
                report += f"   - Duration: {pattern['duration_minutes']:.1f} minutes\n"
                report += f"   - Trades: {pattern['trade_count']} "
                report += f"(Buy: {pattern['buy_count']}, Sell: {pattern['sell_count']})\n"
                report += f"   - Price Range: {pattern['price_range']:.4f}\n"
                report += f"   - Time: {pattern['start_time'].strftime('%Y-%m-%d %H:%M')}\n\n"
        else:
            report += "*No clear scalping patterns detected*\n\n"

        # Delta neutral patterns
        report += "### Potential Delta-Neutral/Hedging Strategies\n\n"
        delta_patterns = self.detect_delta_neutral_patterns()

        if delta_patterns:
            report += f"Found {len(delta_patterns)} potential hedging patterns:\n\n"
            for i, pattern in enumerate(delta_patterns[:5], 1):  # Show top 5
                report += f"{i}. **{pattern['event_slug']}**\n"
                report += f"   - Outcomes Traded: {pattern['outcomes_traded']}\n"
                report += f"   - Positions: {pattern['outcome_positions']}\n"
                report += f"   - Total Trades: {pattern['total_trades']}\n"
                report += f"   - Time Window: {pattern['time_window_start'].strftime('%Y-%m-%d %H:%M')}\n\n"
        else:
            report += "*No clear delta-neutral patterns detected*\n\n"

        # Notes
        report += "## Notes\n\n"
        report += "- All timestamps are in UTC\n"
        report += "- Pattern detection is based on heuristics and may not reflect actual trading strategies\n"
        report += "- PnL calculations are based on available closed position data\n"
        report += "- This analysis is for informational purposes only\n\n"

        report += "---\n"
        report += f"*Report generated at {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC*\n"

        return report

    def save_report(self, filename: str = "report.md"):
        """Generate and save report to file."""
        report = self.generate_report()
        report_path = self.data_dir / filename
        with open(report_path, "w") as f:
            f.write(report)
        logger.info(f"Report saved to {report_path}")
        return report_path