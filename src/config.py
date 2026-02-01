"""Central configuration for the Poly monitoring system.

Consolidates all URLs, thresholds, and intervals from 6+ files into
frozen dataclasses with environment variable overrides.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class GammaConfig:
    base_url: str = "https://gamma-api.polymarket.com"
    fetch_limit: int = 100
    fetch_delay: float = 0.3
    timeout: int = 15


@dataclass(frozen=True)
class CLOBConfig:
    base_url: str = "https://clob.polymarket.com"
    timeout: int = 10


@dataclass(frozen=True)
class OddsAPIConfig:
    key: str = ""
    base_url: str = "https://api.the-odds-api.com/v4"
    sport: str = "basketball_nba"
    bookmaker: str = "pinnacle"
    timeout: int = 15


@dataclass(frozen=True)
class DataAPIConfig:
    base_url: str = "https://data-api.polymarket.com"
    timeout: int = 15


@dataclass(frozen=True)
class WebSocketConfig:
    url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    ping_interval: int = 30
    ping_timeout: int = 10
    reconnect_initial: float = 1.0
    reconnect_max: float = 60.0
    reconnect_multiplier: float = 2.0
    subscribe_batch_size: int = 500


@dataclass(frozen=True)
class LagConfig:
    normal_interval: int = 3600
    trigger_interval: int = 900
    trigger_cooldown: int = 7200
    poly_interval: int = 30
    line_move_threshold: float = 1.5
    implied_move_threshold: float = 0.06
    active_start_hour: int = 10
    active_end_hour: int = 3
    bot_check_interval: int = 60
    refresh_interval: int = 600
    status_interval: int = 300


@dataclass(frozen=True)
class RebalanceConfig:
    threshold: float = 1.0
    strong_threshold: float = 0.995
    min_depth: float = 100.0
    refresh_interval: int = 600
    status_interval: int = 60
    status_top_n: int = 15
    min_markets: int = 3
    seed_workers: int = 50


@dataclass(frozen=True)
class AnomalyConfig:
    price_change_threshold: float = 0.05
    price_window_seconds: int = 300
    bid_ask_spread_threshold: float = 0.05
    yes_no_deviation_threshold: float = 0.03
    pinnacle_cooldown_seconds: int = 1800


@dataclass(frozen=True)
class HiResConfig:
    offsets: tuple[int, ...] = (3, 10, 30)
    actionable_gap: float = 0.04


@dataclass
class AppConfig:
    odds: OddsAPIConfig
    gamma: GammaConfig = field(default_factory=GammaConfig)
    clob: CLOBConfig = field(default_factory=CLOBConfig)
    data_api: DataAPIConfig = field(default_factory=DataAPIConfig)
    ws: WebSocketConfig = field(default_factory=WebSocketConfig)
    lag: LagConfig = field(default_factory=LagConfig)
    rebalance: RebalanceConfig = field(default_factory=RebalanceConfig)
    anomaly: AnomalyConfig = field(default_factory=AnomalyConfig)
    hi_res: HiResConfig = field(default_factory=HiResConfig)
    db_path: Path = Path("data/snapshots.db")
    bot_address: str = ""
    alert_file: Path = Path("data/rebalance_alerts.jsonl")


def load_config(env_file: Path | None = None) -> AppConfig:
    """Load configuration from environment variables + defaults.

    Args:
        env_file: Path to .env file. If None, searches project root.
    """
    if env_file and env_file.exists():
        load_dotenv(env_file)
    else:
        # Search standard locations
        for candidate in [Path(".env"), Path(__file__).parent.parent / ".env"]:
            if candidate.exists():
                load_dotenv(candidate)
                break

    odds_key = os.environ.get("ODDS_API_KEY", "")
    bot_address = os.environ.get("BOT_ADDRESS", "")
    db_path = Path(os.environ.get("DB_PATH", "data/snapshots.db"))

    return AppConfig(
        odds=OddsAPIConfig(key=odds_key),
        db_path=db_path,
        bot_address=bot_address,
    )
