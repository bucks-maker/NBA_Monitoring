"""Entry point for the rebalance arbitrage monitor.

Usage:
    python -m scripts.run_rebalance
"""
from __future__ import annotations

import sys
from pathlib import Path

# Ensure project root is on path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import load_config
from src.strategies.rebalance.monitor import RebalanceMonitor


def main():
    config = load_config()
    monitor = RebalanceMonitor(config)
    monitor.run()


if __name__ == "__main__":
    main()
