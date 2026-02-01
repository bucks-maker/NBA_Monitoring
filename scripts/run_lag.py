"""Entry point for the Pinnacle-Polymarket lag monitor.

Usage:
    python -m scripts.run_lag          # REST polling mode
    python -m scripts.run_lag --ws     # WebSocket mode (recommended)
    python -m scripts.run_lag --report # Print analysis report
"""
from __future__ import annotations

import sys
from pathlib import Path

# Ensure project root is on path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import load_config
from src.strategies.lag.monitor import LagMonitor


def main():
    config = load_config()

    if "--report" in sys.argv:
        from src.strategies.lag.report import report
        report(config.db_path)
    elif "--analysis" in sys.argv:
        from src.strategies.lag.analysis import main as analysis_main
        analysis_main(config.db_path)
    elif "--ws" in sys.argv or "-w" in sys.argv:
        monitor = LagMonitor(config)
        monitor.run_ws()
    else:
        monitor = LagMonitor(config)
        monitor.run_rest()


if __name__ == "__main__":
    main()
