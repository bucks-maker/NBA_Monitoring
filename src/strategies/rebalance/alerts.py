"""CLOB verification and alert recording for rebalance opportunities.

Extracted from rebalance_monitor.py: verify_opportunity_with_clob + alert writing.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from src.clients.clob import CLOBClient
from src.strategies.rebalance.tracker import RebalanceTracker

log = logging.getLogger("rebalance")


def verify_opportunity_with_clob(
    opportunity: Dict[str, Any],
    clob: CLOBClient,
    tracker: Optional[RebalanceTracker] = None,
) -> Optional[Dict[str, Any]]:
    """Verify an opportunity via CLOB /book API.

    Returns updated opportunity if verified, None if false positive.
    """
    outcomes = opportunity["outcomes"]
    verified_sum = 0.0
    min_depth = float("inf")

    for oc in outcomes:
        token_id = oc["token_id"]
        try:
            book = clob.get_orderbook(token_id)
        except Exception:
            return None

        asks = book.get("asks", [])
        if not asks:
            return None

        asks_sorted = sorted(asks, key=lambda x: float(x["price"]))
        best_ask = float(asks_sorted[0]["price"])
        best_size = float(asks_sorted[0]["size"])
        depth_dollars = best_ask * best_size

        oc["best_ask"] = best_ask
        oc["depth"] = depth_dollars
        verified_sum += best_ask
        min_depth = min(min_depth, depth_dollars)

        if tracker is not None:
            tracker.update_best_ask(token_id, best_ask)

    if verified_sum >= 1.0:
        return None

    opportunity["sum"] = verified_sum
    opportunity["gap"] = 1.0 - verified_sum
    opportunity["gap_pct"] = (1.0 - verified_sum) * 100
    opportunity["min_depth"] = min_depth
    opportunity["is_executable"] = min_depth >= 100.0
    opportunity["verified"] = True
    return opportunity


def on_opportunity(
    opp: Dict[str, Any],
    clob: CLOBClient,
    tracker: Optional[RebalanceTracker] = None,
    alert_file: Optional[Path] = None,
) -> None:
    """Handle opportunity: verify with CLOB, log, and write alert."""
    verified = verify_opportunity_with_clob(opp, clob, tracker)
    if verified is None:
        log.debug(f"CLOB verification failed (false positive): {opp['title']}")
        return

    strength = ""
    if verified["is_strong"] and verified["is_executable"]:
        strength = " *** EXECUTABLE ***"
    elif verified["is_strong"]:
        strength = " ** STRONG **"

    log.warning(
        f"VERIFIED OPPORTUNITY{strength} | gap={verified['gap_pct']:.2f}% | "
        f"sum={verified['sum']:.4f} | depth>=${verified['min_depth']:.0f} | "
        f"{verified['title']}"
    )
    for o in verified.get("outcomes", []):
        log.info(
            f"  {o['outcome'][:40]:40s} ask={o['best_ask']:.4f} "
            f"depth=${o['depth']:.0f}"
        )

    if alert_file:
        _write_alert(verified, alert_file)


def _write_alert(opp: Dict[str, Any], alert_file: Path) -> None:
    alert_file.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "timestamp": datetime.fromtimestamp(
            opp["timestamp"], tz=timezone.utc
        ).isoformat(),
        "event_id": opp["event_id"],
        "title": opp["title"],
        "n_outcomes": opp["n_outcomes"],
        "sum": round(opp["sum"], 6),
        "gap": round(opp["gap"], 6),
        "gap_pct": round(opp["gap_pct"], 4),
        "is_strong": opp["is_strong"],
        "is_executable": opp["is_executable"],
        "min_depth": round(opp["min_depth"], 2),
        "verified": opp.get("verified", False),
        "outcomes": [
            {"outcome": o["outcome"], "best_ask": round(o["best_ask"], 6), "depth": round(o["depth"], 2)}
            for o in opp.get("outcomes", [])
        ],
    }
    try:
        with open(alert_file, "a") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as e:
        log.error(f"Alert file write failed: {e}")
