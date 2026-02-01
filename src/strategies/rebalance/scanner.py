"""Event scanning and registration for rebalance strategy.

Extracted from rebalance_monitor.py: event classification and tracker registration.
"""
from __future__ import annotations

import json
import logging
from typing import Dict, List

from src.clients.gamma import GammaClient
from src.config import RebalanceConfig
from src.strategies.rebalance.tracker import RebalanceTracker

log = logging.getLogger("rebalance")


def is_negative_risk_event(event: Dict) -> bool:
    if event.get("negativeRisk") is True:
        return True
    markets = event.get("markets", [])
    if markets and any(m.get("negRisk") is True for m in markets):
        return True
    return False


def is_sports_event(event: Dict) -> bool:
    for tag in event.get("tags", []):
        label = tag.get("label", "") if isinstance(tag, dict) else str(tag)
        if label == "Sports":
            return True
    return False


def is_nba_game_event(event: Dict) -> bool:
    if is_negative_risk_event(event):
        return False
    if not is_sports_event(event):
        return False
    for tag in event.get("tags", []):
        label = tag.get("label", "") if isinstance(tag, dict) else str(tag)
        if label == "NBA":
            return True
    if "NBA" in event.get("title", ""):
        return True
    return False


def extract_yes_tokens(event: Dict) -> List[Dict]:
    tokens = []
    for m in event.get("markets", []):
        if m.get("closed"):
            continue
        outcomes = m.get("outcomes", [])
        if isinstance(outcomes, str):
            outcomes = json.loads(outcomes)
        clob_token_ids = m.get("clobTokenIds", [])
        if isinstance(clob_token_ids, str):
            clob_token_ids = json.loads(clob_token_ids)

        if clob_token_ids and outcomes:
            question = m.get("question", "")
            outcome_name = question if question else (outcomes[0] if outcomes else "?")
            tokens.append({
                "token_id": clob_token_ids[0],
                "outcome": outcome_name,
            })
    return tokens


def scan_and_register(
    tracker: RebalanceTracker,
    gamma: GammaClient,
    config: RebalanceConfig,
) -> List[str]:
    """Scan active events and register negativeRisk + NBA binary events.

    Returns list of newly registered token IDs.
    """
    log.info("Scanning Gamma API events...")
    all_events = gamma.get_all_active_events()
    log.info(f"Total active events: {len(all_events)}")

    existing_tokens = set(tracker.registered_token_ids)
    new_token_ids: List[str] = []
    n_new_events = 0

    # Multi-outcome negativeRisk events
    for event in all_events:
        if not is_negative_risk_event(event):
            continue
        if not is_sports_event(event):
            continue

        event_id = str(event.get("id", ""))
        title = event.get("title", "?")
        tokens = extract_yes_tokens(event)

        if len(tokens) < config.min_markets:
            continue
        if any(t["token_id"] in existing_tokens for t in tokens):
            continue

        tracker.register_event(event_id, title, tokens)
        n_new_events += 1
        for t in tokens:
            new_token_ids.append(t["token_id"])

    # NBA binary markets (YES+NO pairs)
    existing_tokens = set(tracker.registered_token_ids)
    n_nba_markets = 0

    for event in all_events:
        if not is_nba_game_event(event):
            continue

        event_title = event.get("title", "?")

        for m in event.get("markets", []):
            if m.get("closed"):
                continue

            outcomes = m.get("outcomes", [])
            if isinstance(outcomes, str):
                outcomes = json.loads(outcomes)
            clob_token_ids = m.get("clobTokenIds", [])
            if isinstance(clob_token_ids, str):
                clob_token_ids = json.loads(clob_token_ids)

            if len(clob_token_ids) < 2:
                continue

            yes_tid = clob_token_ids[0]
            no_tid = clob_token_ids[1]

            if yes_tid in existing_tokens or no_tid in existing_tokens:
                continue

            question = m.get("question", "")
            market_title = f"{event_title} | {question}" if question else event_title
            market_id = str(m.get("id", "") or m.get("conditionId", "") or yes_tid)

            tokens = [
                {"token_id": yes_tid, "outcome": outcomes[0] if outcomes else "Yes"},
                {"token_id": no_tid, "outcome": outcomes[1] if len(outcomes) > 1 else "No"},
            ]

            tracker.register_event(market_id, market_title, tokens)
            n_nba_markets += 1
            new_token_ids.append(yes_tid)
            new_token_ids.append(no_tid)
            existing_tokens.add(yes_tid)
            existing_tokens.add(no_tid)

    log.info(
        f"Scan complete: {n_new_events} multi-outcome + {n_nba_markets} NBA binary | "
        f"Total {tracker.n_events} events, {tracker.n_tokens} tokens"
    )
    return new_token_ids
