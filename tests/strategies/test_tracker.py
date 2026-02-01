"""Tests for rebalance tracker."""
from src.strategies.rebalance.tracker import RebalanceTracker


def test_register_and_sum():
    tracker = RebalanceTracker(threshold=1.0)
    tracker.register_event("e1", "Test Event", [
        {"token_id": "t1", "outcome": "A", "price": 0.30},
        {"token_id": "t2", "outcome": "B", "price": 0.30},
        {"token_id": "t3", "outcome": "C", "price": 0.30},
    ])
    sums = tracker.get_all_event_sums()
    assert len(sums) == 1
    assert abs(sums[0]["sum"] - 0.9) < 1e-9


def test_opportunity_detected():
    opportunities = []

    tracker = RebalanceTracker(
        threshold=1.0,
        strong_threshold=0.995,
        on_opportunity=lambda opp: opportunities.append(opp),
    )
    # Register with no initial prices
    tracker.register_event("e1", "Test", [
        {"token_id": "t1", "outcome": "A"},
        {"token_id": "t2", "outcome": "B"},
        {"token_id": "t3", "outcome": "C"},
    ])
    # Feed prices that sum < 1.0 via update_best_ask (triggers callback)
    tracker.update_best_ask("t1", 0.30)
    tracker.update_best_ask("t2", 0.30)
    tracker.update_best_ask("t3", 0.30)
    # Sum = 0.9, should trigger
    assert len(opportunities) == 1
    assert opportunities[0]["gap_pct"] > 0


def test_no_opportunity_when_sum_above_threshold():
    opportunities = []
    tracker = RebalanceTracker(
        threshold=1.0,
        on_opportunity=lambda opp: opportunities.append(opp),
    )
    tracker.register_event("e1", "Test", [
        {"token_id": "t1", "outcome": "A", "price": 0.50},
        {"token_id": "t2", "outcome": "B", "price": 0.51},
    ])
    # Sum = 1.01, no opportunity
    assert len(opportunities) == 0


def test_update_best_ask():
    tracker = RebalanceTracker(threshold=1.0)
    tracker.register_event("e1", "Test", [
        {"token_id": "t1", "outcome": "A"},
        {"token_id": "t2", "outcome": "B"},
    ])
    tracker.update_best_ask("t1", 0.45)
    tracker.update_best_ask("t2", 0.50)

    sums = tracker.get_all_event_sums()
    assert sums[0]["sum"] == 0.95


def test_update_book():
    tracker = RebalanceTracker(threshold=1.0)
    tracker.register_event("e1", "Test", [
        {"token_id": "t1", "outcome": "A"},
        {"token_id": "t2", "outcome": "B"},
    ])
    tracker.update_book("t1", {"asks": [{"price": "0.40", "size": "100"}]})
    tracker.update_book("t2", {"asks": [{"price": "0.50", "size": "200"}]})

    sums = tracker.get_all_event_sums()
    assert sums[0]["sum"] == 0.9


def test_dead_market_filter():
    tracker = RebalanceTracker(threshold=1.0)
    tracker.register_event("e1", "Test", [
        {"token_id": "t1", "outcome": "A", "price": 0.01},
        {"token_id": "t2", "outcome": "B", "price": 0.01},
    ])
    sums = tracker.get_all_event_sums()
    assert sums[0]["sum"] is None  # dead market filtered
