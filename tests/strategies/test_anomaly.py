"""Tests for anomaly detection."""
import time
from src.strategies.lag.anomaly import AnomalyDetector, AnomalyConfig


def test_price_change_triggers_anomaly():
    detector = AnomalyDetector(AnomalyConfig(
        price_change_threshold=0.05,
        price_window_seconds=300,
    ))
    # Record initial price
    ts = time.time()
    detector.update_price("g1", "total", "Over", 0.50, timestamp=ts)
    # Big move
    event = detector.update_price("g1", "total", "Over", 0.56, timestamp=ts + 1)
    assert event is not None
    assert event.anomaly_type == "price_change"


def test_small_price_change_no_anomaly():
    detector = AnomalyDetector(AnomalyConfig(
        price_change_threshold=0.05,
        price_window_seconds=300,
    ))
    ts = time.time()
    detector.update_price("g1", "total", "Over", 0.50, timestamp=ts)
    event = detector.update_price("g1", "total", "Over", 0.52, timestamp=ts + 1)
    assert event is None


def test_orderbook_spread_anomaly():
    detector = AnomalyDetector(AnomalyConfig(bid_ask_spread_threshold=0.05))
    event = detector.update_orderbook("g1", "total", "Over", 0.45, 0.55)
    assert event is not None
    assert event.anomaly_type == "orderbook_spread"


def test_yes_no_deviation():
    detector = AnomalyDetector(AnomalyConfig(yes_no_deviation_threshold=0.03))
    ts = time.time()
    detector.update_price("g1", "total", "Over", 0.55, timestamp=ts)
    event = detector.update_price("g1", "total", "Under", 0.40, timestamp=ts + 0.1)
    assert event is not None
    assert event.anomaly_type == "yes_no_deviation"


def test_pinnacle_cooldown():
    detector = AnomalyDetector(AnomalyConfig(pinnacle_cooldown_seconds=60))
    assert detector.should_call_pinnacle("g1") is True
    detector.mark_pinnacle_called("g1")
    assert detector.should_call_pinnacle("g1") is False
