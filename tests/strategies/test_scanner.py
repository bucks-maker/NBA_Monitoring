"""Tests for rebalance scanner event classification."""
from src.strategies.rebalance.scanner import (
    is_negative_risk_event,
    is_sports_event,
    is_nba_game_event,
    extract_yes_tokens,
)


def test_negative_risk_flag():
    assert is_negative_risk_event({"negativeRisk": True}) is True
    assert is_negative_risk_event({"negativeRisk": False}) is False
    assert is_negative_risk_event({}) is False


def test_negative_risk_from_markets():
    event = {"markets": [{"negRisk": True}]}
    assert is_negative_risk_event(event) is True


def test_sports_event():
    assert is_sports_event({"tags": [{"label": "Sports"}]}) is True
    assert is_sports_event({"tags": [{"label": "Politics"}]}) is False
    assert is_sports_event({"tags": []}) is False


def test_nba_game_event():
    event = {
        "tags": [{"label": "Sports"}, {"label": "NBA"}],
        "title": "Lakers vs Celtics",
    }
    assert is_nba_game_event(event) is True

    # negativeRisk events should NOT be treated as NBA game events
    event["negativeRisk"] = True
    assert is_nba_game_event(event) is False


def test_extract_yes_tokens():
    event = {
        "markets": [
            {
                "question": "Will Lakers win?",
                "outcomes": '["Yes", "No"]',
                "clobTokenIds": '["token1", "token2"]',
            }
        ]
    }
    tokens = extract_yes_tokens(event)
    assert len(tokens) == 1
    assert tokens[0]["token_id"] == "token1"


def test_closed_market_excluded():
    event = {
        "markets": [
            {
                "closed": True,
                "question": "Closed market",
                "outcomes": '["Yes", "No"]',
                "clobTokenIds": '["token1", "token2"]',
            }
        ]
    }
    tokens = extract_yes_tokens(event)
    assert len(tokens) == 0
