"""Tests for shared NBA utilities."""
from src.shared.nba import (
    classify_market,
    extract_total_line,
    extract_spread_line,
    make_poly_slug,
)


def test_classify_total():
    assert classify_market("NBA Game Total O/U 233.5", "nba-total-233pt5") == "total"


def test_classify_spread():
    assert classify_market("NBA Spread", "nba-spread-8pt5") == "spread"


def test_classify_moneyline():
    assert classify_market("Celtics vs Heat", "nba-bos-mia") == "moneyline"


def test_classify_player_prop():
    assert classify_market("Player Points O/U 25.5", "player-prop") == "player_prop"


def test_classify_quarter():
    assert classify_market("1Q Total", "1q-total") == "other"


def test_extract_total_line_pt_format():
    assert extract_total_line("nba-total-233pt5") == 233.5


def test_extract_total_line_decimal_format():
    assert extract_total_line("line is 233.5") == 233.5


def test_extract_total_line_none():
    assert extract_total_line("no line here") is None


def test_extract_spread_line():
    assert extract_spread_line("home-8pt5") == 8.5


def test_make_poly_slug():
    slug = make_poly_slug(
        "Portland Trail Blazers",
        "Washington Wizards",
        "2026-01-28T00:10:00Z",
    )
    assert slug == "nba-por-was-2026-01-27"  # ET date


def test_make_poly_slug_unknown_team():
    assert make_poly_slug("Unknown Team", "Washington Wizards", "2026-01-28T00:00:00Z") == ""
