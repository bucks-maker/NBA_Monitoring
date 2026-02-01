"""NBA team abbreviations, slug generation, and market classification.

Consolidates duplicated logic from snapshot.py and backtest/poly_fetch.py.
"""
from __future__ import annotations

import re
from datetime import datetime

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

FULL_TO_POLY_ABBR: dict[str, str] = {
    "Atlanta Hawks": "atl",
    "Boston Celtics": "bos",
    "Brooklyn Nets": "bkn",
    "Charlotte Hornets": "cha",
    "Chicago Bulls": "chi",
    "Cleveland Cavaliers": "cle",
    "Dallas Mavericks": "dal",
    "Denver Nuggets": "den",
    "Detroit Pistons": "det",
    "Golden State Warriors": "gsw",
    "Houston Rockets": "hou",
    "Indiana Pacers": "ind",
    "LA Clippers": "lac",
    "Los Angeles Clippers": "lac",
    "Los Angeles Lakers": "lal",
    "Memphis Grizzlies": "mem",
    "Miami Heat": "mia",
    "Milwaukee Bucks": "mil",
    "Minnesota Timberwolves": "min",
    "New Orleans Pelicans": "nop",
    "New York Knicks": "nyk",
    "Oklahoma City Thunder": "okc",
    "Orlando Magic": "orl",
    "Philadelphia 76ers": "phi",
    "Phoenix Suns": "phx",
    "Portland Trail Blazers": "por",
    "Sacramento Kings": "sac",
    "San Antonio Spurs": "sas",
    "Toronto Raptors": "tor",
    "Utah Jazz": "uta",
    "Washington Wizards": "was",
}


def make_poly_slug(away_team: str, home_team: str, commence_time: str) -> str:
    """Generate a Polymarket event slug from team names and start time.

    Args:
        away_team: Full team name (e.g. "Portland Trail Blazers")
        home_team: Full team name (e.g. "Washington Wizards")
        commence_time: ISO 8601 UTC string (e.g. "2026-01-27T00:00:00Z")

    Returns:
        Slug like "nba-por-was-2026-01-27", or "" if teams not found.
    """
    away_abbr = FULL_TO_POLY_ABBR.get(away_team, "")
    home_abbr = FULL_TO_POLY_ABBR.get(home_team, "")

    if not away_abbr or not home_abbr or not commence_time:
        return ""

    dt_utc = datetime.fromisoformat(commence_time.replace("Z", "+00:00"))
    dt_et = dt_utc.astimezone(ET)
    date_str = dt_et.strftime("%Y-%m-%d")
    return f"nba-{away_abbr}-{home_abbr}-{date_str}"


def classify_market(question: str, slug: str) -> str:
    """Classify a Polymarket market type from its question and slug.

    Returns one of: "total", "spread", "moneyline", "player_prop", "other".
    """
    q = question.lower()
    s = slug.lower()

    # Player props
    if any(kw in q for kw in [
        "points o/u", "rebounds o/u", "assists o/u",
        "threes o/u", "steals o/u", "blocks o/u",
    ]):
        return "player_prop"

    # Half/quarter
    if any(kw in q for kw in [
        "1h", "1q", "2q", "3q", "4q", "first half", "first quarter",
    ]):
        return "other"

    if "o/u" in q or "total" in s:
        return "total"
    if "spread" in q or "spread" in s:
        return "spread"

    # Remaining: moneyline
    if " vs" in q or " vs." in q:
        return "moneyline"

    return "other"


def extract_total_line(text: str) -> float | None:
    """Extract total line from question/slug text.

    Examples: "233pt5" -> 233.5, "233.5" -> 233.5
    """
    m = re.search(r"(\d{2,3})pt(\d)", text)
    if m:
        return float(m.group(1)) + float(m.group(2)) / 10
    m = re.search(r"(\d{2,3}\.\d)", text)
    if m:
        return float(m.group(1))
    return None


def extract_spread_line(text: str) -> float:
    """Extract spread line from slug text.

    Examples: "home-8pt5" -> 8.5
    """
    m = re.search(r"(\d{1,2})pt(\d)", text)
    if m:
        return float(m.group(1)) + float(m.group(2)) / 10
    m = re.search(r"(\d{1,2}\.\d)", text)
    if m:
        return float(m.group(1))
    return 0.0
