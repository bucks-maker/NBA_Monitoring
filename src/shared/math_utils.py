"""Mathematical utilities for odds and probability calculations."""
from __future__ import annotations


def de_vig_implied(odds1: float, odds2: float) -> tuple[float, float]:
    """Calculate de-vigged (fair) probabilities from 2-way decimal odds.

    Args:
        odds1: Decimal odds for outcome 1 (e.g. 1.95)
        odds2: Decimal odds for outcome 2 (e.g. 1.95)

    Returns:
        (fair_prob1, fair_prob2) summing to 1.0
    """
    if odds1 <= 0 or odds2 <= 0:
        return (0.5, 0.5)

    implied1 = 1 / odds1
    implied2 = 1 / odds2
    total = implied1 + implied2

    if total <= 0:
        return (0.5, 0.5)

    return (implied1 / total, implied2 / total)
