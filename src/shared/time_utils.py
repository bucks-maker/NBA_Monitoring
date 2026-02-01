"""Timezone and active-window utilities."""
from __future__ import annotations

from datetime import datetime, timezone, timedelta

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")


def now_utc() -> str:
    """Current UTC time as ISO 8601 string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def now_et() -> datetime:
    """Current US Eastern time."""
    return datetime.now(ET)


def now_et_str() -> str:
    """Current ET time formatted as HH:MM:SS ET."""
    return now_et().strftime("%H:%M:%S ET")


def is_active_window(start_hour: int = 10, end_hour: int = 3) -> bool:
    """Check if current ET time is within active monitoring window.

    Default window: 10:00 ET -> 03:00 ET (next day).
    """
    hour = now_et().hour
    if start_hour <= hour or hour < end_hour:
        return True
    return False


def seconds_until_active(start_hour: int = 10, end_hour: int = 3) -> int:
    """Seconds until the next active window starts."""
    et_now = now_et()
    target = et_now.replace(hour=start_hour, minute=0, second=0, microsecond=0)
    diff = (target - et_now).total_seconds()
    if diff <= 0:
        diff += 86400
    return int(diff)
