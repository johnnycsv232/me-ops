from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import os

# IANA timezone that correctly handles DST transitions
LOCAL_TZ = ZoneInfo("America/Chicago")

# Centralized Gemini model ID — change here to update everywhere
DEFAULT_MODEL_ID = "gemini-2.5-flash"

def local_now() -> datetime:
    """Returns the current aware datetime in the local timezone."""
    return datetime.now(LOCAL_TZ)

def to_local(dt: datetime) -> datetime:
    """Converts a potentially naive or different-timezone datetime to local timezone."""
    if dt.tzinfo is None:
        # Assume UTC if naive, as DuckDB usually returns naive UTC or we store it that way
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(LOCAL_TZ)

def local_date(dt: datetime) -> str:
    """Returns the YYYY-MM-DD date string in the local timezone."""
    return to_local(dt).strftime("%Y-%m-%d")

def local_hour(dt: datetime) -> int:
    """Returns the hour (0-23) in the local timezone."""
    return to_local(dt).hour
