from __future__ import annotations

from datetime import datetime, time, timedelta, timezone
from typing import Dict, Optional, Tuple

try:
    from zoneinfo import ZoneInfo
except ImportError as exc:  # pragma: no cover
    raise SystemExit("zoneinfo is required (Python 3.9+)") from exc

CHICAGO_TZ = ZoneInfo("America/Chicago")


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def last_n_days(now_utc: datetime, n: int = 7) -> Tuple[datetime, datetime]:
    if n < 1:
        raise ValueError("n must be >= 1")
    now_utc = _ensure_utc(now_utc)
    now_local = now_utc.astimezone(CHICAGO_TZ)

    start_date = now_local.date() - timedelta(days=n - 1)
    start_local = datetime.combine(start_date, time.min, tzinfo=CHICAGO_TZ)
    start_utc = start_local.astimezone(timezone.utc)

    return start_utc, now_utc


def _iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def compute_time_window_payload(now_utc: Optional[datetime] = None, n: int = 7) -> Dict[str, str]:
    now_utc = _ensure_utc(now_utc or datetime.now(timezone.utc))
    start_utc, end_utc = last_n_days(now_utc, n=n)
    start_local = start_utc.astimezone(CHICAGO_TZ)
    end_local = end_utc.astimezone(CHICAGO_TZ)

    return {
        "start_ts_utc": _iso_z(start_utc),
        "end_ts_utc": _iso_z(end_utc),
        "start_date_ct": start_local.date().isoformat(),
        "end_date_ct": end_local.date().isoformat(),
    }
