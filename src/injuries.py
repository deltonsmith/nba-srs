"""
Injury data helpers (file-based, optional).

Looks for data/injuries_YYYY-MM-DD.json; falls back to data/injuries.json.
If no file exists, callers should treat injury features as missing (null).
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List, Optional

from src.team_normalize import normalize_team_id

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

STAR_MINUTES = 28.0
STAR_POINTS = 15.0


def _load_injuries_file(path: Path) -> Optional[List[Dict]]:
    if not path.exists():
        return None
    try:
        payload = path.read_text(encoding="utf-8")
        data = __import__("json").loads(payload)
    except Exception:
        return None
    if isinstance(data, dict):
        rows = data.get("injuries") or data.get("data") or data.get("rows")
        if isinstance(rows, list):
            return rows
    if isinstance(data, list):
        return data
    return None


def load_injuries_for_date(date_str: str) -> Optional[List[Dict]]:
    if not date_str:
        return None
    dated_path = DATA_DIR / f"injuries_{date_str}.json"
    rows = _load_injuries_file(dated_path)
    if rows is not None:
        return rows
    fallback = DATA_DIR / "injuries.json"
    return _load_injuries_file(fallback)


def _status_key(status: object) -> Optional[str]:
    if status is None:
        return None
    s = str(status).strip().lower()
    if "out" in s:
        return "out"
    if "doubt" in s:
        return "doubtful"
    return None


def _numeric_from_keys(row: Dict, keys: Iterable[str]) -> Optional[float]:
    for key in keys:
        if key in row and row.get(key) is not None:
            try:
                return float(row.get(key))
            except Exception:
                continue
    return None


def compute_team_injury_features(
    rows: Optional[List[Dict]],
    star_min_minutes: float = STAR_MINUTES,
    star_min_points: float = STAR_POINTS,
) -> Optional[Dict[str, Dict[str, int]]]:
    if rows is None:
        return None

    features: Dict[str, Dict[str, int]] = {}

    for row in rows:
        team_id = normalize_team_id(
            row.get("team_id")
            or row.get("team_abbr")
            or row.get("team")
            or row.get("team_name")
            or row.get("teamCode")
        )
        if not team_id:
            continue

        status_key = _status_key(row.get("status") or row.get("injury_status"))
        if status_key not in ("out", "doubtful"):
            continue

        team_feat = features.setdefault(team_id, {"key_injuries_count": 0, "star_absence_proxy": 0})
        team_feat["key_injuries_count"] += 1

        if status_key == "out":
            minutes = _numeric_from_keys(row, ("avg_minutes", "minutes", "mpg", "avg_mpg"))
            points = _numeric_from_keys(row, ("avg_points", "points", "ppg", "avg_ppg", "points_per_game"))
            if (minutes is not None and minutes >= star_min_minutes) or (
                points is not None and points >= star_min_points
            ):
                team_feat["star_absence_proxy"] += 1

    return features
