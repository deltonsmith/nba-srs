"""
Team stats helpers (file-based, optional).

Loads data/team_stats.json if present. If missing, returns None so callers can
leave features null.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional

from src.team_normalize import normalize_team_id

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"


def _load_team_stats(path: Path) -> Optional[List[Dict]]:
    if not path.exists():
        return None
    try:
        payload = path.read_text(encoding="utf-8")
        data = __import__("json").loads(payload)
    except Exception:
        return None
    if isinstance(data, dict):
        rows = data.get("teams") or data.get("data") or data.get("rows")
        if isinstance(rows, list):
            return rows
    if isinstance(data, list):
        return data
    return None


def load_team_stats() -> Optional[Dict[str, Dict]]:
    rows = _load_team_stats(DATA_DIR / "team_stats.json")
    if rows is None:
        return None

    stats_by_team: Dict[str, Dict] = {}
    for row in rows:
        team_id = normalize_team_id(
            row.get("team_id")
            or row.get("team_abbr")
            or row.get("team")
            or row.get("team_name")
            or row.get("abbr")
        )
        if not team_id:
            continue
        stats_by_team[team_id] = row

    return stats_by_team
