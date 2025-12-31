"""
Team normalization utilities.

Canonical team_id is the NBA team abbreviation (e.g., "LAL"). NBA official IDs
are captured as nba_id and used for lookup when present in source data.
"""

from __future__ import annotations

from typing import Dict, Iterable, Optional


CANONICAL_TEAMS: Dict[str, Dict[str, object]] = {
    "ATL": {"abbr": "ATL", "full_name": "Atlanta Hawks", "city": "Atlanta", "nickname": "Hawks", "nba_id": 1},
    "BOS": {"abbr": "BOS", "full_name": "Boston Celtics", "city": "Boston", "nickname": "Celtics", "nba_id": 2},
    "BKN": {"abbr": "BKN", "full_name": "Brooklyn Nets", "city": "Brooklyn", "nickname": "Nets", "nba_id": 3},
    "CHA": {"abbr": "CHA", "full_name": "Charlotte Hornets", "city": "Charlotte", "nickname": "Hornets", "nba_id": 4},
    "CHI": {"abbr": "CHI", "full_name": "Chicago Bulls", "city": "Chicago", "nickname": "Bulls", "nba_id": 5},
    "CLE": {"abbr": "CLE", "full_name": "Cleveland Cavaliers", "city": "Cleveland", "nickname": "Cavaliers", "nba_id": 6},
    "DAL": {"abbr": "DAL", "full_name": "Dallas Mavericks", "city": "Dallas", "nickname": "Mavericks", "nba_id": 7},
    "DEN": {"abbr": "DEN", "full_name": "Denver Nuggets", "city": "Denver", "nickname": "Nuggets", "nba_id": 8},
    "DET": {"abbr": "DET", "full_name": "Detroit Pistons", "city": "Detroit", "nickname": "Pistons", "nba_id": 9},
    "GSW": {"abbr": "GSW", "full_name": "Golden State Warriors", "city": "Golden State", "nickname": "Warriors", "nba_id": 10},
    "HOU": {"abbr": "HOU", "full_name": "Houston Rockets", "city": "Houston", "nickname": "Rockets", "nba_id": 11},
    "IND": {"abbr": "IND", "full_name": "Indiana Pacers", "city": "Indiana", "nickname": "Pacers", "nba_id": 12},
    "LAC": {"abbr": "LAC", "full_name": "Los Angeles Clippers", "city": "Los Angeles", "nickname": "Clippers", "nba_id": 13},
    "LAL": {"abbr": "LAL", "full_name": "Los Angeles Lakers", "city": "Los Angeles", "nickname": "Lakers", "nba_id": 14},
    "MEM": {"abbr": "MEM", "full_name": "Memphis Grizzlies", "city": "Memphis", "nickname": "Grizzlies", "nba_id": 15},
    "MIA": {"abbr": "MIA", "full_name": "Miami Heat", "city": "Miami", "nickname": "Heat", "nba_id": 16},
    "MIL": {"abbr": "MIL", "full_name": "Milwaukee Bucks", "city": "Milwaukee", "nickname": "Bucks", "nba_id": 17},
    "MIN": {"abbr": "MIN", "full_name": "Minnesota Timberwolves", "city": "Minnesota", "nickname": "Timberwolves", "nba_id": 18},
    "NOP": {"abbr": "NOP", "full_name": "New Orleans Pelicans", "city": "New Orleans", "nickname": "Pelicans", "nba_id": 19},
    "NYK": {"abbr": "NYK", "full_name": "New York Knicks", "city": "New York", "nickname": "Knicks", "nba_id": 20},
    "OKC": {"abbr": "OKC", "full_name": "Oklahoma City Thunder", "city": "Oklahoma City", "nickname": "Thunder", "nba_id": 21},
    "ORL": {"abbr": "ORL", "full_name": "Orlando Magic", "city": "Orlando", "nickname": "Magic", "nba_id": 22},
    "PHI": {"abbr": "PHI", "full_name": "Philadelphia 76ers", "city": "Philadelphia", "nickname": "76ers", "nba_id": 23},
    "PHX": {"abbr": "PHX", "full_name": "Phoenix Suns", "city": "Phoenix", "nickname": "Suns", "nba_id": 24},
    "POR": {"abbr": "POR", "full_name": "Portland Trail Blazers", "city": "Portland", "nickname": "Trail Blazers", "nba_id": 25},
    "SAC": {"abbr": "SAC", "full_name": "Sacramento Kings", "city": "Sacramento", "nickname": "Kings", "nba_id": 26},
    "SAS": {"abbr": "SAS", "full_name": "San Antonio Spurs", "city": "San Antonio", "nickname": "Spurs", "nba_id": 27},
    "TOR": {"abbr": "TOR", "full_name": "Toronto Raptors", "city": "Toronto", "nickname": "Raptors", "nba_id": 28},
    "UTA": {"abbr": "UTA", "full_name": "Utah Jazz", "city": "Utah", "nickname": "Jazz", "nba_id": 29},
    "WAS": {"abbr": "WAS", "full_name": "Washington Wizards", "city": "Washington", "nickname": "Wizards", "nba_id": 30},
}

NBA_ID_TO_ABBR = {info["nba_id"]: abbr for abbr, info in CANONICAL_TEAMS.items()}


def _normalize_text(value: str) -> str:
    return value.strip().upper().replace(".", "").replace("-", " ")


def _build_variants() -> Dict[str, str]:
    variants: Dict[str, str] = {}
    for abbr, info in CANONICAL_TEAMS.items():
        full_name = info["full_name"]
        city = info["city"]
        nickname = info["nickname"]

        for raw in (abbr, full_name, f"{city} {nickname}", city, nickname):
            key = _normalize_text(str(raw))
            variants[key] = abbr

        if abbr in ("LAL", "LAC"):
            variants[_normalize_text(f"LA {nickname}")] = abbr
            variants[_normalize_text(f"L A {nickname}")] = abbr
        if abbr == "GSW":
            variants[_normalize_text("Golden State")] = abbr
        if abbr == "NOP":
            variants[_normalize_text("New Orleans")] = abbr
            variants[_normalize_text("NO Pelicans")] = abbr
            variants[_normalize_text("New Orleans Hornets")] = abbr
        if abbr == "NYK":
            variants[_normalize_text("New York")] = abbr
            variants[_normalize_text("NY Knicks")] = abbr
        if abbr == "BKN":
            variants[_normalize_text("Brooklyn")] = abbr
            variants[_normalize_text("New Jersey Nets")] = abbr
        if abbr == "CHA":
            variants[_normalize_text("Charlotte Bobcats")] = abbr

    return variants


TEAM_VARIANTS = _build_variants()


def normalize_team_id(value: object) -> Optional[str]:
    if value is None:
        return None

    if isinstance(value, dict):
        for key in ("id", "team_id", "nba_id"):
            if key in value:
                normalized = normalize_team_id(value.get(key))
                if normalized:
                    return normalized
        for key in ("abbreviation", "abbr", "code", "short_name", "full_name", "name", "city"):
            if key in value:
                normalized = normalize_team_id(value.get(key))
                if normalized:
                    return normalized
        return None

    if isinstance(value, (int, float)) and int(value) in NBA_ID_TO_ABBR:
        return NBA_ID_TO_ABBR[int(value)]

    try:
        as_int = int(str(value))
    except Exception:
        as_int = None
    if as_int is not None and as_int in NBA_ID_TO_ABBR:
        return NBA_ID_TO_ABBR[as_int]

    key = _normalize_text(str(value))
    return TEAM_VARIANTS.get(key)


def normalize_team_payload(value: object) -> Optional[Dict[str, object]]:
    abbr = normalize_team_id(value)
    if not abbr:
        return None
    info = CANONICAL_TEAMS.get(abbr)
    if not info:
        return None
    return {
        "team_id": abbr,
        "team_abbr": abbr,
        "team_nba_id": info["nba_id"],
        "team_name": info["full_name"],
    }


def canonical_team_ids() -> Iterable[str]:
    return CANONICAL_TEAMS.keys()


def validate_canonical_teams() -> None:
    abbrs = list(CANONICAL_TEAMS.keys())
    if len(abbrs) != 30:
        raise AssertionError(f"Expected 30 teams, got {len(abbrs)}")
    if len(set(abbrs)) != 30:
        raise AssertionError("Duplicate team abbreviations detected.")
    nba_ids = [info["nba_id"] for info in CANONICAL_TEAMS.values()]
    if len(set(nba_ids)) != 30:
        raise AssertionError("Duplicate NBA IDs detected.")
