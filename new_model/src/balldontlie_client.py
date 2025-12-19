"""
Balldontlie v2 lightweight client with cursor pagination helpers.
"""

import os
from typing import Dict, Iterable, List, Optional

import requests


BASE_URL = "https://api.balldontlie.io/v2"
API_KEY = os.environ.get("BALLDONTLIE_API_KEY")

if not API_KEY:
    raise RuntimeError("BALLDONTLIE_API_KEY is required for balldontlie_client")


def _get(path: str, params: Optional[Dict] = None) -> Dict:
    url = f"{BASE_URL}{path}"
    headers = {"Authorization": f"Bearer {API_KEY}"}
    resp = requests.get(url, headers=headers, params=params or {}, timeout=(10, 30))
    resp.raise_for_status()
    return resp.json()


def _paginate(path: str, params: Optional[Dict] = None, per_page: int = 100) -> Iterable[Dict]:
    cursor: Optional[int] = None
    while True:
        page_params = dict(params or {})
        page_params["per_page"] = min(per_page, 100)
        if cursor is not None:
            page_params["cursor"] = cursor
        data = _get(path, page_params)
        items = data.get("data", []) or []
        meta = data.get("meta") or {}
        for item in items:
            yield item
        next_cursor = meta.get("next_cursor")
        if next_cursor is None:
            break
        cursor = next_cursor


def fetch_games(date_str: str) -> List[Dict]:
    """Fetch all games for a given date (YYYY-MM-DD)."""
    return list(_paginate("/nba/v1/games", params={"dates[]": [date_str]}))


def fetch_odds_by_date(date_str: str) -> List[Dict]:
    """Fetch all odds for a given date (all vendors returned)."""
    return list(_paginate("/nba/v2/odds", params={"dates": [date_str]}))


def fetch_odds_by_game_ids(game_ids: List[int], chunk_size: int = 50) -> List[Dict]:
    """Fetch odds for a list of game IDs in chunks."""
    results: List[Dict] = []
    for i in range(0, len(game_ids), chunk_size):
        chunk = game_ids[i : i + chunk_size]
        results.extend(_paginate("/nba/v2/odds", params={"game_ids": chunk}))
    return results
