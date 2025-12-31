"""
Pull final games for the current season and store them locally.
Output: data/games/games_YYYY_season.json
"""

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import requests

from src.team_normalize import normalize_team_id

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data" / "games"
BALLDONTLIE_BASE = "https://api.balldontlie.io/v1"


def request_with_retries(url: str, session: requests.Session, params: Optional[Dict] = None, max_retry_429: int = 6):
    attempt_429 = 0
    while True:
        resp = session.get(url, params=params, timeout=(10, 30))
        if resp.status_code in (401, 403):
            raise SystemExit("Balldontlie authentication failed; check BALLDONTLIE_API_KEY.")
        if resp.status_code == 429:
            attempt_429 += 1
            if attempt_429 > max_retry_429:
                resp.raise_for_status()
            delay = min(2 ** attempt_429, 60)
            time.sleep(delay)
            continue
        resp.raise_for_status()
        return resp


def fetch_games_for_season(season_int: int, session: requests.Session) -> List[Dict]:
    api_season = season_int - 1
    cursor = None
    results: List[Dict] = []

    while True:
        params = {"seasons[]": api_season, "per_page": 100}
        if cursor is not None:
            params["cursor"] = cursor
        resp = request_with_retries(f"{BALLDONTLIE_BASE}/games", session, params=params)
        payload = resp.json()
        data = payload.get("data", []) or []
        meta = payload.get("meta") or {}
        if not data:
            break
        results.extend(data)
        next_cursor = meta.get("next_cursor")
        if next_cursor is None:
            break
        cursor = next_cursor

    return results


def normalize_game(game: Dict) -> Optional[Dict]:
    status = (game.get("status") or "").lower()
    home_score = game.get("home_team_score")
    away_score = game.get("visitor_team_score")

    if "final" not in status:
        return None
    if home_score is None or away_score is None:
        return None

    start_time = game.get("datetime") or game.get("start_time") or game.get("start_time_utc")
    if start_time:
        try:
            as_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00")).astimezone(timezone.utc)
            date_utc = as_dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        except Exception:
            date_utc = start_time
    else:
        date_utc = None

    return {
        "game_id": int(game.get("id")),
        "date_utc": date_utc,
        "home_team_id": normalize_team_id(game.get("home_team") or {}),
        "visitor_team_id": normalize_team_id(game.get("visitor_team") or {}),
        "home_score": int(home_score),
        "visitor_score": int(away_score),
        "status": game.get("status"),
    }


def load_existing(path: Path) -> Dict[int, Dict]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    rows = payload.get("games") if isinstance(payload, dict) else payload
    if not isinstance(rows, list):
        return {}
    return {int(r.get("game_id")): r for r in rows if r.get("game_id") is not None}


def write_output(path: Path, season_int: int, games: Dict[int, Dict]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "season": int(season_int),
        "updated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "games": list(games.values()),
    }
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp_path.replace(path)


def resolve_season(today: datetime.date) -> int:
    # NBA season labeled by end year (Oct–Jun). If month >= 7, next season end year.
    return today.year + 1 if today.month >= 7 else today.year


def main():
    parser = argparse.ArgumentParser(description="Update finalized games for the current season.")
    parser.add_argument("--season", type=int, help="Season end year (e.g., 2026 for 2025-26)")
    args = parser.parse_args()

    season_int = args.season or resolve_season(datetime.utcnow().date())
    api_key = os.environ.get("BALLDONTLIE_API_KEY")
    if not api_key:
        raise SystemExit("BALLDONTLIE_API_KEY is required.")

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {api_key}"})

    raw_games = fetch_games_for_season(season_int, session)
    normalized = [normalize_game(g) for g in raw_games]
    normalized = [g for g in normalized if g is not None]

    out_path = DATA_DIR / f"games_{season_int}_season.json"
    existing = load_existing(out_path)
    for row in normalized:
        existing[int(row["game_id"])] = row

    write_output(out_path, season_int, existing)
    print(f"Wrote {len(existing)} games to {out_path}")


if __name__ == "__main__":
    main()
