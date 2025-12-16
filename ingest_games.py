# ingest_games.py
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import requests

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "nba_ratings.db"

BALLDONTLIE_BASE = "https://api.balldontlie.io/v1"
# Fallback API key provided by user; prefer environment variable in production.
HARDCODED_API_KEY = "8935ae6b-a84f-419b-a2f9-e7ebaf67a98f"
API_KEY = os.environ.get("BALLDONTLIE_API_KEY") or HARDCODED_API_KEY
SESSION = requests.Session()
if API_KEY:
    SESSION.headers.update({"Authorization": f"Bearer {API_KEY}"})


def fetch_balldontlie_games(
    season_int: int, postseason: Optional[bool] = None
) -> Iterable[Dict]:
    """
    Yield all games for a given season from the Balldontlie API.
    Set postseason to True/False to force that filter, or None for both.
    """
    if not API_KEY:
        raise SystemExit("Missing BALldontLIE_API_KEY; set it for Balldontlie access.")

    api_season = season_int - 1  # Balldontlie seasons[] expects start year (e.g., 2025 for 2025-26)

    page = 1
    while True:
        params = {
            "seasons[]": api_season,
            "per_page": 100,
            "page": page,
        }
        if API_KEY:
            params["api_key"] = API_KEY  # some hosts require key in query
        if postseason is not None:
            params["postseason"] = str(postseason).lower()

        resp = SESSION.get(f"{BALLDONTLIE_BASE}/games", params=params, timeout=30)
        resp.raise_for_status()
        payload = resp.json()

        data = payload.get("data", [])
        meta = payload.get("meta") or {}

        if not data:
            break

        for g in data:
            yield g

        # Robust pagination: use next_page if present, else keep paging while full.
        per_page = int(meta.get("per_page", params["per_page"]))
        next_page = meta.get("next_page")
        total_pages = meta.get("total_pages")

        if next_page:
            page = int(next_page)
            continue

        if total_pages and page < int(total_pages):
            page += 1
            continue

        if len(data) == per_page:
            page += 1
            continue

        break


def normalize_game_row(game: Dict, season_int: int) -> Optional[Dict]:
    """Convert a Balldontlie game payload into the schema our DB expects."""
    home = game.get("home_team") or {}
    away = game.get("visitor_team") or {}

    home_abbr = home.get("abbreviation")
    away_abbr = away.get("abbreviation")

    home_pts = game.get("home_team_score")
    away_pts = game.get("visitor_team_score")

    status = (game.get("status") or "").lower()

    # Skip games without scores or not marked final
    if home_pts is None or away_pts is None:
        return None
    if isinstance(status, str) and "final" not in status and (home_pts == 0 or away_pts == 0):
        return None

    date_raw = game.get("date")
    date = date_raw[:10] if isinstance(date_raw, str) else datetime.utcnow().date().isoformat()

    return {
        "game_id": int(game["id"]),
        "season": int(season_int),
        "date": date,
        "home_team_id": home_abbr,
        "away_team_id": away_abbr,
        "home_pts": int(home_pts),
        "away_pts": int(away_pts),
    }


def build_games_table(season_int: int) -> List[Dict]:
    """
    Pull regular season and playoff games from Balldontlie and shape them for the DB.
    """
    rows: List[Dict] = []

    for postseason_flag in (False, True):
        for game in fetch_balldontlie_games(season_int, postseason=postseason_flag):
            parsed = normalize_game_row(game, season_int)
            if parsed:
                rows.append(parsed)

    return rows


def upsert_games(games: List[Dict]):
    if not games:
        print("No games to upsert.")
        return

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    for r in games:
        cur.execute(
            """
            INSERT OR REPLACE INTO games
                (game_id, season, date, home_team_id, away_team_id, home_pts, away_pts)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                r["game_id"],
                int(r["season"]),
                r["date"],
                r["home_team_id"],
                r["away_team_id"],
                int(r["home_pts"]),
                int(r["away_pts"]),
            ),
        )

    conn.commit()
    conn.close()
    print(f"Upserted {len(games)} rows into games.")


def main():
    # live season
    season_int = 2026
    print(f"Fetching Balldontlie games for season {season_int} (API season {season_int - 1}) ...")

    games = build_games_table(season_int)
    if not games:
        raise SystemExit("No games fetched; aborting to avoid stale ratings.")

    dates = sorted({g["date"] for g in games})
    if dates:
        print(f"Fetched games span {dates[0]} through {dates[-1]}.")

    print(f"Prepared {len(games)} games (regular season + playoffs). Writing to DB...")
    upsert_games(games)
    print("Done.")


if __name__ == "__main__":
    main()
