# ingest_games.py
import json
import argparse
import os
import random
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional

import requests

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "nba_ratings.db"
STATE_PATH = DATA_DIR / "ingest_state.json"
def games_json_path(season: int) -> Path:
    return DATA_DIR / f"games_{season}.json"
def temp_games_json_path(season: int) -> Path:
    return DATA_DIR / f"games_{season}_tmp.json"

BALLDONTLIE_BASE = "https://api.balldontlie.io/v1"
# Fallback API key provided by user; prefer environment variable in production.
HARDCODED_API_KEY = "8935ae6b-a84f-419b-a2f9-e7ebaf67a98f"
API_KEY = os.environ.get("BALLDONTLIE_API_KEY") or HARDCODED_API_KEY
SESSION = requests.Session()
if API_KEY:
    SESSION.headers.update({"Authorization": f"Bearer {API_KEY}"})
BASE_DELAY_SECONDS = float(os.environ.get("BALDONTLIE_BASE_DELAY") or 0.35)
LAST_REQUEST_TS: float = 0.0
try:
    _per_page_env = int(os.environ.get("BALDONTLIE_PER_PAGE", "100"))
except ValueError:
    _per_page_env = 100
PER_PAGE = max(1, min(100, _per_page_env))


def load_ingest_state() -> Optional[Dict]:
    if not STATE_PATH.exists():
        return None
    try:
        return json.loads(STATE_PATH.read_text())
    except Exception:
        return None


def save_ingest_state(endpoint: str, season: int, postseason: bool, next_cursor: Optional[int]):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "endpoint": endpoint,
        "season": int(season),
        "postseason": bool(postseason),
        "next_cursor": next_cursor,
        "updated_at": datetime.utcnow().isoformat(),
    }
    tmp_path = STATE_PATH.with_suffix(STATE_PATH.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2))
    os.replace(tmp_path, STATE_PATH)


def write_games_json_atomic(season: int, games: List[Dict]):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp_path = temp_games_json_path(season)
    final_path = games_json_path(season)
    tmp_path.write_text(json.dumps(games, indent=2))
    os.replace(tmp_path, final_path)


def request_with_retries(
    url: str,
    headers: Optional[Dict] = None,
    params: Optional[Dict] = None,
    max_retry_429: int = 8,
    max_retry_5xx: int = 5,
):
    attempt_429 = 0
    attempt_5xx = 0
    while True:
        global LAST_REQUEST_TS
        if LAST_REQUEST_TS:
            elapsed = time.time() - LAST_REQUEST_TS
            if elapsed < BASE_DELAY_SECONDS:
                time.sleep(BASE_DELAY_SECONDS - elapsed)

        resp = SESSION.get(url, headers=headers, params=params, timeout=(10, 30))
        LAST_REQUEST_TS = time.time()
        status = resp.status_code

        if status in (401, 403):
            raise SystemExit(f"Balldontlie authentication failed (status {status}); check API key and access.")

        if status == 429:
            attempt_429 += 1
            if attempt_429 > max_retry_429:
                resp.raise_for_status()
            retry_after = resp.headers.get("Retry-After")
            if retry_after:
                try:
                    delay = float(retry_after)
                except ValueError:
                    delay = None
            else:
                delay = None
            if delay is None:
                delay = min(2**attempt_429, 60) + random.uniform(0, 0.5)
            time.sleep(delay)
            continue

        if 500 <= status < 600:
            attempt_5xx += 1
            if attempt_5xx > max_retry_5xx:
                resp.raise_for_status()
            delay = min(2**attempt_5xx, 60) + random.uniform(0, 0.5)
            time.sleep(delay)
            continue

        resp.raise_for_status()
        return resp


def fetch_balldontlie_games(
    season_int: int,
    postseason: Optional[bool] = None,
    start_cursor: Optional[int] = None,
    save_checkpoint: Optional[Callable[[Optional[int]], None]] = None,
) -> Iterable[Dict]:
    """
    Yield all games for a given season from the Balldontlie API.
    Set postseason to True/False to force that filter, or None for both.
    """
    if not API_KEY:
        raise SystemExit("Missing BALldontLIE_API_KEY; set it for Balldontlie access.")

    api_season = season_int - 1  # Balldontlie seasons[] expects start year (e.g., 2025 for 2025-26)

    cursor = start_cursor
    while True:
        params = {
            "seasons[]": api_season,
            "per_page": PER_PAGE,
        }
        if cursor is not None:
            params["cursor"] = cursor
        else:
            params["page"] = 1
        if API_KEY:
            params["api_key"] = API_KEY  # some hosts require key in query
        if postseason is not None:
            params["postseason"] = str(postseason).lower()

        resp = request_with_retries(f"{BALLDONTLIE_BASE}/games", params=params)
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
        next_cursor = meta.get("next_cursor")

        if save_checkpoint:
            save_checkpoint(next_cursor)

        if next_cursor is not None:
            cursor = int(next_cursor)
            continue

        if next_page:
            cursor = int(next_page)
            continue

        if total_pages and (cursor or 1) < int(total_pages):
            cursor = (cursor or 1) + 1
            continue

        if len(data) == per_page:
            cursor = (cursor or 1) + 1
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
    state = load_ingest_state()
    resume_postseason: Optional[bool] = None
    resume_cursor: Optional[int] = None
    if state and state.get("endpoint") == "games" and state.get("season") == season_int:
        resume_postseason = bool(state.get("postseason"))
        resume_cursor = state.get("next_cursor")

    for postseason_flag in (False, True):
        if resume_postseason is True and postseason_flag is False:
            continue

        start_cursor = resume_cursor if resume_postseason is not None and postseason_flag == resume_postseason else None
        save_ingest_state("games", season_int, postseason_flag, start_cursor)

        def checkpoint(next_cursor: Optional[int]):
            save_ingest_state("games", season_int, postseason_flag, next_cursor)

        for game in fetch_balldontlie_games(
            season_int, postseason=postseason_flag, start_cursor=start_cursor, save_checkpoint=checkpoint
        ):
            parsed = normalize_game_row(game, season_int)
            if parsed:
                rows.append(parsed)

        if resume_postseason is not None and postseason_flag == resume_postseason:
            resume_postseason = None
            resume_cursor = None

        if postseason_flag is False:
            save_ingest_state("games", season_int, True, None)

    save_ingest_state("games", season_int, True, None)

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
    parser = argparse.ArgumentParser(description="Ingest NBA games from Balldontlie.")
    parser.add_argument("--resume", action=argparse.BooleanOptionalAction, default=True, help="Resume from checkpoint (default: true)")
    parser.add_argument("--reset", action="store_true", help="Delete ingest checkpoint before running")
    args = parser.parse_args()

    if args.reset and STATE_PATH.exists():
        STATE_PATH.unlink()
        print("Checkpoint reset: deleted data/ingest_state.json")

    # live season
    season_int = 2026
    print(f"Fetching Balldontlie games for season {season_int} (API season {season_int - 1}) ...")

    try:
        if args.resume:
            games = build_games_table(season_int)
        else:
            if STATE_PATH.exists():
                STATE_PATH.unlink()
            games = build_games_table(season_int)
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 429:
            raise SystemExit("Balldontlie rate limit reached even after retries; dataset not updated.")
        raise

    if not games:
        raise SystemExit("No games fetched; aborting to avoid stale ratings.")

    dates = sorted({g["date"] for g in games})
    if dates:
        print(f"Fetched games span {dates[0]} through {dates[-1]}.")

    print(f"Prepared {len(games)} games (regular season + playoffs). Writing atomically...")
    write_games_json_atomic(season_int, games)
    upsert_games(games)
    print("Done.")


if __name__ == "__main__":
    main()
