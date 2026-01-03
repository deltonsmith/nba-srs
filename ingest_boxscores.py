# ingest_boxscores.py
import argparse
import json
import os
import sqlite3
from pathlib import Path
from time import sleep
from typing import Dict, List, Optional

import requests

from src.team_normalize import normalize_team_id
DB_PATH = "data/nba_ratings.db"
STATE_PATH = Path("data") / "boxscore_ingest_state.json"
BALLDONTLIE_BASE = "https://api.balldontlie.io/v1"
API_KEY = os.environ.get("BALLDONTLIE_API_KEY")
SESSION = requests.Session()
if API_KEY:
    SESSION.headers.update({"Authorization": f"Bearer {API_KEY}"})


def get_all_game_ids(season_int: int):
    """Return all game IDs for a given season, zero-padded to 10 chars."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT game_id FROM games WHERE season = ?", (season_int,))
    ids = [str(row[0]).zfill(10) for row in cur.fetchall()]
    conn.close()
    return ids


def game_already_ingested(game_id: str) -> bool:
    """Return True if we already have any appearances for this game."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM appearances WHERE game_id = ? LIMIT 1;",
        (game_id,),
    )
    row = cur.fetchone()
    conn.close()
    return row is not None


def load_state() -> Optional[Dict]:
    if not STATE_PATH.exists():
        return None
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None


def save_state(season_int: int, next_index: int) -> None:
    payload = {"season": int(season_int), "next_index": int(next_index)}
    tmp_path = STATE_PATH.with_suffix(STATE_PATH.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp_path.replace(STATE_PATH)


def parse_minutes_to_float(value) -> float:
    """
    Convert various minute formats to float minutes.

    Handles:
      - 'MM:SS'
      - 'PT23M17S' or 'PT12M'
      - blanks / None -> 0.0
    """
    if not isinstance(value, str):
        return 0.0

    s = value.strip()
    if not s:
        return 0.0

    # Old style: "MM:SS"
    if ":" in s:
        try:
            mm, ss = s.split(":")
            return int(mm) + int(ss) / 60.0
        except Exception:
            return 0.0

    # ISO style: "PT23M17S", "PT12M"
    if s.startswith("PT"):
        s2 = s[2:]
        mins = 0
        secs = 0
        if "M" in s2:
            m_part, rest = s2.split("M", 1)
            if m_part:
                try:
                    mins = int(m_part)
                except Exception:
                    mins = 0
            if rest.endswith("S"):
                sec_part = rest[:-1]
                if sec_part:
                    try:
                        secs = int(sec_part)
                    except Exception:
                        secs = 0
        return mins + secs / 60.0

    return 0.0


def fetch_stats_for_game(game_id: str) -> List[Dict]:
    """
    Fetch player stats for a single game from Balldontlie.
    """
    if not API_KEY:
        raise SystemExit("Missing BALldontLIE_API_KEY; set it for Balldontlie access.")

    stats: List[Dict] = []
    page = 1
    while True:
        params = {"game_ids[]": game_id, "per_page": 100, "page": page}
        if API_KEY:
            params["api_key"] = API_KEY
        resp = SESSION.get(f"{BALLDONTLIE_BASE}/stats", params=params, timeout=30)
        resp.raise_for_status()
        payload = resp.json()

        data = payload.get("data", [])
        meta = payload.get("meta") or {}

        if not data:
            break

        stats.extend(data)
        total_pages = int(meta.get("total_pages", page))
        if page >= total_pages:
            break
        page += 1
        sleep(1)  # polite throttle across pages

    return stats


def save_appearances(game_id: str, season_int: int):
    """Pull one game's stats and write player appearances."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    api_game_id = game_id.lstrip("0") or game_id
    stats_rows = fetch_stats_for_game(api_game_id)

    if not stats_rows:
        print(f"    no stats returned for {game_id}")
        conn.close()
        return

    for row in stats_rows:
        player = row.get("player") or {}
        team = row.get("team") or {}

        player_name = f"{player.get('first_name', '').strip()} {player.get('last_name', '').strip()}".strip()
        team_abbr = normalize_team_id(team)

        if not player_name or not team_abbr:
            continue

        minutes_val = parse_minutes_to_float(row.get("min"))

        # Find or create player
        cur.execute(
            """
            SELECT player_id FROM players
            WHERE name = ? AND team_id = ? AND season = ?
            """,
            (player_name, team_abbr, season_int),
        )
        row_found = cur.fetchone()

        if row_found:
            player_id = row_found[0]
        else:
            cur.execute(
                """
                INSERT INTO players (name, team_id, season)
                VALUES (?, ?, ?)
                """,
                (player_name, team_abbr, season_int),
            )
            player_id = cur.lastrowid

        # Insert appearance
        cur.execute(
            """
            INSERT OR REPLACE INTO appearances
                (game_id, team_id, player_id, minutes)
            VALUES (?, ?, ?, ?)
            """,
            (game_id, team_abbr, player_id, minutes_val),
        )

    conn.commit()
    conn.close()


def main():
    parser = argparse.ArgumentParser(description="Ingest player boxscores into appearances table.")
    parser.add_argument("--season", type=int, help="Season end year (e.g., 2025 for 2024-25).")
    parser.add_argument("--max-games", type=int, help="Maximum games to process this run.")
    parser.add_argument("--resume", action=argparse.BooleanOptionalAction, default=True, help="Resume from checkpoint (default: true).")
    parser.add_argument("--reset", action="store_true", help="Delete checkpoint before running.")
    args = parser.parse_args()

    if args.reset and STATE_PATH.exists():
        STATE_PATH.unlink()
        print("Checkpoint reset: deleted data/boxscore_ingest_state.json")

    # season you are ingesting
    SEASON_INT = args.season or 2026  # 2025–26 season

    game_ids = get_all_game_ids(SEASON_INT)
    start_index = 0
    if args.resume:
        state = load_state()
        if state and state.get("season") == SEASON_INT:
            start_index = int(state.get("next_index", 0))
    print(f"Found {len(game_ids)} games. Fetching box scores...")

    processed = 0
    for idx, gid in enumerate(game_ids[start_index:], start=start_index):
        if game_already_ingested(gid):
            print(f"  skipping {gid} (already in DB)")
            processed += 1
            save_state(SEASON_INT, idx + 1)
            if args.max_games and processed >= args.max_games:
                break
            continue

        print(f"  pulling box score for {gid} ...")
        try:
            save_appearances(gid, SEASON_INT)
        except Exception as e:
            print(f"    ERROR on {gid}: {e}")
        sleep(1)  # throttle requests
        processed += 1
        save_state(SEASON_INT, idx + 1)
        if args.max_games and processed >= args.max_games:
            break

    if start_index + processed >= len(game_ids) and STATE_PATH.exists():
        STATE_PATH.unlink()
        print("Completed full season; removed checkpoint.")
    print("Done.")


if __name__ == "__main__":
    main()
