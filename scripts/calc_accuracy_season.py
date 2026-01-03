import argparse
import json
from bisect import bisect_right
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
SNAPSHOT_DIR = DATA_DIR / "ratings_snapshots"
GAMES_DIR = DATA_DIR / "games"


def parse_iso(dt_str: Optional[str]) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def load_snapshots_for_season(season: int) -> List[Dict]:
    rows: List[Dict] = []
    for path in sorted(SNAPSHOT_DIR.glob("ratings_*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if payload.get("season") != season:
            continue
        as_of_utc = payload.get("as_of_utc")
        ratings = payload.get("ratings")
        if not as_of_utc or not isinstance(ratings, list):
            continue
        as_of_dt = parse_iso(as_of_utc)
        if as_of_dt is None:
            continue
        rating_map = {}
        for row in ratings:
            team_id = row.get("team_id")
            rating = row.get("rating")
            if team_id is None or rating is None:
                continue
            rating_map[str(team_id)] = float(rating)
        rows.append({"as_of_utc": as_of_utc, "as_of_dt": as_of_dt, "ratings": rating_map})
    rows.sort(key=lambda r: r["as_of_dt"])
    return rows


def load_games(season: int) -> List[Dict]:
    path = GAMES_DIR / f"games_{season}_season.json"
    if not path.exists():
        raise SystemExit(f"Missing {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("games") if isinstance(payload, dict) else payload
    if not isinstance(rows, list):
        return []
    return rows


def find_snapshot(snapshots: List[Dict], game_dt: datetime) -> Optional[Dict]:
    if not snapshots:
        return None
    times = [s["as_of_dt"] for s in snapshots]
    idx = bisect_right(times, game_dt) - 1
    if idx < 0:
        return None
    return snapshots[idx]


def determine_winner(game: Dict) -> Optional[str]:
    home_score = game.get("home_score")
    away_score = game.get("visitor_score")
    home_id = game.get("home_team_id")
    away_id = game.get("visitor_team_id")
    if home_score is None or away_score is None:
        return None
    try:
        home_score = float(home_score)
        away_score = float(away_score)
    except Exception:
        return None
    if home_score > away_score:
        return str(home_id)
    if away_score > home_score:
        return str(away_id)
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute season-to-date higher-rated win rate.")
    parser.add_argument("--season", type=int, default=2025, help="Season end year (default 2025).")
    args = parser.parse_args()

    snapshots = load_snapshots_for_season(args.season)
    if not snapshots:
        raise SystemExit(f"No snapshots found for season {args.season}.")
    games = load_games(args.season)

    total = 0
    correct = 0
    for g in games:
        game_dt = parse_iso(g.get("date_utc"))
        if game_dt is None:
            continue
        snap = find_snapshot(snapshots, game_dt)
        if snap is None:
            continue
        ratings = snap["ratings"]
        home_id = str(g.get("home_team_id"))
        away_id = str(g.get("visitor_team_id"))
        if home_id not in ratings or away_id not in ratings:
            continue
        home_rating = ratings[home_id]
        away_rating = ratings[away_id]
        if home_rating == away_rating:
            continue
        higher_team = home_id if home_rating > away_rating else away_id
        winner = determine_winner(g)
        if winner is None:
            continue
        total += 1
        if winner == higher_team:
            correct += 1

    win_rate = correct / total if total else None
    print(f"Season {args.season} higher-rated win rate: {win_rate:.4f}" if win_rate is not None else "n/a")
    print(f"Games counted: {total}")


if __name__ == "__main__":
    main()
