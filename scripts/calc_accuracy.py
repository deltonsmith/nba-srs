"""
Compute accuracy metrics using rating snapshots and final games.

Policy:
- Ties in ratings are skipped (not counted in accuracy).
"""

import csv
import json
from bisect import bisect_right
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
SNAPSHOT_DIR = DATA_DIR / "ratings_snapshots"
GAMES_DIR = DATA_DIR / "games"
METRICS_DIR = DATA_DIR / "metrics"


def parse_iso(dt_str: Optional[str]) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def load_snapshots() -> Dict[int, List[Dict]]:
    snapshots_by_season: Dict[int, List[Dict]] = {}
    if not SNAPSHOT_DIR.exists():
        return snapshots_by_season

    for path in sorted(SNAPSHOT_DIR.glob("ratings_*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        as_of_utc = payload.get("as_of_utc")
        season = payload.get("season")
        ratings = payload.get("ratings")
        if not as_of_utc or not season or not isinstance(ratings, list):
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
        snapshots_by_season.setdefault(int(season), []).append(
            {
                "as_of_utc": as_of_utc,
                "as_of_dt": as_of_dt,
                "ratings": rating_map,
            }
        )

    for season, rows in snapshots_by_season.items():
        rows.sort(key=lambda r: r["as_of_dt"])
    return snapshots_by_season


def load_games(season: int) -> List[Dict]:
    path = GAMES_DIR / f"games_{season}_season.json"
    if not path.exists():
        return []
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


def build_history_rows(season: int, snapshots: List[Dict], games: List[Dict]) -> List[Dict]:
    rows = []
    for g in games:
        date_utc = g.get("date_utc")
        game_dt = parse_iso(date_utc)
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
        rating_gap = abs(home_rating - away_rating)
        winner = determine_winner(g)
        if winner is None:
            continue
        correct = 1 if winner == higher_team else 0

        rows.append(
            {
                "season": season,
                "game_id": g.get("game_id"),
                "date_utc": date_utc,
                "higher_team_id": higher_team,
                "winner_team_id": winner,
                "correct": correct,
                "rating_gap": rating_gap,
                "snapshot_as_of_utc": snap["as_of_utc"],
            }
        )
    return rows


def summarize(rows: List[Dict], now_utc: datetime) -> Dict:
    def win_rate(subset: List[Dict]) -> Optional[float]:
        if not subset:
            return None
        return sum(r["correct"] for r in subset) / len(subset)

    last_7_cutoff = now_utc - timedelta(days=7)
    last_30_cutoff = now_utc - timedelta(days=30)

    parsed_rows = []
    for r in rows:
        dt = parse_iso(r["date_utc"])
        if dt is None:
            continue
        parsed = dict(r)
        parsed["_dt"] = dt
        parsed_rows.append(parsed)

    last_7 = [r for r in parsed_rows if r["_dt"] >= last_7_cutoff]
    last_30 = [r for r in parsed_rows if r["_dt"] >= last_30_cutoff]

    return {
        "games_total": len(parsed_rows),
        "win_rate_total": win_rate(parsed_rows),
        "games_last_7": len(last_7),
        "win_rate_last_7": win_rate(last_7),
        "games_last_30": len(last_30),
        "win_rate_last_30": win_rate(last_30),
    }


def main():
    snapshots_by_season = load_snapshots()
    if not snapshots_by_season:
        raise SystemExit("No snapshots found in data/ratings_snapshots.")

    now_utc = datetime.now(timezone.utc)
    history_rows: List[Dict] = []
    season_summaries = {}

    for season, snapshots in snapshots_by_season.items():
        games = load_games(season)
        if not games:
            continue
        rows = build_history_rows(season, snapshots, games)
        history_rows.extend(rows)
        season_summaries[str(season)] = summarize(rows, now_utc)

    overall_summary = summarize(history_rows, now_utc)
    payload = {
        "as_of_utc": now_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "tie_policy": "skip",
        "overall": overall_summary,
        "seasons": season_summaries,
    }

    METRICS_DIR.mkdir(parents=True, exist_ok=True)
    accuracy_path = METRICS_DIR / "accuracy.json"
    accuracy_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    history_path = METRICS_DIR / "accuracy_history.csv"
    with history_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "season",
                "game_id",
                "date_utc",
                "higher_team_id",
                "winner_team_id",
                "correct",
                "rating_gap",
                "snapshot_as_of_utc",
            ]
        )
        for row in history_rows:
            w.writerow(
                [
                    row["season"],
                    row["game_id"],
                    row["date_utc"],
                    row["higher_team_id"],
                    row["winner_team_id"],
                    row["correct"],
                    row["rating_gap"],
                    row["snapshot_as_of_utc"],
                ]
            )

    print(f"Wrote {accuracy_path} and {history_path} ({len(history_rows)} rows).")


if __name__ == "__main__":
    main()
