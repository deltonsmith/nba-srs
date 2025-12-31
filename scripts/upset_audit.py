import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

from src.injuries import compute_team_injury_features, load_injuries_for_date
from src.team_stats import load_team_stats
from src.team_normalize import normalize_team_id
from src.time_window import last_n_days

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
        rank_map = {}
        for row in ratings:
            team_id = normalize_team_id(row.get("team_id") or row.get("team_abbr"))
            rating = row.get("rating")
            rank = row.get("rank")
            if team_id is None or rating is None:
                continue
            rating_map[str(team_id)] = float(rating)
            if rank is not None:
                rank_map[str(team_id)] = int(rank)
        snapshots_by_season.setdefault(int(season), []).append(
            {
                "as_of_utc": as_of_utc,
                "as_of_dt": as_of_dt,
                "ratings": rating_map,
                "ranks": rank_map,
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
    # snapshots sorted by as_of_dt
    for snap in reversed(snapshots):
        if snap["as_of_dt"] <= game_dt:
            return snap
    return None


def determine_winner(game: Dict) -> Optional[str]:
    home_score = game.get("home_score")
    away_score = game.get("visitor_score")
    home_id = normalize_team_id(game.get("home_team_id"))
    away_id = normalize_team_id(game.get("visitor_team_id"))
    if home_score is None or away_score is None:
        return None
    try:
        home_score = float(home_score)
        away_score = float(away_score)
    except Exception:
        return None
    if home_score > away_score:
        return str(home_id) if home_id else None
    if away_score > home_score:
        return str(away_id) if away_id else None
    return None


def _travel_proxy(prev_is_home: Optional[bool], is_home: bool) -> str:
    if prev_is_home is None:
        return "no_prior"
    if prev_is_home and is_home:
        return "home_home"
    if prev_is_home and not is_home:
        return "home_to_away"
    if not prev_is_home and is_home:
        return "away_to_home"
    return "away_away"


def build_game_context(games: List[Dict]) -> Dict[int, Dict[str, object]]:
    rows: List[Tuple[datetime, int, str, str]] = []
    for g in games:
        game_id = g.get("game_id")
        game_dt = parse_iso(g.get("date_utc"))
        if game_dt is None or game_id is None:
            continue
        home_id = normalize_team_id(g.get("home_team_id"))
        away_id = normalize_team_id(g.get("visitor_team_id"))
        if not home_id or not away_id:
            continue
        rows.append((game_dt, int(game_id), home_id, away_id))

    rows.sort(key=lambda r: (r[0], r[1]))
    prev_by_team: Dict[str, Tuple[datetime, bool]] = {}
    context: Dict[int, Dict[str, object]] = {}

    for game_dt, game_id, home_id, away_id in rows:
        home_prev = prev_by_team.get(home_id)
        away_prev = prev_by_team.get(away_id)

        home_rest = None
        home_b2b = None
        home_travel = "no_prior"
        if home_prev:
            prev_dt, prev_is_home = home_prev
            home_rest = (game_dt.date() - prev_dt.date()).days
            home_b2b = home_rest <= 1
            home_travel = _travel_proxy(prev_is_home, True)

        away_rest = None
        away_b2b = None
        away_travel = "no_prior"
        if away_prev:
            prev_dt, prev_is_home = away_prev
            away_rest = (game_dt.date() - prev_dt.date()).days
            away_b2b = away_rest <= 1
            away_travel = _travel_proxy(prev_is_home, False)

        context[int(game_id)] = {
            "rest_days_home": home_rest,
            "rest_days_away": away_rest,
            "back_to_back_home": home_b2b,
            "back_to_back_away": away_b2b,
            "travel_proxy_home": home_travel,
            "travel_proxy_away": away_travel,
        }

        prev_by_team[home_id] = (game_dt, True)
        prev_by_team[away_id] = (game_dt, False)

    return context


def build_matchup_context(games: List[Dict], repeat_days: int) -> Dict[int, Dict[str, object]]:
    rows: List[Tuple[datetime, int, str, str]] = []
    for g in games:
        game_id = g.get("game_id")
        game_dt = parse_iso(g.get("date_utc"))
        if game_dt is None or game_id is None:
            continue
        home_id = normalize_team_id(g.get("home_team_id"))
        away_id = normalize_team_id(g.get("visitor_team_id"))
        if not home_id or not away_id:
            continue
        rows.append((game_dt, int(game_id), home_id, away_id))

    rows.sort(key=lambda r: (r[0], r[1]))
    last_meeting: Dict[Tuple[str, str], datetime] = {}
    context: Dict[int, Dict[str, object]] = {}

    for game_dt, game_id, home_id, away_id in rows:
        pair = tuple(sorted([home_id, away_id]))
        prev_dt = last_meeting.get(pair)
        repeat_matchup = False
        days_since = None
        if prev_dt is not None:
            days_since = (game_dt.date() - prev_dt.date()).days
            repeat_matchup = days_since <= repeat_days

        context[int(game_id)] = {
            "repeat_matchup": repeat_matchup,
            "days_since_last_matchup": days_since,
        }

        last_meeting[pair] = game_dt

    return context


def _stat_value(stats: Dict, key: str) -> Optional[float]:
    if stats is None:
        return None
    candidates = [
        key,
        key.lower(),
        key.upper(),
        key.replace("_", ""),
        key.replace("_", "").lower(),
        key.replace("_", "").upper(),
    ]
    for cand in candidates:
        if cand in stats and stats.get(cand) is not None:
            try:
                return float(stats.get(cand))
            except Exception:
                return None
    return None


def _collect_team_stat_subset(stats: Optional[Dict], keys: List[str]) -> Dict[str, Optional[float]]:
    return {key: _stat_value(stats or {}, key) for key in keys}


def _delta_lower_minus_higher(
    stats_by_team: Optional[Dict[str, Dict]],
    lower_team: str,
    higher_team: str,
    keys: List[str],
) -> Dict[str, Optional[float]]:
    if stats_by_team is None:
        return {key: None for key in keys}
    lower_stats = stats_by_team.get(lower_team) if stats_by_team else None
    higher_stats = stats_by_team.get(higher_team) if stats_by_team else None
    deltas: Dict[str, Optional[float]] = {}
    for key in keys:
        lower_val = _stat_value(lower_stats or {}, key)
        higher_val = _stat_value(higher_stats or {}, key)
        if lower_val is None or higher_val is None:
            deltas[key] = None
        else:
            deltas[key] = lower_val - higher_val
    return deltas


def _date_from_game(g: Dict) -> Optional[str]:
    game_dt = parse_iso(g.get("date_utc"))
    if game_dt is None:
        return None
    return game_dt.date().isoformat()


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit upsets from last N days using rating snapshots.")
    parser.add_argument("--days", type=int, default=7, help="Number of days to include (default: 7)")
    parser.add_argument("--season", type=int, help="Season end year (e.g., 2026)")
    parser.add_argument("--repeat-days", type=int, default=7, help="Repeat matchup window in days")
    args = parser.parse_args()

    now_utc = datetime.now(timezone.utc)
    start_utc, _end_utc = last_n_days(now_utc, n=args.days)

    snapshots_by_season = load_snapshots()
    if not snapshots_by_season:
        raise SystemExit("No snapshots found in data/ratings_snapshots.")

    seasons = [args.season] if args.season else sorted(snapshots_by_season.keys())
    all_rows: List[Dict] = []
    injuries_cache: Dict[str, Optional[Dict[str, Dict[str, int]]]] = {}
    team_stats = load_team_stats()
    stat_keys = [
        "pace",
        "ortg",
        "drtg",
        "three_pa_rate",
        "reb_pct",
        "orb",
        "tov",
        "efg",
        "ftr",
    ]

    for season in seasons:
        snapshots = snapshots_by_season.get(season, [])
        if not snapshots:
            continue
        games = load_games(season)
        game_context = build_game_context(games)
        matchup_context = build_matchup_context(games, repeat_days=args.repeat_days)

        for g in games:
            game_dt = parse_iso(g.get("date_utc"))
            if game_dt is None or game_dt < start_utc:
                continue

            home_id = normalize_team_id(g.get("home_team_id"))
            away_id = normalize_team_id(g.get("visitor_team_id"))
            if not home_id or not away_id:
                continue

            snap = find_snapshot(snapshots, game_dt)
            if snap is None:
                continue

            ratings = snap["ratings"]
            ranks = snap["ranks"]
            if home_id not in ratings or away_id not in ratings:
                continue

            home_rating = ratings[home_id]
            away_rating = ratings[away_id]
            if home_rating == away_rating:
                continue

            higher_team = home_id if home_rating > away_rating else away_id
            lower_team = away_id if higher_team == home_id else home_id

            predicted_winner = higher_team
            actual_winner = determine_winner(g)
            if actual_winner is None:
                continue

            home_score = g.get("home_score")
            away_score = g.get("visitor_score")
            margin_abs = None
            margin_higher = None
            try:
                if home_score is not None and away_score is not None:
                    home_score = float(home_score)
                    away_score = float(away_score)
                    margin_abs = abs(home_score - away_score)
                    if higher_team == home_id:
                        margin_higher = home_score - away_score
                    else:
                        margin_higher = away_score - home_score
            except Exception:
                margin_abs = None
            margin_higher = None

            context = game_context.get(int(g.get("game_id")) or 0, {})
            matchup = matchup_context.get(int(g.get("game_id")) or 0, {})
            game_date = _date_from_game(g)
            injury_features = None
            if game_date:
                if game_date not in injuries_cache:
                    rows = load_injuries_for_date(game_date)
                    injuries_cache[game_date] = compute_team_injury_features(rows)
                injury_features = injuries_cache.get(game_date)

            if injury_features is None:
                key_injuries_home = None
                key_injuries_away = None
                star_absence_home = None
                star_absence_away = None
            else:
                home_inj = injury_features.get(home_id, {"key_injuries_count": 0, "star_absence_proxy": 0})
                away_inj = injury_features.get(away_id, {"key_injuries_count": 0, "star_absence_proxy": 0})
                key_injuries_home = home_inj.get("key_injuries_count")
                key_injuries_away = away_inj.get("key_injuries_count")
                star_absence_home = home_inj.get("star_absence_proxy")
                star_absence_away = away_inj.get("star_absence_proxy")

            home_stats_subset = _collect_team_stat_subset(team_stats.get(home_id) if team_stats else None, stat_keys)
            away_stats_subset = _collect_team_stat_subset(team_stats.get(away_id) if team_stats else None, stat_keys)
            style_deltas = _delta_lower_minus_higher(team_stats, lower_team, higher_team, stat_keys)

            row = {
                "game_id": g.get("game_id"),
                "season": season,
                "date_utc": g.get("date_utc"),
                "home_team_id": home_id,
                "visitor_team_id": away_id,
                "home_score": g.get("home_score"),
                "visitor_score": g.get("visitor_score"),
                "snapshot_as_of_utc": snap["as_of_utc"],
                "home_rating": home_rating,
                "away_rating": away_rating,
                "home_rank": ranks.get(home_id),
                "away_rank": ranks.get(away_id),
                "higher_ranked_team": higher_team,
                "lower_ranked_team": lower_team,
                "predicted_winner": predicted_winner,
                "actual_winner": actual_winner,
                "upset": actual_winner == lower_team,
                "home_indicator": lower_team == home_id,
                "margin_abs": margin_abs,
                "margin_higher": margin_higher,
                "rest_days_home": context.get("rest_days_home"),
                "rest_days_away": context.get("rest_days_away"),
                "back_to_back_home": context.get("back_to_back_home"),
                "back_to_back_away": context.get("back_to_back_away"),
                "travel_proxy_home": context.get("travel_proxy_home"),
                "travel_proxy_away": context.get("travel_proxy_away"),
                "repeat_matchup": matchup.get("repeat_matchup"),
                "days_since_last_matchup": matchup.get("days_since_last_matchup"),
                "key_injuries_count_home": key_injuries_home,
                "key_injuries_count_away": key_injuries_away,
                "star_absence_proxy_home": star_absence_home,
                "star_absence_proxy_away": star_absence_away,
                "team_stats_home": home_stats_subset,
                "team_stats_away": away_stats_subset,
                "style_deltas": style_deltas,
            }
            all_rows.append(row)

    all_rows.sort(key=lambda r: (r.get("date_utc") or "", r.get("game_id") or 0))
    upsets = [r for r in all_rows if r.get("upset")]

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    (DATA_DIR / "upset_audit_last7d.json").write_text(json.dumps(all_rows, indent=2), encoding="utf-8")
    (DATA_DIR / "upsets_last7d.json").write_text(json.dumps(upsets, indent=2), encoding="utf-8")

    total_games = len(all_rows)
    upset_count = len(upsets)
    win_rate = None
    if total_games:
        win_rate = (total_games - upset_count) / total_games

    print(f"Total games: {total_games}")
    print(f"Upsets: {upset_count}")
    print(f"Higher-rated win rate: {win_rate:.3f}" if win_rate is not None else "Higher-rated win rate: n/a")


if __name__ == "__main__":
    main()
