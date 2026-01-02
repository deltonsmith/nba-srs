# compute_ratings.py
#
# Lineup-adjusted SRS ratings for multiple NBA seasons.
# - Uses games(season), appearances, players, player_values(v_p).
# - Outputs canonical ratings_current.json into data/ and dated snapshots into data/history/YYYY-MM-DD.json.
# - Also writes a dated CSV snapshot to data/csv/ratings_<SEASON_INT>_YYYYMMDD.csv.
# - Includes last_week_rank based on a weekly snapshot file or CSV fallback.
# - Includes yest_rank based on the prior daily ratings file or CSV fallback.

import csv
import json
import shutil
import sqlite3
from collections import defaultdict
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ------------ CONFIGURATION ------------

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "nba_ratings.db"

# Seasons are labeled by the year they END:
# 2023-24 -> 2024, 2024-25 -> 2025, 2025-26 -> 2026, etc.
SEASONS = [2026]

HCA = 2.5          # home-court advantage in points
LINEUP_SHRINK = 0.5  # shrink lineup adjustment to reduce over-correction
BLOWOUT_CAP = 20.0   # cap margins to limit outlier blowouts
WIN_BLEND_W = 0.25   # blend weight toward win%
WIN_BLEND_SCALE = 20.0  # scale win% into rating points
RECENCY_HALF_LIFE_GAMES = 15.0  # exponential decay half-life in games
MAX_ITERS = 100    # SRS iteration count


# ------------ DATA LOADING ------------

def load_player_values(conn, season_int):
    """Return dict: player_id -> v_p (per-game value) for this season."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT player_id, v_p
        FROM player_values
        WHERE season = ?
        """,
        (season_int,),
    )
    return {pid: v for (pid, v) in cur.fetchall()}


def compute_team_full_values(conn, player_values, season_int, core_size=8):
    """
    For each team, compute its 'full-strength' value by summing v_p
    for its top N (core_size) players by minutes played over the season.
    Returns dict: team_id -> full_strength_v.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT a.team_id, p.player_id, SUM(a.minutes) AS total_min
        FROM appearances a
        JOIN players p ON a.player_id = p.player_id
        WHERE p.season = ?
        GROUP BY a.team_id, p.player_id
        """,
        (season_int,),
    )
    rows = cur.fetchall()

    by_team = defaultdict(list)
    for team_id, player_id, total_min in rows:
        v = player_values.get(player_id, 0.0)
        by_team[team_id].append((player_id, total_min, v))

    full_values = {}
    for team_id, plist in by_team.items():
        plist.sort(key=lambda x: x[1], reverse=True)
        core = plist[:core_size]
        full_values[team_id] = sum(v for _, _, v in core)

    return full_values


def compute_game_records(conn, player_values, team_full_values, season_int):
    """
    Build per-team game records with lineup-adjusted margins.

    Returns:
        records_by_team: dict[team_id] -> list of records, each:
            {
                "opp": opp_team_id,
                "M_adj": adjusted_margin,
                "home_flag": +1 for home, -1 for away
            }
    """
    cur = conn.cursor()

    cur.execute(
        """
        SELECT game_id, date, home_team_id, away_team_id, home_pts, away_pts
        FROM games
        WHERE season = ?
        """,
        (season_int,),
    )
    games = cur.fetchall()

    cur.execute(
        """
        SELECT game_id, team_id, player_id
        FROM appearances
        """
    )
    app_rows = cur.fetchall()

    appearances_by_game_team = defaultdict(list)
    for game_id_raw, team_id, player_id in app_rows:
        gid = str(game_id_raw).zfill(10)
        appearances_by_game_team[(gid, team_id)].append(player_id)

    records_by_team = defaultdict(list)

    for game_id_raw, date, home_team, away_team, home_pts, away_pts in games:
        game_id = str(game_id_raw).zfill(10)

        home_players = appearances_by_game_team[(game_id, home_team)]
        away_players = appearances_by_game_team[(game_id, away_team)]

        v_full_home = team_full_values.get(home_team, 0.0)
        v_full_away = team_full_values.get(away_team, 0.0)

        v_game_home = sum(player_values.get(pid, 0.0) for pid in home_players)
        v_game_away = sum(player_values.get(pid, 0.0) for pid in away_players)

        L_home = v_game_home - v_full_home
        L_away = v_game_away - v_full_away

        E_home = L_home - L_away
        margin_home = home_pts - away_pts
        if margin_home > BLOWOUT_CAP:
            margin_home = BLOWOUT_CAP
        elif margin_home < -BLOWOUT_CAP:
            margin_home = -BLOWOUT_CAP
        M_adj_home = margin_home - LINEUP_SHRINK * E_home

        records_by_team[home_team].append(
            {
                "opp": away_team,
                "M_adj": M_adj_home,
                "home_flag": 1,
                "date": date,
            }
        )

        E_away = -E_home
        margin_away = -margin_home
        M_adj_away = margin_away - LINEUP_SHRINK * E_away

        records_by_team[away_team].append(
            {
                "opp": home_team,
                "M_adj": M_adj_away,
                "home_flag": -1,
                "date": date,
            }
        )

    return records_by_team


# ------------ COMPONENT HELPERS ------------

def compute_team_results(conn, season_int):
    """
    Return per-team aggregates based on actual scores:
    games, wins, losses, point_diff_sum.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT home_team_id, away_team_id, home_pts, away_pts
        FROM games
        WHERE season = ? AND home_pts IS NOT NULL AND away_pts IS NOT NULL
        """,
        (season_int,),
    )
    rows = cur.fetchall()
    stats = defaultdict(lambda: {"games": 0, "wins": 0, "losses": 0, "point_diff_sum": 0.0})
    for home_team, away_team, home_pts, away_pts in rows:
        try:
            home_pts = float(home_pts)
            away_pts = float(away_pts)
        except Exception:
            continue
        margin = home_pts - away_pts
        stats[home_team]["games"] += 1
        stats[away_team]["games"] += 1
        stats[home_team]["point_diff_sum"] += margin
        stats[away_team]["point_diff_sum"] -= margin
        if margin > 0:
            stats[home_team]["wins"] += 1
            stats[away_team]["losses"] += 1
        elif margin < 0:
            stats[away_team]["wins"] += 1
            stats[home_team]["losses"] += 1
    return stats


def compute_component_stats(conn, records_by_team, ratings, season_int):
    """
    Build per-team components to log alongside ratings.
    Components are intentionally simple and derived from existing data.
    """
    results = compute_team_results(conn, season_int)
    components = {}
    for team, rating in ratings.items():
        recs = records_by_team.get(team, [])
        games = len(recs)
        avg_adj_margin = None
        avg_home_flag = None
        avg_opp_rating = None
        if recs:
            avg_adj_margin = sum(r["M_adj"] for r in recs) / games
            avg_home_flag = sum(r["home_flag"] for r in recs) / games
            avg_opp_rating = sum(ratings[r["opp"]] for r in recs) / games

        res = results.get(team, {})
        games_played = res.get("games", 0)
        wins = res.get("wins", 0)
        win_pct = (wins / games_played) if games_played else None
        avg_margin = (res.get("point_diff_sum", 0.0) / games_played) if games_played else None

        components[team] = {
            "games_played": games_played if games_played else None,
            "win_pct": win_pct,
            "avg_margin": avg_margin,
            "avg_adj_margin": avg_adj_margin,
            "sos_avg_opp_rating": avg_opp_rating,
            "avg_home_flag": avg_home_flag,
        }

    return components


# ------------ SRS ITERATION ------------

def iterate_ratings(records_by_team):
    """
    Solve SRS-style ratings via fixed-point iteration.

    ratings[t] <- average( M_adj - HCA*home_flag + opp_rating )
    """
    teams = sorted(records_by_team.keys())
    ratings = {t: 0.0 for t in teams}

    for _ in range(MAX_ITERS):
        new_ratings = {}

        for t in teams:
            recs = records_by_team[t]
            if not recs:
                new_ratings[t] = ratings[t]
                continue

            total_weight = 0.0
            sum_M_adj = 0.0
            sum_home_flag = 0.0
            sum_opp_rating = 0.0
            # Recency weights: newer games count more (half-life in games).
            n = len(recs)
            for idx, r in enumerate(recs):
                age = (n - 1) - idx
                weight = 0.5 ** (age / RECENCY_HALF_LIFE_GAMES)
                total_weight += weight
                sum_M_adj += weight * r["M_adj"]
                sum_home_flag += weight * r["home_flag"]
                sum_opp_rating += weight * ratings[r["opp"]]

            if total_weight == 0:
                new_ratings[t] = ratings[t]
                continue

            avg_M_adj = sum_M_adj / total_weight
            avg_home_flag = sum_home_flag / total_weight
            avg_opp_rating = sum_opp_rating / total_weight

            new_ratings[t] = (avg_M_adj - HCA * avg_home_flag) + avg_opp_rating

        ratings = new_ratings

    if ratings:
        mean_rating = sum(ratings.values()) / len(ratings)
        ratings = {t: r - mean_rating for t, r in ratings.items()}

    return ratings


# ------------ OUTPUT HELPERS ------------

def load_yesterday_ranks(daily_json_path: Path):
    """Load yesterday's ranks from the existing ratings file, if present."""
    if not daily_json_path.exists():
        print("No prior ratings file found; Yest column will be blank this run.")
        return {}

    try:
        with daily_json_path.open("r", encoding="utf-8") as f:
            prev_data = json.load(f)
    except Exception as e:
        print(f"Warning: could not load yesterday's ranks from {daily_json_path}: {e}")
        return {}

    mapping = {}
    entries = []
    if isinstance(prev_data, list):
        entries = prev_data
    elif isinstance(prev_data, dict):
        entries = prev_data.get("ratings") or []

    for entry in entries:
        team_id = entry.get("team")
        prev_rank = entry.get("rank")
        if team_id is not None and prev_rank is not None:
            try:
                mapping[team_id] = int(prev_rank)
            except (TypeError, ValueError):
                continue

    print(f"Loaded yesterday's ranks for {len(mapping)} teams from {daily_json_path}.")
    return mapping


def parse_ranks_from_csv(csv_path: Path):
    """Return dict of team -> rank from a ratings CSV snapshot."""
    ranks = {}
    try:
        with csv_path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                team = row.get("team")
                rank = row.get("rank")
                if team and rank is not None:
                    try:
                        ranks[team] = int(rank)
                    except (TypeError, ValueError):
                        continue
    except Exception as e:
        print(f"Warning: could not parse ranks from {csv_path}: {e}")
        return {}

    return ranks


def load_prev_day_ranks_from_csv(season_int: int, today):
    """Load the most recent CSV snapshot before 'today' for yesterday ranks."""
    csv_dir = DATA_DIR / "csv"
    pattern = f"ratings_{season_int}_*.csv"
    candidates = []

    for path in csv_dir.glob(pattern):
        try:
            date_part = path.stem.split("_")[-1]
            snap_date = datetime.strptime(date_part, "%Y%m%d").date()
        except Exception:
            continue
        if snap_date < today:
            candidates.append((snap_date, path))

    if not candidates:
        return {}

    latest_date, latest_path = max(candidates, key=lambda x: x[0])
    ranks = parse_ranks_from_csv(latest_path)
    print(f"Loaded prev-day ranks ({latest_date}) from {latest_path}.")
    return ranks


def load_last_week_ranks_from_csv(season_int: int, today):
    """
    Load ranks from the most recent Monday snapshot on or before last Monday.
    If no Monday snapshot exists, fall back to the latest snapshot at least 7 days old.
    """
    csv_dir = DATA_DIR / "csv"
    pattern = f"ratings_{season_int}_*.csv"
    target_monday = today - timedelta(days=today.weekday() or 7)

    best = None
    for path in csv_dir.glob(pattern):
        try:
            date_part = path.stem.split("_")[-1]
            snap_date = datetime.strptime(date_part, "%Y%m%d").date()
        except Exception:
            continue

        if snap_date <= target_monday:
            if snap_date.weekday() == 0:
                if best is None or snap_date > best[0]:
                    best = (snap_date, path)
            elif best is None:
                best = (snap_date, path)

    if best is None:
        return {}

    snap_date, snap_path = best
    ranks = parse_ranks_from_csv(snap_path)
    print(f"Loaded last-week ranks ({snap_date}) from {snap_path}.")
    return ranks


def build_ratings_payload(ratings, last_week_ranks, yesterday_ranks, season_int, as_of_utc, run_date_utc, components=None):
    """Build ratings payload with metadata and ranks."""
    sorted_items = sorted(ratings.items(), key=lambda x: x[1], reverse=True)

    rows = []
    for rank, (team, rating) in enumerate(sorted_items, start=1):
        lw_rank = last_week_ranks.get(team)
        yest_rank = yesterday_ranks.get(team)
        rows.append(
            {
                "team": team,
                "rating": float(rating),
                "rank": rank,
                "yest_rank": int(yest_rank) if yest_rank is not None else None,
                "last_week_rank": int(lw_rank) if lw_rank is not None else None,
                "components": (components or {}).get(team),
            }
        )

    return {
        "as_of_utc": as_of_utc,
        "run_date_utc": run_date_utc,
        "season": int(season_int),
        "source": "balldontlie",
        "ratings": rows,
    }


def build_snapshot_payload(ratings, season_int, as_of_utc):
    sorted_items = sorted(ratings.items(), key=lambda x: x[1], reverse=True)
    rows = []
    for rank, (team_abbr, rating) in enumerate(sorted_items, start=1):
        rows.append(
            {
                "team_id": team_abbr,
                "team_abbr": team_abbr,
                "rating": float(rating),
                "rank": rank,
            }
        )
    return {
        "as_of_utc": as_of_utc,
        "season": int(season_int),
        "ratings": rows,
    }


def write_ratings_json(payload, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def write_ratings_csv(ratings_dict, season_int):
    """Write a dated CSV snapshot of ratings for a season."""
    today = datetime.today().date()
    date_str = today.strftime("%Y%m%d")

    out_dir = DATA_DIR / "csv"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"ratings_{season_int}_{date_str}.csv"

    sorted_items = sorted(ratings_dict.items(), key=lambda x: x[1], reverse=True)

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date", "season", "rank", "team", "rating"])
        for rank, (team, rating) in enumerate(sorted_items, start=1):
            w.writerow([today.isoformat(), season_int, rank, team, float(rating)])
    print(f"Saved CSV to {out_path}")


def compute_accuracy_metrics(conn, ratings, season_int):
    """
    Compute simple accuracy metrics using final game margins vs rating-based spread.
    pred_margin = rating_home - rating_away + HCA
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT home_team_id, away_team_id, home_pts, away_pts
        FROM games
        WHERE season = ? AND home_pts IS NOT NULL AND away_pts IS NOT NULL
        """,
        (season_int,),
    )
    rows = cur.fetchall()
    if not rows:
        return None

    errors = []
    for home_team, away_team, home_pts, away_pts in rows:
        if home_team not in ratings or away_team not in ratings:
            continue
        actual = float(home_pts) - float(away_pts)
        pred = float(ratings[home_team]) - float(ratings[away_team]) + HCA
        errors.append(actual - pred)

    if not errors:
        return None

    mae = sum(abs(e) for e in errors) / len(errors)
    rmse = math.sqrt(sum(e * e for e in errors) / len(errors))
    return {
        "games_count": len(errors),
        "mae": mae,
        "rmse": rmse,
    }


def write_accuracy_metrics(metrics, season_int, run_date_utc):
    metrics_dir = DATA_DIR / "metrics"
    metrics_dir.mkdir(parents=True, exist_ok=True)

    accuracy_path = metrics_dir / "accuracy.json"
    payload = {
        "season": int(season_int),
        "run_date_utc": run_date_utc,
        "games_count": metrics["games_count"],
        "mae": metrics["mae"],
        "rmse": metrics["rmse"],
    }
    accuracy_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    history_path = metrics_dir / "accuracy_history.csv"
    write_header = not history_path.exists()
    with history_path.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(["date_utc", "season", "games_count", "mae", "rmse"])
        w.writerow([run_date_utc, season_int, metrics["games_count"], metrics["mae"], metrics["rmse"]])


# ------------ PER-SEASON RUNNER ------------

def run_season(season_int):
    conn = sqlite3.connect(DB_PATH)

    player_values = load_player_values(conn, season_int)
    print(f"Loaded {len(player_values)} player_values rows for season {season_int}.")

    team_full_values = compute_team_full_values(conn, player_values, season_int)
    print(f"Computed full-strength values for {len(team_full_values)} teams.")

    records_by_team = compute_game_records(conn, player_values, team_full_values, season_int)
    print(f"Built game records for {len(records_by_team)} teams.")

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    history_dir = DATA_DIR / "history"
    history_dir.mkdir(parents=True, exist_ok=True)

    canonical_json_path = DATA_DIR / "ratings_current.json"

    as_of_dt = datetime.now(timezone.utc).replace(microsecond=0)
    as_of_utc_str = as_of_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    as_of_date = as_of_dt.date()
    run_date_utc = as_of_date.isoformat()

    yesterday_ranks = load_prev_day_ranks_from_csv(season_int, as_of_date)
    if not yesterday_ranks:
        yesterday_ranks = load_yesterday_ranks(canonical_json_path)

    last_week_ranks = load_last_week_ranks_from_csv(season_int, as_of_date)

    ratings = iterate_ratings(records_by_team)
    # Blend ratings toward win% to improve winner prediction.
    team_results = compute_team_results(conn, season_int)
    blended = {}
    for team, rating in ratings.items():
        wins = team_results.get(team, {}).get("wins", 0)
        games_played = team_results.get(team, {}).get("games", 0)
        win_pct = (wins / games_played) if games_played else 0.0
        win_component = WIN_BLEND_SCALE * (win_pct - 0.5)
        blended[team] = (1 - WIN_BLEND_W) * rating + WIN_BLEND_W * win_component
    ratings = blended
    print(f"Final ratings for season {season_int}:")
    for team, r in sorted(ratings.items(), key=lambda x: x[1], reverse=True):
        print(f"{team}: {r:.3f}")

    components = compute_component_stats(conn, records_by_team, ratings, season_int)
    payload = build_ratings_payload(
        ratings,
        last_week_ranks,
        yesterday_ranks,
        season_int,
        as_of_utc_str,
        run_date_utc,
        components,
    )

    write_ratings_json(payload, canonical_json_path)
    history_path = history_dir / f"{run_date_utc}.json"
    write_ratings_json(payload, history_path)
    print(f"Saved canonical JSON to {canonical_json_path}")
    print(f"Saved history snapshot to {history_path}")

    write_ratings_csv(ratings, season_int)

    snapshots_dir = DATA_DIR / "ratings_snapshots"
    snapshots_dir.mkdir(parents=True, exist_ok=True)
    snapshot_payload = build_snapshot_payload(ratings, season_int, as_of_utc_str)
    snapshot_name = f"ratings_{as_of_dt.strftime('%Y%m%d_%H%M%SZ')}.json"
    snapshot_path = snapshots_dir / snapshot_name
    write_ratings_json(snapshot_payload, snapshot_path)
    latest_path = DATA_DIR / "ratings_latest.json"
    write_ratings_json(snapshot_payload, latest_path)
    print(f"Saved ratings snapshot to {snapshot_path}")
    print(f"Updated ratings latest pointer to {latest_path}")

    conn.close()

    with sqlite3.connect(DB_PATH) as metrics_conn:
        metrics = compute_accuracy_metrics(metrics_conn, ratings, season_int)
    if metrics:
        write_accuracy_metrics(metrics, season_int, run_date_utc)
        print(f"Saved accuracy metrics to {DATA_DIR / 'metrics' / 'accuracy.json'}")
    else:
        print("No accuracy metrics computed (missing games or ratings).")

    print("-" * 40)


# ------------ MAIN ------------

def main():
    for season_int in SEASONS:
        print(f"=== Running ratings for season {season_int} ===")
        run_season(season_int)


if __name__ == "__main__":
    main()
