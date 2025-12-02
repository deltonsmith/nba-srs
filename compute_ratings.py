# compute_ratings.py
#
# Lineup-adjusted SRS ratings for multiple NBA seasons.
# - Uses games(season), appearances, players, player_values(v_p).
# - Outputs ratings_<SEASON_INT>.json in the project root.

import sqlite3
import json
from collections import defaultdict

# ------------ CONFIGURATION ------------

DB_PATH = "nba_ratings.db"

# Seasons are labeled by the year they END:
# 2023-24 -> 2024, 2024-25 -> 2025, 2025-26 -> 2026, etc.
SEASONS = [2024, 2025]     # run ratings for each of these season ints

HCA = 2.5                  # home-court advantage in points
MAX_ITERS = 100            # SRS iteration count


# ------------ DATA LOADING ------------

def load_player_values(conn, season_int):
    """
    Return dict: player_id -> v_p (per-game value) for this season.
    """
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
        # sort by minutes desc, take core_size
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

    # All games for this season
    cur.execute(
        """
        SELECT game_id, date, home_team_id, away_team_id, home_pts, away_pts
        FROM games
        WHERE season = ?
        """,
        (season_int,),
    )
    games = cur.fetchall()

    # All appearances (all seasons) - we key by (game_id, team_id)
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

        # Lineup deviations from full-strength
        L_home = v_game_home - v_full_home
        L_away = v_game_away - v_full_away

        # Expected margin from lineups
        E_home = L_home - L_away

        # Actual margin from the box score
        margin_home = home_pts - away_pts

        # Lineup-adjusted margin from home perspective
        M_adj_home = margin_home - E_home

        records_by_team[home_team].append(
            {
                "opp": away_team,
                "M_adj": M_adj_home,
                "home_flag": 1,
            }
        )

        # Away perspective (mirror)
        E_away = -E_home
        margin_away = -margin_home
        M_adj_away = margin_away - E_away

        records_by_team[away_team].append(
            {
                "opp": home_team,
                "M_adj": M_adj_away,
                "home_flag": -1,
            }
        )

    return records_by_team


# ------------ SRS ITERATION ------------

def iterate_ratings(records_by_team):
    """
    Solve SRS-style ratings via fixed-point iteration.

    ratings[t] ≈ average( M_adj - HCA*home_flag + opp_rating )
    """
    teams = sorted(records_by_team.keys())
    ratings = {t: 0.0 for t in teams}

    for _ in range(MAX_ITERS):
        new_ratings = {}

        for t in teams:
            recs = records_by_team[t]
            if not recs:
                # no games: carry previous rating
                new_ratings[t] = ratings[t]
                continue

            avg_M_adj = sum(r["M_adj"] for r in recs) / len(recs)
            avg_home_flag = sum(r["home_flag"] for r in recs) / len(recs)
            avg_opp_rating = sum(ratings[r["opp"]] for r in recs) / len(recs)

            new_ratings[t] = (avg_M_adj - HCA * avg_home_flag) + avg_opp_rating

        ratings = new_ratings

    # Normalize so league average = 0
    mean_rating = sum(ratings.values()) / len(ratings)
    ratings = {t: r - mean_rating for t, r in ratings.items()}
    return ratings


# ------------ OUTPUT ------------

def save_ratings_json(ratings, path):
    data = [
        {"team": team, "rating": float(rating)}
        for team, rating in sorted(ratings.items(), key=lambda x: x[1], reverse=True)
    ]
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


# ------------ PER-SEASON RUNNER ------------

def run_season(season_int):
    conn = sqlite3.connect(DB_PATH)

    player_values = load_player_values(conn, season_int)
    print(f"Loaded {len(player_values)} player_values rows for season {season_int}.")

    team_full_values = compute_team_full_values(conn, player_values, season_int)
    print(f"Computed full-strength values for {len(team_full_values)} teams.")

    records_by_team = compute_game_records(conn, player_values, team_full_values, season_int)
    print(f"Built game records for {len(records_by_team)} teams.")

    conn.close()

    ratings = iterate_ratings(records_by_team)
    print(f"Final ratings for season {season_int}:")
    for team, r in sorted(ratings.items(), key=lambda x: x[1], reverse=True):
        print(f"{team}: {r:.3f}")

    out_path = f"ratings_{season_int}.json"
    save_ratings_json(ratings, out_path)
    print(f"Saved JSON to {out_path}")
    print("-" * 40)


# ------------ MAIN ------------

def main():
    for season_int in SEASONS:
        print(f"=== Running ratings for season {season_int} ===")
        run_season(season_int)


if __name__ == "__main__":
    main()
