# ingest_games.py
import sqlite3
from datetime import datetime

import pandas as pd
from nba_api.stats.endpoints import leaguegamefinder

DB_PATH = "nba_ratings.db"

def get_games_for_season(season_str: str, season_type: str) -> pd.DataFrame:
    """
    Pull games for a given season + season type using nba_api.

    season_str examples: "2023-24", "2024-25"
    season_type: "Regular Season" or "Playoffs"
    """
    gamefinder = leaguegamefinder.LeagueGameFinder(
        season_nullable=season_str,
        season_type_nullable=season_type
    )
    df = gamefinder.get_data_frames()[0]

    # Only keep needed columns
    df = df[[
        "GAME_ID",
        "GAME_DATE",
        "TEAM_ID",
        "TEAM_ABBREVIATION",
        "MATCHUP",
        "PTS",
    ]]
    df["SEASON_TYPE"] = season_type
    return df

def build_games_table(df: pd.DataFrame, season_int: int) -> pd.DataFrame:
    """
    Convert the team-game rows into one row per game with home/away, scores.
    """
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"])

    games = []

    for game_id, g in df.groupby("GAME_ID"):
        if len(g) != 2:
            # skip malformed games
            continue

        row1, row2 = g.iloc[0], g.iloc[1]
        season_type = row1["SEASON_TYPE"]

        def parse_side(r):
            matchup = r["MATCHUP"]
            team = r["TEAM_ABBREVIATION"]
            if "vs." in matchup:
                return "HOME", team, r["PTS"]
            elif "@" in matchup:
                return "AWAY", team, r["PTS"]
            else:
                return "NEUTRAL", team, r["PTS"]

        side1, team1, pts1 = parse_side(row1)
        side2, team2, pts2 = parse_side(row2)

        if side1 == "HOME" and side2 == "AWAY":
            home_team = team1
            away_team = team2
            home_pts = int(pts1)
            away_pts = int(pts2)
        elif side1 == "AWAY" and side2 == "HOME":
            home_team = team2
            away_team = team1
            home_pts = int(pts2)
            away_pts = int(pts1)
        else:
            # ignore neutral/unknown games for now
            continue

        game_date = row1["GAME_DATE"].strftime("%Y-%m-%d")

        games.append({
            "game_id": game_id,
            "season": season_int,
            "date": game_date,
            "home_team_id": home_team,
            "away_team_id": away_team,
            "home_pts": home_pts,
            "away_pts": away_pts,
        })

    return pd.DataFrame(games)

def upsert_games(games_df: pd.DataFrame):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    for _, r in games_df.iterrows():
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
            )
        )

    conn.commit()
    conn.close()

def main():
    # 2025–26 season (this is what you need right now)
    season_str = "2025-26"
    season_int = 2026

    print(f"Fetching REGULAR SEASON games for {season_str}...")
    df_reg = get_games_for_season(season_str, "Regular Season")
    print(f"Regular season rows: {len(df_reg)}")

    print(f"Fetching PLAYOFF games for {season_str}...")
    df_po = get_games_for_season(season_str, "Playoffs")
    print(f"Playoff rows: {len(df_po)}")

    df_all = pd.concat([df_reg, df_po], ignore_index=True)

    games_df = build_games_table(df_all, season_int)
    print(f"Prepared {len(games_df)} games (regular season + playoffs). Writing to DB...")
    upsert_games(games_df)
    print("Done.")

if __name__ == "__main__":
    main()
