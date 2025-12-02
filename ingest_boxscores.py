# ingest_boxscores.py
import sqlite3
from time import sleep
from nba_api.stats.endpoints import boxscoretraditionalv2

DB_PATH = "nba_ratings.db"

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
        (game_id,)
    )
    row = cur.fetchone()
    conn.close()
    return row is not None

def save_appearances(game_id: str, season_int: int):
    """Pull one game's box score and write player appearances."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    data = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
    df = data.get_data_frames()[0]

    for _, row in df.iterrows():
        player_name = row["PLAYER_NAME"]
        team = row["TEAM_ABBREVIATION"]
        minutes_str = row["MIN"]

        # Convert "MM:SS" to float minutes
        if isinstance(minutes_str, str) and ":" in minutes_str:
            mins = int(minutes_str.split(":")[0]) + int(minutes_str.split(":")[1]) / 60.0
        else:
            mins = 0.0

        # Find or create player
        cur.execute(
            """
            SELECT player_id FROM players
            WHERE name = ? AND team_id = ? AND season = ?
            """,
            (player_name, team, season_int),
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
                (player_name, team, season_int),
            )
            player_id = cur.lastrowid

        # Insert appearance
        cur.execute(
            """
            INSERT OR REPLACE INTO appearances (game_id, team_id, player_id, minutes)
            VALUES (?, ?, ?, ?)
            """,
            (game_id, team, player_id, mins),
        )

    conn.commit()
    conn.close()

def main():
    # set this to the season you are ingesting
    SEASON_INT = 2026  # 2025–26 season

    game_ids = get_all_game_ids(SEASON_INT)
    print(f"Found {len(game_ids)} games. Fetching box scores...")

    for gid in game_ids:
        if game_already_ingested(gid):
            print(f"  skipping {gid} (already in DB)")
            continue

        print(f"  pulling box score for {gid} ...")
        try:
            save_appearances(gid, SEASON_INT)
        except Exception as e:
            print(f"    ERROR on {gid}: {e}")
        sleep(2)  # throttle requests

    print("Done.")

if __name__ == "__main__":
    main()
