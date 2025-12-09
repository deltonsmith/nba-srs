import sqlite3
from pathlib import Path

# Single canonical DB path for all scripts
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / "nba_ratings.db"


def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS teams (
            team_id TEXT,
            name TEXT NOT NULL,
            season INTEGER NOT NULL,
            PRIMARY KEY (team_id, season)
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS players (
            player_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            team_id TEXT,
            season INTEGER NOT NULL
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS games (
            game_id INTEGER PRIMARY KEY AUTOINCREMENT,
            season INTEGER NOT NULL,
            date TEXT NOT NULL,
            home_team_id TEXT NOT NULL,
            away_team_id TEXT NOT NULL,
            home_pts INTEGER NOT NULL,
            away_pts INTEGER NOT NULL
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS appearances (
            game_id INTEGER NOT NULL,
            team_id TEXT NOT NULL,
            player_id INTEGER NOT NULL,
            minutes REAL,
            PRIMARY KEY (game_id, team_id, player_id)
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS player_values (
            player_id INTEGER NOT NULL,
            season INTEGER NOT NULL,
            metric_name TEXT NOT NULL,
            metric_raw REAL NOT NULL,
            v_p REAL NOT NULL,
            PRIMARY KEY (player_id, season, metric_name)
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS team_ratings (
            season INTEGER NOT NULL,
            team_id TEXT NOT NULL,
            rating REAL NOT NULL,
            last_updated TEXT NOT NULL,
            PRIMARY KEY (season, team_id)
        );
        """
    )

    conn.commit()
    conn.close()


if __name__ == "__main__":
    init_db()
    print("Initialized database.")
