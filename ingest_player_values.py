# ingest_player_values.py
#
# Load player_values_YYYY.csv into nba_ratings.db for a given season.
#
# Usage examples (run from repo root):
#   python ingest_player_values.py 2024
#   python ingest_player_values.py 2025
#   python ingest_player_values.py 2026

import sys
import sqlite3
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "nba_ratings.db"

METRIC_NAME = "EPM"  # label for the metric you're using

# Season is the year the season ENDS:
# 2023-24 -> 2024, 2024-25 -> 2025, 2025-26 -> 2026
SEASON_TO_CSV = {
    2024: DATA_DIR / "player_values_2024.csv",
    2025: DATA_DIR / "player_values_2025.csv",
    2026: DATA_DIR / "player_values_2026.csv",
}

DEFAULT_SEASON = 2026


def get_season_from_argv() -> int:
    if len(sys.argv) >= 2:
        try:
            season_int = int(sys.argv[1])
        except ValueError:
            raise SystemExit("Season must be an integer like 2024, 2025, 2026.")

        if season_int not in SEASON_TO_CSV:
            valid = ", ".join(str(s) for s in sorted(SEASON_TO_CSV.keys()))
            raise SystemExit(f"Unsupported season {season_int}. Valid: {valid}")
        return season_int

    # If no CLI arg, fall back to default
    return DEFAULT_SEASON


def main():
    season_int = get_season_from_argv()
    csv_path = SEASON_TO_CSV[season_int]

    if not csv_path.exists():
        raise SystemExit(f"CSV not found for season {season_int}: {csv_path}")

    print(f"Loading {csv_path} for season {season_int} ...")

    df = pd.read_csv(csv_path)

    required = {"PLAYER_NAME", "TEAM_ABBREVIATION", "METRIC_RAW", "MIN_PER_GAME"}
    missing = required - set(df.columns)
    if missing:
        raise SystemExit(f"CSV missing columns: {missing}")

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    upserted = 0

    for _, row in df.iterrows():
        name = row["PLAYER_NAME"]
        team = row["TEAM_ABBREVIATION"]
        metric_raw = float(row["METRIC_RAW"])
        mpg = float(row["MIN_PER_GAME"])

        # Convert per-100-pos metric to per-game value v_p
        v_p = metric_raw * (mpg / 48.0)

        # Find matching players in DB for this season
        cur.execute(
            """
            SELECT player_id FROM players
            WHERE name = ? AND season = ?
            """,
            (name, season_int),
        )
        player_rows = cur.fetchall()

        if not player_rows:
            # player never appears in the season's boxscores; skip
            continue

        for (player_id,) in player_rows:
            cur.execute(
                """
                INSERT OR REPLACE INTO player_values
                    (player_id, season, metric_name, metric_raw, v_p)
                VALUES (?, ?, ?, ?, ?)
                """,
                (player_id, season_int, METRIC_NAME, metric_raw, v_p),
            )
            upserted += 1

    conn.commit()
    conn.close()

    print(f"Upserted {upserted} player_values rows for season {season_int}.")


if __name__ == "__main__":
    main()
