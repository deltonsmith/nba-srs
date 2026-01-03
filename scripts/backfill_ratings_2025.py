import argparse
import sqlite3
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))
DB_PATH = BASE_DIR / "data" / "nba_ratings.db"

from compute_ratings import run_season


def load_season_date_range(conn: sqlite3.Connection, season: int) -> tuple[date, date]:
    cur = conn.cursor()
    cur.execute("SELECT MIN(date), MAX(date) FROM games WHERE season = ?", (season,))
    row = cur.fetchone()
    if not row or not row[0] or not row[1]:
        raise SystemExit(f"No games found for season {season}")
    start = datetime.strptime(row[0], "%Y-%m-%d").date()
    end = datetime.strptime(row[1], "%Y-%m-%d").date()
    return start, end


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill daily PowerIndex snapshots for a season.")
    parser.add_argument("--season", type=int, default=2025, help="Season end year (default 2025).")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)
    try:
        start_date, end_date = load_season_date_range(conn, args.season)
    finally:
        conn.close()

    current = start_date
    while current <= end_date:
        as_of_utc = datetime.combine(current, datetime.min.time(), tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        cutoff_date = (current - timedelta(days=1)).isoformat()
        print(f"Backfill {current.isoformat()} using games <= {cutoff_date}")
        run_season(
            args.season,
            as_of_date=cutoff_date,
            as_of_utc=as_of_utc,
            skip_current_output=True,
            skip_csv=True,
            skip_metrics=True,
        )
        current += timedelta(days=1)


if __name__ == "__main__":
    main()
