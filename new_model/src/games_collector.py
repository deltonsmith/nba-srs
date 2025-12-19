"""
Games collector for the new model pipeline.
Usage:
  python games_collector.py --date YYYY-MM-DD
  python games_collector.py --date-range YYYY-MM-DD:YYYY-MM-DD
"""

import argparse
from datetime import datetime, timedelta
from typing import Iterable, List, Mapping

from config import DB_PATH
from balldontlie_client import fetch_games
from db import init_db, upsert_games


def daterange(start_date: datetime.date, end_date: datetime.date) -> Iterable[datetime.date]:
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)


def normalize_games(raw_games: List[Mapping]) -> List[Mapping]:
    normalized = []
    for g in raw_games:
        game_id = g.get("id")
        if game_id is None:
            continue
        start_time = g.get("datetime") or g.get("start_time") or g.get("start_time_utc")
        normalized.append(
            {
                "game_id": int(game_id),
                "season": g.get("season"),
                "date": g.get("date"),
                "home_team_id": (g.get("home_team") or {}).get("abbreviation"),
                "away_team_id": (g.get("visitor_team") or {}).get("abbreviation"),
                "home_score": g.get("home_team_score"),
                "away_score": g.get("visitor_team_score"),
                "status": g.get("status"),
                "start_time_utc": start_time,
            }
        )
    return normalized


def collect_for_dates(dates: Iterable[str], db_path: str):
    all_games: List[Mapping] = []
    for d in dates:
        raw = fetch_games(d)
        all_games.extend(normalize_games(raw))
    if all_games:
        init_db(db_path)
        upsert_games(db_path, all_games)
        print(f"Upserted {len(all_games)} games into {db_path}")
    else:
        print("No games to upsert.")


def main():
    parser = argparse.ArgumentParser(description="Collect games from Balldontlie by date or date range.")
    parser.add_argument("--date", help="Single date YYYY-MM-DD")
    parser.add_argument("--date-range", help="Date range YYYY-MM-DD:YYYY-MM-DD")
    args = parser.parse_args()

    if not args.date and not args.date_range:
        parser.error("Must provide --date or --date-range")

    dates: List[str] = []
    if args.date:
        dates.append(args.date)
    if args.date_range:
        try:
            start_str, end_str = args.date_range.split(":", 1)
            start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
            end_date = datetime.strptime(end_str, "%Y-%m-%d").date()
        except Exception as e:
            parser.error(f"Invalid --date-range format: {e}")
        for d in daterange(start_date, end_date):
            dates.append(d.isoformat())

    collect_for_dates(dates, DB_PATH)


if __name__ == "__main__":
    main()
