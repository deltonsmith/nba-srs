"""
Collect team boxscore aggregates from Balldontlie stats endpoint.
Usage:
  python boxscore_collector.py --date YYYY-MM-DD
  python boxscore_collector.py --date-range YYYY-MM-DD:YYYY-MM-DD
"""

import argparse
import os
from datetime import datetime, timedelta
from typing import Dict, Iterable, List

import requests

from config import DB_PATH
from db import get_conn, init_db, upsert_team_game_stats


BALLDONTLIE_BASE = "https://api.balldontlie.io/v1"


def daterange(start_date: datetime.date, end_date: datetime.date) -> Iterable[datetime.date]:
    for n in range((end_date - start_date).days + 1):
        yield start_date + timedelta(n)


def get_game_ids_for_dates(conn, dates: List[str]) -> List[int]:
    placeholders = ",".join(["?"] * len(dates))
    rows = conn.execute(
        f"SELECT game_id FROM games WHERE date IN ({placeholders})",
        dates,
    ).fetchall()
    return [int(r[0]) for r in rows]


def fetch_stats_for_game(session: requests.Session, game_id: int) -> List[Dict]:
    stats: List[Dict] = []
    page = 1
    while True:
        params = {"game_ids[]": game_id, "per_page": 100, "page": page}
        resp = session.get(f"{BALLDONTLIE_BASE}/stats", params=params, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        data = payload.get("data", []) or []
        meta = payload.get("meta") or {}
        if not data:
            break
        stats.extend(data)
        total_pages = int(meta.get("total_pages", page))
        if page >= total_pages:
            break
        page += 1
    return stats


def _int_or_zero(val) -> int:
    try:
        return int(val)
    except Exception:
        return 0


def normalize_team_stats(game_id: int, stats_rows: List[Dict]) -> List[Dict]:
    agg: Dict[str, Dict] = {}
    for row in stats_rows:
        team = row.get("team") or {}
        team_id = team.get("abbreviation")
        team_bdl_id = team.get("id")
        if not team_id:
            continue
        entry = agg.setdefault(
            team_id,
            {
                "game_id": int(game_id),
                "team_id": team_id,
                "team_bdl_id": int(team_bdl_id) if team_bdl_id is not None else None,
                "fgm": 0,
                "fga": 0,
                "fg3m": 0,
                "ftm": 0,
                "fta": 0,
                "oreb": 0,
                "dreb": 0,
                "reb": 0,
                "ast": 0,
                "stl": 0,
                "blk": 0,
                "tov": 0,
                "pf": 0,
                "pts": 0,
            },
        )
        entry["fgm"] += _int_or_zero(row.get("fgm"))
        entry["fga"] += _int_or_zero(row.get("fga"))
        entry["fg3m"] += _int_or_zero(row.get("fg3m"))
        entry["ftm"] += _int_or_zero(row.get("ftm"))
        entry["fta"] += _int_or_zero(row.get("fta"))
        entry["oreb"] += _int_or_zero(row.get("oreb"))
        entry["dreb"] += _int_or_zero(row.get("dreb"))
        entry["reb"] += _int_or_zero(row.get("reb"))
        entry["ast"] += _int_or_zero(row.get("ast"))
        entry["stl"] += _int_or_zero(row.get("stl"))
        entry["blk"] += _int_or_zero(row.get("blk"))
        entry["tov"] += _int_or_zero(row.get("turnover"))
        entry["pf"] += _int_or_zero(row.get("pf"))
        entry["pts"] += _int_or_zero(row.get("pts"))
    return list(agg.values())


def collect_for_dates(dates: List[str]) -> None:
    init_db(DB_PATH)
    conn = get_conn(DB_PATH)
    try:
        game_ids = get_game_ids_for_dates(conn, dates)
    finally:
        conn.close()

    if not game_ids:
        print("No games found for requested dates.")
        return

    session = requests.Session()
    api_key = os.environ.get("BALLDONTLIE_API_KEY")
    if not api_key:
        raise SystemExit("BALLDONTLIE_API_KEY is required for boxscore collection.")
    session.headers.update({"Authorization": f"Bearer {api_key}"})

    total_rows = 0
    for game_id in game_ids:
        stats_rows = fetch_stats_for_game(session, game_id)
        if not stats_rows:
            continue
        team_rows = normalize_team_stats(game_id, stats_rows)
        total_rows += upsert_team_game_stats(DB_PATH, team_rows)
    print(f"Upserted {total_rows} team-game stat rows.")


def main():
    parser = argparse.ArgumentParser(description="Collect team boxscore aggregates for a date or date range.")
    parser.add_argument("--date", help="Date YYYY-MM-DD")
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

    collect_for_dates(dates)


if __name__ == "__main__":
    import os

    main()
