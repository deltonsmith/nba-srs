"""
Odds collector for a given date.
Usage:
  python odds_collector.py --date YYYY-MM-DD [--once]
  python odds_collector.py --date-range YYYY-MM-DD:YYYY-MM-DD
"""

import argparse
from datetime import datetime, timezone, timedelta
from typing import List, Mapping

from balldontlie_client import fetch_odds_by_date, fetch_odds_by_game_ids
from config import DB_PATH
from db import init_db, insert_odds_snapshot
import sqlite3


def normalize_odds(odds: List[Mapping]) -> List[Mapping]:
    normalized = []
    for o in odds:
        game_id = o.get("game_id")
        vendor = o.get("vendor")
        updated_at = o.get("updated_at")
        market_type = o.get("market_type") or o.get("type") or o.get("market_type_slug")
        if not game_id or not vendor or not updated_at:
            continue

        market_type = str(market_type).lower() if market_type else ""

        def base_row(mt: str) -> Mapping:
            return {
                "game_id": int(game_id),
                "vendor": vendor,
                "market_type": mt,
                "home_line": None,
                "away_line": None,
                "total": None,
                "home_ml": None,
                "away_ml": None,
                "updated_at": updated_at,
            }

        if market_type:
            row = base_row(market_type)
            # Spread
            if market_type == "spread":
                row["home_line"] = o.get("spread_home_value")
                row["away_line"] = o.get("spread_away_value")
                row["home_ml"] = o.get("spread_home_odds")
                row["away_ml"] = o.get("spread_away_odds")

            # Total
            if market_type == "total":
                row["total"] = o.get("total_value")
                row["home_ml"] = o.get("total_over_odds")
                row["away_ml"] = o.get("total_under_odds")

            # Moneyline
            if market_type == "moneyline":
                row["home_ml"] = o.get("moneyline_home_odds")
                row["away_ml"] = o.get("moneyline_away_odds")

            normalized.append(row)
            continue

        # market_type missing: infer from available fields
        if o.get("spread_home_value") is not None or o.get("spread_away_value") is not None:
            row = base_row("spread")
            row["home_line"] = o.get("spread_home_value")
            row["away_line"] = o.get("spread_away_value")
            row["home_ml"] = o.get("spread_home_odds")
            row["away_ml"] = o.get("spread_away_odds")
            normalized.append(row)

        if o.get("total_value") is not None:
            row = base_row("total")
            row["total"] = o.get("total_value")
            row["home_ml"] = o.get("total_over_odds")
            row["away_ml"] = o.get("total_under_odds")
            normalized.append(row)

        if o.get("moneyline_home_odds") is not None or o.get("moneyline_away_odds") is not None:
            row = base_row("moneyline")
            row["home_ml"] = o.get("moneyline_home_odds")
            row["away_ml"] = o.get("moneyline_away_odds")
            normalized.append(row)
    return normalized


def collect_odds_for_date(date_str: str):
    try:
        raw_odds = fetch_odds_by_date(date_str)
    except Exception as exc:
        print(f"Odds fetch failed for {date_str}; skipping. Error: {exc}")
        return

    # Fallback: if no odds by date, try by game_ids for that date.
    if not raw_odds:
        with sqlite3.connect(DB_PATH) as conn:
            game_ids = [
                row[0]
                for row in conn.execute("SELECT game_id FROM games WHERE date = ?", (date_str,))
                .fetchall()
            ]
        if game_ids:
            raw_odds = fetch_odds_by_game_ids(game_ids)

    rows = normalize_odds(raw_odds)
    init_db(DB_PATH)

    inserted = 0
    skipped = 0
    for row in rows:
        try:
            insert_odds_snapshot(DB_PATH, row, pulled_at=datetime.now(timezone.utc).isoformat())
            inserted += 1
        except Exception:
            # Unique constraint ignore happens inside insert_odds_snapshot; any other error should bubble
            skipped += 1
            continue
    print(f"Odds for {date_str}: inserted={inserted}, skipped={skipped}")


def daterange(start_date: datetime.date, end_date: datetime.date):
    for n in range((end_date - start_date).days + 1):
        yield start_date + timedelta(n)


def main():
    parser = argparse.ArgumentParser(description="Collect odds snapshots for a given date.")
    parser.add_argument("--date", help="Date in YYYY-MM-DD")
    parser.add_argument("--date-range", help="Date range YYYY-MM-DD:YYYY-MM-DD")
    parser.add_argument("--once", action="store_true", help="Run once and exit (default behavior)")
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

    for d in dates:
        collect_odds_for_date(d)


if __name__ == "__main__":
    main()
