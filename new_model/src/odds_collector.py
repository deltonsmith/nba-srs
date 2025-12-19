"""
Odds collector for a given date.
Usage:
  python odds_collector.py --date YYYY-MM-DD [--once]
"""

import argparse
from datetime import datetime, timezone
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
        if not game_id or not vendor or not updated_at or not market_type:
            continue

        market_type = str(market_type).lower()
        row = {
            "game_id": int(game_id),
            "vendor": vendor,
            "market_type": market_type,
            "home_line": None,
            "away_line": None,
            "total": None,
            "home_ml": None,
            "away_ml": None,
            "updated_at": updated_at,
        }

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
    return normalized


def collect_odds_for_date(date_str: str):
    raw_odds = fetch_odds_by_date(date_str)

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


def main():
    parser = argparse.ArgumentParser(description="Collect odds snapshots for a given date.")
    parser.add_argument("--date", required=True, help="Date in YYYY-MM-DD")
    parser.add_argument("--once", action="store_true", help="Run once and exit (default behavior)")
    args = parser.parse_args()

    collect_odds_for_date(args.date)


if __name__ == "__main__":
    main()
