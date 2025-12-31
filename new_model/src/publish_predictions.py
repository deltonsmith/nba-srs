"""
Copy a dated predictions JSON to a stable public path for the site to fetch.
Usage:
  python publish_predictions.py --date YYYY-MM-DD
"""

import argparse
import json
from pathlib import Path
import sqlite3

from config import DB_PATH
from db import init_db, upsert_model_predictions


def main():
    parser = argparse.ArgumentParser(description="Publish predictions JSON to public/new_model/predictions_today.json")
    parser.add_argument("--date", required=True, help="Date in YYYY-MM-DD")
    args = parser.parse_args()

    base_dir = Path(__file__).resolve().parent.parent
    src = base_dir / "output" / f"predictions_{args.date}.json"
    if not src.exists():
        raise SystemExit(f"Missing predictions file: {src}")

    data = json.loads(src.read_text(encoding="utf-8"))

    dest_dir = Path("public") / "new_model"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / "predictions_today.json"
    dest.write_text(json.dumps(data, indent=2), encoding="utf-8")
    print(f"Wrote {dest}")

    archive_dir = Path("data") / "new_model"
    archive_dir.mkdir(parents=True, exist_ok=True)
    archive_path = archive_dir / f"predictions_{args.date}.json"
    archive_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    print(f"Wrote {archive_path}")

    init_db(DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        games = conn.execute(
            """
            SELECT game_id, date, start_time_utc, home_team_id, away_team_id, home_score, away_score
            FROM games
            WHERE date = ?
            """,
            (args.date,),
        ).fetchall()
    finally:
        conn.close()

    games_map = {int(r["game_id"]): r for r in games}
    rows = []
    for g in data.get("games", []):
        game_id = g.get("gameId")
        if game_id is None:
            continue
        try:
            game_id = int(game_id)
        except Exception:
            continue
        game_row = games_map.get(game_id)
        if not game_row:
            continue

        home_score = game_row["home_score"]
        away_score = game_row["away_score"]
        actual_margin = None
        actual_total = None
        if home_score is not None and away_score is not None:
            actual_margin = float(home_score) - float(away_score)
            actual_total = float(home_score) + float(away_score)

        market = g.get("market") or {}
        real_line = g.get("realLine") or {}
        edge = g.get("edge") or {}

        rows.append(
            {
                "game_id": game_id,
                "as_of_utc": data.get("asOfUtc"),
                "vendor_rule": data.get("vendorRule"),
                "game_date": game_row["date"],
                "start_time_utc": game_row["start_time_utc"],
                "home_team_id": game_row["home_team_id"],
                "away_team_id": game_row["away_team_id"],
                "market_spread_home": market.get("spreadHome"),
                "market_total": market.get("total"),
                "model_spread_home": real_line.get("spreadHome"),
                "model_total": real_line.get("total"),
                "edge_spread": edge.get("spread"),
                "edge_total": edge.get("total"),
                "actual_margin": actual_margin,
                "actual_total": actual_total,
            }
        )

    if rows:
        upsert_model_predictions(DB_PATH, rows)
        print(f"Wrote {len(rows)} rows to model_predictions")


if __name__ == "__main__":
    main()
