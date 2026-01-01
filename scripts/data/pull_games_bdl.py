import argparse
import csv
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests


BASE_URL = "https://api.balldontlie.io/v1/games"


def _yesterday_utc_date() -> datetime.date:
    return (datetime.now(timezone.utc) - timedelta(days=1)).date()


def _resolve_date(date_arg: str) -> datetime.date:
    if date_arg.lower() == "yesterday":
        return _yesterday_utc_date()
    return datetime.strptime(date_arg, "%Y-%m-%d").date()


def _extract_team(team: dict) -> dict:
    return {
        "id": team.get("id"),
        "abbr": team.get("abbreviation"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Pull Balldontlie games for a single date.")
    parser.add_argument("--date", default="yesterday", help="YYYY-MM-DD or 'yesterday' (default).")
    args = parser.parse_args()

    api_key = os.environ.get("BALLDONTLIE_API_KEY")
    if not api_key:
        raise SystemExit("BALLDONTLIE_API_KEY is not set.")

    target_date = _resolve_date(args.date)
    date_str = target_date.isoformat()

    resp = requests.get(
        BASE_URL,
        params={"dates[]": date_str, "per_page": 100},
        headers={"Authorization": api_key},
        timeout=30,
    )
    resp.raise_for_status()
    payload = resp.json()
    games = payload.get("data", [])

    rows = []
    for g in games:
        home = _extract_team(g.get("home_team") or {})
        visitor = _extract_team(g.get("visitor_team") or {})
        rows.append(
            {
                "game_id": g.get("id"),
                "date": g.get("date"),
                "season": g.get("season"),
                "home_team_id": home.get("id"),
                "home_team_abbr": home.get("abbr"),
                "home_score": g.get("home_team_score"),
                "visitor_team_id": visitor.get("id"),
                "visitor_team_abbr": visitor.get("abbr"),
                "visitor_score": g.get("visitor_team_score"),
                "status": g.get("status"),
            }
        )

    out_dir = Path("data") / "games"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{target_date.strftime('%Y%m%d')}_games.csv"

    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "game_id",
                "date",
                "season",
                "home_team_id",
                "home_team_abbr",
                "home_score",
                "visitor_team_id",
                "visitor_team_abbr",
                "visitor_score",
                "status",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    first_ids = [r["game_id"] for r in rows[:5]]
    print(f"Pulled {len(rows)} games for {date_str}")
    print(f"First 5 game_ids: {first_ids}")


if __name__ == "__main__":
    main()
