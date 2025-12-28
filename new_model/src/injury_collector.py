"""
Collect current player injuries from Balldontlie and store snapshots.
Usage:
  python injury_collector.py
"""

import argparse
from datetime import datetime, timezone
from typing import List, Mapping

from balldontlie_client import fetch_player_injuries
from config import DB_PATH
from db import init_db, insert_player_injuries


def normalize_injuries(rows: List[Mapping]) -> List[Mapping]:
    normalized = []
    for r in rows:
        player = r.get("player") or {}
        team_id = player.get("team_id")
        player_id = player.get("id")
        if not team_id or not player_id:
            continue
        normalized.append(
            {
                "player_id": int(player_id),
                "team_id": int(team_id),
                "status": r.get("status"),
                "return_date": r.get("return_date"),
                "description": r.get("description"),
            }
        )
    return normalized


def collect_injuries() -> None:
    raw = fetch_player_injuries()
    rows = normalize_injuries(raw)
    init_db(DB_PATH)
    pulled_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    inserted = insert_player_injuries(DB_PATH, rows, pulled_at=pulled_at)
    print(f"Injuries snapshot: inserted={inserted}")


def main():
    parser = argparse.ArgumentParser(description="Collect current player injuries.")
    parser.parse_args()
    collect_injuries()


if __name__ == "__main__":
    main()
