# pull_balldontlie_player_values.py
#
# Build player_values_<SEASON>.csv using Balldontlie box score stats (no scraping).

import csv
import os
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple

import requests

# ---- CONFIGURE THE SEASON YOU WANT ----
# 2024 = 2023-24 season, 2026 = 2025-26 season, etc.
SEASON_INT = 2026

OUT_CSV = Path("data") / f"player_values_{SEASON_INT}.csv"
BALLDONTLIE_BASE = "https://api.balldontlie.io/v1"
API_KEY = os.environ.get("BALLDONTLIE_API_KEY")
SESSION = requests.Session()
if API_KEY:
    SESSION.headers.update({"Authorization": f"Bearer {API_KEY}"})

API_SEASON = SEASON_INT - 1  # Balldontlie seasons[] uses the start year (e.g., 2025 for 2025-26)


def parse_minutes_to_float(value) -> float:
    """Convert MM:SS or PT##M##S or numeric minutes into float minutes."""
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str):
        return 0.0

    s = value.strip()
    if not s:
        return 0.0

    if ":" in s:
        try:
            mm, ss = s.split(":")
            return int(mm) + int(ss) / 60.0
        except Exception:
            return 0.0

    if s.startswith("PT"):
        s2 = s[2:]
        mins = 0
        secs = 0
        if "M" in s2:
            m_part, rest = s2.split("M", 1)
            if m_part:
                try:
                    mins = int(m_part)
                except Exception:
                    mins = 0
            if rest.endswith("S"):
                sec_part = rest[:-1]
                if sec_part:
                    try:
                        secs = int(sec_part)
                    except Exception:
                        secs = 0
        return mins + secs / 60.0

    return 0.0


def fetch_stats_for_season(season_int: int, postseason: bool) -> List[Dict]:
    if not API_KEY:
        raise SystemExit("Missing BALldontLIE_API_KEY; set it for Balldontlie access.")

    stats: List[Dict] = []
    page = 1

    while True:
        params = {
            "seasons[]": API_SEASON,
            "per_page": 100,
            "page": page,
            "postseason": str(postseason).lower(),
        }
        if API_KEY:
            params["api_key"] = API_KEY
        resp = SESSION.get(f"{BALLDONTLIE_BASE}/stats", params=params, timeout=30)
        resp.raise_for_status()
        payload = resp.json()

        data = payload.get("data", [])
        if not data:
            break

        stats.extend(data)
        if len(data) < params["per_page"]:
            break
        page += 1
        if page > 500:
            break  # safety guard
    return stats


def aggregate_player_values(season_int: int) -> Dict[Tuple[int, str], Dict]:
    """
    Aggregate player totals by (player_id, team_abbr).
    Metric used: plus/minus per 48 minutes, converted to per-game value.
    """
    totals: Dict[Tuple[int, str], Dict] = defaultdict(lambda: {"minutes": 0.0, "plus_minus": 0.0, "games": 0, "name": ""})

    for postseason_flag in (False, True):
        stats_rows = fetch_stats_for_season(season_int, postseason_flag)
        print(f"Fetched {len(stats_rows)} stat rows for postseason={postseason_flag}.")

        for row in stats_rows:
            player = row.get("player") or {}
            team = row.get("team") or {}

            player_id = player.get("id")
            first = player.get("first_name", "").strip()
            last = player.get("last_name", "").strip()
            player_name = f"{first} {last}".strip()
            team_abbr = team.get("abbreviation")

            if player_id is None or not player_name or not team_abbr:
                continue

            minutes_val = parse_minutes_to_float(row.get("min"))
            plus_minus = row.get("plus_minus") or 0

            key = (int(player_id), team_abbr)
            totals[key]["minutes"] += minutes_val
            totals[key]["plus_minus"] += float(plus_minus)
            totals[key]["games"] += 1
            totals[key]["name"] = player_name

    return totals


def build_csv_rows(totals: Dict[Tuple[int, str], Dict]) -> List[Dict]:
    rows: List[Dict] = []

    for (player_id, team_abbr), agg in totals.items():
        minutes = agg["minutes"]
        games = agg["games"]
        if games <= 0:
            continue

        min_per_game = minutes / games if minutes > 0 else 0.0
        # plus/minus per 48; if no minutes, metric_raw = 0 to avoid div-by-zero
        metric_raw = (agg["plus_minus"] / minutes) * 48.0 if minutes > 0 else 0.0

        rows.append(
            {
                "PLAYER_NAME": agg["name"],
                "TEAM_ABBREVIATION": team_abbr,
                "METRIC_RAW": metric_raw,
                "MIN_PER_GAME": min_per_game,
            }
        )

    rows.sort(key=lambda r: (r["TEAM_ABBREVIATION"], r["PLAYER_NAME"]))
    return rows


def main():
    totals = aggregate_player_values(SEASON_INT)
    rows = build_csv_rows(totals)

    if not rows:
        raise SystemExit("No player rows produced from Balldontlie stats; aborting to avoid stale ratings.")

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["PLAYER_NAME", "TEAM_ABBREVIATION", "METRIC_RAW", "MIN_PER_GAME"]
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} player rows to {OUT_CSV}")


if __name__ == "__main__":
    main()
