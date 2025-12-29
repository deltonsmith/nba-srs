"""
Feature builder using rolling team stats from past games (pre-game only).
Currently uses a simple baseline: rolling margin, rolling points for/against, rest days.
TODO: add richer possession/efficiency stats (eFG%, TOV%, ORB%, FTr) when available.
"""

import argparse
from bisect import bisect_right
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Tuple

from config import DB_PATH
from db import get_conn, init_db

ROLLING_WINDOWS = [5, 10, 20]


def _coerce_date(value: Optional[object]) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    s = str(value).strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except Exception:
        try:
            return datetime.strptime(s, "%Y-%m-%d").date()
        except Exception:
            return None


def parse_date(date_str: str) -> datetime.date:
    coerced = _coerce_date(date_str)
    if coerced is None:
        raise ValueError(f"Invalid date: {date_str}")
    return coerced


def daterange(start_date, end_date):
    for n in range((end_date - start_date).days + 1):
        yield start_date + timedelta(n)


def load_games(conn, start_date: str, end_date: str) -> List[Dict]:
    conn.row_factory = sqlite3.Row  # type: ignore
    rows = conn.execute(
        """
        SELECT *
        FROM games
        WHERE date BETWEEN ? AND ?
        ORDER BY date ASC, game_id ASC
        """,
        (start_date, end_date),
    ).fetchall()
    return [dict(r) for r in rows]


def load_games_before(conn, cutoff_date: str) -> List[Dict]:
    conn.row_factory = sqlite3.Row  # type: ignore
    rows = conn.execute(
        """
        SELECT *
        FROM games
        WHERE date < ?
        ORDER BY date ASC, game_id ASC
        """,
        (cutoff_date,),
    ).fetchall()
    return [dict(r) for r in rows]


def compute_rest(prev_game_date: Optional[str], current_date: str) -> Optional[int]:
    if not prev_game_date:
        return None
    prev = _coerce_date(prev_game_date)
    cur = _coerce_date(current_date)
    if prev is None or cur is None:
        return None
    return (cur - prev).days - 1  # days between games minus game day


def _parse_iso(dt_str: Optional[str]) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None
    try:
        prev = parse_date(prev_game_date)
        cur = parse_date(current_date)
        return (cur - prev).days - 1  # days between games minus game day
    except Exception:
        return None


def build_team_history(games: List[Dict]) -> Dict[str, List[Dict]]:
    history: Dict[str, List[Dict]] = {}
    for g in games:
        date = g.get("date")
        date_key = _coerce_date(date)
        gid = g.get("game_id")
        home = g.get("home_team_id")
        away = g.get("away_team_id")
        hs = g.get("home_score")
        as_ = g.get("away_score")
        if date is None or gid is None or home is None or away is None:
            continue
        for team_id, opp_id, pts_for, pts_against in [
            (home, away, hs, as_),
            (away, home, as_, hs),
        ]:
            if pts_for is None or pts_against is None:
                continue
            entry = {
                "game_id": gid,
                "date": date,
                "date_key": date_key,
                "team_id": team_id,
                "opp_id": opp_id,
                "pts_for": int(pts_for),
                "pts_against": int(pts_against),
                "margin": int(pts_for) - int(pts_against),
            }
            history.setdefault(team_id, []).append(entry)
    # Ensure sorted by date then game_id
    for team, games_list in history.items():
        games_list.sort(key=lambda x: (x["date"], x["game_id"]))
    return history


def load_injury_snapshots(conn) -> Dict[int, Dict[str, List]]:
    rows = conn.execute(
        """
        SELECT player_id, team_id, status, pulled_at
        FROM player_injuries
        """
    ).fetchall()

    snapshots = {}
    for player_id, team_id, status, pulled_at in rows:
        if team_id is None or player_id is None or pulled_at is None:
            continue
        pulled_dt = _parse_iso(pulled_at)
        if pulled_dt is None:
            continue
        status_key = str(status or "").strip().lower()
        key = (int(team_id), pulled_dt)
        entry = snapshots.setdefault(key, defaultdict(set))
        entry[status_key].add(int(player_id))

    team_snapshots: Dict[int, Dict[str, List]] = defaultdict(lambda: {"times": [], "counts": []})
    for (team_id, pulled_dt), status_sets in snapshots.items():
        out_count = len(status_sets.get("out", set()))
        day_to_day_count = len(status_sets.get("day-to-day", set()))
        total_count = 0
        for players in status_sets.values():
            total_count += len(players)
        counts = {
            "inj_out": out_count,
            "inj_day_to_day": day_to_day_count,
            "inj_total": total_count,
        }
        team_snapshots[team_id]["times"].append(pulled_dt)
        team_snapshots[team_id]["counts"].append(counts)

    for team_id, payload in team_snapshots.items():
        paired = sorted(zip(payload["times"], payload["counts"]), key=lambda x: x[0])
        payload["times"] = [p[0] for p in paired]
        payload["counts"] = [p[1] for p in paired]

    return team_snapshots


def injury_counts_for_team(team_snapshots: Dict[int, Dict[str, List]], team_id: Optional[int], game_date: str) -> Dict[str, Optional[int]]:
    if not team_id:
        return {"inj_out": None, "inj_day_to_day": None, "inj_total": None}
    payload = team_snapshots.get(int(team_id))
    if not payload:
        return {"inj_out": None, "inj_day_to_day": None, "inj_total": None}
    cutoff = datetime.combine(parse_date(game_date), datetime.max.time()).replace(tzinfo=timezone.utc)
    times = payload["times"]
    idx = bisect_right(times, cutoff) - 1
    if idx < 0:
        return {"inj_out": None, "inj_day_to_day": None, "inj_total": None}
    return payload["counts"][idx]


def rolling_stats(entries: List[Dict], current_date: str) -> Dict[str, Dict[str, float]]:
    """
    For a team's chronological entries (past games), compute rolling stats before current_date.
    """
    current_key = _coerce_date(current_date)
    prev = [e for e in entries if e.get("date_key") and current_key and e["date_key"] < current_key]
    result: Dict[str, Dict[str, float]] = {}

    for window in ROLLING_WINDOWS:
        subset = prev[-window:]
        if not subset:
            result[str(window)] = {}
            continue
        n = len(subset)
        margin = sum(e["margin"] for e in subset) / n
        pts_for = sum(e["pts_for"] for e in subset) / n
        pts_against = sum(e["pts_against"] for e in subset) / n
        result[str(window)] = {
            "avg_margin": margin,
            "avg_pts_for": pts_for,
            "avg_pts_against": pts_against,
        }
    return result


def last_game_date(entries: List[Dict], current_date: str) -> Optional[str]:
    current_key = _coerce_date(current_date)
    prev = [e for e in entries if e.get("date_key") and current_key and e["date_key"] < current_key]
    if not prev:
        return None
    return prev[-1]["date_key"]


def build_features_for_games(conn, games: List[Dict], team_history: Dict[str, List[Dict]], team_snapshots: Dict[int, Dict[str, List]]):
    to_write = []
    for g in games:
        game_id = g.get("game_id")
        date = g.get("date")
        if not game_id or not date:
            continue
        for team_key in ("home_team_id", "away_team_id"):
            team_id = g.get(team_key)
            if not team_id:
                continue
            bdl_id_key = "home_team_bdl_id" if team_key == "home_team_id" else "away_team_bdl_id"
            team_bdl_id = g.get(bdl_id_key)
            history = team_history.get(team_id, [])
            roll = rolling_stats(history, date)
            prev_date = last_game_date(history, date)
            rest = compute_rest(prev_date, date)
            inj_counts = injury_counts_for_team(team_snapshots, team_bdl_id, date)
            # Use last available rolling window as baseline when missing
            features = {
                "game_id": game_id,
                "team_id": team_id,
                "team_bdl_id": team_bdl_id,
                "net_rating": roll.get("5", {}).get("avg_margin"),
                "pace": None,     # TODO: add possession-based pace when available
                "efg": None,      # TODO
                "tov": None,      # TODO
                "orb": None,      # TODO
                "ftr": None,      # TODO
                "rest_days": rest,
                "travel_miles": None,  # TODO: add travel estimates
                "back_to_back": 1 if rest is not None and rest <= 0 else 0 if rest is not None else None,
                "inj_out": inj_counts.get("inj_out"),
                "inj_day_to_day": inj_counts.get("inj_day_to_day"),
                "inj_total": inj_counts.get("inj_total"),
            }
            to_write.append(features)

    if to_write:
        with conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO team_game_features
                    (game_id, team_id, team_bdl_id, net_rating, pace, efg, tov, orb, ftr, rest_days, travel_miles, back_to_back,
                     inj_out, inj_day_to_day, inj_total)
                VALUES
                    (:game_id, :team_id, :team_bdl_id, :net_rating, :pace, :efg, :tov, :orb, :ftr, :rest_days, :travel_miles, :back_to_back,
                     :inj_out, :inj_day_to_day, :inj_total)
                """,
                to_write,
            )
        print(f"Wrote features for {len(to_write)} team-game rows")
    else:
        print("No features to write")


def process_date_range(start_date: str, end_date: str):
    init_db(DB_PATH)
    conn = get_conn(DB_PATH)
    conn.row_factory = sqlite3.Row  # type: ignore
    all_games = load_games(conn, start_date, end_date)
    if not all_games:
        print("No games found in range.")
        conn.close()
        return
    history_games = load_games_before(conn, end_date)
    history = build_team_history(history_games)
    team_snapshots = load_injury_snapshots(conn)
    build_features_for_games(conn, all_games, history, team_snapshots)
    conn.close()


def main():
    parser = argparse.ArgumentParser(description="Build rolling features for games in a date range.")
    parser.add_argument("--date-range", required=True, help="YYYY-MM-DD:YYYY-MM-DD")
    args = parser.parse_args()
    try:
        start_str, end_str = args.date_range.split(":", 1)
    except Exception as e:
        parser.error(f"Invalid --date-range: {e}")
    process_date_range(start_str, end_str)


if __name__ == "__main__":
    import sqlite3  # local import for row_factory use
    main()
