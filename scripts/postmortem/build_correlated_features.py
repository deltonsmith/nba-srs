import argparse
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import sqlite3


DB_PATH = Path("new_model") / "data" / "new_model.sqlite"


def _load_games(conn: sqlite3.Connection, target_date: datetime.date) -> pd.DataFrame:
    games = pd.read_sql(
        """
        SELECT game_id, date, home_team_id, away_team_id
        FROM games
        """,
        conn,
    )
    if not games.empty:
        games["date"] = pd.to_datetime(games["date"]).dt.date
        if target_date in set(games["date"].unique()):
            return games
        # Fallback to dated file when DB lacks target date.
        dated_fallback = Path("data") / "games" / f"{target_date.isoformat()}.json"
        if dated_fallback.exists():
            payload = json.loads(dated_fallback.read_text(encoding="utf-8"))
            rows = []
            for g in payload.get("games", []):
                rows.append(
                    {
                        "game_id": g.get("id"),
                        "date": g.get("date"),
                        "home_team_id": g.get("home_team", {}).get("abbreviation"),
                        "away_team_id": g.get("visitor_team", {}).get("abbreviation"),
                    }
                )
            df = pd.DataFrame(rows)
            df["date"] = pd.to_datetime(df["date"]).dt.date
            games = pd.concat([games, df], ignore_index=True)
        return games

    dated_fallback = Path("data") / "games" / f"{target_date.isoformat()}.json"
    fallback = Path("data") / "games" / "games_2026_season.json"

    payload = None
    if dated_fallback.exists():
        payload = json.loads(dated_fallback.read_text(encoding="utf-8"))
        rows = []
        for g in payload.get("games", []):
            rows.append(
                {
                    "game_id": g.get("id"),
                    "date": g.get("date"),
                    "home_team_id": g.get("home_team", {}).get("abbreviation"),
                    "away_team_id": g.get("visitor_team", {}).get("abbreviation"),
                }
            )
        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        return df

    if fallback.exists():
        payload = json.loads(fallback.read_text(encoding="utf-8"))
        rows = []
        for g in payload.get("games", []):
            rows.append(
                {
                    "game_id": g.get("game_id"),
                    "date": g.get("date_utc"),
                    "home_team_id": g.get("home_team_id"),
                    "away_team_id": g.get("visitor_team_id"),
                }
            )
        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        return df

    print("NEED_GAMES_SOURCE: missing games table and games files in data/games/")
    raise SystemExit(1)


def _load_team_stats(conn: sqlite3.Connection) -> pd.DataFrame:
    stats = pd.read_sql(
        """
        SELECT game_id, team_id, fgm, fga, fg3m, ftm, fta,
               oreb, dreb, reb, tov, pts
        FROM team_game_stats
        """,
        conn,
    )
    return stats


def _build_team_game_rows(games: pd.DataFrame, stats: pd.DataFrame) -> pd.DataFrame:
    df = stats.merge(games, on="game_id", how="left")
    opp = stats.rename(
        columns={
            "team_id": "opp_team_id",
            "fgm": "opp_fgm",
            "fga": "opp_fga",
            "fg3m": "opp_fg3m",
            "ftm": "opp_ftm",
            "fta": "opp_fta",
            "oreb": "opp_oreb",
            "dreb": "opp_dreb",
            "reb": "opp_reb",
            "tov": "opp_tov",
            "pts": "opp_pts",
        }
    )
    df = df.merge(opp, on="game_id", how="left")
    df = df[df["team_id"] != df["opp_team_id"]]
    df["home_indicator"] = (df["team_id"] == df["home_team_id"]).astype(int)
    return df


def _compute_metrics(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["possessions_est"] = df["fga"] - df["oreb"] + df["tov"] + 0.44 * df["fta"]
    df["opp_possessions_est"] = df["opp_fga"] - df["opp_oreb"] + df["opp_tov"] + 0.44 * df["opp_fta"]
    df["pace_est"] = df["possessions_est"]
    df["tov_pct"] = df["tov"] / df["possessions_est"]
    df["orb_pct"] = df["oreb"] / (df["oreb"] + df["opp_dreb"])
    df["orb_pct_approx"] = False

    df["ortg"] = 100 * df["pts"] / df["possessions_est"]
    df["drtg"] = 100 * df["opp_pts"] / df["opp_possessions_est"]
    df["netrtg"] = df["ortg"] - df["drtg"]

    df["efg"] = (df["fgm"] + 0.5 * df["fg3m"]) / df["fga"]
    df["ts"] = df["pts"] / (2 * (df["fga"] + 0.44 * df["fta"]))
    df["ftr"] = df["fta"] / df["fga"]
    df["opp_ftr"] = df["opp_fta"] / df["opp_fga"]

    return df


def _rolling_means(df: pd.DataFrame, team_id: str, window: int) -> Dict[str, Optional[float]]:
    slice_df = df[df["team_id"] == team_id].tail(window)
    if slice_df.empty:
        return {}
    metrics = [
        "possessions_est",
        "pace_est",
        "tov_pct",
        "orb_pct",
        "ortg",
        "drtg",
        "netrtg",
        "efg",
        "ts",
        "ftr",
        "opp_ftr",
    ]
    return {f"{m}_r{window}": slice_df[m].mean() for m in metrics}


def _rest_context(games: pd.DataFrame, team_id: str, target_date: datetime.date) -> Tuple[Optional[int], Optional[bool]]:
    team_games = games[(games["home_team_id"] == team_id) | (games["away_team_id"] == team_id)]
    team_games = team_games[team_games["date"] < target_date].sort_values("date")
    if team_games.empty:
        return None, None
    last_date = team_games.iloc[-1]["date"]
    days_rest = (target_date - last_date).days
    back_to_back = days_rest == 1
    return days_rest, back_to_back


def main() -> None:
    parser = argparse.ArgumentParser(description="Build correlated features for a given date.")
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    if not DB_PATH.exists():
        raise SystemExit("Missing new_model SQLite DB: new_model/data/new_model.sqlite")

    conn = sqlite3.connect(DB_PATH)
    try:
        tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        table_names = {t[0] for t in tables}
        if "team_game_stats" not in table_names:
            print("NEED_TEAM_BOXSTATS_SOURCE: team_game_stats table missing.")
            raise SystemExit(1)

        games = _load_games(conn, target_date)
        stats = _load_team_stats(conn)
    finally:
        conn.close()

    if stats.empty:
        print("NEED_TEAM_BOXSTATS_SOURCE: team_game_stats table is empty.")
        raise SystemExit(1)

    team_games = _build_team_game_rows(games, stats)
    team_games = _compute_metrics(team_games)

    history = team_games[team_games["date"] < target_date].copy()
    todays_games = games[games["date"] == target_date].copy()
    if todays_games.empty:
        raise SystemExit(f"No games found for {target_date}")

    rows = []
    for _, game in todays_games.iterrows():
        for side in ("home", "away"):
            team_id = game["home_team_id"] if side == "home" else game["away_team_id"]
            opp_id = game["away_team_id"] if side == "home" else game["home_team_id"]
            roll10 = _rolling_means(history, team_id, 10)
            roll30 = _rolling_means(history, team_id, 30)

            days_rest_team, back_to_back = _rest_context(games, team_id, target_date)
            days_rest_opp, _ = _rest_context(games, opp_id, target_date)
            rest_diff = None
            if days_rest_team is not None and days_rest_opp is not None:
                rest_diff = days_rest_team - days_rest_opp

            row = {
                "game_id": int(game["game_id"]),
                "date": target_date.isoformat(),
                "team_id": team_id,
                "side": side,
                "home_indicator": 1 if side == "home" else 0,
                "days_rest_team": days_rest_team,
                "days_rest_opp": days_rest_opp,
                "rest_diff": rest_diff,
                "back_to_back": back_to_back,
                "orb_pct_approx": False,
            }
            row.update(roll10)
            row.update(roll30)
            rows.append(row)

    out_dir = Path("data") / "features"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"correlated_features_{target_date.strftime('%Y%m%d')}.csv"
    if not rows:
        raise SystemExit("No rows built for target date.")

    fieldnames = list(rows[0].keys())
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {out_path} with {len(rows)} rows")
    print("NEED_PLAYER_MINUTES_SOURCE: player minutes data not available for star proxies.")
    print("NEED_LINEUP_ONOFF_SOURCE: lineup on/off data not available for lineup proxies.")


if __name__ == "__main__":
    main()
