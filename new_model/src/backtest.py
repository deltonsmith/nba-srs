"""
Simple walk-forward backtest for margin/total models.
Trains on data before each day in range, predicts that day's games, and logs metrics.
Outputs CSV: new_model/reports/backtest_results.csv
"""

import argparse
import csv
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Tuple

import joblib
import pandas as pd
import sqlite3
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error

from config import DB_PATH


FEATURE_COLS = [
    "net_rating",
    "pace",
    "efg",
    "tov",
    "orb",
    "ftr",
    "rest_days",
    "travel_miles",
    "back_to_back",
]


def daterange(start_date, end_date):
    for n in range((end_date - start_date).days + 1):
        yield start_date + timedelta(n)


def load_games_with_features(conn) -> pd.DataFrame:
    games = pd.read_sql(
        """
        SELECT g.game_id, g.date, g.home_score, g.away_score,
               g.home_team_id, g.away_team_id,
               ml.closing_spread_home, ml.closing_total, ml.closing_home_ml
        FROM games g
        LEFT JOIN market_lines ml ON ml.game_id = g.game_id
        ORDER BY g.date ASC
        """,
        conn,
    )
    feats = pd.read_sql("SELECT * FROM team_game_features", conn)

    # Home features
    home_feats = feats.merge(
        games[["game_id", "home_team_id"]],
        left_on=["game_id", "team_id"],
        right_on=["game_id", "home_team_id"],
        how="inner",
    )
    home_feats = home_feats.drop(columns=["team_id", "home_team_id"])
    home_feats = home_feats.add_suffix("_home")

    # Away features
    away_feats = feats.merge(
        games[["game_id", "away_team_id"]],
        left_on=["game_id", "team_id"],
        right_on=["game_id", "away_team_id"],
        how="inner",
    )
    away_feats = away_feats.drop(columns=["team_id", "away_team_id"])
    away_feats = away_feats.add_suffix("_away")

    df = games.merge(home_feats, on="game_id", how="left").merge(away_feats, on="game_id", how="left")
    df["date"] = pd.to_datetime(df["date"])
    df["margin"] = df["home_score"] - df["away_score"]
    df["total"] = df["home_score"] + df["away_score"]

    feat_cols = []
    for col in FEATURE_COLS:
        feat_cols.append(f"{col}_home")
        feat_cols.append(f"{col}_away")

    df[feat_cols] = df[feat_cols].fillna(0)
    return df, feat_cols


def train_models(train_df: pd.DataFrame, feat_cols: List[str]) -> Tuple[HistGradientBoostingRegressor, HistGradientBoostingRegressor]:
    m_margin = HistGradientBoostingRegressor(random_state=42)
    m_total = HistGradientBoostingRegressor(random_state=42)
    m_margin.fit(train_df[feat_cols], train_df["margin"])
    m_total.fit(train_df[feat_cols], train_df["total"])
    return m_margin, m_total


def run_backtest(start_str: str, end_str: str, edge_threshold: float):
    conn = sqlite3.connect(DB_PATH)
    df, feat_cols = load_games_with_features(conn)
    conn.close()

    start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_str, "%Y-%m-%d").date()

    records = []
    for day in daterange(start_date, end_date):
        day_df = df[df["date"] == pd.Timestamp(day)]
        if day_df.empty:
            continue

        train_df = df[df["date"] < pd.Timestamp(day)]
        if train_df.empty:
            continue

        m_margin, m_total = train_models(train_df, feat_cols)

        preds_margin = m_margin.predict(day_df[feat_cols])
        preds_total = m_total.predict(day_df[feat_cols])

        mae_margin = mean_absolute_error(day_df["margin"], preds_margin) if day_df["home_score"].notna().all() else None
        mae_total = mean_absolute_error(day_df["total"], preds_total) if day_df["home_score"].notna().all() else None

        for (_, row), pm, pt in zip(day_df.iterrows(), preds_margin, preds_total):
            edge_spread = None
            edge_total = None
            if pd.notna(row.get("closing_spread_home")):
                edge_spread = pm - float(row["closing_spread_home"])
            if pd.notna(row.get("closing_total")):
                edge_total = pt - float(row["closing_total"])

            signal_spread = abs(edge_spread) > edge_threshold if edge_spread is not None else False
            signal_total = abs(edge_total) > edge_threshold if edge_total is not None else False

            records.append(
                {
                    "date": row["date"].strftime("%Y-%m-%d"),
                    "game_id": row["game_id"],
                    "pred_margin": pm,
                    "pred_total": pt,
                    "actual_margin": row["margin"],
                    "actual_total": row["total"],
                    "closing_spread_home": row.get("closing_spread_home"),
                    "closing_total": row.get("closing_total"),
                    "edge_spread": edge_spread,
                    "edge_total": edge_total,
                    "signal_spread": signal_spread,
                    "signal_total": signal_total,
                    "mae_margin_day": mae_margin,
                    "mae_total_day": mae_total,
                }
            )

    reports_dir = Path(__file__).resolve().parent.parent / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    out_csv = reports_dir / "backtest_results.csv"
    if records:
        keys = list(records[0].keys())
        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(records)
        print(f"Wrote backtest results to {out_csv} ({len(records)} rows)")
    else:
        print("No records written; check date range or data availability.")


def main():
    parser = argparse.ArgumentParser(description="Walk-forward backtest for margin/total models.")
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--edge-threshold", type=float, default=1.0, help="Edge threshold for signals (default 1.0)")
    parser.add_argument("--vendor-rule", default="median", help="(unused placeholder) vendor rule context")
    args = parser.parse_args()

    run_backtest(args.start, args.end, args.edge_threshold)


if __name__ == "__main__":
    main()
