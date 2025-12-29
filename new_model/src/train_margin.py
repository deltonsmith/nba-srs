"""
Train a baseline margin model using rolling pre-game features.
Label: home_score - away_score.
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import List

import joblib
import math
import pandas as pd
import sqlite3
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error

from config import DB_PATH
from db import init_db


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
    "inj_out",
    "inj_day_to_day",
    "inj_total",
]

GAME_FEATURE_COLS = [
    "closing_spread_home",
    "closing_total",
    "closing_home_ml",
]


def load_dataset(db_path: str) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    try:
        games = pd.read_sql(
            """
            SELECT g.game_id, g.date, g.home_score, g.away_score,
                   g.home_team_id, g.away_team_id,
                   ml.closing_spread_home, ml.closing_total, ml.closing_home_ml
            FROM games g
            LEFT JOIN market_lines ml ON ml.game_id = g.game_id
            WHERE g.home_score IS NOT NULL AND g.away_score IS NOT NULL
            ORDER BY g.date ASC
            """,
            conn,
        )

        feats = pd.read_sql("SELECT * FROM team_game_features", conn)
    finally:
        conn.close()

    if games.empty:
        raise SystemExit("No games with scores found for training.")

    # Home features
    home_feats = feats.merge(
        games[["game_id", "home_team_id"]],
        left_on=["game_id", "team_id"],
        right_on=["game_id", "home_team_id"],
        how="inner",
    )
    home_feats = home_feats.drop(columns=["team_id", "home_team_id"])
    home_feats = home_feats.add_suffix("_home")
    home_feats = home_feats.rename(columns={"game_id_home": "game_id"})

    # Away features
    away_feats = feats.merge(
        games[["game_id", "away_team_id"]],
        left_on=["game_id", "team_id"],
        right_on=["game_id", "away_team_id"],
        how="inner",
    )
    away_feats = away_feats.drop(columns=["team_id", "away_team_id"])
    away_feats = away_feats.add_suffix("_away")
    away_feats = away_feats.rename(columns={"game_id_away": "game_id"})

    df = games.merge(home_feats, on="game_id", how="left").merge(away_feats, on="game_id", how="left")

    df["date"] = pd.to_datetime(df["date"])
    df["margin"] = df["home_score"] - df["away_score"]

    # Build feature columns with suffixes
    feat_cols = []
    for col in FEATURE_COLS:
        feat_cols.append(f"{col}_home")
        feat_cols.append(f"{col}_away")
    feat_cols.extend(GAME_FEATURE_COLS)

    # Fill missing with 0 for now (TODO: consider better imputation)
    df[feat_cols] = df[feat_cols].fillna(0)

    return df, feat_cols


def time_split(df: pd.DataFrame, feat_cols: List[str], label_col: str):
    df_sorted = df.sort_values("date").reset_index(drop=True)
    split_idx = max(1, int(0.8 * len(df_sorted)))
    train = df_sorted.iloc[:split_idx]
    val = df_sorted.iloc[split_idx:]
    X_train, y_train = train[feat_cols], train[label_col]
    X_val, y_val = val[feat_cols], val[label_col]
    return X_train, X_val, y_train, y_val


def train_and_eval(df: pd.DataFrame, feat_cols: List[str], label_col: str):
    X_train, X_val, y_train, y_val = time_split(df, feat_cols, label_col)
    model = HistGradientBoostingRegressor(random_state=42)
    model.fit(X_train, y_train)

    preds = model.predict(X_val)
    mae = mean_absolute_error(y_val, preds)
    rmse = math.sqrt(mean_squared_error(y_val, preds))

    return model, {"mae": mae, "rmse": rmse, "n_train": len(X_train), "n_val": len(X_val)}, preds, X_val


def ensure_dirs():
    (Path(__file__).resolve().parent.parent / "models").mkdir(parents=True, exist_ok=True)
    (Path(__file__).resolve().parent.parent / "reports").mkdir(parents=True, exist_ok=True)


def main():
    ensure_dirs()
    init_db(DB_PATH)
    df, feat_cols = load_dataset(DB_PATH)
    # Train on residual vs market (edge-focused)
    df = df[df["closing_spread_home"].notna()].copy()
    if df.empty:
        raise SystemExit("No games with closing spreads found for training.")
    df["residual"] = df["margin"] - df["closing_spread_home"]

    model, metrics, preds, X_val = train_and_eval(df, feat_cols, "residual")
    val = df.loc[X_val.index]
    pred_margin = preds + val["closing_spread_home"].to_numpy()
    mae_margin = mean_absolute_error(val["margin"], pred_margin)
    metrics.update({"mae_margin": mae_margin, "target": "residual"})

    models_dir = Path(__file__).resolve().parent.parent / "models"
    reports_dir = Path(__file__).resolve().parent.parent / "reports"

    joblib.dump(model, models_dir / "margin_model.joblib")

    metrics.update(
        {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "train_end_date": df["date"].max().strftime("%Y-%m-%d"),
            "git_sha": os.environ.get("GIT_SHA"),
        }
    )
    with open(reports_dir / "margin_metrics.json", "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)

    print(f"Saved model to {models_dir / 'margin_model.joblib'}")
    print(f"Metrics: {metrics}")


if __name__ == "__main__":
    main()
