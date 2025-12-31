"""
Fit a simple win-probability calibration layer using recent games.
"""

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import sqlite3
from sklearn.linear_model import LogisticRegression

from config import DB_PATH
from feature_engineering import add_game_deltas, EXTRA_GAME_FEATURE_COLS

BASE_DIR = Path(__file__).resolve().parent.parent
CALIBRATION_PATH = Path("data") / "new_model" / "winprob_calibration.json"

FEATURES = [
    "model_spread_home",
    "home_court",
    "rest_days_diff",
    "back_to_back_diff",
    "inj_out_diff",
    "pace_diff",
]


def load_training_data(conn, start_date: str, end_date: str) -> pd.DataFrame:
    preds = pd.read_sql(
        """
        SELECT mp.game_id, mp.game_date, mp.model_spread_home,
               g.home_score, g.away_score, g.home_team_id, g.away_team_id
        FROM model_predictions mp
        JOIN games g ON g.game_id = mp.game_id
        WHERE mp.game_date BETWEEN ? AND ?
        """,
        conn,
        params=[start_date, end_date],
    )
    feats = pd.read_sql("SELECT * FROM team_game_features", conn)

    if preds.empty or feats.empty:
        return pd.DataFrame()

    home_feats = feats.merge(
        preds[["game_id", "home_team_id"]],
        left_on=["game_id", "team_id"],
        right_on=["game_id", "home_team_id"],
        how="inner",
    )
    home_feats = home_feats.drop(columns=["team_id", "home_team_id"])
    home_feats = home_feats.add_suffix("_home")
    home_feats = home_feats.rename(columns={"game_id_home": "game_id"})

    away_feats = feats.merge(
        preds[["game_id", "away_team_id"]],
        left_on=["game_id", "team_id"],
        right_on=["game_id", "away_team_id"],
        how="inner",
    )
    away_feats = away_feats.drop(columns=["team_id", "away_team_id"])
    away_feats = away_feats.add_suffix("_away")
    away_feats = away_feats.rename(columns={"game_id_away": "game_id"})

    df = preds.merge(home_feats, on="game_id", how="left").merge(away_feats, on="game_id", how="left")
    df = add_game_deltas(df)
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description="Calibrate home win probability using recent games.")
    parser.add_argument("--window-days", type=int, default=60, help="Rolling window in days (default 60).")
    parser.add_argument("--min-samples", type=int, default=100, help="Minimum games required (default 100).")
    args = parser.parse_args()

    now_utc = datetime.now(timezone.utc)
    window_end = now_utc - timedelta(days=1)
    window_start = window_end - timedelta(days=args.window_days)

    conn = sqlite3.connect(DB_PATH)
    try:
        df = load_training_data(conn, window_start.date().isoformat(), window_end.date().isoformat())
    finally:
        conn.close()

    if df.empty:
        print("No training data found for win-probability calibration.")
        return

    df = df[df["home_score"].notna() & df["away_score"].notna() & df["model_spread_home"].notna()].copy()
    if df.empty or len(df) < args.min_samples:
        print(f"Not enough samples for calibration ({len(df)}).")
        return

    df["home_win"] = (df["home_score"] > df["away_score"]).astype(int)
    df[FEATURES] = df[FEATURES].fillna(0)

    X = df[FEATURES]
    y = df["home_win"]

    model = LogisticRegression(max_iter=1000, solver="liblinear")
    model.fit(X, y)

    coef_map = {feat: float(coef) for feat, coef in zip(FEATURES, model.coef_[0])}
    payload = {
        "as_of_utc": now_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "window_start": window_start.date().isoformat(),
        "window_end": window_end.date().isoformat(),
        "samples": int(len(df)),
        "features": FEATURES,
        "coef": coef_map,
        "intercept": float(model.intercept_[0]),
        "scale": 0.1,
        "notes": "Logistic calibration on recent games; uses model spread + matchup deltas.",
    }
    CALIBRATION_PATH.parent.mkdir(parents=True, exist_ok=True)
    CALIBRATION_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote {CALIBRATION_PATH}")


if __name__ == "__main__":
    main()
