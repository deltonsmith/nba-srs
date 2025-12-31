import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import sqlite3
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, log_loss

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "new_model" / "data" / "new_model.sqlite"

FEATURES = [
    "model_spread_home",
    "home_court",
    "rest_days_diff",
    "back_to_back_diff",
    "inj_out_diff",
    "pace_diff",
]


def _sigmoid(x: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-x))


def load_eval_data(conn, start_date: str, end_date: str) -> pd.DataFrame:
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
    df["home_court"] = 1.0
    df["rest_days_diff"] = df["rest_days_home"] - df["rest_days_away"]
    df["back_to_back_diff"] = df["back_to_back_home"] - df["back_to_back_away"]
    df["inj_out_diff"] = df["inj_out_home"] - df["inj_out_away"]
    df["pace_diff"] = df["pace_home"] - df["pace_away"]
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate calibrated win probability over last N days.")
    parser.add_argument("--days", type=int, default=7, help="Days to evaluate (default 7).")
    parser.add_argument("--scale", type=float, default=0.1, help="Scale for base spread->prob (default 0.1).")
    args = parser.parse_args()

    now_utc = datetime.now(timezone.utc)
    end_date = (now_utc - timedelta(days=1)).date()
    start_date = end_date - timedelta(days=args.days - 1)

    conn = sqlite3.connect(DB_PATH)
    try:
        df = load_eval_data(conn, start_date.isoformat(), end_date.isoformat())
    finally:
        conn.close()

    if df.empty:
        raise SystemExit("No data available for evaluation window.")

    df = df[df["home_score"].notna() & df["away_score"].notna() & df["model_spread_home"].notna()].copy()
    if df.empty:
        raise SystemExit("No scored games with model spreads for evaluation window.")

    df["home_win"] = (df["home_score"] > df["away_score"]).astype(int)
    df[FEATURES] = df[FEATURES].fillna(0)

    X = df[FEATURES]
    y = df["home_win"].to_numpy()

    base_logits = args.scale * (-df["model_spread_home"].to_numpy())
    base_probs = _sigmoid(base_logits)

    model = LogisticRegression(max_iter=1000, solver="liblinear")
    model.fit(X, y)
    calib_probs = model.predict_proba(X)[:, 1]

    base_logloss = log_loss(y, base_probs, labels=[0, 1])
    calib_logloss = log_loss(y, calib_probs, labels=[0, 1])
    base_acc = accuracy_score(y, base_probs >= 0.5)
    calib_acc = accuracy_score(y, calib_probs >= 0.5)

    print(f"Window: {start_date} to {end_date} ({len(df)} games)")
    print(f"Base log loss: {base_logloss:.4f} | Calibrated log loss: {calib_logloss:.4f}")
    print(f"Base accuracy: {base_acc:.3f} | Calibrated accuracy: {calib_acc:.3f}")


if __name__ == \"__main__\":
    main()
