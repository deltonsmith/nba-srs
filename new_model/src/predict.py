"""
Generate predictions for a given date (not integrated into frontend).
Outputs JSON: new_model/output/predictions_YYYY-MM-DD.json
"""

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import joblib
import pandas as pd
import sqlite3

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


def load_models(base_dir: Path):
    models_dir = base_dir / "models"
    try:
        m_margin = joblib.load(models_dir / "margin_model.joblib")
        m_total = joblib.load(models_dir / "total_model.joblib")
        return m_margin, m_total
    except FileNotFoundError:
        print("Model artifacts missing; continuing with market-only output (no model lines/edges).")
        return None, None


def load_games_and_features(conn, target_date: str):
    games = pd.read_sql(
        """
        SELECT g.game_id, g.date, g.home_team_id, g.away_team_id, g.status,
               g.start_time_utc,
               ml.closing_spread_home, ml.closing_total, ml.closing_home_ml
        FROM games g
        LEFT JOIN market_lines ml ON ml.game_id = g.game_id
        WHERE g.date = ?
        """,
        conn,
        params=[target_date],
    )
    feats = pd.read_sql("SELECT * FROM team_game_features", conn)

    # If no features yet, fall back to zeros so we still emit payload.
    required_cols = {"game_id", "team_id"}
    if feats.empty or not required_cols.issubset(set(feats.columns)):
        df = games.copy()
        feat_cols = []
        for col in FEATURE_COLS:
            h = f"{col}_home"
            a = f"{col}_away"
            df[h] = 0
            df[a] = 0
            feat_cols.extend([h, a])
        return df, feat_cols

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
    feat_cols = []
    for col in FEATURE_COLS:
        feat_cols.append(f"{col}_home")
        feat_cols.append(f"{col}_away")
    df[feat_cols] = df[feat_cols].fillna(0)
    return df, feat_cols


def build_predictions(df: pd.DataFrame, feat_cols: List[str], m_margin, m_total, vendor_rule: str, target_date: str) -> Dict:
    if m_margin is None or m_total is None:
        preds_margin = [None] * len(df)
        preds_total = [None] * len(df)
    else:
        preds_margin = m_margin.predict(df[feat_cols])
        preds_total = m_total.predict(df[feat_cols])

    games_out: List[Dict] = []
    for (_, row), pm, pt in zip(df.iterrows(), preds_margin, preds_total):
        market_spread = row.get("closing_spread_home")
        market_total = row.get("closing_total")

        model_spread = float(pm) if pd.notna(pm) else None
        model_total = float(pt) if pd.notna(pt) else None

        market_spread_val = float(market_spread) if pd.notna(market_spread) else None
        market_total_val = float(market_total) if pd.notna(market_total) else None

        edge_spread = None
        edge_total = None
        if model_spread is not None and market_spread_val is not None:
            edge_spread = model_spread - market_spread_val
        if model_total is not None and market_total_val is not None:
            edge_total = model_total - market_total_val

        games_out.append({
            "gameId": int(row["game_id"]),
            "startTimeUtc": row.get("start_time_utc"),
            "away": {"id": row.get("away_team_id"), "name": None, "abbr": row.get("away_team_id")},
            "home": {"id": row.get("home_team_id"), "name": None, "abbr": row.get("home_team_id")},
            "market": {
                "spreadHome": market_spread_val,
                "total": market_total_val,
            },
            "realLine": {
                "spreadHome": model_spread,
                "total": model_total,
            },
            "edge": {
                "spread": edge_spread,
                "total": edge_total,
            },
        })

    return {
        "asOfUtc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "date": target_date,
        "timezone": "America/Chicago",
        "vendorRule": vendor_rule,
        "games": games_out,
    }


def main():
    parser = argparse.ArgumentParser(description="Generate model predictions for a given date.")
    parser.add_argument("--date", required=True, help="Date YYYY-MM-DD")
    parser.add_argument("--vendor-rule", default="median", help="Vendor rule context")
    args = parser.parse_args()

    base_dir = Path(__file__).resolve().parent.parent
    output_dir = base_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    m_margin, m_total = load_models(base_dir)

    conn = sqlite3.connect(DB_PATH)
    try:
        df, feat_cols = load_games_and_features(conn, args.date)
    finally:
        conn.close()

    if df.empty:
        print(f"No games found for {args.date}; writing empty payload.")

    payload = build_predictions(df, feat_cols, m_margin, m_total, args.vendor_rule, args.date)
    out_path = output_dir / f"predictions_{args.date}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print(f"Wrote predictions to {out_path}")


if __name__ == "__main__":
    main()
