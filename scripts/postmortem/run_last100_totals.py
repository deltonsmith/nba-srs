import json
import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests


API_BASE = "https://api.balldontlie.io/v1/games"

REQUIRED_COLUMNS = {
    "game_id",
    "bet_date",
    "bet_type",
    "market_line",
    "model_line",
    "edge",
    "recommended_bet",
}
MASTER_BETS_PATH = Path("data") / "bets" / "bets_master.csv"


def _parse_total(text: str) -> Optional[float]:
    if text is None:
        return None
    s = str(text).strip()
    if not s:
        return None
    s = s.replace("Over", "").replace("Under", "").strip()
    try:
        return float(s)
    except ValueError:
        return None


def _fetch_game(game_id: int, api_key: str) -> Dict:
    resp = requests.get(f"{API_BASE}/{game_id}", headers={"Authorization": api_key}, timeout=30)
    resp.raise_for_status()
    return resp.json().get("data", {})


def _standardize(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series, pd.Series]:
    means = df.mean()
    stds = df.std().replace(0, 1)
    return (df - means) / stds, means, stds


def _ridge_fit(X: np.ndarray, y: np.ndarray, alpha: float = 1.0) -> np.ndarray:
    xtx = X.T @ X
    ridge = alpha * np.eye(xtx.shape[0])
    return np.linalg.solve(xtx + ridge, X.T @ y)


def _feature_correlations(df: pd.DataFrame, target: pd.Series) -> pd.Series:
    corrs = {}
    for col in df.columns:
        if df[col].std() == 0:
            corrs[col] = 0.0
        else:
            corrs[col] = float(np.corrcoef(df[col], target)[0, 1])
    return pd.Series(corrs)


def _ensure_features_for_dates(dates: List[str]) -> None:
    for date_str in dates:
        out_path = Path("data") / "features" / f"correlated_features_{date_str.replace('-', '')}.csv"
        if out_path.exists():
            continue
        subprocess.check_call(["python", "scripts/postmortem/build_correlated_features.py", "--date", date_str])


def _load_features(date_str: str) -> pd.DataFrame:
    path = Path("data") / "features" / f"correlated_features_{date_str.replace('-', '')}.csv"
    if not path.exists():
        raise SystemExit(f"Missing correlated features file: {path}")
    return pd.read_csv(path)


def main() -> None:
    if not MASTER_BETS_PATH.exists():
        raise SystemExit("NO_BET_LOG_FOUND: create data/bets/bets_master.csv with required columns.")

    bets = pd.read_csv(MASTER_BETS_PATH)
    print(f"BET_LOG_SOURCE={MASTER_BETS_PATH}")
    print(f"BET_LOG_ROWS={len(bets)}")
    print(f"MASTER_BETS_ROWS={len(bets)}")
    print(f"MASTER_TOTAL_ROWS={len(bets[bets['bet_type'] == 'Total'])}")

    totals = bets[bets["bet_type"] == "Total"].copy()
    print(f"TOTAL_ROWS={len(totals)}")
    totals["bet_date"] = pd.to_datetime(totals["bet_date"]).dt.date
    totals = totals.sort_values("bet_date", ascending=False).head(100)
    print(f"LAST100_ROWS={len(totals)}")
    if not totals.empty:
        min_date = totals["bet_date"].min().isoformat()
        max_date = totals["bet_date"].max().isoformat()
        print(f"DATE_RANGE={min_date} to {max_date}")

    if len(totals) != 100:
        raise SystemExit(
            f"HARD FAIL: expected 100 total bets, found {len(totals)}. Create/point to a master bet log with >=100 totals."
        )

    out_bets = Path("data") / "bets" / "last100_totals.csv"
    out_bets.parent.mkdir(parents=True, exist_ok=True)
    totals.to_csv(out_bets, index=False)

    dates = sorted({d.isoformat() for d in totals["bet_date"].unique()})
    _ensure_features_for_dates(dates)

    api_key = os.environ.get("BALLDONTLIE_API_KEY")
    if not api_key:
        raise SystemExit("BALLDONTLIE_API_KEY is not set.")

    records = []
    for _, bet in totals.iterrows():
        game_id = int(bet["game_id"])
        game = _fetch_game(game_id, api_key)
        home_team = game.get("home_team", {}).get("abbreviation")
        away_team = game.get("visitor_team", {}).get("abbreviation")
        home_score = game.get("home_team_score")
        away_score = game.get("visitor_team_score")
        actual_total = None
        if home_score is not None and away_score is not None:
            actual_total = float(home_score) + float(away_score)

        date_str = str(bet["bet_date"])
        features = _load_features(date_str)
        team_rows = features[features["game_id"] == game_id]
        if team_rows.empty:
            continue
        home_row = team_rows[team_rows["side"] == "home"].iloc[0]
        away_row = team_rows[team_rows["side"] == "away"].iloc[0]

        market_total = _parse_total(str(bet["market_line"]))
        model_total = _parse_total(str(bet["model_line"]))
        total_error = actual_total - model_total if (actual_total is not None and model_total is not None) else None

        combined = {}
        for prefix in ("r10", "r30"):
            for metric in ("ortg", "pace_est"):
                t_val = home_row.get(f"{metric}_{prefix}")
                o_val = away_row.get(f"{metric}_{prefix}")
                combined[f"{metric}_{prefix}_sum"] = (t_val + o_val) if (t_val is not None and o_val is not None) else None
            for metric in ("efg", "ts", "ftr", "tov_pct", "orb_pct"):
                t_val = home_row.get(f"{metric}_{prefix}")
                o_val = away_row.get(f"{metric}_{prefix}")
                combined[f"{metric}_{prefix}_mean"] = (
                    (t_val + o_val) / 2.0 if (t_val is not None and o_val is not None) else None
                )

        combined["rest_sum"] = None
        if home_row.get("days_rest_team") is not None and away_row.get("days_rest_team") is not None:
            combined["rest_sum"] = home_row.get("days_rest_team") + away_row.get("days_rest_team")

        ou_result = None
        if actual_total is not None and market_total is not None:
            if actual_total == float(market_total):
                ou_result = None
            else:
                ou_result = actual_total > float(market_total)

        records.append(
            {
                "game_id": game_id,
                "bet_date": date_str,
                "market_line": bet["market_line"],
                "model_line": bet["model_line"],
                "edge": bet["edge"],
                "home_team": home_team,
                "away_team": away_team,
                "home_score": home_score,
                "away_score": away_score,
                "actual_total": actual_total,
                "market_total": market_total,
                "model_total": model_total,
                "total_error": total_error,
                "ou_win": ou_result,
                **combined,
            }
        )

    df = pd.DataFrame(records)
    out_dir = Path("data") / "postmortem" / "last100_totals"
    out_dir.mkdir(parents=True, exist_ok=True)
    diag_path = out_dir / "diagnostics.csv"
    df.to_csv(diag_path, index=False)

    feature_cols = [c for c in df.columns if c.endswith("_sum") or c.endswith("_mean") or c == "rest_sum"]
    X = df[feature_cols].astype(float).fillna(0)
    y = df["total_error"].astype(float)

    standardized, means, stds = _standardize(X)
    coefs = _ridge_fit(standardized.to_numpy(), y.to_numpy(), alpha=1.0)
    coef_series = pd.Series(coefs, index=feature_cols).abs().sort_values(ascending=False)
    corr_series = _feature_correlations(X, y).abs().sort_values(ascending=False)

    coef_path = out_dir / "total_coefficients.csv"
    corr_path = out_dir / "total_correlations.csv"
    coef_series.to_csv(coef_path, header=["coef_abs"])
    corr_series.to_csv(corr_path, header=["corr_abs"])

    ou_games = df[df["ou_win"].notna()]
    ou_wins = int((ou_games["ou_win"] == True).sum())
    ou_losses = int((ou_games["ou_win"] == False).sum())
    ou_rate = (ou_wins / len(ou_games)) if len(ou_games) else 0.0

    mae = float(df["total_error"].abs().mean()) if df["total_error"].notna().any() else 0.0

    report_lines = []
    report_lines.append("# Last 100 total bets postmortem")
    report_lines.append("")
    report_lines.append(f"O/U record: {ou_wins}-{ou_losses} (hit rate {ou_rate:.1%})")
    report_lines.append(f"MAE (model_total vs actual_total): {mae:.2f}")
    report_lines.append("")
    report_lines.append("Top 15 features by standardized coefficient magnitude:")
    report_lines.extend([f"- {k}: {v:.3f}" for k, v in coef_series.head(15).items()])

    report_path = out_dir / "report.md"
    report_path.write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    print(f"Wrote {report_path}")


if __name__ == "__main__":
    main()
