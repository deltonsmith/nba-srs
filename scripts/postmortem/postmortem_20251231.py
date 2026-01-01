import argparse
import csv
import json
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests


API_BASE = "https://api.balldontlie.io/v1/games"
MIN_N_CORR = 5
MIN_N_MODEL = 20


def _parse_team_line(text: str) -> Tuple[Optional[str], Optional[float]]:
    if not text:
        return None, None
    m = re.match(r"([A-Z]{2,3})\s*([+-]?\d+(\.\d+)?)", text.strip())
    if not m:
        return None, None
    return m.group(1), float(m.group(2))


def _parse_total(text: str) -> Optional[float]:
    try:
        return float(text)
    except Exception:
        return None


def _parse_matchup(teams: str) -> Tuple[Optional[str], Optional[str]]:
    if not teams:
        return None, None
    parts = teams.replace('"', "").split("@")
    if len(parts) != 2:
        return None, None
    away = parts[0].strip().split()[-1]
    home = parts[1].strip().split()[0]
    return away, home


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


def _feature_correlations(df: pd.DataFrame, target: pd.Series) -> Dict[str, float]:
    corrs = {}
    for col in df.columns:
        if df[col].std() == 0:
            corrs[col] = 0.0
        else:
            corrs[col] = float(np.corrcoef(df[col], target)[0, 1])
    return corrs


def _load_features(date_str: str) -> pd.DataFrame:
    path = Path("data") / "features" / f"correlated_features_{date_str}.csv"
    if not path.exists():
        raise SystemExit(f"Missing correlated features file: {path}")
    return pd.read_csv(path)


def _line_to_home_spread(line_team: str, line_value: float, home_team: str, away_team: str) -> Optional[float]:
    if line_team is None or line_value is None:
        return None
    if line_team == home_team:
        return line_value
    if line_team == away_team:
        return -line_value
    return None


def _collect_training_rows(target_date: datetime.date, feature_cols: List[str]) -> Tuple[pd.DataFrame, pd.Series, pd.Series]:
    feature_dir = Path("data") / "features"
    pred_dir = Path("data") / "new_model"
    if not feature_dir.exists():
        return pd.DataFrame(), pd.Series(dtype=float), pd.Series(dtype=float)

    feature_files = sorted(feature_dir.glob("correlated_features_*.csv"))
    cutoff = target_date - timedelta(days=60)
    rows_spread = []
    rows_total = []

    for fpath in feature_files:
        date_str = fpath.stem.replace("correlated_features_", "")
        try:
            file_date = datetime.strptime(date_str, "%Y%m%d").date()
        except Exception:
            continue
        if file_date >= target_date or file_date < cutoff:
            continue

        pred_path = pred_dir / f"predictions_{file_date.isoformat()}.json"
        if not pred_path.exists():
            continue
        preds = json.loads(pred_path.read_text(encoding="utf-8"))
        pred_games = {int(g["gameId"]): g for g in preds.get("games", []) if g.get("gameId") is not None}

        features = pd.read_csv(fpath)
        for game_id, g in pred_games.items():
            game_rows = features[features["game_id"] == game_id]
            if game_rows.empty:
                continue
            home_row = game_rows[game_rows["side"] == "home"]
            away_row = game_rows[game_rows["side"] == "away"]
            if home_row.empty or away_row.empty:
                continue
            home_row = home_row.iloc[0]
            away_row = away_row.iloc[0]

            market = g.get("market") or {}
            real_line = g.get("realLine") or {}
            model_spread_home = real_line.get("spreadHome")
            model_total = real_line.get("total")
            market_spread_home = market.get("spreadHome")
            market_total = market.get("total")
            if model_spread_home is None or model_total is None:
                continue

            # Outcome from scores embedded in predictions not present; skip if missing.
            # Use games data from predictions? Not available, so skip if scores missing.
            continue

    return pd.DataFrame(), pd.Series(dtype=float), pd.Series(dtype=float)


def main() -> None:
    parser = argparse.ArgumentParser(description="Postmortem for 2025-12-31 bets.")
    parser.add_argument("--date", default="2025-12-31", help="Date YYYY-MM-DD")
    args = parser.parse_args()

    api_key = os.environ.get("BALLDONTLIE_API_KEY")
    if not api_key:
        raise SystemExit("BALLDONTLIE_API_KEY is not set.")

    target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    date_str = target_date.strftime("%Y%m%d")

    bets_path = Path("data") / "bets" / f"{date_str}_bets.csv"
    if not bets_path.exists():
        raise SystemExit(f"Missing bets file: {bets_path}")
    bets = pd.read_csv(bets_path)

    features = _load_features(date_str)

    records = []
    for _, bet in bets.iterrows():
        game_id = int(bet["game_id"])
        game = _fetch_game(game_id, api_key)
        home_team = game.get("home_team", {}).get("abbreviation")
        away_team = game.get("visitor_team", {}).get("abbreviation")
        home_score = game.get("home_team_score")
        away_score = game.get("visitor_team_score")
        actual_margin = None
        actual_total = None
        if home_score is not None and away_score is not None:
            actual_margin = float(home_score) - float(away_score)
            actual_total = float(home_score) + float(away_score)

        team_rows = features[features["game_id"] == game_id]
        if team_rows.empty:
            continue
        home_row = team_rows[team_rows["side"] == "home"].iloc[0]
        away_row = team_rows[team_rows["side"] == "away"].iloc[0]

        bet_type = bet["bet_type"]
        market_team, market_val = _parse_team_line(str(bet["market_line"])) if bet_type == "Spread" else (None, None)
        model_team, model_val = _parse_team_line(str(bet["model_line"])) if bet_type == "Spread" else (None, None)
        if bet_type == "Total":
            market_val = _parse_total(str(bet["market_line"]))
            model_val = _parse_total(str(bet["model_line"]))

        market_spread_home = _line_to_home_spread(market_team, market_val, home_team, away_team) if bet_type == "Spread" else None
        model_spread_home = _line_to_home_spread(model_team, model_val, home_team, away_team) if bet_type == "Spread" else None

        predicted_margin = None
        if bet_type == "Spread" and model_spread_home is not None:
            predicted_margin = -float(model_spread_home)
        predicted_total = model_val if bet_type == "Total" else None

        spread_error = None
        total_error = None
        if predicted_margin is not None and actual_margin is not None:
            spread_error = actual_margin - predicted_margin
        if predicted_total is not None and actual_total is not None:
            total_error = actual_total - predicted_total

        # Build deltas/combined features using rolling 10 and 30 windows where available.
        line_team = market_team or model_team
        team_row = home_row
        opp_row = away_row
        if bet_type == "Spread" and line_team:
            if line_team == away_team:
                team_row = away_row
                opp_row = home_row
        feat_pairs = [
            ("ortg", "drtg", "netrtg", "pace_est", "efg", "ts", "ftr", "tov_pct", "orb_pct"),
        ]
        feature_record = {}
        for prefix in ("r10", "r30"):
            for metric in feat_pairs[0]:
                h_val = team_row.get(f"{metric}_{prefix}")
                a_val = opp_row.get(f"{metric}_{prefix}")
                if bet_type == "Spread":
                    feature_record[f"{metric}_{prefix}_delta"] = (h_val - a_val) if (h_val is not None and a_val is not None) else None
                else:
                    if metric in ("ortg", "pace_est"):
                        feature_record[f"{metric}_{prefix}_sum"] = (h_val + a_val) if (h_val is not None and a_val is not None) else None
                    else:
                        feature_record[f"{metric}_{prefix}_mean"] = (h_val + a_val) / 2 if (h_val is not None and a_val is not None) else None

        feature_record["rest_diff"] = home_row.get("rest_diff")
        feature_record["home_indicator"] = home_row.get("home_indicator")

        records.append(
            {
                "game_id": game_id,
                "bet_type": bet_type,
                "teams": bet["teams"],
                "market_line": bet["market_line"],
                "model_line": bet["model_line"],
                "edge": bet["edge"],
                "home_team": home_team,
                "away_team": away_team,
                "home_score": home_score,
                "away_score": away_score,
                "actual_margin": actual_margin,
                "actual_total": actual_total,
                "spread_error": spread_error,
                "total_error": total_error,
                **feature_record,
            }
        )

    df = pd.DataFrame(records)
    if df.empty:
        raise SystemExit("No postmortem rows built. Check features and bets.")

    out_dir = Path("data") / "postmortem" / date_str
    out_dir.mkdir(parents=True, exist_ok=True)
    diag_path = out_dir / "diagnostics.csv"
    df.to_csv(diag_path, index=False)

    # Feature ranking (use available rows only)
    feature_cols = [c for c in df.columns if c.endswith("_delta") or c.endswith("_sum") or c.endswith("_mean") or c in ("rest_diff", "home_indicator")]

    report_lines = []
    report_lines.append(f"# Postmortem {args.date}")
    report_lines.append("")

    n_spread = int((df["bet_type"] == "Spread").sum())
    n_total = int((df["bet_type"] == "Total").sum())
    spread_corr_ok = n_spread >= MIN_N_CORR
    total_corr_ok = n_total >= MIN_N_CORR
    spread_model_ok = n_spread >= MIN_N_MODEL
    total_model_ok = n_total >= MIN_N_MODEL
    spread_haircut_ok = n_spread >= 100
    total_haircut_ok = n_total >= 100

    report_lines.append("## SAMPLE SIZE WARNINGS")
    report_lines.append(f"- spread: n={n_spread} (corr>={MIN_N_CORR}: {spread_corr_ok}, model>={MIN_N_MODEL}: {spread_model_ok}, haircut>=100: {spread_haircut_ok})")
    report_lines.append(f"- total: n={n_total} (corr>={MIN_N_CORR}: {total_corr_ok}, model>={MIN_N_MODEL}: {total_model_ok}, haircut>=100: {total_haircut_ok})")
    report_lines.append("")

    # Spread corrections
    spread_df = df[df["bet_type"] == "Spread"].copy()
    if not spread_df.empty:
        spread_features = spread_df[feature_cols].astype(float)
        spread_target = spread_df["spread_error"].astype(float)
        spread_features = spread_features.fillna(0)
        report_lines.append("## Spread correction")
        if not spread_model_ok:
            report_lines.append(f"INSUFFICIENT SAMPLE (n={n_spread}): correction model suppressed (need >= {MIN_N_MODEL}).")
            report_lines.append("")
        else:
            standardized, means, stds = _standardize(spread_features)
            coefs = _ridge_fit(standardized.to_numpy(), spread_target.to_numpy(), alpha=1.0)
            coef_series = pd.Series(coefs, index=spread_features.columns).abs().sort_values(ascending=False)
            if np.isnan(coef_series.values).any():
                report_lines.append(f"INSUFFICIENT SAMPLE (n={n_spread}): correction model suppressed (need >= {MIN_N_MODEL}).")
                report_lines.append("")
            else:
                report_lines.append("Top coefficients (standardized magnitude):")
                report_lines.extend([f"- {k}: {v:.3f}" for k, v in coef_series.head(10).items()])
                report_lines.append("")

        if not spread_corr_ok:
            report_lines.append(f"INSUFFICIENT SAMPLE (n={n_spread}): correlations suppressed (need >= {MIN_N_CORR}).")
            report_lines.append("")
        else:
            corr_series = pd.Series(_feature_correlations(spread_features, spread_target)).abs().sort_values(ascending=False)
            if np.isnan(corr_series.values).any():
                report_lines.append(f"INSUFFICIENT SAMPLE (n={n_spread}): correlations suppressed (need >= {MIN_N_CORR}).")
                report_lines.append("")
            else:
                report_lines.append("Top correlations (abs):")
                report_lines.extend([f"- {k}: {v:.3f}" for k, v in corr_series.head(10).items()])
                report_lines.append("")
    else:
        report_lines.append("## Spread correction")
        report_lines.append("No spread rows available.")
        report_lines.append("")

    # Total corrections
    total_df = df[df["bet_type"] == "Total"].copy()
    if not total_df.empty:
        total_features = total_df[feature_cols].astype(float)
        total_target = total_df["total_error"].astype(float)
        total_features = total_features.fillna(0)
        report_lines.append("## Total correction")
        if not total_model_ok:
            report_lines.append(f"INSUFFICIENT SAMPLE (n={n_total}): correction model suppressed (need >= {MIN_N_MODEL}).")
            report_lines.append("")
        else:
            standardized, means, stds = _standardize(total_features)
            coefs = _ridge_fit(standardized.to_numpy(), total_target.to_numpy(), alpha=1.0)
            coef_series = pd.Series(coefs, index=total_features.columns).abs().sort_values(ascending=False)
            if np.isnan(coef_series.values).any():
                report_lines.append(f"INSUFFICIENT SAMPLE (n={n_total}): correction model suppressed (need >= {MIN_N_MODEL}).")
                report_lines.append("")
            else:
                report_lines.append("Top coefficients (standardized magnitude):")
                report_lines.extend([f"- {k}: {v:.3f}" for k, v in coef_series.head(10).items()])
                report_lines.append("")

        if not total_corr_ok:
            report_lines.append(f"INSUFFICIENT SAMPLE (n={n_total}): correlations suppressed (need >= {MIN_N_CORR}).")
            report_lines.append("")
        else:
            corr_series = pd.Series(_feature_correlations(total_features, total_target)).abs().sort_values(ascending=False)
            if np.isnan(corr_series.values).any():
                report_lines.append(f"INSUFFICIENT SAMPLE (n={n_total}): correlations suppressed (need >= {MIN_N_CORR}).")
                report_lines.append("")
            else:
                report_lines.append("Top correlations (abs):")
                report_lines.extend([f"- {k}: {v:.3f}" for k, v in corr_series.head(10).items()])
                report_lines.append("")
    else:
        report_lines.append("## Total correction")
        report_lines.append("No total rows available.")
        report_lines.append("")

    # Tweaks to test next
    report_lines.append("## Tweaks to test next")
    if spread_haircut_ok:
        report_lines.append("EDGE_HAIRCUT_SKIPPED: missing probability/ROI inputs (spread).")
    else:
        report_lines.append("EDGE_HAIRCUT_SKIPPED: insufficient sample (<100 spread bets).")
    if total_haircut_ok:
        report_lines.append("EDGE_HAIRCUT_SKIPPED: missing probability/ROI inputs (total).")
    else:
        report_lines.append("EDGE_HAIRCUT_SKIPPED: insufficient sample (<100 total bets).")
    report_lines.append("- cap pace contribution to +/- 5 possessions")
    report_lines.append("- cap ORtg/DRtg deltas to +/- 8 points")
    report_lines.append("- increase/decrease FTr sensitivity by 10%")

    report_path = out_dir / "report.md"
    report_path.write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    print(f"Wrote {report_path}")


if __name__ == "__main__":
    main()
