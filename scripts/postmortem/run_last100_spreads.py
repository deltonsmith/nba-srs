import csv
import json
import os
import re
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


def _parse_team_line(text: str) -> Tuple[Optional[str], Optional[float]]:
    if not text:
        return None, None
    m = re.match(r"([A-Z]{2,3})\s*([+-]?\d+(\.\d+)?)", str(text).strip())
    if not m:
        return None, None
    return m.group(1), float(m.group(2))


def _line_to_home_spread(line_team: str, line_value: float, home_team: str, away_team: str) -> Optional[float]:
    if line_team is None or line_value is None:
        return None
    if line_team == home_team:
        return line_value
    if line_team == away_team:
        return -line_value
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


def _find_bet_logs() -> List[Path]:
    paths = list(Path("data").rglob("*.csv")) + list(Path("data").rglob("*.json"))
    candidates = []
    for p in paths:
        try:
            if p.suffix == ".csv":
                header = pd.read_csv(p, nrows=0).columns
                if REQUIRED_COLUMNS.issubset(set(header)):
                    candidates.append(p)
            else:
                payload = json.loads(p.read_text(encoding="utf-8"))
                if isinstance(payload, list) and payload:
                    if REQUIRED_COLUMNS.issubset(set(payload[0].keys())):
                        candidates.append(p)
        except Exception:
            continue
    return candidates


def _load_bets(path: Path) -> pd.DataFrame:
    if path.suffix == ".csv":
        return pd.read_csv(path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    return pd.DataFrame(payload)


def _build_master_from_daily() -> Path:
    bet_dir = Path("data") / "bets"
    bet_dir.mkdir(parents=True, exist_ok=True)
    daily_files = sorted([p for p in bet_dir.glob("*.csv") if p.name != "bets_master.csv"])
    if not daily_files:
        return MASTER_BETS_PATH

    frames = []
    for path in daily_files:
        df = pd.read_csv(path)
        if REQUIRED_COLUMNS.issubset(set(df.columns)):
            frames.append(df[list(REQUIRED_COLUMNS | {"teams"}) if "teams" in df.columns else list(REQUIRED_COLUMNS)])
    if not frames:
        return MASTER_BETS_PATH

    combined = pd.concat(frames, ignore_index=True)
    if "teams" not in combined.columns:
        combined["teams"] = ""
    combined = combined[
        [
            "game_id",
            "bet_date",
            "bet_type",
            "teams",
            "market_line",
            "model_line",
            "edge",
            "recommended_bet",
        ]
    ]
    combined = combined.drop_duplicates(subset=["game_id", "bet_type", "recommended_bet", "bet_date"])
    combined.to_csv(MASTER_BETS_PATH, index=False)
    return MASTER_BETS_PATH


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
    bet_log_path = None
    if not MASTER_BETS_PATH.exists():
        _build_master_from_daily()
    if MASTER_BETS_PATH.exists():
        bet_log_path = MASTER_BETS_PATH
    else:
        bet_logs = _find_bet_logs()
        if not bet_logs:
            searched = list(Path("data").rglob("*.csv")) + list(Path("data").rglob("*.json"))
            print("NO_BET_LOG_FOUND: create data/bets/bets_master.csv with required columns.")
            print("Searched paths:")
            for p in searched:
                print(f"- {p}")
            print(f"Expected columns: {sorted(REQUIRED_COLUMNS)}")
            raise SystemExit(1)
        bet_log_path = bet_logs[0]

    if not bet_log_path.exists():
        searched = list(Path("data").rglob("*.csv")) + list(Path("data").rglob("*.json"))
        print("NO_BET_LOG_FOUND: create data/bets/bets_master.csv with required columns.")
        print("Searched paths:")
        for p in searched:
            print(f"- {p}")
        print(f"Expected columns: {sorted(REQUIRED_COLUMNS)}")
        raise SystemExit(1)

    bets = _load_bets(bet_log_path)
    print(f"BET_LOG_SOURCE={bet_log_path}")
    print(f"BET_LOG_ROWS={len(bets)}")
    if bet_log_path == MASTER_BETS_PATH:
        print(f"MASTER_BETS_ROWS={len(bets)}")
        print(f"MASTER_SPREAD_ROWS={len(bets[bets['bet_type'] == 'Spread'])}")

    spreads = bets[bets["bet_type"] == "Spread"].copy()
    print(f"SPREAD_ROWS={len(spreads)}")
    spreads["bet_date"] = pd.to_datetime(spreads["bet_date"]).dt.date
    spreads = spreads.sort_values("bet_date", ascending=False).head(100)
    print(f"LAST100_ROWS={len(spreads)}")
    if not spreads.empty:
        min_date = spreads["bet_date"].min().isoformat()
        max_date = spreads["bet_date"].max().isoformat()
        print(f"DATE_RANGE={min_date} to {max_date}")

    if len(spreads) != 100:
        raise SystemExit(
            f"HARD FAIL: expected 100 spread bets, found {len(spreads)}. Create/point to a master bet log with >=100 spread bets."
        )

    out_bets = Path("data") / "bets" / "last100_spreads.csv"
    out_bets.parent.mkdir(parents=True, exist_ok=True)
    spreads.to_csv(out_bets, index=False)

    dates = sorted({d.isoformat() for d in spreads["bet_date"].unique()})
    _ensure_features_for_dates(dates)

    api_key = os.environ.get("BALLDONTLIE_API_KEY")
    if not api_key:
        raise SystemExit("BALLDONTLIE_API_KEY is not set.")

    records = []
    for _, bet in spreads.iterrows():
        game_id = int(bet["game_id"])
        game = _fetch_game(game_id, api_key)
        home_team = game.get("home_team", {}).get("abbreviation")
        away_team = game.get("visitor_team", {}).get("abbreviation")
        home_score = game.get("home_team_score")
        away_score = game.get("visitor_team_score")
        actual_margin = None
        if home_score is not None and away_score is not None:
            actual_margin = float(home_score) - float(away_score)

        date_str = str(bet["bet_date"])
        features = _load_features(date_str)
        team_rows = features[features["game_id"] == game_id]
        if team_rows.empty:
            continue
        home_row = team_rows[team_rows["side"] == "home"].iloc[0]
        away_row = team_rows[team_rows["side"] == "away"].iloc[0]

        market_team, market_val = _parse_team_line(str(bet["market_line"]))
        model_team, model_val = _parse_team_line(str(bet["model_line"]))
        market_spread_home = _line_to_home_spread(market_team, market_val, home_team, away_team)
        model_spread_home = _line_to_home_spread(model_team, model_val, home_team, away_team)
        predicted_margin = -float(model_spread_home) if model_spread_home is not None else None
        spread_error = actual_margin - predicted_margin if (actual_margin is not None and predicted_margin is not None) else None

        team_row = home_row
        opp_row = away_row
        if market_team == away_team:
            team_row = away_row
            opp_row = home_row

        feature_record = {}
        for prefix in ("r10", "r30"):
            for metric in ("ortg", "netrtg", "pace_est", "efg", "ts", "ftr", "tov_pct", "orb_pct"):
                t_val = team_row.get(f"{metric}_{prefix}")
                o_val = opp_row.get(f"{metric}_{prefix}")
                feature_record[f"{metric}_{prefix}_delta"] = (t_val - o_val) if (t_val is not None and o_val is not None) else None
        t_drtg_30 = team_row.get("drtg_r30")
        t_drtg_10 = team_row.get("drtg_r10")
        o_drtg_30 = opp_row.get("drtg_r30")
        o_drtg_10 = opp_row.get("drtg_r10")
        if None not in (t_drtg_30, t_drtg_10, o_drtg_30, o_drtg_10):
            t_blend = 0.7 * t_drtg_30 + 0.3 * t_drtg_10
            o_blend = 0.7 * o_drtg_30 + 0.3 * o_drtg_10
            feature_record["drtg_blend_delta"] = t_blend - o_blend
        else:
            feature_record["drtg_blend_delta"] = None

        feature_record["rest_diff"] = team_row.get("rest_diff")
        feature_record["home_indicator"] = team_row.get("home_indicator")

        ats_result = None
        if actual_margin is not None and market_spread_home is not None:
            if actual_margin == -float(market_spread_home):
                ats_result = None
            else:
                ats_result = actual_margin > -float(market_spread_home)

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
                "actual_margin": actual_margin,
                "market_spread_home": market_spread_home,
                "model_spread_home": model_spread_home,
                "predicted_margin": predicted_margin,
                "spread_error": spread_error,
                "ats_win": ats_result,
                **feature_record,
            }
        )

    df = pd.DataFrame(records)
    out_dir = Path("data") / "postmortem" / "last100_spreads"
    out_dir.mkdir(parents=True, exist_ok=True)
    diag_path = out_dir / "diagnostics.csv"
    df.to_csv(diag_path, index=False)

    feature_cols = [c for c in df.columns if c.endswith("_delta") or c in ("rest_diff", "home_indicator")]
    X = df[feature_cols].astype(float).fillna(0)
    y = df["spread_error"].astype(float)

    standardized, means, stds = _standardize(X)
    coefs = _ridge_fit(standardized.to_numpy(), y.to_numpy(), alpha=1.0)
    coef_series = pd.Series(coefs, index=feature_cols).abs().sort_values(ascending=False)
    corr_series = _feature_correlations(X, y).abs().sort_values(ascending=False)

    coef_path = out_dir / "spread_coefficients.csv"
    corr_path = out_dir / "spread_correlations.csv"
    coef_series.to_csv(coef_path, header=["coef_abs"])
    corr_series.to_csv(corr_path, header=["corr_abs"])

    ats_games = df[df["ats_win"].notna()]
    ats_wins = int((ats_games["ats_win"] == True).sum())
    ats_losses = int((ats_games["ats_win"] == False).sum())
    ats_rate = (ats_wins / len(ats_games)) if len(ats_games) else 0.0

    mae = float(df["spread_error"].abs().mean()) if df["spread_error"].notna().any() else 0.0

    edge_diff = df["model_spread_home"] - df["market_spread_home"]
    edge_stats = {
        "mean": float(edge_diff.mean()),
        "median": float(edge_diff.median()),
        "p90": float(edge_diff.quantile(0.9)),
        "p95": float(edge_diff.quantile(0.95)),
        "monster_edge_count": int((edge_diff.abs() >= 18).sum()),
        "monster_edge_rate": float((edge_diff.abs() >= 18).mean()),
    }

    report_lines = []
    report_lines.append("# Last 100 spread bets postmortem")
    report_lines.append("")
    report_lines.append(f"ATS record: {ats_wins}-{ats_losses} (hit rate {ats_rate:.1%})")
    report_lines.append(f"MAE (model_line vs actual_margin): {mae:.2f}")
    report_lines.append("")
    report_lines.append("Model-market divergence distribution (model_line - market_line, home line):")
    report_lines.append(f"- mean: {edge_stats['mean']:.2f}")
    report_lines.append(f"- median: {edge_stats['median']:.2f}")
    report_lines.append(f"- p90: {edge_stats['p90']:.2f}")
    report_lines.append(f"- p95: {edge_stats['p95']:.2f}")
    report_lines.append(f"- abs>=18 count: {edge_stats['monster_edge_count']} ({edge_stats['monster_edge_rate']:.1%})")
    report_lines.append("")
    report_lines.append("Top 15 features by standardized coefficient magnitude:")
    report_lines.extend([f"- {k}: {v:.3f}" for k, v in coef_series.head(15).items()])
    report_lines.append("")
    report_lines.append("TWEAKS TO TEST NEXT")
    report_lines.append("- cap abs(model-market divergence) at 18")
    report_lines.append("- cap short-term efficiency contribution to spread at +/- 4 points")
    report_lines.append("- DRtg blend: 0.7*30g + 0.3*10g")
    report_lines.append("- pace cap: +/- 6 total possessions")

    report_path = out_dir / "report.md"
    report_path.write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    print(f"Wrote {report_path}")


if __name__ == "__main__":
    main()
