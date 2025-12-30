"""
Fit a simple online calibration layer for model residuals.
Uses archived predictions and actual outcomes to compute linear correction.
"""

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

import sqlite3

from config import DB_PATH


BASE_DIR = Path(__file__).resolve().parent.parent
PRED_DIR = Path("data") / "new_model"
CALIBRATION_PATH = PRED_DIR / "calibration.json"


def _parse_date(date_str: str) -> Optional[datetime]:
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except Exception:
        return None


def _load_predictions(window_start: datetime, window_end: datetime) -> Dict[int, Dict]:
    preds: Dict[int, Dict] = {}
    if not PRED_DIR.exists():
        return preds
    for path in sorted(PRED_DIR.glob("predictions_*.json")):
        date_part = path.stem.split("_", 1)[-1]
        if not date_part:
            continue
        try:
            file_date = datetime.strptime(date_part, "%Y-%m-%d")
        except Exception:
            continue
        if file_date < window_start.date() or file_date > window_end.date():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        for g in payload.get("games", []):
            game_id = g.get("gameId")
            if game_id is None:
                continue
            try:
                game_id = int(game_id)
            except Exception:
                continue
            preds[game_id] = g
    return preds


def _linear_fit(xs: List[float], ys: List[float]) -> Dict[str, float]:
    if not xs or not ys or len(xs) != len(ys):
        return {"slope": 1.0, "intercept": 0.0}
    n = len(xs)
    x_mean = sum(xs) / n
    y_mean = sum(ys) / n
    var = sum((x - x_mean) ** 2 for x in xs)
    if var == 0:
        return {"slope": 1.0, "intercept": 0.0}
    cov = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys))
    slope = cov / var
    intercept = y_mean - slope * x_mean
    return {"slope": slope, "intercept": intercept}


def main():
    parser = argparse.ArgumentParser(description="Calibrate model residuals using recent results.")
    parser.add_argument("--window-days", type=int, default=60, help="Rolling window in days (default 60).")
    parser.add_argument("--min-samples", type=int, default=200, help="Minimum games required (default 200).")
    args = parser.parse_args()

    now_utc = datetime.now(timezone.utc)
    window_end = now_utc - timedelta(days=1)
    window_start = window_end - timedelta(days=args.window_days)

    preds = _load_predictions(window_start, window_end)
    if not preds:
        print("No predictions found for calibration window.")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT g.game_id, g.date, g.home_score, g.away_score,
                   ml.closing_spread_home, ml.closing_total
            FROM games g
            LEFT JOIN market_lines ml ON ml.game_id = g.game_id
            WHERE g.date BETWEEN ? AND ?
            """,
            (window_start.date().isoformat(), window_end.date().isoformat()),
        ).fetchall()
    finally:
        conn.close()

    spread_x: List[float] = []
    spread_y: List[float] = []
    total_x: List[float] = []
    total_y: List[float] = []

    for r in rows:
        game_id = r["game_id"]
        pred = preds.get(int(game_id))
        if not pred:
            continue
        model = pred.get("model") or {}
        raw_spread = model.get("resid_spread_raw")
        raw_total = model.get("resid_total_raw")
        if raw_spread is None or raw_total is None:
            # Skip legacy files without raw residuals to avoid feedback loops.
            continue

        home_score = r["home_score"]
        away_score = r["away_score"]
        if home_score is None or away_score is None:
            continue
        closing_spread = r["closing_spread_home"]
        closing_total = r["closing_total"]
        if closing_spread is None or closing_total is None:
            continue

        margin = float(home_score) - float(away_score)
        total = float(home_score) + float(away_score)
        resid_spread_actual = margin + float(closing_spread)
        resid_total_actual = total - float(closing_total)

        spread_x.append(float(raw_spread))
        spread_y.append(resid_spread_actual)
        total_x.append(float(raw_total))
        total_y.append(resid_total_actual)

    payload = {
        "as_of_utc": now_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "window_days": args.window_days,
        "samples": {
            "spread": len(spread_x),
            "total": len(total_x),
        },
        "spread": {"slope": 1.0, "intercept": 0.0},
        "total": {"slope": 1.0, "intercept": 0.0},
    }

    if len(spread_x) >= args.min_samples:
        payload["spread"] = _linear_fit(spread_x, spread_y)
    else:
        print(f"Not enough spread samples for calibration ({len(spread_x)}).")

    if len(total_x) >= args.min_samples:
        payload["total"] = _linear_fit(total_x, total_y)
    else:
        print(f"Not enough total samples for calibration ({len(total_x)}).")

    PRED_DIR.mkdir(parents=True, exist_ok=True)
    CALIBRATION_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote calibration to {CALIBRATION_PATH}")


if __name__ == "__main__":
    main()
