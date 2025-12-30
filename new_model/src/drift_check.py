"""
Compute simple drift checks on model residuals over recent vs prior windows.
Outputs JSON to new_model/reports/drift_report.json.
"""

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Optional

import sqlite3

from config import DB_PATH


def _summarize(rows) -> Dict[str, float]:
    if not rows:
        return {"mae_spread": 0.0, "mae_total": 0.0, "n": 0}
    mae_spread = sum(abs(r["resid_spread"]) for r in rows) / len(rows)
    mae_total = sum(abs(r["resid_total"]) for r in rows) / len(rows)
    return {"mae_spread": mae_spread, "mae_total": mae_total, "n": len(rows)}


def _load_residuals(conn, start_date: str, end_date: str):
    rows = conn.execute(
        """
        SELECT g.home_score, g.away_score, ml.closing_spread_home, ml.closing_total
        FROM games g
        LEFT JOIN market_lines ml ON ml.game_id = g.game_id
        WHERE g.date BETWEEN ? AND ?
          AND g.home_score IS NOT NULL
          AND g.away_score IS NOT NULL
          AND ml.closing_spread_home IS NOT NULL
          AND ml.closing_total IS NOT NULL
        """,
        (start_date, end_date),
    ).fetchall()
    out = []
    for r in rows:
        margin = float(r[0]) - float(r[1])
        total = float(r[0]) + float(r[1])
        resid_spread = margin + float(r[2])
        resid_total = total - float(r[3])
        out.append({"resid_spread": resid_spread, "resid_total": resid_total})
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute drift checks for model residuals.")
    parser.add_argument("--window-days", type=int, default=30, help="Recent window size in days.")
    parser.add_argument("--baseline-days", type=int, default=30, help="Baseline window size in days.")
    parser.add_argument("--alert-threshold", type=float, default=0.1, help="Alert when MAE worsens by this fraction.")
    args = parser.parse_args()

    now_utc = datetime.now(timezone.utc)
    recent_end = now_utc.date() - timedelta(days=1)
    recent_start = recent_end - timedelta(days=args.window_days - 1)
    baseline_end = recent_start - timedelta(days=1)
    baseline_start = baseline_end - timedelta(days=args.baseline_days - 1)

    conn = sqlite3.connect(DB_PATH)
    try:
        recent_rows = _load_residuals(conn, recent_start.isoformat(), recent_end.isoformat())
        baseline_rows = _load_residuals(conn, baseline_start.isoformat(), baseline_end.isoformat())
    finally:
        conn.close()

    recent = _summarize(recent_rows)
    baseline = _summarize(baseline_rows)

    spread_alert = False
    total_alert = False
    if baseline["n"] > 0 and recent["n"] > 0:
        spread_alert = (recent["mae_spread"] - baseline["mae_spread"]) / baseline["mae_spread"] >= args.alert_threshold
        total_alert = (recent["mae_total"] - baseline["mae_total"]) / baseline["mae_total"] >= args.alert_threshold

    payload = {
        "as_of_utc": now_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "recent_window": {"start": recent_start.isoformat(), "end": recent_end.isoformat(), **recent},
        "baseline_window": {"start": baseline_start.isoformat(), "end": baseline_end.isoformat(), **baseline},
        "alerts": {"spread_mae_worse": spread_alert, "total_mae_worse": total_alert},
        "alert_threshold": args.alert_threshold,
    }

    reports_dir = Path(__file__).resolve().parent.parent / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    out_path = reports_dir / "drift_report.json"
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote drift report to {out_path}")


if __name__ == "__main__":
    main()
