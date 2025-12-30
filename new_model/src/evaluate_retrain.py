"""
Evaluate retrain metrics vs prior metrics and decide whether to publish new models.
Writes MODEL_IMPROVED=true/false to GITHUB_ENV when available.
"""

import argparse
import json
import os
from pathlib import Path
from typing import Optional, Tuple


def _load_metrics(path: Path) -> Optional[dict]:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _extract_mae(metrics: dict, key: str) -> Optional[float]:
    val = metrics.get(key)
    if val is None:
        val = metrics.get("mae")
    return float(val) if val is not None else None


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate retrain metrics for publish gating.")
    parser.add_argument("--prev-margin", required=True, help="Path to previous margin metrics JSON.")
    parser.add_argument("--prev-total", required=True, help="Path to previous total metrics JSON.")
    parser.add_argument("--new-margin", required=True, help="Path to new margin metrics JSON.")
    parser.add_argument("--new-total", required=True, help="Path to new total metrics JSON.")
    parser.add_argument("--min-improvement", type=float, default=0.0, help="Required improvement in combined MAE.")
    args = parser.parse_args()

    prev_margin = _load_metrics(Path(args.prev_margin))
    prev_total = _load_metrics(Path(args.prev_total))
    new_margin = _load_metrics(Path(args.new_margin))
    new_total = _load_metrics(Path(args.new_total))

    if not prev_margin or not prev_total or not new_margin or not new_total:
        print("Missing metrics; defaulting to publish new models.")
        improved = True
    else:
        prev_m = _extract_mae(prev_margin, "mae_margin")
        prev_t = _extract_mae(prev_total, "mae_total")
        new_m = _extract_mae(new_margin, "mae_margin")
        new_t = _extract_mae(new_total, "mae_total")
        prev_sum = (prev_m or 0.0) + (prev_t or 0.0)
        new_sum = (new_m or 0.0) + (new_t or 0.0)
        improved = (prev_sum - new_sum) >= args.min_improvement
        print(f"Prev combined MAE: {prev_sum:.4f}, New combined MAE: {new_sum:.4f}")

    env_path = os.environ.get("GITHUB_ENV")
    if env_path:
        with open(env_path, "a", encoding="utf-8") as f:
            f.write(f"MODEL_IMPROVED={'true' if improved else 'false'}\n")
    print(f"MODEL_IMPROVED={'true' if improved else 'false'}")


if __name__ == "__main__":
    main()
