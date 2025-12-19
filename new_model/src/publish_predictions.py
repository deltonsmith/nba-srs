"""
Copy a dated predictions JSON to a stable public path for the site to fetch.
Usage:
  python publish_predictions.py --date YYYY-MM-DD
"""

import argparse
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="Publish predictions JSON to public/new_model/predictions_today.json")
    parser.add_argument("--date", required=True, help="Date in YYYY-MM-DD")
    args = parser.parse_args()

    base_dir = Path(__file__).resolve().parent.parent
    src = base_dir / "output" / f"predictions_{args.date}.json"
    if not src.exists():
        raise SystemExit(f"Missing predictions file: {src}")

    dest_dir = Path("public") / "new_model"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / "predictions_today.json"

    data = json.loads(src.read_text(encoding="utf-8"))
    dest.write_text(json.dumps(data, indent=2), encoding="utf-8")
    print(f"Wrote {dest}")


if __name__ == "__main__":
    main()
