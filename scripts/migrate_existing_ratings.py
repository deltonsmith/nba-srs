import json
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Optional

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
HISTORY_DIR = DATA_DIR / "history"
CANONICAL = DATA_DIR / "ratings_current.json"


def infer_date_from_content(path: Path) -> Optional[str]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    date_str = None
    if isinstance(payload, dict):
        as_of = payload.get("as_of_utc") or payload.get("as_of")
        if as_of:
            try:
                dt = datetime.fromisoformat(as_of.replace("Z", "+00:00"))
                date_str = dt.date().isoformat()
            except Exception:
                pass
    return date_str


def infer_date_from_git(path: Path) -> Optional[str]:
    try:
        result = subprocess.run(
            ["git", "log", "-1", "--format=%cI", "--", str(path)],
            check=True,
            capture_output=True,
            text=True,
            cwd=BASE_DIR,
        )
        out = result.stdout.strip()
        if out:
            dt = datetime.fromisoformat(out.replace("Z", "+00:00"))
            return dt.date().isoformat()
    except Exception:
        return None
    return None


def main():
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    moved = 0

    for path in BASE_DIR.glob("ratings_*.json"):
        date_str = infer_date_from_content(path)
        if not date_str:
            date_str = infer_date_from_git(path)
        if not date_str:
            print(f"Skipping {path} (no date inferred)")
            continue

        dest = HISTORY_DIR / f"{date_str}.json"
        print(f"Moving {path} -> {dest}")
        dest.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
        path.unlink()
        moved += 1

    if not CANONICAL.exists():
        # Try to pick latest history as canonical
        history_files = sorted(HISTORY_DIR.glob("*.json"))
        if history_files:
            latest = history_files[-1]
            print(f"Setting canonical from {latest}")
            CANONICAL.write_text(latest.read_text(encoding="utf-8"), encoding="utf-8")

    print(f"Done. Moved {moved} files.")


if __name__ == "__main__":
    main()
