import json
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
CANONICAL_PATH = DATA_DIR / "ratings_current.json"
TEAM_COUNT = 30


def fail(msg: str) -> None:
    print(f"VALIDATION FAILED: {msg}")
    sys.exit(1)


def main():
    if not CANONICAL_PATH.exists():
        fail(f"Missing {CANONICAL_PATH}")

    try:
        payload = json.loads(CANONICAL_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        fail(f"Invalid JSON in {CANONICAL_PATH}: {e}")

    if not isinstance(payload, dict):
        fail("Payload is not an object")

    as_of = payload.get("as_of_utc") or payload.get("as_of")
    if not as_of:
        fail("Missing as_of_utc")
    try:
        datetime.fromisoformat(as_of.replace("Z", "+00:00"))
    except Exception as e:
        fail(f"as_of_utc not ISO8601: {e}")

    ratings = payload.get("ratings")
    if not isinstance(ratings, list):
        fail("ratings is missing or not a list")

    if len(ratings) != TEAM_COUNT:
        fail(f"Expected {TEAM_COUNT} ratings entries, found {len(ratings)}")

    teams = [r.get("team") for r in ratings]
    dupes = [t for t, c in Counter(teams).items() if t and c > 1]
    if dupes:
        fail(f"Duplicate team entries: {dupes}")

    print(f"Validation OK: {CANONICAL_PATH} has {len(ratings)} unique teams and as_of_utc={as_of}")


if __name__ == "__main__":
    main()
