import json
import sys
from collections import Counter
from datetime import datetime, timezone, timedelta
from pathlib import Path
import math
import re

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

    if CANONICAL_PATH.stat().st_size == 0:
        fail(f"{CANONICAL_PATH} is empty")

    try:
        payload = json.loads(CANONICAL_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        fail(f"Invalid JSON in {CANONICAL_PATH}: {e}")

    if not isinstance(payload, dict):
        fail("Payload is not an object")

    as_of = payload.get("as_of_utc")
    if not as_of:
        fail("Missing as_of_utc")
    pattern = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
    if not pattern.match(as_of):
        fail(f"as_of_utc not matching pattern YYYY-MM-DDTHH:MM:SSZ: {as_of}")
    try:
        as_dt = datetime.strptime(as_of, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except Exception as e:
        fail(f"as_of_utc not ISO8601 Z (YYYY-MM-DDTHH:MM:SSZ): {e}")

    now = datetime.now(timezone.utc)
    if as_dt - now > timedelta(minutes=10):
        fail(f"as_of_utc is in the future beyond allowed skew: as_of_utc={as_of}, now={now.isoformat()}")

    ratings = payload.get("ratings")
    if not isinstance(ratings, list):
        fail("ratings is missing or not a list")

    if len(ratings) != TEAM_COUNT:
        fail(f"Expected {TEAM_COUNT} ratings entries, found {len(ratings)}")

    teams = []
    for r in ratings:
        if not isinstance(r, dict):
            fail("rating entry is not an object")
        team_id = r.get("team")
        if not team_id:
            fail("rating entry missing team")
        teams.append(team_id)
        rating_val = r.get("rating")
        if rating_val is None:
            fail(f"team {team_id} missing rating")
        if not isinstance(rating_val, (int, float)) or not math.isfinite(rating_val):
            fail(f"team {team_id} has non-finite rating: {rating_val}")
        # Frontend expects rank and rating at minimum; yest_rank/last_week_rank optional
        if "rank" not in r:
            fail(f"team {team_id} missing rank")

    dupes = [t for t, c in Counter(teams).items() if t and c > 1]
    if dupes:
        fail(f"Duplicate team entries: {dupes}")

    print(f"Validation OK: {CANONICAL_PATH} has {len(ratings)} unique teams and as_of_utc={as_of}")


if __name__ == "__main__":
    main()
