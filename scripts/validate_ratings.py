import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

CANONICAL = Path("data/ratings_current.json")
ASOF_REGEX = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")

def fail(msg: str):
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(1)

def main():
    if not CANONICAL.exists():
        fail(f"File missing: {CANONICAL}")
    if CANONICAL.stat().st_size == 0:
        fail(f"File is empty: {CANONICAL}")

    try:
        with CANONICAL.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        fail(f"JSON parse failed for {CANONICAL}: {e}")

    if not isinstance(data, dict):
        fail("Top-level JSON must be an object")

    as_of = data.get("as_of_utc")
    if not as_of:
        fail("Missing as_of_utc")

    if not isinstance(as_of, str) or not ASOF_REGEX.match(as_of):
        fail(f"as_of_utc must match YYYY-MM-DDTHH:MM:SSZ, got: {as_of}")

    try:
        as_of_dt = datetime.strptime(as_of, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except Exception as e:
        fail(f"as_of_utc not parseable: {e}")

    now = datetime.now(timezone.utc)
    if as_of_dt > now + timedelta(minutes=10):
        fail(f"as_of_utc is more than 10 minutes in the future: as_of_utc={as_of_dt.isoformat()} now={now.isoformat()}")

    # Optional: ensure ratings present
    ratings = data.get("ratings")
    if ratings is None or not isinstance(ratings, list):
        fail("ratings field missing or not a list")

    print("ratings_current.json validation passed")

if __name__ == "__main__":
    main()
