import argparse
from datetime import datetime, timezone

from src.time_window import compute_time_window_payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Print last-N-days time window (America/Chicago cutoffs, UTC timestamps).")
    parser.add_argument("--days", type=int, default=7, help="Number of days to include (default: 7)")
    parser.add_argument("--now-utc", help="Override current time with ISO UTC timestamp")
    args = parser.parse_args()

    if args.now_utc:
        now_utc = datetime.fromisoformat(args.now_utc.replace("Z", "+00:00")).astimezone(timezone.utc)
    else:
        now_utc = datetime.now(timezone.utc)

    payload = compute_time_window_payload(now_utc=now_utc, n=args.days)
    print(f"start_ts_utc: {payload['start_ts_utc']}")
    print(f"end_ts_utc: {payload['end_ts_utc']}")
    print(f"start_date_ct: {payload['start_date_ct']}")
    print(f"end_date_ct: {payload['end_date_ct']}")


if __name__ == "__main__":
    main()
