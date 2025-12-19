"""
Compute and store closing market lines based on odds snapshots.
Usage:
  python market_line.py --date YYYY-MM-DD --vendor-rule draftkings|median [--minutes-before-tip 1]
"""

import argparse
import statistics
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Tuple

from config import DB_PATH
from db import get_conn, init_db
from balldontlie_client import fetch_games


def compute_cutoff_time(game_start_time_utc: str, minutes_before_tip: int = 1) -> Optional[datetime]:
    if not game_start_time_utc:
        return None
    try:
        dt = datetime.fromisoformat(game_start_time_utc.replace("Z", "+00:00"))
    except Exception:
        return None
    return dt - timedelta(minutes=minutes_before_tip)


def _latest_per_vendor(conn, game_id: int, cutoff_iso: str) -> Dict[Tuple[str, str], Dict]:
    """
    For a game, pull the latest snapshot per (vendor, market_type) up to cutoff.
    Returns a dict keyed by (vendor, market_type) -> row dict.
    """
    rows = conn.execute(
        """
        SELECT *
        FROM odds_snapshots
        WHERE game_id = ? AND updated_at <= ?
        ORDER BY vendor, market_type, datetime(updated_at) DESC
        """,
        (game_id, cutoff_iso),
    ).fetchall()

    latest = {}
    for r in rows:
        key = (r["vendor"], r["market_type"])
        if key not in latest:
            latest[key] = dict(r)
    return latest


def derive_closing_line(game_id: int, vendor_rule: str, cutoff_time_utc: Optional[datetime], conn) -> Optional[Dict]:
    """
    Derive closing line for a game based on vendor_rule and cutoff.
    vendor_rule can be a specific vendor name or "median".
    """
    if cutoff_time_utc is None:
        return None
    cutoff_iso = cutoff_time_utc.replace(microsecond=0, tzinfo=timezone.utc).isoformat()

    latest = _latest_per_vendor(conn, game_id, cutoff_iso)
    if not latest:
        return None

    if vendor_rule != "median":
        # pick latest snapshot for that vendor per market
        filtered = {k: v for k, v in latest.items() if k[0].lower() == vendor_rule.lower()}
        if not filtered:
            return None
        # pick spread, total, moneyline
        spread = filtered.get((vendor_rule, "spread"))
        total = filtered.get((vendor_rule, "total"))
        moneyline = filtered.get((vendor_rule, "moneyline"))
        return {
            "game_id": game_id,
            "cutoff_time_utc": cutoff_iso,
            "vendor_rule": vendor_rule,
            "closing_spread_home": _safe_float(spread, "home_line"),
            "closing_total": _safe_float(total, "total"),
            "closing_home_ml": _safe_int(moneyline, "home_ml"),
            "source_snapshot_id": _pick_source_id(spread, total, moneyline),
        }

    # vendor_rule == "median"
    # For each market_type, gather latest per vendor and compute median of home_line/total/home_ml as applicable
    def median_or_none(values: List[float]) -> Optional[float]:
        values = [v for v in values if v is not None]
        if not values:
            return None
        return float(statistics.median(values))

    spreads = [v for (vendor, mt), v in latest.items() if mt == "spread"]
    totals = [v for (vendor, mt), v in latest.items() if mt == "total"]
    mls = [v for (vendor, mt), v in latest.items() if mt == "moneyline"]

    return {
        "game_id": game_id,
        "cutoff_time_utc": cutoff_iso,
        "vendor_rule": "median",
        "closing_spread_home": median_or_none([_safe_float(s, "home_line") for s in spreads]),
        "closing_total": median_or_none([_safe_float(t, "total") for t in totals]),
        "closing_home_ml": _safe_int_median([_safe_int(m, "home_ml") for m in mls]),
        "source_snapshot_id": None,
    }


def _safe_float(row: Optional[Dict], key: str) -> Optional[float]:
    if row is None:
        return None
    try:
        val = row.get(key)
        return float(val) if val is not None else None
    except Exception:
        return None


def _safe_int(row: Optional[Dict], key: str) -> Optional[int]:
    if row is None:
        return None
    try:
        val = row.get(key)
        return int(val) if val is not None else None
    except Exception:
        return None


def _safe_int_median(values: List[Optional[int]]) -> Optional[int]:
    vals = [v for v in values if v is not None]
    if not vals:
        return None
    return int(statistics.median(vals))


def _pick_source_id(spread, total, moneyline) -> Optional[int]:
    # Prefer spread ID, else total, else moneyline
    for r in (spread, total, moneyline):
        if r and r.get("id") is not None:
            return r["id"]
    return None


def _load_games_for_date(date_str: str) -> List[Dict]:
    return fetch_games(date_str)


def upsert_market_line(conn, closing: Dict):
    conn.execute(
        """
        INSERT INTO market_lines
            (game_id, cutoff_time_utc, vendor_rule, closing_spread_home, closing_total, closing_home_ml, source_snapshot_id)
        VALUES
            (:game_id, :cutoff_time_utc, :vendor_rule, :closing_spread_home, :closing_total, :closing_home_ml, :source_snapshot_id)
        ON CONFLICT(game_id) DO UPDATE SET
            cutoff_time_utc=excluded.cutoff_time_utc,
            vendor_rule=excluded.vendor_rule,
            closing_spread_home=excluded.closing_spread_home,
            closing_total=excluded.closing_total,
            closing_home_ml=excluded.closing_home_ml,
            source_snapshot_id=excluded.source_snapshot_id
        """,
        closing,
    )


def process_date(date_str: str, vendor_rule: str, minutes_before_tip: int):
    init_db(DB_PATH)
    games = _load_games_for_date(date_str)
    conn = get_conn(DB_PATH)
    conn.row_factory = sqlite3.Row  # type: ignore  # for dict-like rows in _latest_per_vendor
    processed = 0
    with conn:
        for g in games:
            game_id = g.get("id")
            start_time = g.get("datetime") or g.get("start_time") or g.get("start_time_utc")
            cutoff = compute_cutoff_time(start_time, minutes_before_tip=minutes_before_tip)
            closing = derive_closing_line(int(game_id), vendor_rule, cutoff, conn)
            if closing:
                upsert_market_line(conn, closing)
                processed += 1
    conn.close()
    print(f"Processed {processed} games for {date_str} with vendor_rule={vendor_rule}")


def main():
    parser = argparse.ArgumentParser(description="Compute and store closing market lines for a date.")
    parser.add_argument("--date", required=True, help="Date in YYYY-MM-DD")
    parser.add_argument("--vendor-rule", required=True, help="Vendor name or 'median'")
    parser.add_argument("--minutes-before-tip", type=int, default=1, help="Cutoff minutes before tip (default 1)")
    args = parser.parse_args()

    process_date(args.date, args.vendor_rule, args.minutes_before_tip)


if __name__ == "__main__":
    import sqlite3  # placed here to keep top clean
    main()
