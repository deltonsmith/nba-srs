"""
Generate predictions for a given date (not integrated into frontend).
Outputs JSON: new_model/output/predictions_YYYY-MM-DD.json
"""

import argparse
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import joblib
import pandas as pd
import sqlite3

from config import DB_PATH
from db import init_db


FEATURE_COLS = [
    "net_rating",
    "pace",
    "efg",
    "tov",
    "orb",
    "ftr",
    "rest_days",
    "travel_miles",
    "back_to_back",
    "inj_out",
    "inj_day_to_day",
    "inj_total",
]

GAME_FEATURE_COLS = [
    "closing_spread_home",
    "closing_total",
    "closing_home_ml",
]


def load_models(base_dir: Path):
    models_dir = base_dir / "models"
    try:
        m_margin = joblib.load(models_dir / "margin_model.joblib")
        m_total = joblib.load(models_dir / "total_model.joblib")
        return m_margin, m_total
    except FileNotFoundError:
        print("Model artifacts missing; continuing with market-only output (no model lines/edges).")
        return None, None


def load_games_and_features(conn, target_date: str):
    games = pd.read_sql(
        """
        SELECT g.game_id, g.date, g.home_team_id, g.away_team_id, g.status,
               g.start_time_utc,
               ml.closing_spread_home, ml.closing_total, ml.closing_home_ml
        FROM games g
        LEFT JOIN market_lines ml ON ml.game_id = g.game_id
        WHERE g.date = ?
        """,
        conn,
        params=[target_date],
    )
    feats = pd.read_sql("SELECT * FROM team_game_features", conn)

    # If no features yet, fall back to zeros so we still emit payload.
    required_cols = {"game_id", "team_id"}
    if feats.empty or not required_cols.issubset(set(feats.columns)):
        df = games.copy()
        feat_cols = []
        for col in FEATURE_COLS:
            h = f"{col}_home"
            a = f"{col}_away"
            df[h] = 0
            df[a] = 0
            feat_cols.extend([h, a])
        return df, feat_cols

    # Home features
    home_feats = feats.merge(
        games[["game_id", "home_team_id"]],
        left_on=["game_id", "team_id"],
        right_on=["game_id", "home_team_id"],
        how="inner",
    )
    home_feats = home_feats.drop(columns=["team_id", "home_team_id"])
    home_feats = home_feats.add_suffix("_home")
    home_feats = home_feats.rename(columns={"game_id_home": "game_id"})

    # Away features
    away_feats = feats.merge(
        games[["game_id", "away_team_id"]],
        left_on=["game_id", "team_id"],
        right_on=["game_id", "away_team_id"],
        how="inner",
    )
    away_feats = away_feats.drop(columns=["team_id", "away_team_id"])
    away_feats = away_feats.add_suffix("_away")
    away_feats = away_feats.rename(columns={"game_id_away": "game_id"})

    df = games.merge(home_feats, on="game_id", how="left").merge(away_feats, on="game_id", how="left")
    feat_cols = []
    for col in FEATURE_COLS:
        feat_cols.append(f"{col}_home")
        feat_cols.append(f"{col}_away")
    feat_cols.extend(GAME_FEATURE_COLS)
    df[feat_cols] = df[feat_cols].fillna(0)
    return df, feat_cols


def compute_baseline_lines(conn) -> (float, float):
    """Compute simple baseline spread/total from historical scores."""
    df = pd.read_sql(
        """
        SELECT home_score, away_score
        FROM games
        WHERE home_score IS NOT NULL AND away_score IS NOT NULL
        """,
        conn,
    )
    # Ignore rows with zeros/placeholders
    df = df[(df["home_score"] > 0) & (df["away_score"] > 0)]
    if df.empty:
        return 0.0, 220.0  # fallback defaults
    margins = df["home_score"] - df["away_score"]
    totals = df["home_score"] + df["away_score"]
    return float(margins.mean()), float(totals.mean())


def _parse_iso(dt_str: Optional[str]) -> Optional[datetime]:
    if not dt_str:
        return None
    if isinstance(dt_str, datetime):
        dt = dt_str
    else:
        s = str(dt_str).strip()
        dt = None
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        except ValueError:
            for fmt in (
                "%Y-%m-%dT%H:%M:%S%z",
                "%Y-%m-%dT%H:%M:%S.%f%z",
                "%Y-%m-%d %H:%M:%S%z",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d %H:%M:%S",
            ):
                try:
                    dt = datetime.strptime(s, fmt)
                    break
                except ValueError:
                    continue
        if dt is None:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _extract_team_abbr(team) -> Optional[str]:
    if not team:
        return None
    if isinstance(team, str):
        return team
    return team.get("abbreviation") or team.get("abbr") or team.get("code") or team.get("short_name")


def _extract_odds_team_abbr(odds: Dict, side: str) -> Optional[str]:
    keys = [
        f"{side}_team_abbr",
        f"{side}_team_abbreviation",
        f"{side}_abbreviation",
        f"{side}_abbr",
    ]
    for key in keys:
        val = odds.get(key)
        if val:
            return val
    team = odds.get(f"{side}_team") or odds.get(f"{side}Team")
    abbr = _extract_team_abbr(team)
    if abbr:
        return abbr
    if side == "away":
        team = odds.get("visitor_team") or odds.get("visitorTeam")
        return _extract_team_abbr(team)
    return None


def _extract_odds_start_time(odds: Dict) -> Optional[str]:
    for key in ("starts_at", "start_time_utc", "start_time", "game_start_time", "datetime"):
        val = odds.get(key)
        if val:
            return val
    game = odds.get("game") or {}
    for key in ("start_time_utc", "start_time", "datetime", "starts_at"):
        val = game.get(key)
        if val:
            return val
    return None


def _normalize_spread_home(home_line, away_line) -> Optional[float]:
    if home_line is None and away_line is None:
        return None
    if home_line is None:
        try:
            return -float(away_line)
        except (TypeError, ValueError):
            return None
    try:
        home_val = float(home_line)
    except (TypeError, ValueError):
        return None
    if away_line is None:
        return home_val
    try:
        away_val = float(away_line)
    except (TypeError, ValueError):
        return home_val
    if abs(home_val + away_val) <= 0.1:
        return home_val
    if home_val == 0 and away_val != 0:
        return -away_val
    if home_val * away_val > 0:
        return -away_val
    return home_val


def _build_team_index(df: pd.DataFrame) -> Dict[Tuple[str, str], List[Tuple[int, Optional[datetime]]]]:
    team_index: Dict[Tuple[str, str], List[Tuple[int, Optional[datetime]]]] = {}
    for _, row in df.iterrows():
        away = row.get("away_team_id")
        home = row.get("home_team_id")
        if not away or not home:
            continue
        start_dt = _parse_iso(row.get("start_time_utc"))
        key = (str(away).upper(), str(home).upper())
        team_index.setdefault(key, []).append((int(row["game_id"]), start_dt))
    return team_index


def _match_game_id_by_fallback(odds: Dict, team_index: Dict[Tuple[str, str], List[Tuple[int, Optional[datetime]]]]) -> Optional[int]:
    away = _extract_odds_team_abbr(odds, "away")
    home = _extract_odds_team_abbr(odds, "home")
    if not away or not home:
        return None
    key = (str(away).upper(), str(home).upper())
    candidates = team_index.get(key, [])
    if not candidates:
        return None
    start_dt = _parse_iso(_extract_odds_start_time(odds))
    if start_dt is None:
        return candidates[0][0] if len(candidates) == 1 else None
    closest = None
    for game_id, game_dt in candidates:
        if game_dt is None:
            continue
        delta = abs(game_dt - start_dt)
        if delta <= timedelta(minutes=30):
            if closest is None or delta < closest[1]:
                closest = (game_id, delta)
    return closest[0] if closest else None


def _build_market_map(odds: List[Dict], df: pd.DataFrame, vendor_rule: str) -> Dict[int, Dict[str, Optional[float]]]:
    market_map: Dict[int, Dict[str, Optional[float]]] = {}
    if not odds:
        return market_map
    team_index = _build_team_index(df)
    vendor = str(vendor_rule).lower()

    def _is_newer(current: Optional[datetime], updated_at: Optional[datetime]) -> bool:
        if current is None:
            return True
        if updated_at is None:
            return False
        return updated_at > current

    for o in odds:
        if str(o.get("vendor") or "").lower() != vendor:
            continue
        game_id = o.get("game_id")
        if game_id is None:
            game_id = _match_game_id_by_fallback(o, team_index)
        if game_id is None:
            continue

        market_type = o.get("market_type") or o.get("type") or o.get("market_type_slug") or ""
        market_type = str(market_type).lower()
        updated_at = _parse_iso(o.get("updated_at") or o.get("last_update") or o.get("updatedAt"))

        entry = market_map.setdefault(int(game_id), {"spreadHome": None, "total": None, "_spread_updated": None, "_total_updated": None})

        if market_type == "spread" or (not market_type and (o.get("spread_home_value") is not None or o.get("spread_away_value") is not None)):
            spread_home = _normalize_spread_home(o.get("spread_home_value") or o.get("home_line"), o.get("spread_away_value") or o.get("away_line"))
            if spread_home is None:
                pass
            elif _is_newer(entry["_spread_updated"], updated_at):
                entry["spreadHome"] = spread_home
                entry["_spread_updated"] = updated_at

        if market_type == "total" or (not market_type and o.get("total_value") is not None):
            total_val = o.get("total_value") or o.get("total")
            try:
                total_val = float(total_val)
            except (TypeError, ValueError):
                total_val = None
            if total_val is None:
                pass
            elif _is_newer(entry["_total_updated"], updated_at):
                entry["total"] = total_val
                entry["_total_updated"] = updated_at

    for entry in market_map.values():
        entry.pop("_spread_updated", None)
        entry.pop("_total_updated", None)

    return market_map


def build_predictions(
    df: pd.DataFrame,
    feat_cols: List[str],
    m_margin,
    m_total,
    vendor_rule: str,
    target_date: str,
    baseline: Optional[tuple],
    market_map: Dict[int, Dict[str, Optional[float]]],
) -> Dict:
    if m_margin is None or m_total is None:
        base_margin, base_total = baseline if baseline else (0.0, 220.0)
        preds_margin = [base_margin] * len(df)
        preds_total = [base_total] * len(df)
    else:
        preds_margin = m_margin.predict(df[feat_cols])
        preds_total = m_total.predict(df[feat_cols])

    games_out: List[Dict] = []
    for (_, row), pm, pt in zip(df.iterrows(), preds_margin, preds_total):
        market_entry = market_map.get(int(row["game_id"]), {})
        market_spread_val = market_entry.get("spreadHome")
        market_total_val = market_entry.get("total")

        model_spread = float(pm) if pd.notna(pm) else None
        model_total = float(pt) if pd.notna(pt) else None

        edge_spread = None
        edge_total = None
        if model_spread is not None and market_spread_val is not None:
            edge_spread = model_spread - market_spread_val
        if model_total is not None and market_total_val is not None:
            edge_total = model_total - market_total_val

        games_out.append({
            "gameId": int(row["game_id"]),
            "startTimeUtc": row.get("start_time_utc"),
            "away": {"id": row.get("away_team_id"), "name": None, "abbr": row.get("away_team_id")},
            "home": {"id": row.get("home_team_id"), "name": None, "abbr": row.get("home_team_id")},
            "market": {
                "spreadHome": market_spread_val,
                "total": market_total_val,
            },
            "realLine": {
                "spreadHome": model_spread,
                "total": model_total,
            },
            "edge": {
                "spread": edge_spread,
                "total": edge_total,
            },
        })

    return {
        "asOfUtc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "date": target_date,
        "timezone": "America/Chicago",
        "vendorRule": vendor_rule,
        "games": games_out,
    }


def main():
    parser = argparse.ArgumentParser(description="Generate model predictions for a given date.")
    parser.add_argument("--date", required=True, help="Date YYYY-MM-DD")
    parser.add_argument("--vendor-rule", default="median", help="Vendor rule context")
    args = parser.parse_args()

    base_dir = Path(__file__).resolve().parent.parent
    output_dir = base_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    init_db(DB_PATH)
    m_margin, m_total = load_models(base_dir)

    conn = sqlite3.connect(DB_PATH)
    try:
        df, feat_cols = load_games_and_features(conn, args.date)
        baseline = compute_baseline_lines(conn)
    finally:
        conn.close()

    if df.empty:
        print(f"No games found for {args.date}; writing empty payload.")
        payload = {
            "asOfUtc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "date": args.date,
            "timezone": "America/Chicago",
            "vendorRule": args.vendor_rule,
            "games": [],
        }
        out_path = output_dir / f"predictions_{args.date}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        print(f"Wrote predictions to {out_path}")
        return

    odds_failed = False
    odds: Optional[List[Dict]] = None
    try:
        from balldontlie_client import fetch_odds_by_date, fetch_odds_by_game_ids
        try:
            odds = fetch_odds_by_date(args.date)
            if not odds:
                game_ids = [int(gid) for gid in df["game_id"].tolist()] if not df.empty else []
                if game_ids:
                    odds = fetch_odds_by_game_ids(game_ids)
        except Exception as exc:
            print(f"Odds fetch failed; continuing with null market lines. Error: {exc}")
            odds_failed = True
    except Exception as exc:
        print(f"Odds fetch skipped; continuing with null market lines. Error: {exc}")
        odds_failed = True

    market_map = _build_market_map(odds or [], df, args.vendor_rule) if not odds_failed else {}

    payload = build_predictions(
        df,
        feat_cols,
        m_margin,
        m_total,
        args.vendor_rule,
        args.date,
        baseline,
        market_map,
    )

    if payload["games"]:
        missing_market = sum(
            1
            for g in payload["games"]
            if g["market"]["spreadHome"] is None or g["market"]["total"] is None
        )
        if missing_market / len(payload["games"]) > 0.5:
            print(
                f"Warning: {missing_market}/{len(payload['games'])} games missing market lines "
                f"for vendor={args.vendor_rule}.",
            )
    out_path = output_dir / f"predictions_{args.date}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print(f"Wrote predictions to {out_path}")


if __name__ == "__main__":
    main()
