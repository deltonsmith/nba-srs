"""
Analyze new model prediction coverage and join to outcomes for a date range.
"""

import argparse
import csv
import json
import statistics
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import sqlite3


BASE_DIR = Path(__file__).resolve().parent.parent
PRED_DIR = BASE_DIR / "data" / "new_model"
PUBLIC_PRED = BASE_DIR / "public" / "new_model" / "predictions_today.json"
DB_PATH = BASE_DIR / "new_model" / "data" / "new_model.sqlite"
DEFAULT_LEAGUE_TOTAL = 220.0


def _parse_date(s: str) -> Optional[datetime.date]:
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None


def _daterange(start_date, end_date):
    for n in range((end_date - start_date).days + 1):
        yield start_date + timedelta(days=n)


def _load_predictions_for_date(date_str: str) -> Optional[dict]:
    path = PRED_DIR / f"predictions_{date_str}.json"
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    if PUBLIC_PRED.exists():
        payload = json.loads(PUBLIC_PRED.read_text(encoding="utf-8"))
        if payload.get("date") == date_str:
            return payload
    return None


def _edge_mag(edge_spread: Optional[float], edge_total: Optional[float]) -> Optional[float]:
    vals = []
    if edge_spread is not None:
        vals.append(abs(edge_spread))
    if edge_total is not None:
        vals.append(abs(edge_total))
    return max(vals) if vals else None


def _sign(val: Optional[float]) -> Optional[int]:
    if val is None:
        return None
    if val > 0:
        return 1
    if val < 0:
        return -1
    return 0


def _league_baseline_total() -> float:
    """Baseline total for model_total_pick when no market total is available."""
    if not DB_PATH.exists():
        return DEFAULT_LEAGUE_TOTAL
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT home_score, away_score
            FROM games
            WHERE home_score IS NOT NULL AND away_score IS NOT NULL
            """,
        ).fetchall()
    finally:
        conn.close()
    totals = []
    for r in rows:
        try:
            totals.append(float(r["home_score"]) + float(r["away_score"]))
        except Exception:
            continue
    if not totals:
        return DEFAULT_LEAGUE_TOTAL
    return float(sum(totals) / len(totals))


def _load_outcomes(start_date: str, end_date: str) -> List[dict]:
    if not DB_PATH.exists():
        return []
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT game_id, date, home_team_id, away_team_id, home_score, away_score
            FROM games
            WHERE date BETWEEN ? AND ?
            """,
            (start_date, end_date),
        ).fetchall()
    finally:
        conn.close()

    outcomes = []
    for r in rows:
        home_score = r["home_score"]
        away_score = r["away_score"]
        actual_margin = None
        actual_total = None
        if home_score is not None and away_score is not None:
            actual_margin = float(home_score) - float(away_score)
            actual_total = float(home_score) + float(away_score)
        outcomes.append(
            {
                "game_id": int(r["game_id"]),
                "date": r["date"],
                "home_team_id": r["home_team_id"],
                "away_team_id": r["away_team_id"],
                "home_score": home_score,
                "away_score": away_score,
                "actual_margin": actual_margin,
                "actual_total": actual_total,
            }
        )
    return outcomes


def _load_team_features(start_date: str, end_date: str) -> Dict[Tuple[int, str], dict]:
    if not DB_PATH.exists():
        return {}
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT g.game_id, g.date, t.team_id,
                   t.pace, t.efg, t.tov, t.orb, t.ftr,
                   t.inj_out, t.inj_day_to_day, t.inj_total,
                   t.back_to_back, t.rest_days
            FROM team_game_features t
            INNER JOIN games g ON g.game_id = t.game_id
            WHERE g.date BETWEEN ? AND ?
            """,
            (start_date, end_date),
        ).fetchall()
    finally:
        conn.close()

    features = {}
    for r in rows:
        try:
            game_id = int(r["game_id"])
        except Exception:
            continue
        team_id = str(r["team_id"])
        features[(game_id, team_id)] = {
            "pace": r["pace"],
            "efg": r["efg"],
            "tov": r["tov"],
            "orb": r["orb"],
            "ftr": r["ftr"],
            "inj_out": r["inj_out"],
            "inj_day_to_day": r["inj_day_to_day"],
            "inj_total": r["inj_total"],
            "back_to_back": r["back_to_back"],
            "rest_days": r["rest_days"],
        }
    return features


def analyze_range(start_date, end_date, min_edge: float, out_dir: str) -> Dict[str, object]:
    predictions = []
    missing_pred_dates = []
    for d in _daterange(start_date, end_date):
        date_str = d.isoformat()
        payload = _load_predictions_for_date(date_str)
        if payload is None:
            missing_pred_dates.append(date_str)
            continue
        for g in payload.get("games", []):
            game_id = g.get("gameId")
            try:
                game_id = int(game_id) if game_id is not None else None
            except Exception:
                game_id = None
            home = (g.get("home") or {}).get("abbr") or (g.get("home") or {}).get("id")
            away = (g.get("away") or {}).get("abbr") or (g.get("away") or {}).get("id")
            market = g.get("market") or {}
            real_line = g.get("realLine") or {}
            edge = g.get("edge") or {}

            edge_spread = edge.get("spread")
            edge_total = edge.get("total")
            edge_mag = _edge_mag(edge_spread, edge_total)
            if edge_mag is not None and edge_mag < min_edge:
                continue

            market_spread = market.get("spreadHome")
            market_total = market.get("total")
            model_spread = real_line.get("spreadHome")
            model_total = real_line.get("total")
            spread_edge = None
            total_edge = None
            if model_spread is not None and market_spread is not None:
                spread_edge = model_spread - market_spread
            if model_total is not None and market_total is not None:
                total_edge = model_total - market_total

            predictions.append(
                {
                    "game_id": game_id,
                    "date": date_str,
                    "home_team_id": home,
                    "away_team_id": away,
                    "market_spread_home": market_spread,
                    "market_total": market_total,
                    "model_spread_home": model_spread,
                    "model_total": model_total,
                    "spread_edge": spread_edge,
                    "total_edge": total_edge,
                    "edge_spread": edge_spread,
                    "edge_total": edge_total,
                }
            )

    outcomes = _load_outcomes(start_date.isoformat(), end_date.isoformat())
    team_features = _load_team_features(start_date.isoformat(), end_date.isoformat())
    outcomes_by_id: Dict[int, dict] = {o["game_id"]: o for o in outcomes}
    outcomes_by_key: Dict[Tuple[str, str, str], dict] = {
        (o["date"], str(o["home_team_id"]), str(o["away_team_id"])): o for o in outcomes
    }

    joined = []
    unjoined_predictions = []
    matched_outcomes = set()

    league_baseline_total = _league_baseline_total()
    total_residuals = []
    ppp_residuals = []

    for p in predictions:
        reason = None
        outcome = None
        if p["game_id"] is not None and p["game_id"] in outcomes_by_id:
            outcome = outcomes_by_id[p["game_id"]]
        else:
            key = (p["date"], str(p["home_team_id"]), str(p["away_team_id"]))
            outcome = outcomes_by_key.get(key)
            if outcome is None:
                reason = "no_outcome_match"

        if outcome is None:
            unjoined_predictions.append({**p, "reason": reason or "missing_outcome"})
            continue

        matched_outcomes.add(outcome["game_id"])
        actual_margin = outcome.get("actual_margin")
        actual_total = outcome.get("actual_total")
        home_team_id = outcome.get("home_team_id")
        away_team_id = outcome.get("away_team_id")
        game_id = outcome.get("game_id")

        home_feats = team_features.get((game_id, str(home_team_id)), {})
        away_feats = team_features.get((game_id, str(away_team_id)), {})
        pace_home = home_feats.get("pace")
        pace_away = away_feats.get("pace")
        efg_home = home_feats.get("efg")
        efg_away = away_feats.get("efg")
        tov_home = home_feats.get("tov")
        tov_away = away_feats.get("tov")
        orb_home = home_feats.get("orb")
        orb_away = away_feats.get("orb")
        ftr_home = home_feats.get("ftr")
        ftr_away = away_feats.get("ftr")
        inj_out_home = home_feats.get("inj_out")
        inj_out_away = away_feats.get("inj_out")
        inj_total_home = home_feats.get("inj_total")
        inj_total_away = away_feats.get("inj_total")
        avg_pace = None
        if pace_home is not None and pace_away is not None:
            try:
                avg_pace = (float(pace_home) + float(pace_away)) / 2.0
            except Exception:
                avg_pace = None
        home_b2b = home_feats.get("back_to_back")
        away_b2b = away_feats.get("back_to_back")

        # Repo convention: model spread is "spreadHome" (home line).
        # Convert to implied margin (home - away) by negating the spread.
        model_spread = p.get("model_spread_home")
        market_spread = p.get("market_spread_home")
        predicted_margin = None
        if model_spread is not None:
            predicted_margin = -float(model_spread)
        market_margin = None
        if market_spread is not None:
            market_margin = -float(market_spread)

        if market_margin is not None:
            model_side_pick = _sign(predicted_margin - market_margin) if predicted_margin is not None else None
        else:
            model_side_pick = _sign(predicted_margin)
        actual_margin_sign = _sign(actual_margin)

        wrong_spread = None
        if actual_margin is not None:
            if actual_margin == 0:
                wrong_spread = None
            elif market_margin is not None and actual_margin == market_margin:
                wrong_spread = None
            elif model_side_pick is None or actual_margin_sign in (None, 0):
                wrong_spread = None
            else:
                wrong_spread = model_side_pick != actual_margin_sign

        model_total = p.get("model_total")
        market_total = p.get("market_total")
        if market_total is not None:
            model_total_pick = _sign(model_total - market_total) if model_total is not None else None
        else:
            model_total_pick = _sign(model_total - league_baseline_total) if model_total is not None else None

        wrong_total = None
        if actual_total is not None and market_total is not None:
            if actual_total == market_total:
                wrong_total = None
            else:
                actual_total_sign = _sign(actual_total - market_total)
                if model_total_pick is None or actual_total_sign in (None, 0):
                    wrong_total = None
                else:
                    wrong_total = model_total_pick != actual_total_sign

        spread_residual = None
        if actual_margin is not None and predicted_margin is not None:
            spread_residual = actual_margin - predicted_margin

        total_residual = None
        if actual_total is not None and model_total is not None:
            total_residual = actual_total - model_total
            total_residuals.append(total_residual)

        # Efficiency proxy: use points per possession if available; otherwise use pace proxy.
        ppp_residual = None
        if avg_pace is not None and actual_total is not None and model_total is not None and avg_pace != 0:
            ppp_actual = actual_total / avg_pace
            ppp_pred = model_total / avg_pace
            ppp_residual = ppp_actual - ppp_pred
            ppp_residuals.append(ppp_residual)

        joined.append(
            {
                **p,
                **outcome,
                "pace_home": pace_home,
                "pace_away": pace_away,
                "efg_home": efg_home,
                "efg_away": efg_away,
                "tov_home": tov_home,
                "tov_away": tov_away,
                "orb_home": orb_home,
                "orb_away": orb_away,
                "ftr_home": ftr_home,
                "ftr_away": ftr_away,
                "inj_out_home": inj_out_home,
                "inj_out_away": inj_out_away,
                "inj_total_home": inj_total_home,
                "inj_total_away": inj_total_away,
                "back_to_back_home": home_b2b,
                "back_to_back_away": away_b2b,
                "predicted_margin": predicted_margin,
                "model_side_pick": model_side_pick,
                "model_total_pick": model_total_pick,
                "spread_residual": spread_residual,
                "total_residual": total_residual,
                "ppp_residual": ppp_residual,
                "wrong_spread": wrong_spread,
                "wrong_total": wrong_total,
            }
        )

    unjoined_outcomes = [o for o in outcomes if o["game_id"] not in matched_outcomes]

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / "new_model_miss_coverage.json"
    report = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "games_in_range": len(outcomes),
        "games_with_predictions": len(predictions),
        "games_with_outcomes": len(outcomes),
        "games_joined": len(joined),
        "missing_prediction_dates": missing_pred_dates,
    }
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    pred_path = out_dir / "unjoined_predictions.csv"
    with pred_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "game_id",
                "date",
                "home_team_id",
                "away_team_id",
                "market_spread_home",
                "market_total",
                "model_spread_home",
                "model_total",
                "spread_edge",
                "total_edge",
                "edge_spread",
                "edge_total",
                "reason",
            ],
        )
        w.writeheader()
        for row in unjoined_predictions:
            w.writerow(row)

    out_path = out_dir / "unjoined_outcomes.csv"
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "game_id",
                "date",
                "home_team_id",
                "away_team_id",
                "actual_margin",
                "actual_total",
            ],
        )
        w.writeheader()
        for row in unjoined_outcomes:
            w.writerow(row)

    std_total = statistics.stdev(total_residuals) if len(total_residuals) > 1 else 0.0
    std_ppp = statistics.stdev(ppp_residuals) if len(ppp_residuals) > 1 else 0.0
    total_outlier_threshold = max(12.0, 1.5 * std_total)
    ppp_outlier_threshold = 1.5 * std_ppp if std_ppp > 0 else None
    if not ppp_residuals:
        print("Efficiency outlier flag skipped: missing pace/possession data for the date range.")

    rows = []
    for row in joined:
        actual_margin = row.get("actual_margin")
        actual_total = row.get("actual_total")
        model_total = row.get("model_total")
        model_spread = row.get("model_spread_home")
        market_spread = row.get("market_spread_home")
        market_total = row.get("market_total")
        predicted_margin = row.get("predicted_margin")

        blowout_flag = actual_margin is not None and abs(actual_margin) >= 15
        clutch_flag = actual_margin is not None and abs(actual_margin) <= 3

        pace_outlier_flag = False
        if actual_total is not None and model_total is not None:
            pace_outlier_flag = abs(actual_total - model_total) >= total_outlier_threshold

        efficiency_outlier_flag = None
        if row.get("ppp_residual") is not None and ppp_outlier_threshold is not None:
            efficiency_outlier_flag = abs(row.get("ppp_residual")) >= ppp_outlier_threshold

        home_adv_miss_flag = False
        if actual_margin is not None and predicted_margin is not None and model_spread is not None:
            model_side_pick = _sign(predicted_margin - (-market_spread)) if market_spread is not None else _sign(predicted_margin)
            if actual_margin > 0 and model_side_pick == -1 and abs(float(model_spread)) <= 3:
                home_adv_miss_flag = True

        back_to_back_flag = None
        if row.get("back_to_back_home") is not None or row.get("back_to_back_away") is not None:
            try:
                back_to_back_flag = bool(int(row.get("back_to_back_home") or 0) or int(row.get("back_to_back_away") or 0))
            except Exception:
                back_to_back_flag = None

        # Reason codes: pick up to 3 largest contributors by simple magnitude ranking.
        # Spread reasons consider margin size/closeness, home-advantage misses, and back-to-backs.
        # Total reasons consider total residual (pace outlier) and PPP residual (efficiency outlier).
        reason_candidates = []
        spread_reason_candidates = []
        total_reason_candidates = []
        if row.get("wrong_spread"):
            if blowout_flag:
                spread_reason_candidates.append(("blowout", abs(actual_margin or 0)))
            if clutch_flag:
                spread_reason_candidates.append(("clutch", 3 - abs(actual_margin or 0)))
            if home_adv_miss_flag:
                spread_reason_candidates.append(("home_adv_miss", 3 - abs(float(model_spread) if model_spread is not None else 0)))
            if back_to_back_flag:
                spread_reason_candidates.append(("back_to_back", 1))
            reason_candidates.extend(spread_reason_candidates)
        if row.get("wrong_total"):
            if pace_outlier_flag:
                total_reason_candidates.append(("pace_outlier", abs(row.get("total_residual") or 0)))
            if efficiency_outlier_flag:
                total_reason_candidates.append(("efficiency_outlier", abs(row.get("ppp_residual") or 0)))
            reason_candidates.extend(total_reason_candidates)

        reason_candidates.sort(key=lambda x: x[1], reverse=True)
        reason_codes = [r for r, _ in reason_candidates[:3]]
        spread_reason_candidates.sort(key=lambda x: x[1], reverse=True)
        total_reason_candidates.sort(key=lambda x: x[1], reverse=True)
        spread_reason_codes = [r for r, _ in spread_reason_candidates[:3]]
        total_reason_codes = [r for r, _ in total_reason_candidates[:3]]

        rows.append(
            {
                "date": row.get("date"),
                "game_id": row.get("game_id"),
                "away_team": row.get("away_team_id"),
                "home_team": row.get("home_team_id"),
                "market_spread": row.get("market_spread_home"),
                "model_spread": row.get("model_spread_home"),
                "spread_edge": row.get("spread_edge"),
                "market_total": row.get("market_total"),
                "model_total": row.get("model_total"),
                "total_edge": row.get("total_edge"),
                "away_score": row.get("away_score"),
                "home_score": row.get("home_score"),
                "actual_margin": row.get("actual_margin"),
                "actual_total": row.get("actual_total"),
                "predicted_margin": row.get("predicted_margin"),
                "predicted_total": row.get("model_total"),
                "spread_residual": row.get("spread_residual"),
                "total_residual": row.get("total_residual"),
                "wrong_spread": row.get("wrong_spread"),
                "wrong_total": row.get("wrong_total"),
                "blowout_flag": blowout_flag,
                "clutch_flag": clutch_flag,
                "pace_outlier_flag": pace_outlier_flag,
                "efficiency_outlier_flag": efficiency_outlier_flag,
                "home_adv_miss_flag": home_adv_miss_flag,
                "back_to_back_flag": back_to_back_flag,
                "reason_codes": "|".join(reason_codes) if reason_codes else "",
                "spread_reason_codes": "|".join(spread_reason_codes) if spread_reason_codes else "",
                "total_reason_codes": "|".join(total_reason_codes) if total_reason_codes else "",
            }
        )

    rows.sort(
        key=lambda r: max(
            abs(r.get("spread_edge") or 0.0),
            abs(r.get("total_edge") or 0.0),
        ),
        reverse=True,
    )

    misses_csv = out_dir / "new_model_misses_last7d.csv"
    with misses_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "date",
                "game_id",
                "away_team",
                "home_team",
                "market_spread",
                "model_spread",
                "spread_edge",
                "market_total",
                "model_total",
                "total_edge",
                "away_score",
                "home_score",
                "actual_margin",
                "actual_total",
                "predicted_margin",
                "predicted_total",
                "spread_residual",
                "total_residual",
                "wrong_spread",
                "wrong_total",
                "blowout_flag",
                "clutch_flag",
                "pace_outlier_flag",
                "efficiency_outlier_flag",
                "home_adv_miss_flag",
                "back_to_back_flag",
                "reason_codes",
                "spread_reason_codes",
                "total_reason_codes",
            ],
        )
        w.writeheader()
        for row in rows:
            w.writerow(row)

    misses_json = out_dir / "new_model_misses_last7d.json"
    misses_json.write_text(json.dumps(rows, indent=2), encoding="utf-8")

    misses_reasons_csv = out_dir / "new_model_misses_last7d_with_reasons.csv"
    with misses_reasons_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "date",
                "game_id",
                "away_team",
                "home_team",
                "market_spread",
                "model_spread",
                "spread_edge",
                "market_total",
                "model_total",
                "total_edge",
                "away_score",
                "home_score",
                "actual_margin",
                "actual_total",
                "predicted_margin",
                "predicted_total",
                "spread_residual",
                "total_residual",
                "wrong_spread",
                "wrong_total",
                "blowout_flag",
                "clutch_flag",
                "pace_outlier_flag",
                "efficiency_outlier_flag",
                "home_adv_miss_flag",
                "back_to_back_flag",
                "reason_codes",
                "spread_reason_codes",
                "total_reason_codes",
            ],
        )
        w.writeheader()
        for row in rows:
            w.writerow(row)

    report_path = out_dir / "new_model_misses_last7d_report.md"
    spread_decisions = [r for r in rows if r.get("wrong_spread") is not None]
    total_decisions = [r for r in rows if r.get("wrong_total") is not None]
    spread_hits = len([r for r in spread_decisions if r.get("wrong_spread") is False])
    total_hits = len([r for r in total_decisions if r.get("wrong_total") is False])
    spread_hit_rate = (spread_hits / len(spread_decisions)) if spread_decisions else 0.0
    total_hit_rate = (total_hits / len(total_decisions)) if total_decisions else 0.0

    spread_residuals = [abs(r.get("spread_residual")) for r in rows if r.get("spread_residual") is not None]
    total_residuals = [abs(r.get("total_residual")) for r in rows if r.get("total_residual") is not None]
    avg_abs_spread_residual = (sum(spread_residuals) / len(spread_residuals)) if spread_residuals else 0.0
    avg_abs_total_residual = (sum(total_residuals) / len(total_residuals)) if total_residuals else 0.0

    worst_spread = sorted(
        [r for r in rows if r.get("spread_residual") is not None],
        key=lambda r: abs(r.get("spread_residual")),
        reverse=True,
    )[:10]
    worst_total = sorted(
        [r for r in rows if r.get("total_residual") is not None],
        key=lambda r: abs(r.get("total_residual")),
        reverse=True,
    )[:10]

    def _count_reason_codes(items, key):
        counts = {}
        for r in items:
            codes = (r.get(key) or "").split("|")
            codes = [c for c in codes if c]
            for c in codes:
                counts[c] = counts.get(c, 0) + 1
        return counts

    spread_reason_counts = _count_reason_codes([r for r in rows if r.get("wrong_spread")], "spread_reason_codes")
    total_reason_counts = _count_reason_codes([r for r in rows if r.get("wrong_total")], "total_reason_codes")

    def _render_reason_table(counts, total):
        lines = []
        for code, count in sorted(counts.items(), key=lambda x: x[1], reverse=True):
            pct = (count / total) if total else 0.0
            lines.append(f"- {code}: {count} ({pct:.1%})")
        if not lines:
            lines.append("- none")
        return lines

    spread_reason_lines = _render_reason_table(spread_reason_counts, len([r for r in rows if r.get("wrong_spread")]))
    total_reason_lines = _render_reason_table(total_reason_counts, len([r for r in rows if r.get("wrong_total")]))

    def _game_label(r):
        return f"{r.get('away_team')} @ {r.get('home_team')} ({r.get('date')})"

    top_spread_examples = [r for r in worst_spread if r.get("wrong_spread")][:5]
    top_total_examples = [r for r in worst_total if r.get("wrong_total")][:5]

    report_lines = []
    report_lines.append("# New model misses last 7 days")
    report_lines.append("")
    report_lines.append("## 1) Coverage / integrity checks")
    report_lines.append(f"- games in date range: {report['games_in_range']}")
    report_lines.append(f"- games with predictions: {report['games_with_predictions']}")
    report_lines.append(f"- games with outcomes: {report['games_with_outcomes']}")
    report_lines.append(f"- games successfully joined: {report['games_joined']}")
    if missing_pred_dates:
        report_lines.append(f"- missing prediction dates: {', '.join(missing_pred_dates)}")
    if not ppp_residuals:
        report_lines.append("- efficiency outlier flag: skipped (missing pace/possession data in range)")
    report_lines.append("")

    report_lines.append("## 2) Summary stats")
    report_lines.append(f"- spread picks: {len(spread_decisions)} decisions, {spread_hits} hits, hit rate {spread_hit_rate:.1%}")
    report_lines.append(f"- total picks: {len(total_decisions)} decisions, {total_hits} hits, hit rate {total_hit_rate:.1%}")
    report_lines.append(f"- average abs spread residual: {avg_abs_spread_residual:.2f}")
    report_lines.append(f"- average abs total residual: {avg_abs_total_residual:.2f}")
    report_lines.append("")
    report_lines.append("Worst 10 by abs spread residual:")
    for r in worst_spread:
        report_lines.append(f"- {_game_label(r)}: spread_residual={r.get('spread_residual'):.2f}")
    report_lines.append("")
    report_lines.append("Worst 10 by abs total residual:")
    for r in worst_total:
        report_lines.append(f"- {_game_label(r)}: total_residual={r.get('total_residual'):.2f}")
    report_lines.append("")

    report_lines.append("## 3) Reason-code attribution")
    report_lines.append("Spread reasons:")
    report_lines.extend(spread_reason_lines)
    report_lines.append("")
    report_lines.append("Total reasons:")
    report_lines.extend(total_reason_lines)
    report_lines.append("")

    report_lines.append("## 4) Narrative")
    report_lines.append("Here is why the model line was wrong the past 7 days:")
    if top_spread_examples:
        for r in top_spread_examples:
            reasons = r.get("spread_reason_codes") or "no tagged reason"
            report_lines.append(f"- {_game_label(r)}: spread_residual={r.get('spread_residual'):.2f}. Reasons: {reasons}.")
    else:
        report_lines.append("- No wrong spread picks with residuals in this window.")
    report_lines.append("")
    report_lines.append("Here is why the model totals were wrong the past 7 days:")
    if top_total_examples:
        for r in top_total_examples:
            reasons = r.get("total_reason_codes") or "no tagged reason"
            report_lines.append(f"- {_game_label(r)}: total_residual={r.get('total_residual'):.2f}. Reasons: {reasons}.")
    else:
        report_lines.append("- No wrong total picks with residuals in this window.")
    report_lines.append("")

    report_lines.append("## 5) Concrete fixes")
    if spread_reason_counts.get("home_adv_miss"):
        report_lines.append(
            "- home_adv_miss: add an explicit home-court feature (e.g., constant or travel-adjusted) in `new_model/src/features.py`, include it in `FEATURE_COLS` used by `new_model/src/train_margin.py`, and re-train; validate by tracking hit rate for close home wins."
        )
    if spread_reason_counts.get("back_to_back"):
        report_lines.append(
            "- back_to_back: add interaction features like `back_to_back_home` and `back_to_back_away` in `new_model/src/features.py` and include in `FEATURE_COLS`; validate with a before/after split on back-to-back games."
        )
    if spread_reason_counts.get("blowout"):
        report_lines.append(
            "- blowout: add a margin volatility feature (rolling std dev of margin) in `new_model/src/features.py` and re-train in `new_model/src/train_margin.py`; validate by reducing worst-10 spread residuals."
        )
    if total_reason_counts.get("pace_outlier"):
        report_lines.append(
            "- pace_outlier: include pace volatility or last-N pace delta features in `new_model/src/features.py`, then re-train totals in `new_model/src/train_total.py`; validate by reducing total residuals on high-pace games."
        )
    if total_reason_counts.get("efficiency_outlier"):
        report_lines.append(
            "- efficiency_outlier: add shooting efficiency trend features (e.g., rolling eFG variance) in `new_model/src/features.py` and re-train totals; validate by reducing total residuals when eFG swings."
        )
    if len(report_lines) == 0:
        report_lines.append("- No fixes suggested; insufficient failure signal.")

    # Indicator study: what would have helped
    spread_games = [r for r in joined if r.get("wrong_spread") is not None]
    total_games = [r for r in joined if r.get("wrong_total") is not None]

    def _miss_rate(items):
        if not items:
            return 0.0
        misses = len([r for r in items if r.get("wrong_spread") is True or r.get("wrong_total") is True])
        return misses / len(items)

    def _spread_miss_rate(items):
        if not items:
            return 0.0
        misses = len([r for r in items if r.get("wrong_spread") is True])
        return misses / len(items)

    def _total_miss_rate(items):
        if not items:
            return 0.0
        misses = len([r for r in items if r.get("wrong_total") is True])
        return misses / len(items)

    report_lines.append("## Indicator study: what would have helped")
    report_lines.append("")
    report_lines.append("Home/away pick miss rates (spread):")
    home_picks = [r for r in spread_games if r.get("model_side_pick") == 1]
    away_picks = [r for r in spread_games if r.get("model_side_pick") == -1]
    report_lines.append(f"- home picks: {len(home_picks)} games, miss rate {_spread_miss_rate(home_picks):.1%}")
    report_lines.append(f"- away picks: {len(away_picks)} games, miss rate {_spread_miss_rate(away_picks):.1%}")
    report_lines.append("")

    report_lines.append("Blowout/clutch miss rates (spread):")
    blowout_games = [r for r in spread_games if r.get("actual_margin") is not None and abs(r.get("actual_margin")) >= 15]
    non_blowout_games = [r for r in spread_games if r.get("actual_margin") is not None and abs(r.get("actual_margin")) < 15]
    clutch_games = [r for r in spread_games if r.get("actual_margin") is not None and abs(r.get("actual_margin")) <= 3]
    non_clutch_games = [r for r in spread_games if r.get("actual_margin") is not None and abs(r.get("actual_margin")) > 3]
    report_lines.append(f"- blowouts: {len(blowout_games)} games, miss rate {_spread_miss_rate(blowout_games):.1%}")
    report_lines.append(f"- non-blowouts: {len(non_blowout_games)} games, miss rate {_spread_miss_rate(non_blowout_games):.1%}")
    report_lines.append(f"- clutch: {len(clutch_games)} games, miss rate {_spread_miss_rate(clutch_games):.1%}")
    report_lines.append(f"- non-clutch: {len(non_clutch_games)} games, miss rate {_spread_miss_rate(non_clutch_games):.1%}")
    report_lines.append("")

    # Injuries
    injury_source = None
    if any(r.get("inj_out_home") is not None or r.get("inj_out_away") is not None for r in joined):
        injury_source = "inj_out"
    elif any(r.get("inj_total_home") is not None or r.get("inj_total_away") is not None for r in joined):
        injury_source = "inj_total"

    report_lines.append("Injury buckets (spread + total miss rates):")
    if injury_source is None:
        report_lines.append("- injury data not available; skipping")
    else:
        def _inj_bucket(r):
            if injury_source == "inj_out":
                home_val = r.get("inj_out_home") or 0
                away_val = r.get("inj_out_away") or 0
            else:
                home_val = r.get("inj_total_home") or 0
                away_val = r.get("inj_total_away") or 0
            try:
                max_val = max(int(home_val), int(away_val))
            except Exception:
                max_val = 0
            if max_val == 0:
                return "0"
            if max_val <= 2:
                return "1-2"
            return "3+"

        injury_buckets = {}
        for r in joined:
            bucket = _inj_bucket(r)
            injury_buckets.setdefault(bucket, []).append(r)
        for bucket, items in sorted(injury_buckets.items()):
            report_lines.append(
                f"- {bucket}: {len(items)} games, spread miss rate {_spread_miss_rate([r for r in items if r.get('wrong_spread') is not None]):.1%}, total miss rate {_total_miss_rate([r for r in items if r.get('wrong_total') is not None]):.1%}"
            )
    report_lines.append("")

    # Matchups / style mismatches using available team features
    report_lines.append("Matchup mismatches (spread + total miss rates):")
    mismatch_thresholds = {
        "pace_mismatch": 5.0,
        "efg_mismatch": 0.03,
        "tov_mismatch": 2.0,
        "orb_mismatch": 0.08,
        "ftr_mismatch": 0.1,
    }
    mismatch_flags = {k: [] for k in mismatch_thresholds}
    missing_inputs = set()
    for r in joined:
        try:
            pace_home = float(r.get("pace_home")) if r.get("pace_home") is not None else None
            pace_away = float(r.get("pace_away")) if r.get("pace_away") is not None else None
            efg_home = float(r.get("efg_home")) if r.get("efg_home") is not None else None
            efg_away = float(r.get("efg_away")) if r.get("efg_away") is not None else None
            tov_home = float(r.get("tov_home")) if r.get("tov_home") is not None else None
            tov_away = float(r.get("tov_away")) if r.get("tov_away") is not None else None
            orb_home = float(r.get("orb_home")) if r.get("orb_home") is not None else None
            orb_away = float(r.get("orb_away")) if r.get("orb_away") is not None else None
            ftr_home = float(r.get("ftr_home")) if r.get("ftr_home") is not None else None
            ftr_away = float(r.get("ftr_away")) if r.get("ftr_away") is not None else None
        except Exception:
            pace_home = pace_away = efg_home = efg_away = tov_home = tov_away = orb_home = orb_away = ftr_home = ftr_away = None

        if pace_home is None or pace_away is None:
            missing_inputs.add("pace")
        else:
            if abs(pace_home - pace_away) >= mismatch_thresholds["pace_mismatch"]:
                mismatch_flags["pace_mismatch"].append(r)

        if efg_home is None or efg_away is None:
            missing_inputs.add("efg")
        else:
            if abs(efg_home - efg_away) >= mismatch_thresholds["efg_mismatch"]:
                mismatch_flags["efg_mismatch"].append(r)

        if tov_home is None or tov_away is None:
            missing_inputs.add("tov")
        else:
            if abs(tov_home - tov_away) >= mismatch_thresholds["tov_mismatch"]:
                mismatch_flags["tov_mismatch"].append(r)

        if orb_home is None or orb_away is None:
            missing_inputs.add("orb")
        else:
            if abs(orb_home - orb_away) >= mismatch_thresholds["orb_mismatch"]:
                mismatch_flags["orb_mismatch"].append(r)

        if ftr_home is None or ftr_away is None:
            missing_inputs.add("ftr")
        else:
            if abs(ftr_home - ftr_away) >= mismatch_thresholds["ftr_mismatch"]:
                mismatch_flags["ftr_mismatch"].append(r)

    for flag, items in mismatch_flags.items():
        spread_items = [r for r in items if r.get("wrong_spread") is not None]
        total_items = [r for r in items if r.get("wrong_total") is not None]
        report_lines.append(
            f"- {flag}: {len(items)} games, spread miss rate {_spread_miss_rate(spread_items):.1%}, total miss rate {_total_miss_rate(total_items):.1%}"
        )
    if missing_inputs:
        report_lines.append(f"- missing matchup inputs: {', '.join(sorted(missing_inputs))}")
    report_lines.append("")

    # Implementation plan
    report_lines.append("## Implementation Plan")
    plan_lines = []
    plan_tasks = []

    def _add_task(title, files, functions, acceptance, validation):
        plan_tasks.append(
            {
                "title": title,
                "files": files,
                "functions": functions,
                "acceptance": acceptance,
                "validation": validation,
            }
        )

    _add_task(
        "Add home-court/close-game adjustment feature",
        ["new_model/src/features.py", "new_model/src/train_margin.py"],
        ["compute_features", "train_and_eval"],
        "Close-game spread hit rate improves in backtest; report shows lower home_adv_miss rate.",
        "Backtest: compare spread hit rate on games with abs(market_spread)<=3 before/after.",
    )
    _add_task(
        "Add back-to-back interaction features",
        ["new_model/src/features.py", "new_model/src/train_margin.py"],
        ["compute_features", "train_and_eval"],
        "Back-to-back bucket miss rate decreases in indicator study output.",
        "Backtest: segment by back_to_back flag and compare miss rate.",
    )
    _add_task(
        "Add pace volatility features for totals",
        ["new_model/src/features.py", "new_model/src/train_total.py"],
        ["compute_features", "train_and_eval"],
        "Average abs total residual drops; pace_outlier flag rate decreases.",
        "Backtest: compare MAE total and pace_outlier miss rate before/after.",
    )
    _add_task(
        "Add efficiency volatility features for totals",
        ["new_model/src/features.py", "new_model/src/train_total.py"],
        ["compute_features", "train_and_eval"],
        "Total miss rate decreases on efficiency mismatch games.",
        "Backtest: segment by efficiency_outlier flag and compare miss rate.",
    )
    _add_task(
        "Add matchup mismatch feature flags to model inputs",
        ["new_model/src/features.py", "new_model/src/train_margin.py", "new_model/src/train_total.py"],
        ["compute_features", "train_and_eval"],
        "Mismatch buckets in report show reduced miss rates vs baseline.",
        "Backtest: compare miss rates for pace/efg/tov/orb/ftr mismatch buckets.",
    )
    _add_task(
        "Add injury bucket features to model inputs",
        ["new_model/src/features.py", "new_model/src/train_margin.py", "new_model/src/train_total.py"],
        ["compute_features", "train_and_eval"],
        "Injury bucket miss rates decrease in report; model learns higher variance when injuries spike.",
        "Backtest: segment by injury buckets and compare miss rates.",
    )
    _add_task(
        "Expand miss analysis outputs for QA",
        ["scripts/analyze_new_model_misses.py"],
        ["main"],
        "Report includes Implementation Plan and updated indicator tables.",
        "Run script and verify `data/analysis/new_model_misses_last7d_report.md` and plan file updated.",
    )

    # Keep 5–12 tasks
    plan_tasks = plan_tasks[:12]
    for idx, task in enumerate(plan_tasks, 1):
        report_lines.append(f"{idx}. {task['title']}")
        report_lines.append(f"   - Files to change: {', '.join(task['files'])}")
        report_lines.append(f"   - Exact code location: {', '.join(task['functions'])}")
        report_lines.append(f"   - Acceptance criteria: {task['acceptance']}")
        report_lines.append(f"   - Validation method: {task['validation']}")

    plan_path = out_dir / "new_model_improvement_plan_last7d.md"
    plan_lines.append("# New model improvement plan (last 7 days)")
    plan_lines.append("")
    plan_lines.append("## Implementation Plan")
    for idx, task in enumerate(plan_tasks, 1):
        plan_lines.append(f"{idx}. {task['title']}")
        plan_lines.append(f"   - Files to change: {', '.join(task['files'])}")
        plan_lines.append(f"   - Exact code location: {', '.join(task['functions'])}")
        plan_lines.append(f"   - Acceptance criteria: {task['acceptance']}")
        plan_lines.append(f"   - Validation method: {task['validation']}")

    plan_path.write_text("\n".join(plan_lines) + "\n", encoding="utf-8")

    report_path.write_text("\n".join(report_lines) + "\n", encoding="utf-8")

    return {
        "report": report,
        "missing_pred_dates": missing_pred_dates,
        "unjoined_predictions": unjoined_predictions,
        "unjoined_outcomes": unjoined_outcomes,
        "rows": rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze new model misses coverage.")
    parser.add_argument("--days", type=int, default=7, help="Days back from today (default 7).")
    parser.add_argument("--model", default="new", help="Model key (default new).")
    parser.add_argument("--min_edge", type=float, default=0.0, help="Minimum absolute edge to include.")
    parser.add_argument("--out_dir", default="data/analysis", help="Output directory.")
    args = parser.parse_args()

    if args.model != "new":
        raise SystemExit(f"Unsupported model: {args.model}")

    now_utc = datetime.now(timezone.utc)
    end_date = now_utc.date()
    start_date = end_date - timedelta(days=max(args.days - 1, 0))

    result = analyze_range(start_date, end_date, args.min_edge, args.out_dir)
    report = result["report"]
    missing_pred_dates = result["missing_pred_dates"]
    unjoined_predictions = result["unjoined_predictions"]
    unjoined_outcomes = result["unjoined_outcomes"]

    print("Data coverage report:")
    print(f"- games in date range: {report['games_in_range']}")
    print(f"- games with predictions: {report['games_with_predictions']}")
    print(f"- games with outcomes: {report['games_with_outcomes']}")
    print(f"- games successfully joined: {report['games_joined']}")
    if missing_pred_dates:
        print(f"- missing prediction dates: {', '.join(missing_pred_dates)}")

    if unjoined_predictions:
        print("Unjoined predictions:")
        for row in unjoined_predictions:
            print(
                "  game_id={game_id} date={date} home={home_team_id} away={away_team_id} reason={reason}".format(
                    **row
                )
            )
    if unjoined_outcomes:
        print("Unjoined outcomes:")
        for row in unjoined_outcomes:
            print(f"  game_id={row.get('game_id')} date={row.get('date')}")


if __name__ == "__main__":
    main()
