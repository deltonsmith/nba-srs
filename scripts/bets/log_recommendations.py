import argparse
import csv
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import json
from zoneinfo import ZoneInfo


PRED_DIR = Path("data") / "new_model"
PUBLIC_PRED = Path("public") / "new_model" / "predictions_today.json"
MASTER_BETS_PATH = Path("data") / "bets" / "bets_master.csv"


def _resolve_date(date_arg: str) -> datetime.date:
    if date_arg.lower() == "yesterday":
        tz = ZoneInfo("America/Chicago")
        return (datetime.now(tz) - timedelta(days=1)).date()
    return datetime.strptime(date_arg, "%Y-%m-%d").date()


def _load_predictions(date_str: str) -> dict:
    path = PRED_DIR / f"predictions_{date_str}.json"
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    if PUBLIC_PRED.exists():
        payload = json.loads(PUBLIC_PRED.read_text(encoding="utf-8"))
        if payload.get("date") == date_str:
            return payload
    raise SystemExit(f"Missing predictions for {date_str}: {path}")


def _fmt_team_line(team: str, line: float) -> str:
    return f"{team} {line:+.1f}"


def _build_rows(date_str: str, payload: dict) -> List[Dict[str, object]]:
    rows = []
    for g in payload.get("games", []):
        game_id = g.get("gameId")
        if game_id is None:
            continue
        home = (g.get("home") or {}).get("abbr") or (g.get("home") or {}).get("id")
        away = (g.get("away") or {}).get("abbr") or (g.get("away") or {}).get("id")
        market = g.get("market") or {}
        real_line = g.get("realLine") or {}
        edge = g.get("edge") or {}

        market_spread = market.get("spreadHome")
        model_spread = real_line.get("spreadHome")
        edge_spread = edge.get("spread")

        spread_row = None
        if market_spread is not None and model_spread is not None and edge_spread is not None:
            market_line = _fmt_team_line(home, float(market_spread))
            model_line = _fmt_team_line(home, float(model_spread))
            if edge_spread < 0:
                recommended_bet = _fmt_team_line(home, float(market_spread))
            elif edge_spread > 0:
                recommended_bet = _fmt_team_line(away, -float(market_spread))
            else:
                recommended_bet = ""
            spread_row = {
                "game_id": int(game_id),
                "bet_date": date_str,
                "bet_type": "Spread",
                "teams": f"{away} @ {home}",
                "market_line": market_line,
                "model_line": model_line,
                "edge": float(edge_spread),
                "recommended_bet": recommended_bet,
            }

        market_total = market.get("total")
        model_total = real_line.get("total")
        edge_total = edge.get("total")
        total_row = None
        if market_total is not None and model_total is not None and edge_total is not None:
            market_line = f"{float(market_total):.1f}"
            model_line = f"{float(model_total):.1f}"
            if edge_total > 0:
                recommended_bet = f"Over {float(market_total):.1f}"
            elif edge_total < 0:
                recommended_bet = f"Under {float(market_total):.1f}"
            else:
                recommended_bet = ""
            total_row = {
                "game_id": int(game_id),
                "bet_date": date_str,
                "bet_type": "Total",
                "teams": f"{away} @ {home}",
                "market_line": market_line,
                "model_line": model_line,
                "edge": float(edge_total),
                "recommended_bet": recommended_bet,
            }

        if spread_row:
            rows.append(spread_row)
        if total_row:
            rows.append(total_row)

    return rows


def _load_master() -> List[Dict[str, object]]:
    if not MASTER_BETS_PATH.exists():
        return []
    with MASTER_BETS_PATH.open("r", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _normalize_row(row: Dict[str, object]) -> Dict[str, object]:
    normalized = dict(row)
    if normalized.get("game_id") not in (None, ""):
        normalized["game_id"] = int(normalized["game_id"])
    if normalized.get("edge") not in (None, ""):
        normalized["edge"] = float(normalized["edge"])
    if normalized.get("bet_date") is not None:
        normalized["bet_date"] = str(normalized["bet_date"])
    return normalized


def _write_master(rows: List[Dict[str, object]]) -> None:
    MASTER_BETS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with MASTER_BETS_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "game_id",
                "bet_date",
                "bet_type",
                "teams",
                "market_line",
                "model_line",
                "edge",
                "recommended_bet",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Log daily model recommendations into the master bet log.")
    parser.add_argument("--date", default="yesterday", help="YYYY-MM-DD or 'yesterday' (default).")
    args = parser.parse_args()

    target_date = _resolve_date(args.date)
    date_str = target_date.isoformat()
    payload = _load_predictions(date_str)
    new_rows = [_normalize_row(r) for r in _build_rows(date_str, payload)]

    master_rows = [_normalize_row(r) for r in _load_master()]
    all_rows = master_rows + new_rows

    master_keys = {(r.get("game_id"), r.get("bet_date"), r.get("bet_type")) for r in master_rows}
    deduped = {}
    for row in all_rows:
        key = (row.get("game_id"), row.get("bet_date"), row.get("bet_type"))
        if key in deduped:
            existing = deduped[key]
            if abs(float(row.get("edge", 0))) > abs(float(existing.get("edge", 0))):
                deduped[key] = row
        else:
            deduped[key] = row

    deduped_rows = list(deduped.values())
    _write_master(deduped_rows)

    appended = len({k for k in deduped if k not in master_keys})
    total_spread = len([r for r in deduped_rows if r.get("bet_type") == "Spread"])
    total_total = len([r for r in deduped_rows if r.get("bet_type") == "Total"])

    print(f"appended_rows={appended}")
    print(f"total_rows={len(deduped_rows)}")
    print(f"total_spread_rows={total_spread}")
    print(f"total_total_rows={total_total}")


if __name__ == "__main__":
    main()
