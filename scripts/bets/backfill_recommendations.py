import argparse
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple


PRED_GLOBS = [
    Path("data") / "new_model" / "predictions_*.json",
    Path("public") / "new_model" / "predictions_*.json",
]
MASTER_BETS_PATH = Path("data") / "bets" / "bets_master.csv"


def _parse_date_from_name(path: Path) -> Optional[str]:
    name = path.stem
    if not name.startswith("predictions_"):
        return None
    date_str = name.replace("predictions_", "")
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return None
    return date_str


def _fmt_team_line(team: str, line: float) -> str:
    return f"{team} {line:+.1f}"


def _build_rows(date_str: str, payload: dict) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
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


def _normalize_row(row: Dict[str, object]) -> Dict[str, object]:
    normalized = dict(row)
    if normalized.get("game_id") not in (None, ""):
        normalized["game_id"] = int(normalized["game_id"])
    if normalized.get("edge") not in (None, ""):
        normalized["edge"] = float(normalized["edge"])
    if normalized.get("bet_date") is not None:
        normalized["bet_date"] = str(normalized["bet_date"])
    return normalized


def _load_master() -> List[Dict[str, object]]:
    if not MASTER_BETS_PATH.exists():
        return []
    with MASTER_BETS_PATH.open("r", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


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


def _discover_prediction_files() -> List[Tuple[Path, str]]:
    files: List[Tuple[Path, str]] = []
    for pattern in PRED_GLOBS:
        for candidate in sorted(pattern.parent.glob(pattern.name)):
            date_str = _parse_date_from_name(candidate)
            if date_str:
                files.append((candidate, date_str))
    return files


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill bets_master.csv from historical New Model outputs.")
    parser.add_argument("--min-date", default=None, help="Optional YYYY-MM-DD lower bound.")
    parser.add_argument("--reset", action="store_true", help="Rebuild bets_master.csv from scratch.")
    args = parser.parse_args()

    files = _discover_prediction_files()
    if args.min_date:
        min_date = datetime.strptime(args.min_date, "%Y-%m-%d").date()
        files = [(p, d) for p, d in files if datetime.strptime(d, "%Y-%m-%d").date() >= min_date]

    master_rows = [] if args.reset else [_normalize_row(r) for r in _load_master()]
    all_rows = master_rows[:]

    for path, date_str in files:
        payload = json.loads(path.read_text(encoding="utf-8"))
        rows = [_normalize_row(r) for r in _build_rows(date_str, payload)]
        all_rows.extend(rows)

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

    spread_rows = [r for r in deduped_rows if r.get("bet_type") == "Spread"]
    dates = sorted({r.get("bet_date") for r in deduped_rows if r.get("bet_date")})

    print(f"MASTER_BETS_ROWS={len(deduped_rows)}")
    print(f"MASTER_SPREAD_ROWS={len(spread_rows)}")
    if dates:
        print(f"DATE_RANGE={dates[0]} to {dates[-1]}")

    if len(spread_rows) < 100:
        last_date = dates[-1] if dates else "unknown"
        searched = ", ".join(str(p.parent) for p in PRED_GLOBS)
        print(f"NO_HISTORICAL_RECS_FOUND_BEYOND={last_date}")
        print(f"SEARCHED_FOLDERS={searched}")


if __name__ == "__main__":
    main()
