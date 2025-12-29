"""
Compute accuracy metrics for the new model's lines vs actual game results.

Policy:
- Use archived predictions in data/new_model/predictions_YYYY-MM-DD.json.
- Compare model spread (home line) and total to final scores.
- Skip games missing predictions or final scores.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
GAMES_DIR = DATA_DIR / "games"
PRED_DIR = DATA_DIR / "new_model"
METRICS_DIR = DATA_DIR / "metrics"


def _load_games() -> Dict[int, Tuple[Dict, Optional[int]]]:
    games_map: Dict[int, Tuple[Dict, Optional[int]]] = {}
    for path in sorted(GAMES_DIR.glob("games_*_season.json")):
        season = None
        try:
            season = int(path.stem.split("_")[1])
        except Exception:
            season = None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        rows = payload.get("games") if isinstance(payload, dict) else payload
        if not isinstance(rows, list):
            continue
        for g in rows:
            game_id = g.get("game_id")
            if game_id is None:
                continue
            try:
                games_map[int(game_id)] = (g, season)
            except Exception:
                continue
    return games_map


def _safe_float(val) -> Optional[float]:
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _iter_prediction_files() -> List[Path]:
    if PRED_DIR.exists():
        paths = sorted(PRED_DIR.glob("predictions_*.json"))
        if paths:
            return paths
    fallback = BASE_DIR / "new_model" / "output"
    if fallback.exists():
        return sorted(fallback.glob("predictions_*.json"))
    return []


def _summarize(errors: List[float]) -> Optional[float]:
    if not errors:
        return None
    return sum(abs(e) for e in errors) / len(errors)


def _rmse(errors: List[float]) -> Optional[float]:
    if not errors:
        return None
    return (sum(e * e for e in errors) / len(errors)) ** 0.5


def _bucket_for_spread(spread: float) -> str:
    spread = abs(spread)
    if spread < 2.5:
        return "0-2.5"
    if spread < 5.5:
        return "2.5-5.5"
    if spread < 8.5:
        return "5.5-8.5"
    return "8.5+"


def main() -> None:
    games_map = _load_games()
    pred_paths = _iter_prediction_files()

    spread_errors: List[float] = []
    total_errors: List[float] = []
    spread_correct = 0
    spread_games = 0
    total_games = 0
    ats_games = 0
    ats_correct = 0
    bucket_stats: Dict[str, Dict[str, int]] = {}
    season_stats: Dict[str, Dict[str, List[float]]] = {}
    history_rows = []

    for path in pred_paths:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        for game in payload.get("games", []):
            game_id = game.get("gameId")
            if game_id is None:
                continue
            try:
                game_id = int(game_id)
            except Exception:
                continue

            actual_entry = games_map.get(game_id)
            if not actual_entry:
                continue
            actual, season = actual_entry

            home_score = _safe_float(actual.get("home_score"))
            away_score = _safe_float(actual.get("visitor_score"))
            if home_score is None or away_score is None:
                continue

            actual_margin = home_score - away_score
            actual_total = home_score + away_score

            model_line = (game.get("realLine") or {}).get("spreadHome")
            model_total_line = (game.get("realLine") or {}).get("total")
            model_spread = _safe_float(model_line)
            model_total = _safe_float(model_total_line)

            spread_error = None
            total_error = None
            spread_hit = None

            if model_spread is not None:
                expected_margin = -model_spread
                spread_error = actual_margin - expected_margin
                spread_errors.append(spread_error)
                spread_games += 1
                if expected_margin != 0:
                    spread_hit = 1 if (expected_margin > 0) == (actual_margin > 0) else 0
                    spread_correct += spread_hit

                if actual_margin != expected_margin:
                    ats_hit = 1 if (actual_margin > expected_margin) else 0
                    ats_correct += ats_hit
                    ats_games += 1

                bucket = _bucket_for_spread(model_spread)
                bucket_entry = bucket_stats.setdefault(bucket, {"games": 0, "correct": 0})
                bucket_entry["games"] += 1
                bucket_entry["correct"] += spread_hit if spread_hit is not None else 0

                season_key = str(season) if season is not None else "unknown"
                season_entry = season_stats.setdefault(
                    season_key, {"spread_errors": [], "total_errors": [], "ats_hits": [], "spread_hits": []}
                )
                season_entry["spread_errors"].append(spread_error)
                if spread_hit is not None:
                    season_entry["spread_hits"].append(spread_hit)
                if actual_margin != expected_margin:
                    season_entry["ats_hits"].append(ats_hit)

            if model_total is not None:
                total_error = actual_total - model_total
                total_errors.append(total_error)
                total_games += 1
                season_key = str(season) if season is not None else "unknown"
                season_entry = season_stats.setdefault(
                    season_key, {"spread_errors": [], "total_errors": [], "ats_hits": [], "spread_hits": []}
                )
                season_entry["total_errors"].append(total_error)

            if spread_error is not None or total_error is not None:
                history_rows.append(
                    {
                        "season": season,
                        "game_id": game_id,
                        "date_utc": actual.get("date_utc"),
                        "actual_margin": actual_margin,
                        "actual_total": actual_total,
                        "model_spread_home": model_spread,
                        "model_total": model_total,
                        "spread_error": spread_error,
                        "total_error": total_error,
                        "spread_correct": spread_hit,
                    }
                )

    buckets_out = {}
    for bucket, entry in bucket_stats.items():
        win_rate = (entry["correct"] / entry["games"]) if entry["games"] else None
        buckets_out[bucket] = {
            "games": entry["games"],
            "win_rate": win_rate,
        }

    seasons_out = {}
    for season_key, entry in season_stats.items():
        spread_hits = entry["spread_hits"]
        ats_hits = entry["ats_hits"]
        seasons_out[season_key] = {
            "spread": {
                "games": len(entry["spread_errors"]),
                "mae": _summarize(entry["spread_errors"]),
                "rmse": _rmse(entry["spread_errors"]),
                "win_rate": (sum(spread_hits) / len(spread_hits)) if spread_hits else None,
                "ats_cover_rate": (sum(ats_hits) / len(ats_hits)) if ats_hits else None,
            },
            "total": {
                "games": len(entry["total_errors"]),
                "mae": _summarize(entry["total_errors"]),
                "rmse": _rmse(entry["total_errors"]),
            },
        }

    payload = {
        "as_of_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "games_total": len(history_rows),
        "spread": {
            "games": spread_games,
            "mae": _summarize(spread_errors),
            "rmse": _rmse(spread_errors),
            "win_rate": (spread_correct / spread_games) if spread_games else None,
            "ats_cover_rate": (ats_correct / ats_games) if ats_games else None,
        },
        "total": {
            "games": total_games,
            "mae": _summarize(total_errors),
            "rmse": _rmse(total_errors),
        },
        "buckets": buckets_out,
        "seasons": seasons_out,
    }

    METRICS_DIR.mkdir(parents=True, exist_ok=True)
    accuracy_path = METRICS_DIR / "new_model_accuracy.json"
    accuracy_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    history_path = METRICS_DIR / "new_model_accuracy_history.csv"
    with history_path.open("w", newline="", encoding="utf-8") as f:
        f.write(
            "season,game_id,date_utc,actual_margin,actual_total,"
            "model_spread_home,model_total,spread_error,total_error,spread_correct\n"
        )
        for row in history_rows:
            f.write(
                f"{row.get('season')},{row['game_id']},{row.get('date_utc')},"
                f"{row['actual_margin']},{row['actual_total']},"
                f"{row['model_spread_home']},{row['model_total']},"
                f"{row['spread_error']},{row['total_error']},"
                f"{row['spread_correct']}\n"
            )

    print(f"Wrote {accuracy_path} and {history_path} ({len(history_rows)} rows).")


if __name__ == "__main__":
    main()
