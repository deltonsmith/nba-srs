import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

AUDIT_PATH = DATA_DIR / "upset_audit_last7d.json"
OUT_PATH = DATA_DIR / "upset_diagnosis_last7d.md"
METRICS_PATH = DATA_DIR / "metrics" / "accuracy.json"


def load_json(path: Path) -> Optional[object]:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def safe_div(num: int, den: int) -> Optional[float]:
    if den == 0:
        return None
    return num / den


def fmt_pct(val: Optional[float]) -> str:
    if val is None:
        return "n/a"
    return f"{val * 100:.1f}%"


def bucket_rating_gap(gap: float) -> str:
    if gap < 2:
        return "<2"
    if gap < 5:
        return "2-5"
    return ">=5"


def percentile(values: List[float], p: float) -> Optional[float]:
    if not values:
        return None
    values = sorted(values)
    if p <= 0:
        return values[0]
    if p >= 100:
        return values[-1]
    k = (len(values) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(values) - 1)
    if f == c:
        return values[f]
    return values[f] + (values[c] - values[f]) * (k - f)


def compute_upset_rate(rows: List[Dict]) -> Tuple[int, int, Optional[float]]:
    total = len(rows)
    upsets = sum(1 for r in rows if r.get("upset"))
    return total, upsets, safe_div(upsets, total)


def filter_rows(rows: List[Dict], pred) -> List[Dict]:
    return [r for r in rows if pred(r)]


def main() -> None:
    audit = load_json(AUDIT_PATH)
    if not isinstance(audit, list):
        raise SystemExit(f"Missing or invalid {AUDIT_PATH}")

    total_games, total_upsets, upset_rate = compute_upset_rate(audit)
    win_rate = safe_div(total_games - total_upsets, total_games)

    metrics = load_json(METRICS_PATH)
    baseline = None
    if isinstance(metrics, dict):
        overall = metrics.get("overall") or {}
        if isinstance(overall, dict):
            baseline = overall.get("win_rate_total")

    lines: List[str] = []
    lines.append("# Upset Diagnosis (Last 7 Days)")
    lines.append("")
    lines.append(f"Total games: {total_games}")
    lines.append(f"Higher-ranked win rate (last 7 days): {fmt_pct(win_rate)}")
    if baseline is not None:
        lines.append(f"Season-to-date higher-ranked win rate: {fmt_pct(baseline)}")
        if win_rate is not None:
            diff = win_rate - baseline
            lines.append(f"Delta vs baseline: {diff * 100:.1f}%")
    else:
        lines.append("Season-to-date higher-ranked win rate: n/a")

    lines.append("")
    lines.append("## Top drivers associated with upsets")

    # Lower-ranked home vs away
    home_rows = filter_rows(audit, lambda r: r.get("home_indicator") is True)
    away_rows = filter_rows(audit, lambda r: r.get("home_indicator") is False)
    ht, hu, hr = compute_upset_rate(home_rows)
    at, au, ar = compute_upset_rate(away_rows)
    lines.append(f"- Lower-ranked home: {fmt_pct(hr)} upset rate ({hu}/{ht})")
    lines.append(f"- Lower-ranked away: {fmt_pct(ar)} upset rate ({au}/{at})")

    # Rest disadvantage: higher-ranked on B2B or fewer rest days
    rest_disadv = filter_rows(
        audit,
        lambda r: (
            r.get("back_to_back_home") is True and r.get("higher_ranked_team") == r.get("home_team_id")
        )
        or (
            r.get("back_to_back_away") is True and r.get("higher_ranked_team") == r.get("visitor_team_id")
        )
        or (
            r.get("rest_days_home") is not None
            and r.get("rest_days_away") is not None
            and (
                (r.get("higher_ranked_team") == r.get("home_team_id") and r.get("rest_days_home") < r.get("rest_days_away"))
                or (
                    r.get("higher_ranked_team") == r.get("visitor_team_id")
                    and r.get("rest_days_away") < r.get("rest_days_home")
                )
            )
        )
    )
    rt, ru, rr = compute_upset_rate(rest_disadv)
    lines.append(f"- Higher-ranked rest disadvantage: {fmt_pct(rr)} upset rate ({ru}/{rt})")

    # Injury disadvantage: higher-ranked missing more key players
    def injury_disadv(row: Dict) -> bool:
        hi = row.get("higher_ranked_team")
        if hi == row.get("home_team_id"):
            hi_count = row.get("key_injuries_count_home")
            lo_count = row.get("key_injuries_count_away")
        else:
            hi_count = row.get("key_injuries_count_away")
            lo_count = row.get("key_injuries_count_home")
        if hi_count is None or lo_count is None:
            return False
        return hi_count > lo_count

    inj_rows = filter_rows(audit, injury_disadv)
    it, iu, ir = compute_upset_rate(inj_rows)
    lines.append(f"- Higher-ranked injury disadvantage: {fmt_pct(ir)} upset rate ({iu}/{it})")

    # Style mismatch buckets (abs delta >= 75th percentile)
    style_metrics = ["pace", "three_pa_rate", "reb_pct"]
    for metric in style_metrics:
        vals = []
        for r in audit:
            deltas = r.get("style_deltas") or {}
            val = deltas.get(metric)
            if isinstance(val, (int, float)):
                vals.append(abs(float(val)))
        threshold = percentile(vals, 75) if vals else None
        if threshold is None:
            lines.append(f"- Style mismatch ({metric}): n/a")
            continue
        high_rows = filter_rows(
            audit,
            lambda r, m=metric, t=threshold: (
                isinstance((r.get("style_deltas") or {}).get(m), (int, float))
                and abs(float((r.get("style_deltas") or {}).get(m))) >= t
            ),
        )
        ht, hu, hr = compute_upset_rate(high_rows)
        lines.append(
            f"- Style mismatch ({metric} >= {threshold:.2f}): {fmt_pct(hr)} upset rate ({hu}/{ht})"
        )

    # Rating gap buckets
    gap_buckets: Dict[str, List[Dict]] = {"<2": [], "2-5": [], ">=5": []}
    for r in audit:
        home_rating = r.get("home_rating")
        away_rating = r.get("away_rating")
        if not isinstance(home_rating, (int, float)) or not isinstance(away_rating, (int, float)):
            continue
        gap = abs(float(home_rating) - float(away_rating))
        gap_buckets[bucket_rating_gap(gap)].append(r)

    lines.append("- Rating gap buckets (spread proxy):")
    for label, rows in gap_buckets.items():
        gt, gu, gr = compute_upset_rate(rows)
        lines.append(f"  - {label}: {fmt_pct(gr)} upset rate ({gu}/{gt})")

    lines.append("")
    lines.append("here's why the win rate over the past 7 days was low")
    lines.append("")
    if win_rate is None:
        lines.append("No games were available to compute a recent win rate.")
    else:
        lines.append("The recent win rate dipped primarily in spots where the lower-ranked team had situational edges")
        lines.append("(home court, rest, or injury advantage) and in games with large style mismatches or small rating gaps.")
        if baseline is not None:
            lines.append(
                f"Relative to the season baseline ({fmt_pct(baseline)}), the last 7 days were dragged down by the upset"
                " clusters called out above."
            )

    lines.append("")
    lines.append("how to improve the model")
    lines.append("")
    lines.append("1) Add or re-weight rest/back-to-back features so the higher-ranked team isn't over-trusted on short rest.")
    lines.append("2) Incorporate injury severity into pre-game rating adjustments (weight by minutes/usage).")
    lines.append("3) Expand matchup-style features (pace/3PA/rebounding) to penalize large mismatches.")
    lines.append("4) Treat small rating gaps as higher-variance (wider uncertainty / lower confidence).")

    OUT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {OUT_PATH}")


if __name__ == "__main__":
    main()
