import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

import scripts.analyze_new_model_misses as analyzer


def _write_predictions(pred_dir: Path, date_str: str, game_id: int, market_spread: float, model_spread: float, market_total: float, model_total: float):
    payload = {
        "date": date_str,
        "games": [
            {
                "gameId": game_id,
                "startTimeUtc": f"{date_str}T00:00:00.000Z",
                "away": {"abbr": "AWY"},
                "home": {"abbr": "HOM"},
                "market": {"spreadHome": market_spread, "total": market_total},
                "realLine": {"spreadHome": model_spread, "total": model_total},
                "edge": {"spread": model_spread - market_spread, "total": model_total - market_total},
            }
        ],
    }
    pred_dir.mkdir(parents=True, exist_ok=True)
    (pred_dir / f"predictions_{date_str}.json").write_text(json.dumps(payload), encoding="utf-8")


def _setup_db(db_path: Path, date_str: str, game_id: int, home_score: int, away_score: int):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS games (
                game_id INTEGER PRIMARY KEY,
                season INTEGER,
                date TEXT,
                home_team_id TEXT,
                away_team_id TEXT,
                home_score INTEGER,
                away_score INTEGER,
                status TEXT,
                start_time_utc TEXT
            );
            CREATE TABLE IF NOT EXISTS team_game_features (
                game_id INTEGER,
                team_id TEXT,
                pace REAL,
                efg REAL,
                tov REAL,
                orb REAL,
                ftr REAL,
                inj_out INTEGER,
                inj_day_to_day INTEGER,
                inj_total INTEGER,
                back_to_back INTEGER,
                rest_days INTEGER
            );
            """
        )
        conn.execute(
            """
            INSERT INTO games (game_id, season, date, home_team_id, away_team_id, home_score, away_score, status, start_time_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (game_id, 2026, date_str, "HOM", "AWY", home_score, away_score, "Final", f"{date_str}T00:00:00Z"),
        )
        conn.execute(
            """
            INSERT INTO team_game_features (game_id, team_id, pace, efg, tov, orb, ftr, inj_out, inj_day_to_day, inj_total, back_to_back, rest_days)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (game_id, "HOM", 100.0, 0.52, 12.0, 0.25, 0.2, 0, 0, 0, 0, 1),
        )
        conn.execute(
            """
            INSERT INTO team_game_features (game_id, team_id, pace, efg, tov, orb, ftr, inj_out, inj_day_to_day, inj_total, back_to_back, rest_days)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (game_id, "AWY", 98.0, 0.49, 13.0, 0.22, 0.18, 0, 0, 0, 0, 1),
        )
        conn.commit()
    finally:
        conn.close()


class AnalyzeNewModelMissesTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        base = Path(self.tmp.name)
        analyzer.PRED_DIR = base / "data" / "new_model"
        analyzer.PUBLIC_PRED = base / "public" / "new_model" / "predictions_today.json"
        analyzer.DB_PATH = base / "new_model" / "data" / "new_model.sqlite"
        self.out_dir = base / "data" / "analysis"
        self.date_str = "2025-12-30"

    def tearDown(self):
        self.tmp.cleanup()

    def test_join_correctness_game_id(self):
        _write_predictions(analyzer.PRED_DIR, self.date_str, 1, -5.0, -5.0, 210.0, 210.0)
        _setup_db(analyzer.DB_PATH, self.date_str, 1, 100, 95)
        result = analyzer.analyze_range(
            start_date=analyzer.datetime.strptime(self.date_str, "%Y-%m-%d").date(),
            end_date=analyzer.datetime.strptime(self.date_str, "%Y-%m-%d").date(),
            min_edge=0.0,
            out_dir=str(self.out_dir),
        )
        self.assertEqual(result["report"]["games_joined"], 1)
        self.assertEqual(len(result["rows"]), 1)

    def test_push_handling(self):
        _write_predictions(analyzer.PRED_DIR, self.date_str, 2, -5.0, -5.0, 205.0, 205.0)
        _setup_db(analyzer.DB_PATH, self.date_str, 2, 105, 100)
        result = analyzer.analyze_range(
            start_date=analyzer.datetime.strptime(self.date_str, "%Y-%m-%d").date(),
            end_date=analyzer.datetime.strptime(self.date_str, "%Y-%m-%d").date(),
            min_edge=0.0,
            out_dir=str(self.out_dir),
        )
        row = result["rows"][0]
        self.assertIsNone(row["wrong_spread"])
        self.assertIsNone(row["wrong_total"])

    def test_sign_convention_spread_direction(self):
        _write_predictions(analyzer.PRED_DIR, self.date_str, 3, -1.5, -3.5, 210.0, 210.0)
        _setup_db(analyzer.DB_PATH, self.date_str, 3, 98, 100)
        result = analyzer.analyze_range(
            start_date=analyzer.datetime.strptime(self.date_str, "%Y-%m-%d").date(),
            end_date=analyzer.datetime.strptime(self.date_str, "%Y-%m-%d").date(),
            min_edge=0.0,
            out_dir=str(self.out_dir),
        )
        row = result["rows"][0]
        self.assertTrue(row["wrong_spread"])

    def test_report_created(self):
        _write_predictions(analyzer.PRED_DIR, self.date_str, 4, -2.0, -2.0, 205.0, 205.0)
        _setup_db(analyzer.DB_PATH, self.date_str, 4, 101, 99)
        analyzer.analyze_range(
            start_date=analyzer.datetime.strptime(self.date_str, "%Y-%m-%d").date(),
            end_date=analyzer.datetime.strptime(self.date_str, "%Y-%m-%d").date(),
            min_edge=0.0,
            out_dir=str(self.out_dir),
        )
        report_path = self.out_dir / "new_model_misses_last7d_report.md"
        self.assertTrue(report_path.exists())


if __name__ == "__main__":
    unittest.main()
