"""
SQLite helpers for the new model pipeline.
Provides connection setup, schema initialization, and basic upserts/inserts.
"""

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping, Optional


BASE_DIR = Path(__file__).resolve().parent.parent
SCHEMA_PATH = BASE_DIR / "sql" / "schema.sql"


def get_conn(db_path) -> sqlite3.Connection:
    """Open a SQLite connection with foreign keys enforced."""
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(db_path) -> None:
    """Create tables if missing by executing schema.sql."""
    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
    conn = get_conn(db_path)
    try:
        conn.executescript(schema_sql)
        conn.commit()
    finally:
        conn.close()


def upsert_games(db_path, games: Iterable[Mapping]) -> None:
    """
    Insert or replace games by primary key.
    Expected keys per item: game_id, season, date, home_team_id, away_team_id,
    home_score, away_score, status, start_time_utc.
    """
    games = list(games)
    if not games:
        return

    conn = get_conn(db_path)
    try:
        conn.executemany(
            """
            INSERT OR REPLACE INTO games
                (game_id, season, date, home_team_id, away_team_id, home_score, away_score, status, start_time_utc)
            VALUES
                (:game_id, :season, :date, :home_team_id, :away_team_id, :home_score, :away_score, :status, :start_time_utc)
            """,
            games,
        )
        conn.commit()
    finally:
        conn.close()


def insert_odds_snapshot(db_path, snapshot: Mapping, pulled_at: Optional[str] = None) -> None:
    """
    Insert an odds snapshot; duplicate (by unique constraint) rows are ignored.
    Required keys in snapshot: game_id, vendor, market_type, updated_at.
    Optional keys: home_line, away_line, total, home_ml, away_ml.
    """
    required = ("game_id", "vendor", "market_type", "updated_at")
    for key in required:
        if key not in snapshot:
            raise ValueError(f"insert_odds_snapshot missing required field: {key}")

    payload = dict(snapshot)
    payload["pulled_at"] = pulled_at or datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    conn = get_conn(db_path)
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO odds_snapshots
                (game_id, vendor, market_type, home_line, away_line, total, home_ml, away_ml, updated_at, pulled_at)
            VALUES
                (:game_id, :vendor, :market_type, :home_line, :away_line, :total, :home_ml, :away_ml, :updated_at, :pulled_at)
            """,
            payload,
        )
        conn.commit()
    finally:
        conn.close()
