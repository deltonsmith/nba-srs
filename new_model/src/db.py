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
        _ensure_schema(conn)
        conn.commit()
    finally:
        conn.close()


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r[1] == column for r in rows)


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name = ?", (table,)).fetchone()
    return row is not None


def _ensure_schema(conn: sqlite3.Connection) -> None:
    # Lightweight migrations for existing DBs.
    if not _column_exists(conn, "games", "home_team_bdl_id"):
        conn.execute("ALTER TABLE games ADD COLUMN home_team_bdl_id INTEGER")
    if not _column_exists(conn, "games", "away_team_bdl_id"):
        conn.execute("ALTER TABLE games ADD COLUMN away_team_bdl_id INTEGER")

    if not _column_exists(conn, "team_game_features", "team_bdl_id"):
        conn.execute("ALTER TABLE team_game_features ADD COLUMN team_bdl_id INTEGER")
    if not _column_exists(conn, "team_game_features", "inj_out"):
        conn.execute("ALTER TABLE team_game_features ADD COLUMN inj_out INTEGER")
    if not _column_exists(conn, "team_game_features", "inj_day_to_day"):
        conn.execute("ALTER TABLE team_game_features ADD COLUMN inj_day_to_day INTEGER")
    if not _column_exists(conn, "team_game_features", "inj_total"):
        conn.execute("ALTER TABLE team_game_features ADD COLUMN inj_total INTEGER")

    if not _table_exists(conn, "team_game_stats"):
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS team_game_stats (
                game_id   INTEGER NOT NULL,
                team_id   TEXT NOT NULL,
                team_bdl_id INTEGER,
                fgm       INTEGER,
                fga       INTEGER,
                fg3m      INTEGER,
                ftm       INTEGER,
                fta       INTEGER,
                oreb      INTEGER,
                dreb      INTEGER,
                reb       INTEGER,
                ast       INTEGER,
                stl       INTEGER,
                blk       INTEGER,
                tov       INTEGER,
                pf        INTEGER,
                pts       INTEGER,
                PRIMARY KEY (game_id, team_id),
                FOREIGN KEY (game_id) REFERENCES games (game_id)
            )
            """
        )


def upsert_games(db_path, games: Iterable[Mapping]) -> None:
    """
    Insert or replace games by primary key.
    Expected keys per item: game_id, season, date, home_team_id, away_team_id,
    home_team_bdl_id, away_team_bdl_id, home_score, away_score, status, start_time_utc.
    """
    games = list(games)
    if not games:
        return

    conn = get_conn(db_path)
    try:
        conn.executemany(
            """
            INSERT OR REPLACE INTO games
                (game_id, season, date, home_team_id, away_team_id, home_team_bdl_id, away_team_bdl_id, home_score, away_score, status, start_time_utc)
            VALUES
                (:game_id, :season, :date, :home_team_id, :away_team_id, :home_team_bdl_id, :away_team_bdl_id, :home_score, :away_score, :status, :start_time_utc)
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


def insert_player_injuries(db_path, rows: Iterable[Mapping], pulled_at: Optional[str] = None) -> int:
    """
    Insert player injury rows with a shared pulled_at timestamp.
    Expected keys per item: player_id, team_id, status, return_date, description.
    """
    rows = list(rows)
    if not rows:
        return 0
    payload = []
    stamp = pulled_at or datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    for r in rows:
        item = dict(r)
        item["pulled_at"] = stamp
        payload.append(item)

    conn = get_conn(db_path)
    try:
        conn.executemany(
            """
            INSERT INTO player_injuries
                (player_id, team_id, status, return_date, description, pulled_at)
            VALUES
                (:player_id, :team_id, :status, :return_date, :description, :pulled_at)
            """,
            payload,
        )
        conn.commit()
    finally:
        conn.close()
    return len(payload)


def upsert_team_game_stats(db_path, rows: Iterable[Mapping]) -> int:
    """
    Insert or replace team game stats by (game_id, team_id).
    Expected keys per item: game_id, team_id, team_bdl_id, fgm, fga, fg3m, ftm, fta,
    oreb, dreb, reb, ast, stl, blk, tov, pf, pts.
    """
    rows = list(rows)
    if not rows:
        return 0
    conn = get_conn(db_path)
    try:
        conn.executemany(
            """
            INSERT OR REPLACE INTO team_game_stats
                (game_id, team_id, team_bdl_id, fgm, fga, fg3m, ftm, fta, oreb, dreb, reb, ast, stl, blk, tov, pf, pts)
            VALUES
                (:game_id, :team_id, :team_bdl_id, :fgm, :fga, :fg3m, :ftm, :fta, :oreb, :dreb, :reb, :ast, :stl, :blk, :tov, :pf, :pts)
            """,
            rows,
        )
        conn.commit()
    finally:
        conn.close()
    return len(rows)
