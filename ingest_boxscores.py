# ingest_boxscores.py
import sqlite3
from time import sleep

from nba_api.stats.endpoints import boxscoretraditionalv3

DB_PATH = "data/nba_ratings.db"


def get_all_game_ids(season_int: int):
    """Return all game IDs for a given season, zero-padded to 10 chars."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT game_id FROM games WHERE season = ?", (season_int,))
    ids = [str(row[0]).zfill(10) for row in cur.fetchall()]
    conn.close()
    return ids


def game_already_ingested(game_id: str) -> bool:
    """Return True if we already have any appearances for this game."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM appearances WHERE game_id = ? LIMIT 1;",
        (game_id,),
    )
    row = cur.fetchone()
    conn.close()
    return row is not None


def parse_minutes_to_float(value) -> float:
    """
    Convert various minute formats to float minutes.

    Handles:
      - 'MM:SS'
      - 'PT23M17S' or 'PT12M'
      - blanks / None -> 0.0
    """
    if not isinstance(value, str):
        return 0.0

    s = value.strip()
    if not s:
        return 0.0

    # Old style: "MM:SS"
    if ":" in s:
        try:
            mm, ss = s.split(":")
            return int(mm) + int(ss) / 60.0
        except Exception:
            return 0.0

    # ISO style: "PT23M17S", "PT12M"
    if s.startswith("PT"):
        s2 = s[2:]
        mins = 0
        secs = 0
        if "M" in s2:
            m_part, rest = s2.split("M", 1)
            if m_part:
                try:
                    mins = int(m_part)
                except Exception:
                    mins = 0
            if rest.endswith("S"):
                sec_part = rest[:-1]
                if sec_part:
                    try:
                        secs = int(sec_part)
                    except Exception:
                        secs = 0
        return mins + secs / 60.0

    return 0.0


def save_appearances(game_id: str, season_int: int):
    """Pull one game's box score and write player appearances."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Use V3 – V2 is deprecated for 2025-26+
    bs = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id, timeout=30)
    df = bs.get_data_frames()[0]  # "PlayerStats" table

    # Map columns for both V2-style and V3-style schemas
    if "PLAYER_NAME" in df.columns:
        # Legacy schema
        player_col = "PLAYER_NAME"
        team_col = "TEAM_ABBREVIATION"
        minutes_col = "MIN"
    elif (
        "firstName" in df.columns
        and "familyName" in df.columns
        and "teamTricode" in df.columns
        and "minutes" in df.columns
    ):
        # Current V3 schema
        df["PLAYER_NAME_COMBINED"] = (
            df["firstName"].astype(str).str.strip()
            + " "
            + df["familyName"].astype(str).str.strip()
        )
        player_col = "PLAYER_NAME_COMBINED"
        team_col = "teamTricode"
        minutes_col = "minutes"
    else:
        print(
            f"Unsupported boxscore schema for game {game_id}; "
            f"columns: {list(df.columns)}"
        )
        conn.close()
        return

    for _, row in df.iterrows():
        player_name = row[player_col]
        team = row[team_col]
        minutes_val = parse_minutes_to_float(row[minutes_col])

        # Find or create player
        cur.execute(
            """
            SELECT player_id FROM players
            WHERE name = ? AND team_id = ? AND season = ?
            """,
            (player_name, team, season_int),
        )
        row_found = cur.fetchone()

        if row_found:
            player_id = row_found[0]
        else:
            cur.execute(
                """
                INSERT INTO players (name, team_id, season)
                VALUES (?, ?, ?)
                """,
                (player_name, team, season_int),
            )
            player_id = cur.lastrowid

        # Insert appearance
        cur.execute(
            """
            INSERT OR REPLACE INTO appearances
                (game_id, team_id, player_id, minutes)
            VALUES (?, ?, ?, ?)
            """,
            (game_id, team, player_id, minutes_val),
        )

    conn.commit()
    conn.close()


def main():
    # season you are ingesting
    SEASON_INT = 2026  # 2025–26 season

    game_ids = get_all_game_ids(SEASON_INT)
    print(f"Found {len(game_ids)} games. Fetching box scores...")

    for gid in game_ids:
        if game_already_ingested(gid):
            print(f"  skipping {gid} (already in DB)")
            continue

        print(f"  pulling box score for {gid} ...")
        try:
            save_appearances(gid, SEASON_INT)
        except Exception as e:
            print(f"    ERROR on {gid}: {e}")
        sleep(2)  # throttle requests

    print("Done.")


if __name__ == "__main__":
    main()
