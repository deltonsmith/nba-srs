PRAGMA foreign_keys = ON;

-- Games master table
CREATE TABLE IF NOT EXISTS games (
    game_id         INTEGER PRIMARY KEY,
    season          INTEGER NOT NULL,
    date            TEXT NOT NULL,
    home_team_id    TEXT,
    away_team_id    TEXT,
    home_team_bdl_id INTEGER,
    away_team_bdl_id INTEGER,
    home_score      INTEGER,
    away_score      INTEGER,
    status          TEXT,
    start_time_utc  TEXT
);

-- Odds snapshots (multiple vendors/market types per game over time)
CREATE TABLE IF NOT EXISTS odds_snapshots (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id        INTEGER NOT NULL,
    vendor         TEXT NOT NULL,
    market_type    TEXT NOT NULL, -- spread | total | moneyline
    home_line      REAL,
    away_line      REAL,
    total          REAL,
    home_ml        INTEGER,
    away_ml        INTEGER,
    updated_at     TEXT NOT NULL, -- timestamp from API
    pulled_at      TEXT NOT NULL, -- when we stored it
    FOREIGN KEY (game_id) REFERENCES games (game_id),
    UNIQUE (game_id, vendor, market_type, updated_at)
);

-- Team-level game features (per game per team)
CREATE TABLE IF NOT EXISTS team_game_features (
    game_id      INTEGER NOT NULL,
    team_id      TEXT NOT NULL,
    team_bdl_id  INTEGER,
    net_rating   REAL,
    pace         REAL,
    efg          REAL,
    tov          REAL,
    orb          REAL,
    ftr          REAL,
    rest_days    INTEGER,
    travel_miles REAL,
    back_to_back INTEGER,
    inj_out      INTEGER,
    inj_day_to_day INTEGER,
    inj_total    INTEGER,
    PRIMARY KEY (game_id, team_id),
    FOREIGN KEY (game_id) REFERENCES games (game_id)
);

-- Player injuries snapshots
CREATE TABLE IF NOT EXISTS player_injuries (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id   INTEGER,
    team_id     INTEGER,
    status      TEXT,
    return_date TEXT,
    description TEXT,
    pulled_at   TEXT NOT NULL
);

-- Team boxscore aggregates (for advanced feature computation)
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
);

-- Market lines (closing/consensus)
CREATE TABLE IF NOT EXISTS market_lines (
    game_id             INTEGER PRIMARY KEY,
    cutoff_time_utc     TEXT,
    vendor_rule         TEXT, -- e.g., "draftkings" or "median"
    closing_spread_home REAL,
    closing_total       REAL,
    closing_home_ml     INTEGER,
    source_snapshot_id  INTEGER,
    FOREIGN KEY (game_id) REFERENCES games (game_id),
    FOREIGN KEY (source_snapshot_id) REFERENCES odds_snapshots (id)
);

-- Model runs metadata
CREATE TABLE IF NOT EXISTS model_runs (
    run_id           INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at       TEXT NOT NULL,
    git_sha          TEXT,
    train_start_date TEXT,
    train_end_date   TEXT,
    notes            TEXT
);
