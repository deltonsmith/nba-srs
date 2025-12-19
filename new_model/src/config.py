"""
Configuration defaults for the new model pipeline.
Override via environment variables where appropriate.
"""

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

# SQLite DB path
DB_PATH = os.environ.get("NEW_MODEL_DB_PATH") or str(BASE_DIR / "data" / "new_model.sqlite")

# Pagination size for balldontlie client
PER_PAGE = int(os.environ.get("BALDONTLIE_PER_PAGE", "100"))

# Odds polling frequency (minutes)
ODDS_POLL_MINUTES_GAME_DAY = int(os.environ.get("ODDS_POLL_MINUTES_GAME_DAY", "10"))
ODDS_POLL_MINUTES_NON_GAME_DAY = int(os.environ.get("ODDS_POLL_MINUTES_NON_GAME_DAY", "60"))
