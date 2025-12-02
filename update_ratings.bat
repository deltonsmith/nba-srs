@echo off
cd /d C:\nba-srs

rem activate your venv
call .venv\Scripts\activate.bat

rem 1) pull games + boxscores for live season
python ingest_games.py
python ingest_boxscores.py

rem 2) refresh advanced stats + player values for 2025-26
python pull_bbr_advanced.py
python ingest_player_values.py 2026

rem 3) recompute ratings JSON
python compute_ratings.py

rem 4) commit + push if anything changed
git add ratings_*.json data\player_values_2026.csv nba_ratings.db

git diff --cached --quiet
if %errorlevel%==0 goto :EOF

git commit -m "Auto update 2025-26 ratings"
git push
