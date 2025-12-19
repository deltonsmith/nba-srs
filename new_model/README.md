# New Model Pipeline

Below are one-command steps for the current scaffold. Assume you are in repo root with Python deps installed (`pip install -r new_model/requirements.txt`) and `BALLDONTLIE_API_KEY` set.

## One command per step

1) Init DB schema  
`python - <<'PY'\nfrom new_model.src.db import init_db\nfrom new_model.src.config import DB_PATH\ninit_db(DB_PATH)\nprint(f\"Initialized {DB_PATH}\")\nPY`

2) Backfill games for a date range  
`python new_model/src/games_collector.py --date-range 2025-10-01:2025-12-31`

3) Collect odds once for today  
`python new_model/src/odds_collector.py --date $(date -u +%Y-%m-%d) --once`

4) Derive closing lines (median or a vendor)  
`python new_model/src/market_line.py --date $(date -u +%Y-%m-%d) --vendor-rule median --minutes-before-tip 1`

5) Build features for a date range  
`python new_model/src/features.py --date-range 2025-10-01:2025-12-31`

6) Train models (margin + total)  
`python new_model/src/train_margin.py && python new_model/src/train_total.py`

7) Run backtest (walk-forward)  
`python new_model/src/backtest.py --start 2025-11-01 --end 2025-12-31 --edge-threshold 1.0`

8) Generate predictions JSON for a date  
`python new_model/src/predict.py --date $(date -u +%Y-%m-%d) --vendor-rule median`

## Codespaces quick test (today’s predictions + publish + serve)
1) Generate today’s predictions and publish  
`python new_model/src/predict.py --date 2025-12-18 --vendor-rule draftkings`  
`python new_model/src/publish_predictions.py --date 2025-12-18`

2) Serve site (if no other static server available)  
`python -m http.server 8000 --directory public`

3) Open `/new-model.html` in the served site and confirm the table populates.

## Notes
- Outputs: models in `new_model/models/`, metrics in `new_model/reports/`, predictions in `new_model/output/`.
- DB lives at `new_model/data/new_model.sqlite` (configurable via `NEW_MODEL_DB_PATH`).  
- Schema is defined in `new_model/sql/schema.sql`.  
- Odds workflow uploads `new_model/data/` as an artifact (not committed).  
