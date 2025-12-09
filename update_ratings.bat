name: Update PowerIndex ratings

on:
  schedule:
    # Daily at 09:00 UTC (change if you want a different time)
    - cron: "0 9 * * *"
  workflow_dispatch:   # allows manual run from the Actions tab

permissions:
  contents: write      # needed so the workflow can push commits

jobs:
  update-ratings:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.12"

      - name: Install dependencies
        run: |
          if [ -f requirements.txt ]; then
            pip install -r requirements.txt
          fi

      - name: Ingest latest games and compute ratings
        run: |
          python ingest_games.py
          python compute_ratings.py

      - name: Commit and push updated data
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git status
          git add data/*.json data/csv/*.csv || true
          if git diff --cached --quiet; then
            echo "No changes to commit"
            exit 0
          fi
          git commit -m "Automated PowerIndex ratings update"
          git push
