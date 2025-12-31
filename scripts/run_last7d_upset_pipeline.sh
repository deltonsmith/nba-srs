#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

printf "Computing last-7-days window (America/Chicago cutoffs)\n"
python scripts/last7days_range.py

printf "\nRunning upset audit...\n"
python scripts/upset_audit.py

printf "\nRunning upset diagnosis...\n"
python scripts/upset_diagnosis.py

if [[ "${REGEN_MODEL:-0}" == "1" ]]; then
  printf "\nRegenerating model artifacts...\n"
  python new_model/src/train_margin.py
  python new_model/src/train_total.py
  python new_model/src/calibrate_winprob.py
  echo "Model regeneration complete."
else
  echo "Model regeneration skipped. Set REGEN_MODEL=1 to enable."
fi

printf "\nDone. Outputs written under data/.\n"
