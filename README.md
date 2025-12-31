PowerIndex data pipeline and analysis tools.
Run `make analyze-new-model` to generate the last-7-days miss analysis in `data/analysis`.
Or run: `python scripts/analyze_new_model_misses.py --days 7 --model new --out_dir data/analysis`.
## Last 7 Days Upset Pipeline (Codespaces)

```bash
bash scripts/run_last7d_upset_pipeline.sh
```

Optional (regenerate model artifacts before evaluating):

```bash
REGEN_MODEL=1 bash scripts/run_last7d_upset_pipeline.sh
```
### Windows PowerShell

```powershell
powershell -ExecutionPolicy Bypass -File scripts\run_last7d_upset_pipeline.ps1
```

Optional (regenerate model artifacts before evaluating):

```powershell
powershell -ExecutionPolicy Bypass -File scripts\run_last7d_upset_pipeline.ps1 -RegenModel
```
