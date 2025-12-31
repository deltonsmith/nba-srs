param(
  [switch]$RegenModel
)

$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot
Set-Location $repo

Write-Host "Computing last-7-days window (America/Chicago cutoffs)"
python scripts\last7days_range.py

Write-Host "`nRunning upset audit..."
python scripts\upset_audit.py

Write-Host "`nRunning upset diagnosis..."
python scripts\upset_diagnosis.py

if ($RegenModel) {
  Write-Host "`nRegenerating model artifacts..."
  python new_model\src\train_margin.py
  python new_model\src\train_total.py
  python new_model\src\calibrate_winprob.py
  Write-Host "Model regeneration complete."
} else {
  Write-Host "Model regeneration skipped. Use -RegenModel to enable."
}

Write-Host "`nDone. Outputs written under data/."
