# pull_bbr_advanced.py
#
# Automatically scrape Basketball-Reference advanced stats
# and generate player_values_YYYY.csv for lineup-adjusted SRS.

import pandas as pd
from pathlib import Path

# ---- CHANGE THIS FOR THE SEASON YOU WANT ----
# 2024 = 2023-24 season, 2026 = 2025-26 season, etc.
SEASON_INT = 2026

OUT_CSV = Path("data") / f"player_values_{SEASON_INT}.csv"
URL = f"https://www.basketball-reference.com/leagues/NBA_{SEASON_INT}_advanced.html"


def find_col(df, target):
  """Find a column whose normalized name matches target."""
  target_up = target.upper()
  for c in df.columns:
    name = str(c).strip().upper()
    if name == target_up:
      return c
  raise KeyError(target)


def main():
  print(f"Fetching Basketball-Reference advanced stats for season ending {SEASON_INT}")
  print(f"URL = {URL}")

  tables = pd.read_html(URL, header=0)
  if not tables:
    raise RuntimeError("No tables found on Basketball-Reference page.")

  df = tables[0]
  print("Columns from BBR:", list(df.columns))

  # Locate key columns
  player_col = find_col(df, "Player")

  # Team column: try 'Tm' first, then 'Team'
  try:
    team_col = find_col(df, "Tm")
  except KeyError:
    team_col = find_col(df, "Team")

  g_col = find_col(df, "G")
  mp_col = find_col(df, "MP")
  bpm_col = find_col(df, "BPM")

  # Drop repeated header rows
  df = df[df[player_col] != "Player"]

  # Drop 'TOT' combined rows
  df = df[df[team_col] != "TOT"]

  # Keep only relevant columns
  df = df[[player_col, team_col, g_col, mp_col, bpm_col]].copy()

  # Convert numeric
  for col in [g_col, mp_col, bpm_col]:
    df[col] = pd.to_numeric(df[col], errors="coerce")

  # Drop unusable rows
  df = df.dropna(subset=[player_col, team_col, g_col, mp_col, bpm_col])

  # Filter by total minutes
  df = df[df[mp_col] >= 200]

  # Compute minutes per game
  df["MIN_PER_GAME"] = df[mp_col] / df[g_col]

  # Build output in our schema
  out = df[[player_col, team_col, bpm_col, "MIN_PER_GAME"]].copy()
  out.rename(
    columns={
      player_col: "PLAYER_NAME",
      team_col: "TEAM_ABBREVIATION",
      bpm_col: "METRIC_RAW",
    },
    inplace=True,
  )

  out = out.sort_values(["TEAM_ABBREVIATION", "PLAYER_NAME"]).reset_index(drop=True)

  OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
  print(f"Writing {len(out)} player rows to: {OUT_CSV}")
  out.to_csv(OUT_CSV, index=False)
  print("Done.")


if __name__ == "__main__":
  main()
