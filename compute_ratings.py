# compute_ratings.py

import os
import json
import sqlite3
import csv

from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path


  # ------------ DATA LOADING ------------

  def load_player_values(conn, season_int):
      """
      Return dict: player_id -> v_p (per-game value) for this season.
      """
      cur = conn.cursor()
      cur.execute(
          """
          SELECT player_id, v_p
          FROM player_values
          WHERE season = ?
          """,
          (season_int,),
      )
      return {pid: v for (pid, v) in cur.fetchall()}


  def compute_team_full_values(conn, player_values, season_int, core_size=8):
      """
      For each team, compute its 'full-strength' value by summing v_p
      for its top N (core_size) players by minutes played over the season.
      Returns dict: team_id -> full_strength_v.
      """
      cur = conn.cursor()
      cur.execute(
          """
          SELECT a.team_id, p.player_id, SUM(a.minutes) AS total_min
          FROM appearances a
          JOIN players p ON a.player_id = p.player_id
          WHERE p.season = ?
          GROUP BY a.team_id, p.player_id
          """,
          (season_int,),
      )
      rows = cur.fetchall()

      by_team = defaultdict(list)
      for team_id, player_id, total_min in rows:
          v = player_values.get(player_id, 0.0)
          by_team[team_id].append((player_id, total_min, v))

      full_values = {}
      for team_id, plist in by_team.items():
          plist.sort(key=lambda x: x[1], reverse=True)
          core = plist[:core_size]
          full_values[team_id] = sum(v for _, _, v in core)

      return full_values


  def compute_game_records(conn, player_values, team_full_values, season_int):
      """
      Build per-team game records with lineup-adjusted margins.

      Returns:
          records_by_team: dict[team_id] -> list of records, each:
              {
                  "opp": opp_team_id,
                  "M_adj": adjusted_margin,
                  "home_flag": +1 for home, -1 for away
              }
      """
      cur = conn.cursor()

      # All games for this season
      cur.execute(
          """
          SELECT game_id, date, home_team_id, away_team_id, home_pts, away_pts
          FROM games
          WHERE season = ?
          """,
          (season_int,),
      )
      games = cur.fetchall()

      # All appearances (all seasons) - we key by (game_id, team_id)
      cur.execute(
          """
          SELECT game_id, team_id, player_id
          FROM appearances
          """
      )
      app_rows = cur.fetchall()

      appearances_by_game_team = defaultdict(list)
      for game_id_raw, team_id, player_id in app_rows:
          gid = str(game_id_raw).zfill(10)
          appearances_by_game_team[(gid, team_id)].append(player_id)

      records_by_team = defaultdict(list)

      for game_id_raw, date, home_team, away_team, home_pts, away_pts in games:
          game_id = str(game_id_raw).zfill(10)

          home_players = appearances_by_game_team[(game_id, home_team)]
          away_players = appearances_by_game_team[(game_id, away_team)]

          v_full_home = team_full_values.get(home_team, 0.0)
          v_full_away = team_full_values.get(away_team, 0.0)

          v_game_home = sum(player_values.get(pid, 0.0) for pid in home_players)
          v_game_away = sum(player_values.get(pid, 0.0) for pid in away_players)

          # Lineup deviations from full-strength
          L_home = v_game_home - v_full_home
          L_away = v_game_away - v_full_away

          # Expected margin from lineups
          E_home = L_home - L_away

          # Actual margin from the box score
          margin_home = home_pts - away_pts

          # Lineup-adjusted margin from home perspective
          M_adj_home = margin_home - E_home

          records_by_team[home_team].append(
              {
                  "opp": away_team,
                  "M_adj": M_adj_home,
                  "home_flag": 1,
              }
          )

          # Away perspective (mirror)
          E_away = -E_home
          margin_away = -margin_home
          M_adj_away = margin_away - E_away

          records_by_team[away_team].append(
              {
                  "opp": home_team,
                  "M_adj": M_adj_away,
                  "home_flag": -1,
              }
          )

      return records_by_team


  # ------------ SRS ITERATION ------------

  def iterate_ratings(records_by_team):
      """
      Solve SRS-style ratings via fixed-point iteration.

      ratings[t] ≈ average( M_adj - HCA*home_flag + opp_rating )
      """
      teams = sorted(records_by_team.keys())
      ratings = {t: 0.0 for t in teams}

      for _ in range(MAX_ITERS):
          new_ratings = {}

          for t in teams:
              recs = records_by_team[t]
              if not recs:
                  # no games: carry previous rating
                  new_ratings[t] = ratings[t]
                  continue

              avg_M_adj = sum(r["M_adj"] for r in recs) / len(recs)
              avg_home_flag = sum(r["home_flag"] for r in recs) / len(recs)
              avg_opp_rating = sum(ratings[r["opp"]] for r in recs) / len(recs)

              new_ratings[t] = (avg_M_adj - HCA * avg_home_flag) + avg_opp_rating

          ratings = new_ratings

      # Normalize so league average = 0
      if ratings:
          mean_rating = sum(ratings.values()) / len(ratings)
          ratings = {t: r - mean_rating for t, r in ratings.items()}

      return ratings


  # ------------ OUTPUT HELPERS ------------

  def load_yesterday_ranks(daily_json_path: Path):
      """
      Load yesterday's ranks from the existing daily ratings file, if present.

      Returns dict: team_id -> rank (int)
      """
      if not daily_json_path.exists():
          print("No prior daily ratings file found; Yest column will be blank this run.")
          return {}

      try:
          with daily_json_path.open("r", encoding="utf-8") as f:
              prev_data = json.load(f)
      except Exception as e:
          print(f"Warning: could not load yesterday's ranks from {daily_json_path}: {e}")
          return {}

      mapping = {}
      if isinstance(prev_data, list):
          for entry in prev_data:
              team_id = entry.get("team")
              prev_rank = entry.get("rank")
              if team_id is not None and prev_rank is not None:
                  try:
                      mapping[team_id] = int(prev_rank)
                  except (TypeError, ValueError):
                      continue

      print(f"Loaded yesterday's ranks for {len(mapping)} teams from {daily_json_path}.")
      return mapping


  def parse_ranks_from_csv(csv_path: Path):
      """
      Return dict of team -> rank from a ratings CSV snapshot.
      """
      ranks = {}
      try:
          with csv_path.open("r", encoding="utf-8") as f:
              reader = csv.DictReader(f)
              for row in reader:
                  team = row.get("team")
                  rank = row.get("rank")
                  if team and rank is not None:
                      try:
                          ranks[team] = int(rank)
                      except (TypeError, ValueError):
                          continue
      except Exception as e:
          print(f"Warning: could not parse ranks from {csv_path}: {e}")
          return {}

      return ranks


  def load_prev_day_ranks_from_csv(season_int: int, today: datetime.date):
      """
      Load the most recent CSV snapshot before 'today' for yesterday ranks.
      """
      csv_dir = DATA_DIR / "csv"
      pattern = f"ratings_{season_int}_*.csv"
      candidates = []

      for path in csv_dir.glob(pattern):
          try:
              date_part = path.stem.split("_")[-1]
              snap_date = datetime.strptime(date_part, "%Y%m%d").date()
          except Exception:
              continue
          if snap_date < today:
              candidates.append((snap_date, path))

      if not candidates:
          return {}

      latest_date, latest_path = max(candidates, key=lambda x: x[0])
      ranks = parse_ranks_from_csv(latest_path)
      print(f"Loaded prev-day ranks ({latest_date}) from {latest_path}.")
      return ranks


  def load_last_week_ranks_from_csv(season_int: int, today: datetime.date):
      """
      Load ranks from the most recent Monday snapshot on or before last Monday.
      If no Monday snapshot exists, fall back to the latest snapshot at least 7 days old.
      """
      csv_dir = DATA_DIR / "csv"
      pattern = f"ratings_{season_int}_*.csv"
      target_monday = today - timedelta(days=today.weekday() or 7)

      best = None
      for path in csv_dir.glob(pattern):
          try:
              date_part = path.stem.split("_")[-1]
              snap_date = datetime.strptime(date_part, "%Y%m%d").date()
          except Exception:
              continue

          if snap_date <= target_monday:
              if snap_date.weekday() == 0:  # prefer Mondays
                  if best is None or snap_date > best[0]:
                      best = (snap_date, path)
              elif best is None:
                  best = (snap_date, path)

      if best is None:
          return {}

      snap_date, snap_path = best
      ranks = parse_ranks_from_csv(snap_path)
      print(f"Loaded last-week ranks ({snap_date}) from {snap_path}.")
      return ranks


  def save_ratings_json(ratings, last_week_ranks, yesterday_ranks, path):
      """
      Save ratings to JSON with:
        - current rank
        - yest_rank (yesterday's rank)
        - last_week_rank (rank as of latest weekly snapshot)

      ratings: dict[team] -> current rating
      last_week_ranks: dict[team] -> rank from weekly snapshot
      yesterday_ranks: dict[team] -> rank from prior daily file
      path: output JSON path
      """
      # Sort teams best to worst by current rating
      sorted_items = sorted(ratings.items(), key=lambda x: x[1], reverse=True)

      data = []
      for rank, (team, rating) in enumerate(sorted_items, start=1):
          lw_rank = last_week_ranks.get(team)
          yest_rank = yesterday_ranks.get(team)

          entry = {
              "team": team,
              "rating": float(rating),
              "rank": rank,
              "yest_rank": int(yest_rank) if yest_rank is not None else None,
              "last_week_rank": int(lw_rank) if lw_rank is not None else None,
          }
          data.append(entry)

      with open(path, "w", encoding="utf-8") as f:
          json.dump(data, f, indent=2)


  def write_ratings_csv(ratings_dict, season_int):
      """
      Write a dated CSV snapshot of ratings for a season.

      ratings_dict: dict[team] -> rating
      season_int:   e.g. 2026
      """
      today = datetime.today().date()
      date_str = today.strftime("%Y%m%d")

      out_dir = DATA_DIR / "csv"
      out_dir.mkdir(parents=True, exist_ok=True)

      out_path = out_dir / f"ratings_{season_int}_{date_str}.csv"

      # Sort teams best to worst
      sorted_items = sorted(ratings_dict.items(), key=lambda x: x[1], reverse=True)

      with open(out_path, "w", newline="", encoding="utf-8") as f:
          w = csv.writer(f)
          w.writerow(["date", "season", "rank", "team", "rating"])
          for rank, (team, rating) in enumerate(sorted_items, start=1):
              w.writerow(
                  [
                      today.isoformat(),
                      season_int,
                      rank,
                      team,
                      float(rating),
                  ]
              )
      print(f"Saved CSV to {out_path}")


  # ------------ PER-SEASON RUNNER ------------

  def run_season(season_int):
      conn = sqlite3.connect(DB_PATH)

      player_values = load_player_values(conn, season_int)
      print(f"Loaded {len(player_values)} player_values rows for season {season_int}.")

      team_full_values = compute_team_full_values(conn, player_values, season_int)
      print(f"Computed full-strength values for {len(team_full_values)} teams.")

      records_by_team = compute_game_records(conn, player_values, team_full_values, season_int)
      print(f"Built game records for {len(records_by_team)} teams.")

      conn.close()

      # Paths for daily ratings and the weekly snapshot
      DATA_DIR.mkdir(parents=True, exist_ok=True)

      current_json_path = DATA_DIR / f"ratings_{season_int}.json"
      weekly_json_path = DATA_DIR / f"ratings_{season_int}_weekly.json"

      today_utc = datetime.utcnow().date()

      # Load yesterday's ranks from the most recent CSV snapshot (preferred), or fallback to prior daily JSON
      yesterday_ranks = load_prev_day_ranks_from_csv(season_int, today_utc)
      if not yesterday_ranks:
          yesterday_ranks = load_yesterday_ranks(current_json_path)

      # Load last week's ranks from weekly snapshot, if present (for LW column)
      last_week_ranks = load_last_week_ranks_from_csv(season_int, today_utc)
      if not last_week_ranks:
          last_week_ranks = {}
      if weekly_json_path.exists():
          try:
              with open(weekly_json_path, "r", encoding="utf-8") as f):
                  weekly_data = json.load(f)
              for entry in weekly_data:
                  team_id = entry.get("team")
                  rank_val = entry.get("rank")
                  if team_id is not None and rank_val is not None:
                      try:
                          last_week_ranks[team_id] = int(rank_val)
                      except (TypeError, ValueError):
                          continue
              print(
                  f"Loaded weekly snapshot for {len(last_week_ranks)} teams from {weekly_json_path}."
              )
          except Exception as e:
              print(f"Warning: could not load weekly snapshot from {weekly_json_path}: {e}")
              last_week_ranks = {}
      else:
          if last_week_ranks:
              print("No weekly snapshot file found; using CSV fallback for LW.")
          else:
              print("No weekly snapshot file found and no CSV fallback; LW column will be blank this run.")

      # Compute current ratings from games
      ratings = iterate_ratings(records_by_team)
      print(f"Final ratings for season {season_int}:")
      for team, r in sorted(ratings.items(), key=lambda x: x[1], reverse=True):
          print(f"{team}: {r:.3f}")

      # Save daily JSON with Yest and LW rank fields
      save_ratings_json(ratings, last_week_ranks, yesterday_ranks, current_json_path)
      print(f"Saved daily JSON to {current_json_path}")

      # Save dated CSV snapshot
      write_ratings_csv(ratings, season_int)

      # If today is Monday (UTC), refresh the weekly snapshot file
      if today_utc.weekday() == 0:  # Monday = 0
          snapshot = []
          sorted_items = sorted(ratings.items(), key=lambda x: x[1], reverse=True)
          for rank, (team, rating) in enumerate(sorted_items, start=1):
              snapshot.append(
                      "rating": float(rating),
                      "rank": rank,
                  }
              )
          with open(weekly_json_path, "w", encoding="utf-8") as f:
              json.dump(snapshot, f, indent=2)
          print(f"Updated weekly snapshot {weekly_json_path} for LW reference.")
      else:
  # ------------ MAIN ------------

  def main():
      for season_int in SEASONS:
          print(f"=== Running ratings for season {season_int} ===")
          run_season(season_int)


  if __name__ == "__main__":
      main()


› ok i've replaced it with that. A new workflow is now in progress


• Nice—once the workflow finishes, spot-check data/ratings_2026.json on master to confirm yest_rank and last_week_rank
  differ from today’s rank where prior snapshots exist. If anything fails in the run, paste the log excerpt and I’ll
  help debug.


› I got an error message again under build->compute ratings (includes Lw/Yesterday logic):

  0s
  Run if [ -f compute_ratings.py ]; then
    File "/home/runner/work/nba-srs/nba-srs/compute_ratings.py", line 10
      import sqlite3
  IndentationError: unexpected indent
  Error: Process completed with exit code 1.


• I'm removing leading spaces and any stray characters before imports to ensure they start at column 1, fixing the
  extra closing parenthesis syntax error, and cleaning the file from BOM or non-breaking spaces. I'll provide a clean,
  corrected file version and suggest replacing the file entirely via the GitHub editor.

─ Worked for 5s ────────────────────────────────────────────────────────────────────────────────────────────────────────

• The file in master still has indentation/syntax issues. Easiest fix: replace compute_ratings.py with the clean content
  below (imports start at column 1, no stray spaces, and the weekly-open bug is fixed).

  # compute_ratings.py
  #
  # Lineup-adjusted SRS ratings for multiple NBA seasons.
  # - Uses games(season), appearances, players, player_values(v_p).
  # - Outputs ratings_<SEASON_INT>.json into data/.
  # - Also writes a dated CSV snapshot to data/csv/ratings_<SEASON_INT>_YYYYMMDD.csv.
  # - Includes last_week_rank based on a weekly snapshot file.
  # - Includes yest_rank based on the prior daily ratings file.

  import sqlite3
  import json
  from collections import defaultdict
  from datetime import datetime, timedelta
  from pathlib import Path
  import csv

  # ------------ CONFIGURATION ------------

  BASE_DIR = Path(__file__).resolve().parent
  DATA_DIR = BASE_DIR / "data"
  DB_PATH = DATA_DIR / "nba_ratings.db"

  # Seasons are labeled by the year they END:
  # 2023-24 -> 2024, 2024-25 -> 2025, 2025-26 -> 2026, etc.
  SEASONS = [2026]

  HCA = 2.5          # home-court advantage in points
  MAX_ITERS = 100    # SRS iteration count


  # ------------ DATA LOADING ------------

  def load_player_values(conn, season_int):
      """Return dict: player_id -> v_p (per-game value) for this season."""
      cur = conn.cursor()
      cur.execute(
          """
          SELECT player_id, v_p
          FROM player_values
          WHERE season = ?
          """,
          (season_int,),
      )
      return {pid: v for (pid, v) in cur.fetchall()}


  def compute_team_full_values(conn, player_values, season_int, core_size=8):
      """
      For each team, compute its 'full-strength' value by summing v_p
      for its top N (core_size) players by minutes played over the season.
      Returns dict: team_id -> full_strength_v.
      """
      cur = conn.cursor()
      cur.execute(
          """
          SELECT a.team_id, p.player_id, SUM(a.minutes) AS total_min
          FROM appearances a
          JOIN players p ON a.player_id = p.player_id
          WHERE p.season = ?
          GROUP BY a.team_id, p.player_id
          """,
          (season_int,),
      )
      rows = cur.fetchall()

      by_team = defaultdict(list)
      for team_id, player_id, total_min in rows:
          v = player_values.get(player_id, 0.0)
          by_team[team_id].append((player_id, total_min, v))

      full_values = {}
      for team_id, plist in by_team.items():
          plist.sort(key=lambda x: x[1], reverse=True)
          core = plist[:core_size]
          full_values[team_id] = sum(v for _, _, v in core)

      return full_values


  def compute_game_records(conn, player_values, team_full_values, season_int):
      """
      Build per-team game records with lineup-adjusted margins.
      Returns dict[team_id] -> list of records with opp, M_adj, home_flag.
      """
      cur = conn.cursor()

      cur.execute(
          """
          SELECT game_id, date, home_team_id, away_team_id, home_pts, away_pts
          FROM games
          WHERE season = ?
          """,
          (season_int,),
      )
      games = cur.fetchall()

      cur.execute(
          """
          SELECT game_id, team_id, player_id
          FROM appearances
          """
      )
      app_rows = cur.fetchall()

      appearances_by_game_team = defaultdict(list)
      for game_id_raw, team_id, player_id in app_rows:
          gid = str(game_id_raw).zfill(10)
          appearances_by_game_team[(gid, team_id)].append(player_id)

      records_by_team = defaultdict(list)

      for game_id_raw, date, home_team, away_team, home_pts, away_pts in games:
          game_id = str(game_id_raw).zfill(10)

          home_players = appearances_by_game_team[(game_id, home_team)]
          away_players = appearances_by_game_team[(game_id, away_team)]

          v_full_home = team_full_values.get(home_team, 0.0)
          v_full_away = team_full_values.get(away_team, 0.0)

          v_game_home = sum(player_values.get(pid, 0.0) for pid in home_players)
          v_game_away = sum(player_values.get(pid, 0.0) for pid in away_players)

          L_home = v_game_home - v_full_home
          L_away = v_game_away - v_full_away
          E_home = L_home - L_away

          margin_home = home_pts - away_pts
          M_adj_home = margin_home - E_home

          records_by_team[home_team].append(
              {"opp": away_team, "M_adj": M_adj_home, "home_flag": 1}
          )

          E_away = -E_home
          margin_away = -margin_home
          M_adj_away = margin_away - E_away

          records_by_team[away_team].append(
              {"opp": home_team, "M_adj": M_adj_away, "home_flag": -1}
          )

      return records_by_team


  # ------------ SRS ITERATION ------------

  def iterate_ratings(records_by_team):
      """
      Solve SRS-style ratings via fixed-point iteration.
      ratings[t] ≈ average( M_adj - HCA*home_flag + opp_rating )
      """
      teams = sorted(records_by_team.keys())
      ratings = {t: 0.0 for t in teams}

      for _ in range(MAX_ITERS):
          new_ratings = {}
          for t in teams:
              recs = records_by_team[t]
              if not recs:
                  new_ratings[t] = ratings[t]
                  continue
              avg_M_adj = sum(r["M_adj"] for r in recs) / len(recs)
              avg_home_flag = sum(r["home_flag"] for r in recs) / len(recs)
              avg_opp_rating = sum(ratings[r["opp"]] for r in recs) / len(recs)
              new_ratings[t] = (avg_M_adj - HCA * avg_home_flag) + avg_opp_rating
          ratings = new_ratings

      if ratings:
          mean_rating = sum(ratings.values()) / len(ratings)
          ratings = {t: r - mean_rating for t, r in ratings.items()}

      return ratings


  # ------------ OUTPUT HELPERS ------------

  def load_yesterday_ranks(daily_json_path: Path):
      """Load yesterday's ranks from the existing daily ratings file, if present."""
      if not daily_json_path.exists():
          print("No prior daily ratings file found; Yest column will be blank this run.")
          return {}

      try:
          with daily_json_path.open("r", encoding="utf-8") as f:
              prev_data = json.load(f)
      except Exception as e:
          print(f"Warning: could not load yesterday's ranks from {daily_json_path}: {e}")
          return {}

      mapping = {}
      if isinstance(prev_data, list):
          for entry in prev_data:
              team_id = entry.get("team")
              prev_rank = entry.get("rank")
              if team_id is not None and prev_rank is not None:
                  try:
                      mapping[team_id] = int(prev_rank)
                  except (TypeError, ValueError):
                      continue

      print(f"Loaded yesterday's ranks for {len(mapping)} teams from {daily_json_path}.")
      return mapping


  def parse_ranks_from_csv(csv_path: Path):
      """Return dict of team -> rank from a ratings CSV snapshot."""
      ranks = {}
      try:
          with csv_path.open("r", encoding="utf-8") as f:
              reader = csv.DictReader(f)
              for row in reader:
                  team = row.get("team")
                  rank = row.get("rank")
                  if team and rank is not None:
                      try:
                          ranks[team] = int(rank)
                      except (TypeError, ValueError):
                          continue
      except Exception as e:
          print(f"Warning: could not parse ranks from {csv_path}: {e}")
          return {}
      return ranks


  def load_prev_day_ranks_from_csv(season_int: int, today: datetime.date):
      """Load the most recent CSV snapshot before 'today' for yesterday ranks."""
      csv_dir = DATA_DIR / "csv"
      pattern = f"ratings_{season_int}_*.csv"
      candidates = []
      for path in csv_dir.glob(pattern):
          try:
              date_part = path.stem.split("_")[-1]
              snap_date = datetime.strptime(date_part, "%Y%m%d").date()
          except Exception:
              continue
          if snap_date < today:
              candidates.append((snap_date, path))
      if not candidates:
          return {}
      latest_date, latest_path = max(candidates, key=lambda x: x[0])
      ranks = parse_ranks_from_csv(latest_path)
      print(f"Loaded prev-day ranks ({latest_date}) from {latest_path}.")
      return ranks


  def load_last_week_ranks_from_csv(season_int: int, today: datetime.date):
      """
      Load ranks from the most recent Monday snapshot on or before last Monday.
      If no Monday snapshot exists, fall back to the latest snapshot at least 7 days old.
      """
      csv_dir = DATA_DIR / "csv"
      pattern = f"ratings_{season_int}_*.csv"
      target_monday = today - timedelta(days=today.weekday() or 7)

      best = None
      for path in csv_dir.glob(pattern):
          try:
              date_part = path.stem.split("_")[-1]
              snap_date = datetime.strptime(date_part, "%Y%m%d").date()
          except Exception:
              continue
          if snap_date <= target_monday:
              if snap_date.weekday() == 0:
                  if best is None or snap_date > best[0]:
                      best = (snap_date, path)
              elif best is None:
                  best = (snap_date, path)

      if best is None:
          return {}
      snap_date, snap_path = best
      ranks = parse_ranks_from_csv(snap_path)
      print(f"Loaded last-week ranks ({snap_date}) from {snap_path}.")
      return ranks


  def save_ratings_json(ratings, last_week_ranks, yesterday_ranks, path):
      """Save ratings to JSON with rank, yest_rank, last_week_rank."""
      sorted_items = sorted(ratings.items(), key=lambda x: x[1], reverse=True)
      data = []
      for rank, (team, rating) in enumerate(sorted_items, start=1):
          lw_rank = last_week_ranks.get(team)
          yest_rank = yesterday_ranks.get(team)
          data.append(
              {
                  "team": team,
                  "rating": float(rating),
                  "rank": rank,
                  "yest_rank": int(yest_rank) if yest_rank is not None else None,
                  "last_week_rank": int(lw_rank) if lw_rank is not None else None,
              }
          )
      with open(path, "w", encoding="utf-8") as f:
          json.dump(data, f, indent=2)


  def write_ratings_csv(ratings_dict, season_int):
      """Write a dated CSV snapshot of ratings for a season."""
      today = datetime.today().date()
      date_str = today.strftime("%Y%m%d")
      out_dir = DATA_DIR / "csv"
      out_dir.mkdir(parents=True, exist_ok=True)
      out_path = out_dir / f"ratings_{season_int}_{date_str}.csv"
      sorted_items = sorted(ratings_dict.items(), key=lambda x: x[1], reverse=True)
      with open(out_path, "w", newline="", encoding="utf-8") as f:
          w = csv.writer(f)
          w.writerow(["date", "season", "rank", "team", "rating"])
          for rank, (team, rating) in enumerate(sorted_items, start=1):
              w.writerow([today.isoformat(), season_int, rank, team, float(rating)])
      print(f"Saved CSV to {out_path}")


  # ------------ PER-SEASON RUNNER ------------

  def run_season(season_int):
      conn = sqlite3.connect(DB_PATH)

      player_values = load_player_values(conn, season_int)
      print(f"Loaded {len(player_values)} player_values rows for season {season_int}.")

      team_full_values = compute_team_full_values(conn, player_values, season_int)
      print(f"Computed full-strength values for {len(team_full_values)} teams.")

      records_by_team = compute_game_records(conn, player_values, team_full_values, season_int)
      print(f"Built game records for {len(records_by_team)} teams.")

      conn.close()

      DATA_DIR.mkdir(parents=True, exist_ok=True)
      current_json_path = DATA_DIR / f"ratings_{season_int}.json"
      weekly_json_path = DATA_DIR / f"ratings_{season_int}_weekly.json"

      today_utc = datetime.utcnow().date()

      yesterday_ranks = load_prev_day_ranks_from_csv(season_int, today_utc)
      if not yesterday_ranks:
          yesterday_ranks = load_yesterday_ranks(current_json_path)

      last_week_ranks = load_last_week_ranks_from_csv(season_int, today_utc)
      if not last_week_ranks:
          last_week_ranks = {}
      if weekly_json_path.exists():
          try:
              with open(weekly_json_path, "r", encoding="utf-8") as f:
                  weekly_data = json.load(f)
              for entry in weekly_data:
                  team_id = entry.get("team")
                  rank_val = entry.get("rank")
                  if team_id is not None and rank_val is not None:
                      try:
                          last_week_ranks[team_id] = int(rank_val)
                      except (TypeError, ValueError):
                          continue
              print(f"Loaded weekly snapshot for {len(last_week_ranks)} teams from {weekly_json_path}.")
          except Exception as e:
              print(f"Warning: could not load weekly snapshot from {weekly_json_path}: {e}")
              last_week_ranks = {}
      else:
          if last_week_ranks:
              print("No weekly snapshot file found; using CSV fallback for LW.")
          else:
              print("No weekly snapshot file found and no CSV fallback; LW column will be blank this run.")

      ratings = iterate_ratings(records_by_team)
      print(f"Final ratings for season {season_int}:")
      for team, r in sorted(ratings.items(), key=lambda x: x[1], reverse=True):
          print(f"{team}: {r:.3f}")

      save_ratings_json(ratings, last_week_ranks, yesterday_ranks, current_json_path)
      print(f"Saved daily JSON to {current_json_path}")

      write_ratings_csv(ratings, season_int)

      if today_utc.weekday() == 0:  # Monday
          snapshot = []
          sorted_items = sorted(ratings.items(), key=lambda x: x[1], reverse=True)
          with open(weekly_json_path, "w", encoding="utf-8") as f:
              json.dump(snapshot, f, indent=2)
          print(f"Updated weekly snapshot {weekly_json_path} for LW reference.")
      else:
          print("Not Monday UTC; weekly snapshot left unchanged.")

      print("-" * 40)


  # ------------ MAIN ------------

  def main():
      for season_int in SEASONS:
          print(f"=== Running ratings for season {season_int} ===")
          run_season(season_int)


  if __name__ == "__main__":
      main()
