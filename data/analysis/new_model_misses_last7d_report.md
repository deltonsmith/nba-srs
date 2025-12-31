# New model misses last 7 days

## 1) Coverage / integrity checks
- games in date range: 40
- games with predictions: 53
- games with outcomes: 40
- games successfully joined: 40

## 2) Summary stats
- spread picks: 29 decisions, 2 hits, hit rate 6.9%
- total picks: 40 decisions, 26 hits, hit rate 65.0%
- average abs spread residual: 18.69
- average abs total residual: 70.16

Worst 10 by abs spread residual:
- HOU @ LAL (2025-12-25): spread_residual=-56.64
- IND @ MIA (2025-12-27): spread_residual=48.70
- PHI @ OKC (2025-12-28): spread_residual=46.01
- MIL @ MEM (2025-12-26): spread_residual=38.55
- CLE @ HOU (2025-12-27): spread_residual=36.13
- SAC @ LAL (2025-12-28): spread_residual=35.25
- BOS @ IND (2025-12-26): spread_residual=-34.58
- BKN @ MIN (2025-12-27): spread_residual=-32.77
- SAS @ OKC (2025-12-25): spread_residual=-32.56
- TOR @ WAS (2025-12-26): spread_residual=31.93

Worst 10 by abs total residual:
- CLE @ SAS (2025-12-29): total_residual=-271.18
- NYK @ NOP (2025-12-29): total_residual=-256.37
- MIN @ CHI (2025-12-29): total_residual=-251.21
- DAL @ POR (2025-12-29): total_residual=-245.39
- DEN @ MIA (2025-12-29): total_residual=-242.20
- GSW @ BKN (2025-12-29): total_residual=-232.26
- ATL @ OKC (2025-12-29): total_residual=-229.56
- MIL @ CHA (2025-12-29): total_residual=-227.27
- IND @ HOU (2025-12-29): total_residual=-220.69
- ORL @ TOR (2025-12-29): total_residual=-218.89

## 3) Reason-code attribution
Spread reasons:
- blowout: 13 (48.1%)
- back_to_back: 6 (22.2%)
- clutch: 3 (11.1%)
- home_adv_miss: 1 (3.7%)

Total reasons:
- pace_outlier: 5 (35.7%)
- efficiency_outlier: 5 (35.7%)

## 4) Narrative
Here is why the model line was wrong the past 7 days:
- HOU @ LAL (2025-12-25): spread_residual=-56.64. Reasons: blowout.
- IND @ MIA (2025-12-27): spread_residual=48.70. Reasons: blowout|back_to_back.
- PHI @ OKC (2025-12-28): spread_residual=46.01. Reasons: blowout.
- MIL @ MEM (2025-12-26): spread_residual=38.55. Reasons: blowout.
- CLE @ HOU (2025-12-27): spread_residual=36.13. Reasons: blowout.

Here is why the model totals were wrong the past 7 days:
- CLE @ SAS (2025-12-29): total_residual=-271.18. Reasons: pace_outlier|efficiency_outlier.
- NYK @ NOP (2025-12-29): total_residual=-256.37. Reasons: pace_outlier|efficiency_outlier.
- MIN @ CHI (2025-12-29): total_residual=-251.21. Reasons: pace_outlier|efficiency_outlier.
- DAL @ POR (2025-12-29): total_residual=-245.39. Reasons: pace_outlier|efficiency_outlier.
- GSW @ BKN (2025-12-29): total_residual=-232.26. Reasons: pace_outlier|efficiency_outlier.

## 5) Concrete fixes
- home_adv_miss: add an explicit home-court feature (e.g., constant or travel-adjusted) in `new_model/src/features.py`, include it in `FEATURE_COLS` used by `new_model/src/train_margin.py`, and re-train; validate by tracking hit rate for close home wins.
- back_to_back: add interaction features like `back_to_back_home` and `back_to_back_away` in `new_model/src/features.py` and include in `FEATURE_COLS`; validate with a before/after split on back-to-back games.
- blowout: add a margin volatility feature (rolling std dev of margin) in `new_model/src/features.py` and re-train in `new_model/src/train_margin.py`; validate by reducing worst-10 spread residuals.
- pace_outlier: include pace volatility or last-N pace delta features in `new_model/src/features.py`, then re-train totals in `new_model/src/train_total.py`; validate by reducing total residuals on high-pace games.
- efficiency_outlier: add shooting efficiency trend features (e.g., rolling eFG variance) in `new_model/src/features.py` and re-train totals; validate by reducing total residuals when eFG swings.
## Indicator study: what would have helped

Home/away pick miss rates (spread):
- home picks: 14 games, miss rate 85.7%
- away picks: 15 games, miss rate 100.0%

Blowout/clutch miss rates (spread):
- blowouts: 13 games, miss rate 100.0%
- non-blowouts: 16 games, miss rate 87.5%
- clutch: 4 games, miss rate 75.0%
- non-clutch: 25 games, miss rate 96.0%

Injury buckets (spread + total miss rates):
- 0: 23 games, spread miss rate 95.7%, total miss rate 34.8%
- 1-2: 3 games, spread miss rate 100.0%, total miss rate 66.7%
- 3+: 14 games, spread miss rate 80.0%, total miss rate 28.6%

Matchup mismatches (spread + total miss rates):
- pace_mismatch: 19 games, spread miss rate 93.3%, total miss rate 47.4%
- efg_mismatch: 26 games, spread miss rate 90.0%, total miss rate 42.3%
- tov_mismatch: 0 games, spread miss rate 0.0%, total miss rate 0.0%
- orb_mismatch: 9 games, spread miss rate 71.4%, total miss rate 22.2%
- ftr_mismatch: 7 games, spread miss rate 100.0%, total miss rate 28.6%

## Implementation Plan
1. Add home-court/close-game adjustment feature
   - Files to change: new_model/src/features.py, new_model/src/train_margin.py
   - Exact code location: compute_features, train_and_eval
   - Acceptance criteria: Close-game spread hit rate improves in backtest; report shows lower home_adv_miss rate.
   - Validation method: Backtest: compare spread hit rate on games with abs(market_spread)<=3 before/after.
2. Add back-to-back interaction features
   - Files to change: new_model/src/features.py, new_model/src/train_margin.py
   - Exact code location: compute_features, train_and_eval
   - Acceptance criteria: Back-to-back bucket miss rate decreases in indicator study output.
   - Validation method: Backtest: segment by back_to_back flag and compare miss rate.
3. Add pace volatility features for totals
   - Files to change: new_model/src/features.py, new_model/src/train_total.py
   - Exact code location: compute_features, train_and_eval
   - Acceptance criteria: Average abs total residual drops; pace_outlier flag rate decreases.
   - Validation method: Backtest: compare MAE total and pace_outlier miss rate before/after.
4. Add efficiency volatility features for totals
   - Files to change: new_model/src/features.py, new_model/src/train_total.py
   - Exact code location: compute_features, train_and_eval
   - Acceptance criteria: Total miss rate decreases on efficiency mismatch games.
   - Validation method: Backtest: segment by efficiency_outlier flag and compare miss rate.
5. Add matchup mismatch feature flags to model inputs
   - Files to change: new_model/src/features.py, new_model/src/train_margin.py, new_model/src/train_total.py
   - Exact code location: compute_features, train_and_eval
   - Acceptance criteria: Mismatch buckets in report show reduced miss rates vs baseline.
   - Validation method: Backtest: compare miss rates for pace/efg/tov/orb/ftr mismatch buckets.
6. Add injury bucket features to model inputs
   - Files to change: new_model/src/features.py, new_model/src/train_margin.py, new_model/src/train_total.py
   - Exact code location: compute_features, train_and_eval
   - Acceptance criteria: Injury bucket miss rates decrease in report; model learns higher variance when injuries spike.
   - Validation method: Backtest: segment by injury buckets and compare miss rates.
7. Expand miss analysis outputs for QA
   - Files to change: scripts/analyze_new_model_misses.py
   - Exact code location: main
   - Acceptance criteria: Report includes Implementation Plan and updated indicator tables.
   - Validation method: Run script and verify `data/analysis/new_model_misses_last7d_report.md` and plan file updated.
