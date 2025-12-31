# New model improvement plan (last 7 days)

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
