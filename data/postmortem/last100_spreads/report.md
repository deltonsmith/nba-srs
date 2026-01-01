# Last 100 spread bets postmortem

ATS record: 40-60 (hit rate 40.0%)
MAE (model_line vs actual_margin): 2.61

Model-market divergence distribution (model_line - market_line, home line):
- mean: 0.49
- median: 0.60
- p90: 2.20
- p95: 2.90
- abs>=18 count: 0 (0.0%)

Top 15 features by standardized coefficient magnitude:
- ortg_r30_delta: 1.826
- tov_pct_r30_delta: 1.316
- ortg_r10_delta: 1.043
- orb_pct_r30_delta: 1.027
- efg_r30_delta: 0.826
- drtg_blend_delta: 0.819
- pace_est_r10_delta: 0.597
- netrtg_r10_delta: 0.540
- tov_pct_r10_delta: 0.449
- ftr_r10_delta: 0.430
- efg_r10_delta: 0.268
- pace_est_r30_delta: 0.223
- rest_diff: 0.175
- orb_pct_r10_delta: 0.133
- ts_r10_delta: 0.128

TWEAKS TO TEST NEXT
- cap abs(model-market divergence) at 18
- cap short-term efficiency contribution to spread at +/- 4 points
- DRtg blend: 0.7*30g + 0.3*10g
- pace cap: +/- 6 total possessions
