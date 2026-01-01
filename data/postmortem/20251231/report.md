# Postmortem 2025-12-31

## Spread correction
Top coefficients (standardized magnitude):
- drtg_r30_delta: 7.662
- netrtg_r30_delta: 5.910
- efg_r10_delta: 4.366
- ts_r10_delta: 4.179
- ortg_r10_delta: 4.032
- tov_pct_r30_delta: 3.587
- tov_pct_r10_delta: 2.560
- drtg_r10_delta: 2.484
- netrtg_r10_delta: 2.311
- ftr_r30_delta: 2.303

Top correlations (abs):
- drtg_r30_delta: 0.868
- efg_r10_delta: 0.810
- ts_r10_delta: 0.791
- ortg_r10_delta: 0.733
- drtg_r10_delta: 0.725
- pace_est_r30_delta: 0.582
- pace_est_r10_delta: 0.443
- netrtg_r30_delta: 0.353
- ftr_r10_delta: 0.268
- efg_r30_delta: 0.257

## Total correction
Top coefficients (standardized magnitude):
- ortg_r10_delta: nan
- drtg_r10_delta: nan
- netrtg_r10_delta: nan
- pace_est_r10_delta: nan
- efg_r10_delta: nan
- ts_r10_delta: nan
- ftr_r10_delta: nan
- tov_pct_r10_delta: nan
- orb_pct_r10_delta: nan
- ortg_r30_delta: nan

Top correlations (abs):
- ortg_r10_delta: nan
- drtg_r10_delta: nan
- netrtg_r10_delta: nan
- pace_est_r10_delta: nan
- efg_r10_delta: nan
- ts_r10_delta: nan
- ftr_r10_delta: nan
- tov_pct_r10_delta: nan
- orb_pct_r10_delta: nan
- ortg_r30_delta: nan

## Tweaks to test next
- edge_haircut factor k=0.00 for spread edges
- edge_haircut factor k=0.68 for total edges
- cap pace contribution to +/- 5 possessions
- cap ORtg/DRtg deltas to +/- 8 points
- increase/decrease FTr sensitivity by 10%
