# Upset Diagnosis (Last 7 Days)

Total games: 15
Higher-ranked win rate (last 7 days): 73.3%
Season-to-date higher-ranked win rate: 68.2%
Delta vs baseline: 5.1%

## Top drivers associated with upsets
- Lower-ranked home: 22.2% upset rate (2/9)
- Lower-ranked away: 33.3% upset rate (2/6)
- Higher-ranked rest disadvantage: 0.0% upset rate (0/2)
- Higher-ranked injury disadvantage: n/a upset rate (0/0)
- Style mismatch (pace): n/a
- Style mismatch (three_pa_rate): n/a
- Style mismatch (reb_pct): n/a
- Rating gap buckets (spread proxy):
  - <2: 66.7% upset rate (2/3)
  - 2-5: 33.3% upset rate (1/3)
  - >=5: 11.1% upset rate (1/9)

here's why the win rate over the past 7 days was low

The recent win rate dipped primarily in spots where the lower-ranked team had situational edges
(home court, rest, or injury advantage) and in games with large style mismatches or small rating gaps.
Relative to the season baseline (68.2%), the last 7 days were dragged down by the upset clusters called out above.

how to improve the model

1) Add or re-weight rest/back-to-back features so the higher-ranked team isn't over-trusted on short rest.
2) Incorporate injury severity into pre-game rating adjustments (weight by minutes/usage).
3) Expand matchup-style features (pace/3PA/rebounding) to penalize large mismatches.
4) Treat small rating gaps as higher-variance (wider uncertainty / lower confidence).