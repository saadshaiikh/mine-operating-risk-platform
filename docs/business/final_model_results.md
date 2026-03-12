# Final Model Results (Current MVP)

## Dataset Sizes
- dim_mine: 91,603
- fact_msha_employment_production: 2,712,829
- fact_msha_incident: 271,719
- fact_msha_violation: 3,057,728
- fact_risk_score: 195,964

## Train and Validation Sizes
- 2019 split: train 1,024,681, validation 49,584
- 2020 split: train 1,074,265, validation 49,036
- 2021 split: train 1,123,301, validation 48,672

## Per-Year Backtest Metrics (Top-Decile)
- 2019: ROC-AUC 0.859, PR-AUC 0.527, precision 0.410, recall 0.606, lift 6.061
- 2020: ROC-AUC 0.856, PR-AUC 0.496, precision 0.368, recall 0.618, lift 6.180
- 2021: ROC-AUC 0.848, PR-AUC 0.501, precision 0.367, recall 0.595, lift 5.953

## Pooled Metrics (2019-2021)
- pooled_precision_at_top_decile: 0.382
- pooled_recall_at_top_decile: 0.606
- pooled_lift_vs_base_rate: 6.063

## Naive Baseline (Top-Decile)
- 2019: precision 0.393, recall 0.582, lift 5.816
- 2020: precision 0.354, recall 0.595, lift 5.954
- 2021: precision 0.349, recall 0.566, lift 5.663
- pooled: precision 0.366, recall 0.581, lift 5.810

## Current MVP Scope
- Sources: mines, employment/production, incidents, violations
- Feature set: 11 features
- Validation years: 2019, 2020, 2021

## Deferred Phase 2 Items
- Assessed violations and penalty burden
- Penalty-based features and related compliance expansions
