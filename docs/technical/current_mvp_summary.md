# Current MVP Summary

## Scope
- Sources: mines, employment/production, incidents, violations
- Deferred: assessed violations and penalty burden (Phase 2)
- Grain: mine-quarter
- Label: had_incident_next_qtr
- Validation years: 2019, 2020, 2021

## Feature Set (11)
- feat_prior_incident_count
- feat_rolling_4q_incident_count
- feat_prior_lost_day_incident_count
- feat_incident_rate_per_200k_hours
- feat_prior_violation_count
- feat_rolling_4q_violation_count
- feat_ss_share
- feat_production_per_employee_hour
- feat_production_efficiency_qoq_delta
- feat_violation_burden_qoq_delta
- feat_deterioration_streak_count

## Model
- Logistic regression with standard scaling and class balancing
- Model version: logreg_v1_mvp11_train_to_2018_validate_2019
- Model version: logreg_v1_mvp11_train_to_2019_validate_2020
- Model version: logreg_v1_mvp11_train_to_2020_validate_2021

## Backtest Highlights (Top-Decile)
- 2019: precision 0.410, recall 0.606, lift 6.061
- 2020: precision 0.368, recall 0.618, lift 6.180
- 2021: precision 0.367, recall 0.595, lift 5.953
- Pooled: precision 0.382, recall 0.606, lift 6.063
