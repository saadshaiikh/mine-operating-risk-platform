# Mine Operating Risk MVP

This project is a mining operations review and risk‑prioritization system built on public MSHA data. It combines operations, incidents, and violations to identify higher‑risk mine‑quarters, support structured quarterly review, and focus follow‑up actions where risk is highest.

## Why This Project Exists
The MVP was built to demonstrate operating‑model and performance‑review skills in a mining context:
- Integrating operations, safety, and compliance data into a single quarterly review spine.
- Creating governed review views for consistent oversight.
- Supporting structured quarterly review and follow‑up prioritization.
- Delivering reproducible, version‑controlled analytics and model outputs.

## Current MVP Scope
Included in the current MVP:
- Mines
- Employment/production
- Incidents
- Violations

Deferred to Phase 2:
- Assessed violations / penalty burden

## Architecture and Pipeline
Raw MSHA files → staging tables → warehouse facts/dimensions → mine‑quarter base → labels → feature layer → logistic model → backtesting → BI/demo views → Streamlit dashboard.

## Data Coverage and Core Tables
Proven counts from the full local build:
- `dim_mine`: 91,603
- `fact_msha_employment_production`: 2,712,829
- `fact_msha_incident`: 271,719
- `fact_msha_violation`: 3,057,728
- `vw_mine_quarter_base`: 1,415,688
- `vw_mine_quarter_labels`: 1,415,688
- Positives (had_incident_next_qtr = 1): 120,363
- Negatives (had_incident_next_qtr = 0): 1,231,932
- Null labels: 63,393

## Feature Set (11)
- `feat_prior_incident_count`
- `feat_rolling_4q_incident_count`
- `feat_prior_lost_day_incident_count`
- `feat_incident_rate_per_200k_hours`
- `feat_prior_violation_count`
- `feat_rolling_4q_violation_count`
- `feat_ss_share`
- `feat_production_per_employee_hour`
- `feat_production_efficiency_qoq_delta`
- `feat_violation_burden_qoq_delta`
- `feat_deterioration_streak_count`

## Proven Results
Backtest performance (top‑decile focus) using 2019–2021 validation years:

| Year | ROC‑AUC | PR‑AUC | Precision@Top Decile | Recall@Top Decile | Lift |
| --- | --- | --- | --- | --- | --- |
| 2019 | 0.859 | 0.527 | 0.410 | 0.606 | 6.061 |
| 2020 | 0.856 | 0.496 | 0.368 | 0.618 | 6.180 |
| 2021 | 0.848 | 0.501 | 0.367 | 0.595 | 5.953 |

Pooled:
- Precision@Top Decile: 0.382
- Recall@Top Decile: 0.606
- Lift: 6.063

Naive baseline pooled:
- Precision@Top Decile: 0.366
- Recall@Top Decile: 0.581
- Lift: 5.810

The logistic model beat the naive baseline in every year and in pooled performance.

## Dashboard and Demo Screens
Executive Overview

![Executive Overview](docs/images/executive_overview.png)

Top‑Risk Mines

![Top‑Risk Mines](docs/images/top_risk_mines.png)

Mine Detail Drill‑Through

![Mine Detail Drill‑Through](docs/images/mine_detail_drillthrough.png)

Backtest Performance

![Backtest Performance](docs/images/backtest_performance.png)

Governance & Freshness

![Governance & Freshness](docs/images/governance_freshness.png)

## How to Run Locally
Requirements:
- Local Postgres running
- `DATABASE_URL` set for the app

Run:
```bash
python -m streamlit run app/streamlit_app.py
```

## Hosted Demo Readiness
This project is deployment‑ready using:
- Streamlit Community Cloud for the app
- Neon Postgres for the hosted database

Note: the hosted demo may use a reduced sample dataset due to free‑tier storage limits.

## Repository Structure
- `src/` — ingestion, transforms, modeling, backtesting
- `db/` — schema migrations and view definitions
- `configs/` — pipeline and model configuration
- `app/` — Streamlit demo app
- `docs/` — technical and business documentation

## Limitations
- Assessed violations / penalty burden are not included in the current MVP.
- Hosted demo may use reduced data volume due to storage limits.
- Focus is on mine‑quarter risk review, not full enterprise workflow integration.

## Phase 2 / Future Work
- Assessed violations and penalty‑based features
- Power BI polish and curated executive dashboards
- Hosted demo hardening and scaling
- Optional Canada/weather enrichment
- Expanded explainability

## Portfolio‑Ready Summary
Mine Operating Risk MVP is an operating review and risk prioritization system for mining operations. It integrates production, safety, and compliance data to surface higher‑risk mine‑quarters and guide structured quarterly performance reviews, not just a standalone machine‑learning exercise.
