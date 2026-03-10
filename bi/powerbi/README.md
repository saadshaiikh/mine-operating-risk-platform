# Power BI Contract

## Rule
Power BI must connect only to the curated BI views below. It must never connect directly to raw tables, staging tables, warehouse fact tables, or feature engineering tables.

## Approved source objects

- vw_kpi_summary
- vw_top_risk_mines
- vw_mine_detail
- vw_backtest_summary
- vw_governance_source_freshness

## Disallowed source objects

Do not connect Power BI directly to:
- fact_msha_incident
- fact_msha_violation
- fact_msha_assessed_violation
- fact_msha_employment_production
- fact_risk_score
- fact_backtest_result
- fact_data_quality_result
- any stg_* table
- any raw file export

## View purposes

### vw_kpi_summary
Executive KPI cards and model-period trend summaries.

Grain:
- one row per (model_version, period_key)

Primary fields:
- model_name
- model_version
- period_key
- year
- quarter
- is_latest_period_for_model_flag
- total_mines_scored
- avg_risk_score
- median_risk_score
- max_risk_score
- high_risk_mine_count
- critical_risk_mine_count
- high_or_critical_risk_mine_count
- high_or_critical_risk_share
- actual_positive_count
- flagged_positive_count
- flagged_positive_precision

### vw_top_risk_mines
Ranked mine table for a selected model and period.

Grain:
- one row per (model_version, period_key, mine_key)

Primary fields:
- mine_period_model_key
- summary_key
- model_name
- model_version
- period_key
- risk_rank
- risk_score
- risk_band
- top_driver_1
- top_driver_2
- top_driver_3
- mine_name
- province_state
- commodity_group
- employee_hours
- production_volume
- incident_count_current_qtr
- violation_count_current_qtr

### vw_mine_detail
Mine-level drill-through detail.

Grain:
- one row per (model_version, period_key, mine_key)

Primary fields:
- mine_period_model_key
- summary_key
- model_name
- model_version
- period_key
- mine_name
- risk_score
- risk_band
- top_driver_1..3
- all operational context columns
- all feat_* model input columns

### vw_backtest_summary
Backtesting results by model version and validation year, with pooled model summary columns repeated.

Grain:
- one row per (model_version, validation_year)

Primary fields:
- model_validation_key
- model_name
- model_version
- validation_year
- roc_auc
- pr_auc
- precision_at_top_decile
- recall_at_top_decile
- lift_vs_base_rate
- pooled_recall_at_top_decile
- pooled_lift_vs_base_rate
- business_claim_text

### vw_governance_source_freshness
Governance and freshness monitoring.

Grain:
- one row per entity_name

Primary fields:
- governance_key
- source_system
- entity_name
- row_count
- latest_data_ts
- latest_quality_check_ts
- latest_quality_status
- latest_quality_severity
- freshness_age_hours
- freshness_status

## Recommended relationships

Use these relationships only:

1. vw_kpi_summary.summary_key -> vw_top_risk_mines.summary_key
   - one-to-many

2. vw_top_risk_mines.mine_period_model_key -> vw_mine_detail.mine_period_model_key
   - one-to-one or one-to-many depending on report design
   - recommend single-direction filter from vw_top_risk_mines to vw_mine_detail

Do not create relationships from governance or backtest views into mine-level views.

## Recommended report pages

### Executive Overview
Use:
- vw_kpi_summary

Recommended filters:
- is_latest_period_for_model_flag = 1
- model_name = selected model

### Top Risk Mines
Use:
- vw_top_risk_mines

Recommended filters:
- selected model_version
- selected period_key

### Mine Detail
Use:
- vw_mine_detail

Recommended drill-through key:
- mine_period_model_key

### Backtesting
Use:
- vw_backtest_summary

### Governance
Use:
- vw_governance_source_freshness

## Data mode

Recommended mode:
- Import

Reason:
- stable performance
- predictable refresh
- simple governance for a portfolio project

## Refresh expectation

Refresh only after:
- curated BI views have been rebuilt
- scoring has been completed
- backtest metrics have been persisted

Never use Power BI transforms to recreate business logic already defined in SQL views.
