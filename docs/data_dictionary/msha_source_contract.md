# MSHA Source Contract (V1)

Scope: MSHA-only sources for V1. This document locks the ingestion contract and required fields before any loaders are written.

## Mines Data Set (mine metadata)
- Business purpose: Mine identity and attributes used to populate dim_mine and to map all facts to a conformed mine_key.
- Source name: Mines Data Set
- Source URL: https://catalog.data.gov/dataset/msha-mines-dataset
- Refresh cadence: Weekly (Friday afternoon) full-file replacement unless otherwise noted
- Expected file format: Pipe-delimited text with header row; distributed as .zip
- Source grain: One row per mine_id (current snapshot)
- Key field strategy: Primary key is `mine_id` (unique per mine). Use a surrogate only if duplicates are detected.
- Important date fields: `current_status_dt`, `current_controller_begin_dt`, `current_103i_dt`
- Mine identifier field: `mine_id`
- Target staging table: `stg_msha_mines`
- Target warehouse object(s): `dim_mine`

## Accident Injuries Data Set (incidents)
- Business purpose: Incident history used for label creation and incident burden features.
- Source name: Accident Injuries Data Set
- Source URL: https://catalog.data.gov/dataset/msha-accident-injuries-data-set
- Refresh cadence: Weekly (Friday afternoon) full-file replacement unless otherwise noted
- Expected file format: Pipe-delimited text with header row; distributed as .zip
- Source grain: One row per accident/injury/illness report (`document_no`)
- Key field strategy: Primary key is `document_no` (unique per report). Use a surrogate only if duplicates are detected.
- Important date fields: `accident_dt`, `cal_yr`, `cal_qtr`, `fiscal_yr`, `fiscal_qtr`, `invest_begin_dt`, `return_to_work_dt`
- Mine identifier field: `mine_id`
- Target staging table: `stg_msha_incidents`
- Target warehouse object(s): `fact_msha_incident`

## Violations Data Set
- Business purpose: Compliance burden signals for leading indicators and risk context.
- Source name: Violations Data Set
- Source URL: https://catalog.data.gov/dataset/msha-violations-dataset
- Refresh cadence: Weekly (Friday afternoon) full-file replacement unless otherwise noted
- Expected file format: Pipe-delimited text with header row; distributed as .zip
- Source grain: One row per citation/order (`violation_no`)
- Key field strategy: Primary key is `violation_no` where available; otherwise deterministic composite hash.
- Important date fields: `inspection_begin_dt`, `inspection_end_dt`, `violation_issue_dt`, `violation_occur_dt`, `cal_yr`, `cal_qtr`, `fiscal_yr`, `fiscal_qtr`
- Mine identifier field: `mine_id`
- Target staging table: `stg_msha_violations`
- Target warehouse object(s): `fact_msha_violation`

## Assessed Violations Data Set
- Business purpose: Penalty/assessment burden used for financial/regulatory risk context.
- Source name: Assessed Violations Data Set
- Source URL: https://catalog.data.gov/dataset/msha-assessed-violations
- Refresh cadence: Weekly (Friday afternoon) full-file replacement unless otherwise noted
- Expected file format: Pipe-delimited text with header row; distributed as .zip
- Source grain: One row per assessed violation (case/violation record)
- Key field strategy: Primary key is `violation_no` where available; otherwise deterministic composite hash.
- Important date fields: `occurrence_dt`, `issue_dt`, `final_order_dt`, `bill_print_dt`, `assess_case_status_dt`
- Mine identifier field: `mine_id`
- Target staging table: `stg_msha_assessed_violations`
- Target warehouse object(s): `fact_msha_assessed_violation`

## Operator Employment and Production Data Set - Quarterly
- Business purpose: Operational denominator and period spine for mine-quarter modeling.
- Source name: Operator Employment and Production Data Set - Quarterly
- Source URL: https://catalog.data.gov/dataset/msha-operator-employment-and-production-data-set-quarterly
- Refresh cadence: Weekly (Friday afternoon) full-file replacement unless otherwise noted
- Expected file format: Pipe-delimited text with header row; distributed as .zip
- Source grain: One row per mine_id + subunit_cd + cal_yr + cal_qtr
- Key field strategy: Composite natural key `mine_id`, `subunit_cd`, `cal_yr`, `cal_qtr`. Use a surrogate only if duplicates are detected.
- Important date fields: `cal_yr`, `cal_qtr`, `fiscal_yr`, `fiscal_qtr`
- Mine identifier field: `mine_id`
- Target staging table: `stg_msha_employment_production`
- Target warehouse object(s): `fact_msha_employment_production`
