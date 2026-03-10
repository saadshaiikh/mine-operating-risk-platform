# V1 Scope

## Decisions
- V1 entity = mine
- V1 time grain = quarter
- V1 label = had_incident_next_qtr
- V1 geography = US-only operational backbone
- V1 data sources = MSHA only
- V1 input tables = incidents, violations, assessed violations, employment/production, mine metadata
- V1 outputs = mine-quarter base, label, MVP features, baseline model, first backtest
- Out of scope for V1 = Canada joins, weather, Power BI polish, Word/PPT automation

## Modeling Problem Statement
Given mine-quarter data at quarter t, predict whether the mine has >=1 incident in quarter t+1.

## Why This First
Without this, the schema, features, and model target will keep moving.

## Definition of Done
You can hand this file to a dev and they know exactly what V1 is and what it is not.
