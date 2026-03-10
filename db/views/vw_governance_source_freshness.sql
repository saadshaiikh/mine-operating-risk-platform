CREATE OR REPLACE VIEW vw_governance_source_freshness AS
WITH entity_loads AS (
    SELECT 'MSHA' AS source_system, 'dim_mine' AS entity_name, MAX(updated_at) AS latest_data_ts, COUNT(*) AS row_count
    FROM dim_mine

    UNION ALL

    SELECT 'MSHA', 'fact_msha_incident', MAX(load_ts), COUNT(*)
    FROM fact_msha_incident

    UNION ALL

    SELECT 'MSHA', 'fact_msha_violation', MAX(load_ts), COUNT(*)
    FROM fact_msha_violation

    UNION ALL

    SELECT 'MSHA', 'fact_msha_assessed_violation', MAX(load_ts), COUNT(*)
    FROM fact_msha_assessed_violation

    UNION ALL

    SELECT 'MSHA', 'fact_msha_employment_production', MAX(load_ts), COUNT(*)
    FROM fact_msha_employment_production

    UNION ALL

    SELECT 'MODEL', 'fact_risk_score', MAX(prediction_ts), COUNT(*)
    FROM fact_risk_score

    UNION ALL

    SELECT 'MODEL', 'fact_backtest_result', MAX(created_at), COUNT(*)
    FROM fact_backtest_result
),
dq_ranked AS (
    SELECT
        dqr.entity_name,
        dqr.test_name,
        dqr.status,
        dqr.severity,
        dqr.checked_at,
        ROW_NUMBER() OVER (
            PARTITION BY dqr.entity_name
            ORDER BY dqr.checked_at DESC, dqr.dq_key DESC
        ) AS rn
    FROM fact_data_quality_result dqr
),
dq_rollup AS (
    SELECT
        entity_name,
        MAX(checked_at) AS latest_quality_check_ts,
        MAX(CASE WHEN rn = 1 THEN status END) AS latest_quality_status,
        MAX(CASE WHEN rn = 1 THEN severity END) AS latest_quality_severity
    FROM dq_ranked
    GROUP BY entity_name
)
SELECT
    CONCAT(el.source_system, '|', el.entity_name) AS governance_key,
    el.source_system,
    el.entity_name,
    el.row_count,
    el.latest_data_ts,
    dr.latest_quality_check_ts,
    dr.latest_quality_status,
    dr.latest_quality_severity,
    CASE
        WHEN el.latest_data_ts IS NOT NULL
        THEN EXTRACT(EPOCH FROM (NOW() - el.latest_data_ts)) / 3600.0
        ELSE NULL
    END AS freshness_age_hours,
    CASE
        WHEN el.latest_data_ts IS NULL THEN 'Missing'
        WHEN NOW() - el.latest_data_ts <= INTERVAL '7 days' THEN 'Fresh'
        WHEN NOW() - el.latest_data_ts <= INTERVAL '30 days' THEN 'Warning'
        ELSE 'Stale'
    END AS freshness_status
FROM entity_loads el
LEFT JOIN dq_rollup dr
    ON el.entity_name = dr.entity_name;
