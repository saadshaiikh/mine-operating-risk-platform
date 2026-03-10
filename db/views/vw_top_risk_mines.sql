CREATE OR REPLACE VIEW vw_top_risk_mines AS
WITH scored AS (
    SELECT
        frs.mine_key,
        frs.period_key,
        split_part(frs.period_key, 'Q', 1)::INTEGER AS year,
        split_part(frs.period_key, 'Q', 2)::INTEGER AS quarter,
        CASE
            WHEN frs.model_version LIKE 'rule_score%' THEN 'rule_score'
            WHEN frs.model_version LIKE 'logreg%' THEN 'logreg'
            ELSE 'unknown'
        END AS model_name,
        frs.model_version,
        frs.risk_score,
        frs.risk_band,
        frs.top_driver_1,
        frs.top_driver_2,
        frs.top_driver_3,
        frs.label_next_period
    FROM fact_risk_score frs
),
latest_period_by_model AS (
    SELECT
        model_version,
        MAX(year * 10 + quarter) AS latest_period_sort
    FROM scored
    GROUP BY model_version
),
ranked AS (
    SELECT
        s.*,
        ROW_NUMBER() OVER (
            PARTITION BY s.model_version, s.period_key
            ORDER BY s.risk_score DESC, s.mine_key ASC
        ) AS risk_rank,
        PERCENT_RANK() OVER (
            PARTITION BY s.model_version, s.period_key
            ORDER BY s.risk_score DESC, s.mine_key ASC
        ) AS risk_percent_rank
    FROM scored s
)
SELECT
    CONCAT(r.model_version, '|', r.period_key, '|', r.mine_key) AS mine_period_model_key,
    CONCAT(r.model_version, '|', r.period_key) AS summary_key,
    r.model_name,
    r.model_version,
    r.period_key,
    r.year,
    r.quarter,
    CASE
        WHEN (r.year * 10 + r.quarter) = lp.latest_period_sort THEN 1
        ELSE 0
    END AS is_latest_period_for_model_flag,
    r.mine_key,
    dm.mine_id,
    dm.mine_name,
    dm.country_code,
    dm.province_state,
    dm.commodity_group,
    dm.mine_type,
    r.risk_rank,
    r.risk_percent_rank,
    r.risk_score,
    r.risk_band,
    r.top_driver_1,
    r.top_driver_2,
    r.top_driver_3,
    f.employee_hours,
    f.production_volume,
    f.incident_count_current_qtr,
    f.violation_count_current_qtr,
    f.had_incident_next_qtr
FROM ranked r
LEFT JOIN dim_mine dm
    ON r.mine_key = dm.mine_key
LEFT JOIN vw_mine_quarter_mvp_features f
    ON r.mine_key = f.mine_key
   AND r.period_key = f.period_key
LEFT JOIN latest_period_by_model lp
    ON r.model_version = lp.model_version;
