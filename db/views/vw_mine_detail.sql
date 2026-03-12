CREATE OR REPLACE VIEW vw_mine_detail AS
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
)
SELECT
    CONCAT(s.model_version, '|', s.period_key, '|', s.mine_key) AS mine_period_model_key,
    CONCAT(s.model_version, '|', s.period_key) AS summary_key,
    s.model_name,
    s.model_version,
    s.period_key,
    s.year,
    s.quarter,

    s.mine_key,
    dm.mine_id,
    dm.mine_name,
    dm.country_code,
    dm.province_state,
    dm.commodity_group,
    dm.mine_type,

    s.risk_score,
    s.risk_band,
    s.top_driver_1,
    s.top_driver_2,
    s.top_driver_3,
    s.label_next_period,

    l.avg_employees,
    l.employee_hours,
    l.production_volume,
    l.production_unit,
    l.incident_count_current_qtr,
    l.incident_count_prior_qtr,
    l.violation_count_current_qtr,
    l.violation_count_prior_qtr,
    l.assessed_penalty_amount_current_qtr,
    l.assessed_penalty_amount_prior_qtr,

    f.lost_day_incident_count_current_qtr,
    f.ss_violation_count_current_qtr,
    f.incident_rate_per_200k_hours_current_qtr,
    f.violation_rate_per_10k_hours_current_qtr,
    f.production_per_employee_hour_current_qtr,
    f.incident_rate_qoq_delta,
    f.deterioration_flag_current_qtr,

    f.feat_prior_incident_count,
    f.feat_rolling_4q_incident_count,
    f.feat_prior_lost_day_incident_count,
    f.feat_incident_rate_per_200k_hours,
    f.feat_prior_violation_count,
    f.feat_rolling_4q_violation_count,
    f.feat_ss_share,
    f.feat_production_per_employee_hour,
    f.feat_production_efficiency_qoq_delta,
    f.feat_violation_burden_qoq_delta,
    f.feat_deterioration_streak_count
FROM scored s
LEFT JOIN dim_mine dm
    ON s.mine_key = dm.mine_key
LEFT JOIN vw_mine_quarter_labels l
    ON s.mine_key = l.mine_key
   AND s.period_key = l.period_key
LEFT JOIN vw_mine_quarter_mvp_features f
    ON s.mine_key = f.mine_key
   AND s.period_key = f.period_key;
