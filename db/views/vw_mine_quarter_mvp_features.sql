CREATE OR REPLACE VIEW vw_mine_quarter_mvp_features AS
WITH label_spine AS (
    SELECT
        mine_key,
        period_key,
        year,
        quarter,
        avg_employees,
        employee_hours,
        production_volume,
        production_unit,
        incident_count_current_qtr,
        incident_count_prior_qtr,
        violation_count_current_qtr,
        violation_count_prior_qtr,
        assessed_penalty_amount_current_qtr,
        assessed_penalty_amount_prior_qtr,
        next_period_key,
        next_quarter_exists_flag,
        next_quarter_incident_count,
        had_incident_next_qtr
    FROM vw_mine_quarter_labels
),
lost_day_incident_agg AS (
    SELECT
        i.mine_key,
        i.incident_quarter AS period_key,
        COUNT(*) FILTER (
            WHERE COALESCE(i.lost_days, 0) > 0
        ) AS lost_day_incident_count_current_qtr
    FROM fact_msha_incident i
    GROUP BY
        i.mine_key,
        i.incident_quarter
),
ss_violation_agg AS (
    SELECT
        v.mine_key,
        v.violation_quarter AS period_key,
        COUNT(*) FILTER (
            WHERE v.significant_substantial IS TRUE
        ) AS ss_violation_count_current_qtr
    FROM fact_msha_violation v
    GROUP BY
        v.mine_key,
        v.violation_quarter
),
joined_signal_base AS (
    SELECT
        ls.mine_key,
        ls.period_key,
        ls.year,
        ls.quarter,
        ls.avg_employees,
        ls.employee_hours,
        ls.production_volume,
        ls.production_unit,
        ls.incident_count_current_qtr,
        ls.incident_count_prior_qtr,
        ls.violation_count_current_qtr,
        ls.violation_count_prior_qtr,
        ls.assessed_penalty_amount_current_qtr,
        ls.assessed_penalty_amount_prior_qtr,
        ls.next_period_key,
        ls.next_quarter_exists_flag,
        ls.next_quarter_incident_count,
        ls.had_incident_next_qtr,
        COALESCE(ld.lost_day_incident_count_current_qtr, 0) AS lost_day_incident_count_current_qtr,
        COALESCE(ss.ss_violation_count_current_qtr, 0) AS ss_violation_count_current_qtr
    FROM label_spine ls
    LEFT JOIN lost_day_incident_agg ld
        ON ls.mine_key = ld.mine_key
       AND ls.period_key = ld.period_key
    LEFT JOIN ss_violation_agg ss
        ON ls.mine_key = ss.mine_key
       AND ls.period_key = ss.period_key
),
rate_base AS (
    SELECT
        jsb.*,
        CASE
            WHEN jsb.employee_hours IS NOT NULL
             AND jsb.employee_hours > 0
            THEN (jsb.incident_count_current_qtr::NUMERIC * 200000.0) / jsb.employee_hours
            ELSE NULL
        END AS incident_rate_per_200k_hours_current_qtr,
        CASE
            WHEN jsb.employee_hours IS NOT NULL
             AND jsb.employee_hours > 0
            THEN (jsb.violation_count_current_qtr::NUMERIC * 10000.0) / jsb.employee_hours
            ELSE NULL
        END AS violation_rate_per_10k_hours_current_qtr,
        CASE
            WHEN jsb.employee_hours IS NOT NULL
             AND jsb.employee_hours > 0
             AND jsb.production_volume IS NOT NULL
            THEN jsb.production_volume / jsb.employee_hours
            ELSE NULL
        END AS production_per_employee_hour_current_qtr,
        CASE
            WHEN jsb.violation_count_current_qtr > 0
            THEN jsb.ss_violation_count_current_qtr::NUMERIC / jsb.violation_count_current_qtr
            ELSE 0.0
        END AS ss_share_current_qtr
    FROM joined_signal_base jsb
),
with_windows AS (
    SELECT
        rb.*,
        SUM(rb.incident_count_current_qtr) OVER (
            PARTITION BY rb.mine_key
            ORDER BY rb.year, rb.quarter
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) AS rolling_4q_incident_count,
        SUM(rb.violation_count_current_qtr) OVER (
            PARTITION BY rb.mine_key
            ORDER BY rb.year, rb.quarter
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) AS rolling_4q_violation_count,
        LAG(rb.lost_day_incident_count_current_qtr) OVER (
            PARTITION BY rb.mine_key
            ORDER BY rb.year, rb.quarter
        ) AS prior_lost_day_incident_count,
        LAG(rb.incident_rate_per_200k_hours_current_qtr) OVER (
            PARTITION BY rb.mine_key
            ORDER BY rb.year, rb.quarter
        ) AS incident_rate_per_200k_hours_prior_qtr,
        LAG(rb.violation_rate_per_10k_hours_current_qtr) OVER (
            PARTITION BY rb.mine_key
            ORDER BY rb.year, rb.quarter
        ) AS violation_rate_per_10k_hours_prior_qtr,
        LAG(rb.production_per_employee_hour_current_qtr) OVER (
            PARTITION BY rb.mine_key
            ORDER BY rb.year, rb.quarter
        ) AS production_per_employee_hour_prior_qtr
    FROM rate_base rb
),
with_deltas AS (
    SELECT
        ww.*,
        CASE
            WHEN ww.incident_rate_per_200k_hours_current_qtr IS NOT NULL
             AND ww.incident_rate_per_200k_hours_prior_qtr IS NOT NULL
            THEN ww.incident_rate_per_200k_hours_current_qtr
                 - ww.incident_rate_per_200k_hours_prior_qtr
            ELSE NULL
        END AS incident_rate_qoq_delta,
        CASE
            WHEN ww.violation_rate_per_10k_hours_current_qtr IS NOT NULL
             AND ww.violation_rate_per_10k_hours_prior_qtr IS NOT NULL
            THEN ww.violation_rate_per_10k_hours_current_qtr
                 - ww.violation_rate_per_10k_hours_prior_qtr
            ELSE NULL
        END AS violation_burden_qoq_delta,
        CASE
            WHEN ww.production_per_employee_hour_current_qtr IS NOT NULL
             AND ww.production_per_employee_hour_prior_qtr IS NOT NULL
            THEN ww.production_per_employee_hour_current_qtr
                 - ww.production_per_employee_hour_prior_qtr
            ELSE NULL
        END AS production_efficiency_qoq_delta
    FROM with_windows ww
),
with_deterioration_flag AS (
    SELECT
        wd.*,
        CASE
            WHEN (wd.incident_rate_qoq_delta IS NOT NULL AND wd.incident_rate_qoq_delta > 0)
              OR (wd.violation_burden_qoq_delta IS NOT NULL AND wd.violation_burden_qoq_delta > 0)
              OR (wd.production_efficiency_qoq_delta IS NOT NULL AND wd.production_efficiency_qoq_delta < 0)
            THEN 1
            ELSE 0
        END AS deterioration_flag_current_qtr
    FROM with_deltas wd
),
with_deterioration_groups AS (
    SELECT
        wdf.*,
        SUM(
            CASE
                WHEN wdf.deterioration_flag_current_qtr = 0 THEN 1
                ELSE 0
            END
        ) OVER (
            PARTITION BY wdf.mine_key
            ORDER BY wdf.year, wdf.quarter
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS deterioration_reset_group
    FROM with_deterioration_flag wdf
),
finalized AS (
    SELECT
        wdg.mine_key,
        wdg.period_key,
        wdg.year,
        wdg.quarter,
        wdg.next_period_key,
        wdg.next_quarter_exists_flag,
        wdg.had_incident_next_qtr,

        wdg.avg_employees,
        wdg.employee_hours,
        wdg.production_volume,
        wdg.production_unit,

        wdg.incident_count_current_qtr,
        wdg.incident_count_prior_qtr,
        wdg.violation_count_current_qtr,
        wdg.violation_count_prior_qtr,
        wdg.assessed_penalty_amount_current_qtr,
        wdg.assessed_penalty_amount_prior_qtr,

        wdg.lost_day_incident_count_current_qtr,
        wdg.ss_violation_count_current_qtr,
        wdg.incident_rate_per_200k_hours_current_qtr,
        wdg.violation_rate_per_10k_hours_current_qtr,
        wdg.production_per_employee_hour_current_qtr,
        wdg.incident_rate_qoq_delta,
        wdg.deterioration_flag_current_qtr,

        wdg.incident_count_prior_qtr AS feat_prior_incident_count,
        wdg.rolling_4q_incident_count AS feat_rolling_4q_incident_count,
        wdg.prior_lost_day_incident_count AS feat_prior_lost_day_incident_count,
        wdg.incident_rate_per_200k_hours_current_qtr AS feat_incident_rate_per_200k_hours,
        wdg.violation_count_prior_qtr AS feat_prior_violation_count,
        wdg.rolling_4q_violation_count AS feat_rolling_4q_violation_count,
        wdg.ss_share_current_qtr AS feat_ss_share,
        wdg.assessed_penalty_amount_prior_qtr AS feat_assessed_penalty_amount_lag1,
        wdg.production_per_employee_hour_current_qtr AS feat_production_per_employee_hour,
        wdg.production_efficiency_qoq_delta AS feat_production_efficiency_qoq_delta,
        wdg.violation_burden_qoq_delta AS feat_violation_burden_qoq_delta,
        CASE
            WHEN wdg.deterioration_flag_current_qtr = 1
            THEN SUM(wdg.deterioration_flag_current_qtr) OVER (
                PARTITION BY wdg.mine_key, wdg.deterioration_reset_group
                ORDER BY wdg.year, wdg.quarter
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
            ELSE 0
        END AS feat_deterioration_streak_count
    FROM with_deterioration_groups wdg
)
SELECT
    mine_key,
    period_key,
    year,
    quarter,
    next_period_key,
    next_quarter_exists_flag,
    had_incident_next_qtr,

    avg_employees,
    employee_hours,
    production_volume,
    production_unit,

    incident_count_current_qtr,
    incident_count_prior_qtr,
    violation_count_current_qtr,
    violation_count_prior_qtr,
    assessed_penalty_amount_current_qtr,
    assessed_penalty_amount_prior_qtr,

    lost_day_incident_count_current_qtr,
    ss_violation_count_current_qtr,
    incident_rate_per_200k_hours_current_qtr,
    violation_rate_per_10k_hours_current_qtr,
    production_per_employee_hour_current_qtr,
    incident_rate_qoq_delta,
    deterioration_flag_current_qtr,

    feat_prior_incident_count,
    feat_rolling_4q_incident_count,
    feat_prior_lost_day_incident_count,
    feat_incident_rate_per_200k_hours,
    feat_prior_violation_count,
    feat_rolling_4q_violation_count,
    feat_ss_share,
    feat_assessed_penalty_amount_lag1,
    feat_production_per_employee_hour,
    feat_production_efficiency_qoq_delta,
    feat_violation_burden_qoq_delta,
    feat_deterioration_streak_count
FROM finalized;
