CREATE OR REPLACE VIEW vw_mine_quarter_mvp_features AS
WITH label_spine AS (
    SELECT
        mine_key,
        period_key,
        year,
        quarter,
        avg_employees,
        incident_count_current_qtr,
        incident_count_prior_qtr,
        violation_count_current_qtr,
        violation_count_prior_qtr,
        employee_hours,
        production_volume,
        next_period_key,
        next_quarter_exists_flag,
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
            WHERE COALESCE(v.significant_substantial, FALSE)
        ) AS ss_violation_count_current_qtr
    FROM fact_msha_violation v
    GROUP BY
        v.mine_key,
        v.violation_quarter
),
joined AS (
    SELECT
        ls.*,
        COALESCE(ld.lost_day_incident_count_current_qtr, 0) AS lost_day_incident_count_current_qtr,
        COALESCE(vs.ss_violation_count_current_qtr, 0) AS ss_violation_count_current_qtr
    FROM label_spine ls
    LEFT JOIN lost_day_incident_agg ld
        ON ls.mine_key = ld.mine_key
       AND ls.period_key = ld.period_key
    LEFT JOIN ss_violation_agg vs
        ON ls.mine_key = vs.mine_key
       AND ls.period_key = vs.period_key
),
rates AS (
    SELECT
        j.*,
        CASE
            WHEN j.employee_hours IS NOT NULL
             AND j.employee_hours > 0
            THEN (j.incident_count_current_qtr::NUMERIC * 200000.0) / j.employee_hours
            ELSE NULL
        END AS incident_rate_per_200k_hours_current_qtr,
        CASE
            WHEN j.employee_hours IS NOT NULL
             AND j.employee_hours > 0
            THEN (j.violation_count_current_qtr::NUMERIC * 10000.0) / j.employee_hours
            ELSE NULL
        END AS violation_rate_per_10k_hours_current_qtr,
        CASE
            WHEN j.employee_hours IS NOT NULL
             AND j.employee_hours > 0
            THEN j.production_volume::NUMERIC / j.employee_hours
            ELSE NULL
        END AS production_per_employee_hour_current_qtr
    FROM joined j
),
with_lags AS (
    SELECT
        r.*,
        LAG(r.incident_rate_per_200k_hours_current_qtr) OVER (
            PARTITION BY r.mine_key
            ORDER BY r.year, r.quarter
        ) AS incident_rate_per_200k_hours_prior_qtr,
        LAG(r.violation_rate_per_10k_hours_current_qtr) OVER (
            PARTITION BY r.mine_key
            ORDER BY r.year, r.quarter
        ) AS violation_rate_per_10k_hours_prior_qtr,
        LAG(r.production_per_employee_hour_current_qtr) OVER (
            PARTITION BY r.mine_key
            ORDER BY r.year, r.quarter
        ) AS production_per_employee_hour_prior_qtr
    FROM rates r
),
with_deltas AS (
    SELECT
        wl.*,
        CASE
            WHEN wl.incident_rate_per_200k_hours_current_qtr IS NOT NULL
             AND wl.incident_rate_per_200k_hours_prior_qtr IS NOT NULL
            THEN wl.incident_rate_per_200k_hours_current_qtr - wl.incident_rate_per_200k_hours_prior_qtr
            ELSE NULL
        END AS incident_rate_qoq_delta,
        CASE
            WHEN wl.violation_rate_per_10k_hours_current_qtr IS NOT NULL
             AND wl.violation_rate_per_10k_hours_prior_qtr IS NOT NULL
            THEN wl.violation_rate_per_10k_hours_current_qtr - wl.violation_rate_per_10k_hours_prior_qtr
            ELSE NULL
        END AS violation_rate_qoq_delta,
        CASE
            WHEN wl.production_per_employee_hour_current_qtr IS NOT NULL
             AND wl.production_per_employee_hour_prior_qtr IS NOT NULL
            THEN wl.production_per_employee_hour_current_qtr - wl.production_per_employee_hour_prior_qtr
            ELSE NULL
        END AS production_efficiency_qoq_delta
    FROM with_lags wl
),
with_features AS (
    SELECT
        wd.*,
        wd.incident_count_prior_qtr AS feat_prior_incident_count,
        SUM(wd.incident_count_current_qtr) OVER (
            PARTITION BY wd.mine_key
            ORDER BY wd.year, wd.quarter
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) AS feat_rolling_4q_incident_count,
        LAG(wd.lost_day_incident_count_current_qtr) OVER (
            PARTITION BY wd.mine_key
            ORDER BY wd.year, wd.quarter
        ) AS feat_prior_lost_day_incident_count,
        wd.incident_rate_per_200k_hours_current_qtr AS feat_incident_rate_per_200k_hours,
        wd.violation_count_prior_qtr AS feat_prior_violation_count,
        SUM(wd.violation_count_current_qtr) OVER (
            PARTITION BY wd.mine_key
            ORDER BY wd.year, wd.quarter
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) AS feat_rolling_4q_violation_count,
        CASE
            WHEN wd.violation_count_current_qtr > 0
            THEN wd.ss_violation_count_current_qtr::NUMERIC / wd.violation_count_current_qtr
            ELSE NULL
        END AS feat_ss_share,
        wd.production_per_employee_hour_current_qtr AS feat_production_per_employee_hour,
        wd.production_efficiency_qoq_delta AS feat_production_efficiency_qoq_delta,
        wd.violation_rate_qoq_delta AS feat_violation_burden_qoq_delta,
        CASE
            WHEN wd.incident_rate_qoq_delta IS NULL
             AND wd.violation_rate_qoq_delta IS NULL
             AND wd.production_efficiency_qoq_delta IS NULL
            THEN NULL
            WHEN COALESCE(wd.incident_rate_qoq_delta, 0) > 0
              OR COALESCE(wd.violation_rate_qoq_delta, 0) > 0
              OR COALESCE(wd.production_efficiency_qoq_delta, 0) < 0
            THEN 1
            ELSE 0
        END AS deterioration_flag_current_qtr
    FROM with_deltas wd
),
with_streaks AS (
    SELECT
        wf.*,
        SUM(CASE WHEN wf.deterioration_flag_current_qtr = 0 THEN 1 ELSE 0 END) OVER (
            PARTITION BY wf.mine_key
            ORDER BY wf.year, wf.quarter
        ) AS deterioration_reset_grp
    FROM with_features wf
)
SELECT
    mine_key,
    period_key,
    year,
    quarter,
    had_incident_next_qtr,
    next_period_key,
    next_quarter_exists_flag,
    incident_count_current_qtr,
    incident_count_prior_qtr,
    violation_count_current_qtr,
    violation_count_prior_qtr,
    employee_hours,
    production_volume,
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
    feat_production_per_employee_hour,
    feat_production_efficiency_qoq_delta,
    feat_violation_burden_qoq_delta,
    CASE
        WHEN deterioration_flag_current_qtr = 1 THEN
            SUM(CASE WHEN deterioration_flag_current_qtr = 1 THEN 1 ELSE 0 END) OVER (
                PARTITION BY mine_key, deterioration_reset_grp
                ORDER BY year, quarter
            )
        WHEN deterioration_flag_current_qtr = 0 THEN 0
        ELSE NULL
    END AS feat_deterioration_streak_count
FROM with_streaks;
