CREATE OR REPLACE VIEW vw_mine_quarter_base AS
WITH ep_base AS (
    SELECT
        ep.mine_key,
        ep.period_key,
        ep.year,
        ep.quarter,
        SUM(ep.avg_employees) AS avg_employees,
        SUM(ep.employee_hours) AS employee_hours,
        SUM(ep.production_volume) AS production_volume,
        CASE
            WHEN COUNT(DISTINCT ep.production_unit) = 1 THEN MAX(ep.production_unit)
            ELSE MAX(ep.production_unit)
        END AS production_unit
    FROM fact_msha_employment_production ep
    GROUP BY
        ep.mine_key,
        ep.period_key,
        ep.year,
        ep.quarter
),
incident_agg AS (
    SELECT
        i.mine_key,
        i.incident_quarter AS period_key,
        COUNT(*) AS incident_count_current_qtr
    FROM fact_msha_incident i
    GROUP BY
        i.mine_key,
        i.incident_quarter
),
violation_agg AS (
    SELECT
        v.mine_key,
        v.violation_quarter AS period_key,
        COUNT(*) AS violation_count_current_qtr
    FROM fact_msha_violation v
    GROUP BY
        v.mine_key,
        v.violation_quarter
),
assessed_agg AS (
    SELECT
        av.mine_key,
        av.assessed_quarter AS period_key,
        COALESCE(SUM(av.penalty_amount), 0) AS assessed_penalty_amount_current_qtr
    FROM fact_msha_assessed_violation av
    GROUP BY
        av.mine_key,
        av.assessed_quarter
),
current_joined AS (
    SELECT
        b.mine_key,
        b.period_key,
        b.year,
        b.quarter,
        b.avg_employees,
        b.employee_hours,
        b.production_volume,
        b.production_unit,
        COALESCE(i.incident_count_current_qtr, 0) AS incident_count_current_qtr,
        COALESCE(v.violation_count_current_qtr, 0) AS violation_count_current_qtr,
        COALESCE(a.assessed_penalty_amount_current_qtr, 0) AS assessed_penalty_amount_current_qtr
    FROM ep_base b
    LEFT JOIN incident_agg i
        ON b.mine_key = i.mine_key
       AND b.period_key = i.period_key
    LEFT JOIN violation_agg v
        ON b.mine_key = v.mine_key
       AND b.period_key = v.period_key
    LEFT JOIN assessed_agg a
        ON b.mine_key = a.mine_key
       AND b.period_key = a.period_key
),
with_prior_values AS (
    SELECT
        cj.mine_key,
        cj.period_key,
        cj.year,
        cj.quarter,
        cj.avg_employees,
        cj.employee_hours,
        cj.production_volume,
        cj.production_unit,
        cj.incident_count_current_qtr,
        LAG(cj.incident_count_current_qtr) OVER (
            PARTITION BY cj.mine_key
            ORDER BY cj.year, cj.quarter
        ) AS incident_count_prior_qtr,
        cj.violation_count_current_qtr,
        LAG(cj.violation_count_current_qtr) OVER (
            PARTITION BY cj.mine_key
            ORDER BY cj.year, cj.quarter
        ) AS violation_count_prior_qtr,
        cj.assessed_penalty_amount_current_qtr,
        LAG(cj.assessed_penalty_amount_current_qtr) OVER (
            PARTITION BY cj.mine_key
            ORDER BY cj.year, cj.quarter
        ) AS assessed_penalty_amount_prior_qtr
    FROM current_joined cj
)
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
    assessed_penalty_amount_prior_qtr
FROM with_prior_values;
