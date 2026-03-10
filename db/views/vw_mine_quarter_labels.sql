CREATE OR REPLACE VIEW vw_mine_quarter_labels AS
WITH base_rows AS (
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
    FROM vw_mine_quarter_base
),
with_next_period AS (
    SELECT
        b.*,
        CASE
            WHEN b.quarter IN (1,2,3) THEN b.year
            WHEN b.quarter = 4 THEN b.year + 1
        END AS next_year,
        CASE
            WHEN b.quarter IN (1,2,3) THEN b.quarter + 1
            WHEN b.quarter = 4 THEN 1
        END AS next_quarter
    FROM base_rows b
),
with_next_period_key AS (
    SELECT
        wnp.*,
        CAST(wnp.next_year AS TEXT) || 'Q' || CAST(wnp.next_quarter AS TEXT) AS next_period_key
    FROM with_next_period wnp
),
joined_next AS (
    SELECT
        cur.mine_key,
        cur.period_key,
        cur.year,
        cur.quarter,
        cur.avg_employees,
        cur.employee_hours,
        cur.production_volume,
        cur.production_unit,
        cur.incident_count_current_qtr,
        cur.incident_count_prior_qtr,
        cur.violation_count_current_qtr,
        cur.violation_count_prior_qtr,
        cur.assessed_penalty_amount_current_qtr,
        cur.assessed_penalty_amount_prior_qtr,
        cur.next_period_key,
        nxt.period_key AS matched_next_period_key,
        nxt.incident_count_current_qtr AS next_quarter_incident_count
    FROM with_next_period_key cur
    LEFT JOIN vw_mine_quarter_base nxt
        ON cur.mine_key = nxt.mine_key
       AND cur.next_period_key = nxt.period_key
),
labeled AS (
    SELECT
        jn.mine_key,
        jn.period_key,
        jn.year,
        jn.quarter,
        jn.avg_employees,
        jn.employee_hours,
        jn.production_volume,
        jn.production_unit,
        jn.incident_count_current_qtr,
        jn.incident_count_prior_qtr,
        jn.violation_count_current_qtr,
        jn.violation_count_prior_qtr,
        jn.assessed_penalty_amount_current_qtr,
        jn.assessed_penalty_amount_prior_qtr,
        jn.next_period_key,
        CASE
            WHEN jn.matched_next_period_key IS NOT NULL THEN 1
            ELSE 0
        END AS next_quarter_exists_flag,
        jn.next_quarter_incident_count,
        CASE
            WHEN jn.matched_next_period_key IS NULL THEN NULL
            WHEN COALESCE(jn.next_quarter_incident_count, 0) >= 1 THEN 1
            ELSE 0
        END AS had_incident_next_qtr
    FROM joined_next jn
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
    assessed_penalty_amount_prior_qtr,
    next_period_key,
    next_quarter_exists_flag,
    next_quarter_incident_count,
    had_incident_next_qtr
FROM labeled;
