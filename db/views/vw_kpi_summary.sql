CREATE OR REPLACE VIEW vw_kpi_summary AS
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
aggregated AS (
    SELECT
        s.model_name,
        s.model_version,
        s.period_key,
        s.year,
        s.quarter,
        COUNT(*) AS total_mines_scored,
        AVG(s.risk_score) AS avg_risk_score,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY s.risk_score) AS median_risk_score,
        MAX(s.risk_score) AS max_risk_score,
        COUNT(*) FILTER (WHERE s.risk_band = 'High') AS high_risk_mine_count,
        COUNT(*) FILTER (WHERE s.risk_band = 'Critical') AS critical_risk_mine_count,
        COUNT(*) FILTER (WHERE s.risk_band IN ('High', 'Critical')) AS high_or_critical_risk_mine_count,
        COUNT(*) FILTER (WHERE s.label_next_period = 1) AS actual_positive_count,
        COUNT(*) FILTER (
            WHERE s.risk_band IN ('High', 'Critical')
              AND s.label_next_period = 1
        ) AS flagged_positive_count
    FROM scored s
    GROUP BY
        s.model_name,
        s.model_version,
        s.period_key,
        s.year,
        s.quarter
)
SELECT
    CONCAT(a.model_version, '|', a.period_key) AS summary_key,
    a.model_name,
    a.model_version,
    a.period_key,
    a.year,
    a.quarter,
    CASE
        WHEN (a.year * 10 + a.quarter) = lp.latest_period_sort THEN 1
        ELSE 0
    END AS is_latest_period_for_model_flag,
    a.total_mines_scored,
    a.avg_risk_score,
    a.median_risk_score,
    a.max_risk_score,
    a.high_risk_mine_count,
    a.critical_risk_mine_count,
    a.high_or_critical_risk_mine_count,
    CASE
        WHEN a.total_mines_scored > 0
        THEN a.high_or_critical_risk_mine_count::NUMERIC / a.total_mines_scored
        ELSE NULL
    END AS high_or_critical_risk_share,
    a.actual_positive_count,
    a.flagged_positive_count,
    CASE
        WHEN a.high_or_critical_risk_mine_count > 0
        THEN a.flagged_positive_count::NUMERIC / a.high_or_critical_risk_mine_count
        ELSE NULL
    END AS flagged_positive_precision
FROM aggregated a
LEFT JOIN latest_period_by_model lp
    ON a.model_version = lp.model_version;
