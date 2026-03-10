CREATE OR REPLACE VIEW vw_backtest_summary AS
WITH split_rows AS (
    SELECT
        fbr.model_name,
        fbr.model_version,
        fbr.validation_year,
        fbr.train_start_year,
        fbr.train_end_year,
        fbr.n_train_rows,
        fbr.n_validation_rows,
        fbr.n_validation_positives,
        fbr.base_rate,
        fbr.top_decile_size,
        fbr.top_decile_positive_count,
        fbr.roc_auc,
        fbr.pr_auc,
        fbr.precision_at_top_decile,
        fbr.recall_at_top_decile,
        fbr.lift_vs_base_rate
    FROM fact_backtest_result fbr
),
aggregated AS (
    SELECT
        sr.model_name,
        COUNT(*) AS n_splits_for_model,
        AVG(sr.roc_auc) FILTER (WHERE sr.roc_auc IS NOT NULL) AS macro_roc_auc,
        AVG(sr.pr_auc) FILTER (WHERE sr.pr_auc IS NOT NULL) AS macro_pr_auc,
        AVG(sr.precision_at_top_decile) FILTER (WHERE sr.precision_at_top_decile IS NOT NULL) AS macro_precision_at_top_decile,
        AVG(sr.recall_at_top_decile) FILTER (WHERE sr.recall_at_top_decile IS NOT NULL) AS macro_recall_at_top_decile,
        AVG(sr.lift_vs_base_rate) FILTER (WHERE sr.lift_vs_base_rate IS NOT NULL) AS macro_lift_vs_base_rate,

        SUM(sr.n_validation_rows) AS pooled_validation_rows,
        SUM(sr.n_validation_positives) AS pooled_validation_positives,
        SUM(sr.top_decile_size) AS pooled_top_decile_size,
        SUM(sr.top_decile_positive_count) AS pooled_top_decile_positive_count
    FROM split_rows sr
    GROUP BY sr.model_name
),
final_agg AS (
    SELECT
        a.*,
        CASE
            WHEN a.pooled_validation_rows > 0
            THEN a.pooled_validation_positives::NUMERIC / a.pooled_validation_rows
            ELSE NULL
        END AS pooled_base_rate,
        CASE
            WHEN a.pooled_top_decile_size > 0
            THEN a.pooled_top_decile_positive_count::NUMERIC / a.pooled_top_decile_size
            ELSE NULL
        END AS pooled_precision_at_top_decile,
        CASE
            WHEN a.pooled_validation_positives > 0
            THEN a.pooled_top_decile_positive_count::NUMERIC / a.pooled_validation_positives
            ELSE NULL
        END AS pooled_recall_at_top_decile
    FROM aggregated a
)
SELECT
    CONCAT(sr.model_version, '|', sr.validation_year) AS model_validation_key,
    sr.model_name,
    sr.model_version,
    sr.validation_year,
    sr.train_start_year,
    sr.train_end_year,
    sr.n_train_rows,
    sr.n_validation_rows,
    sr.n_validation_positives,
    sr.base_rate,
    sr.top_decile_size,
    sr.top_decile_positive_count,
    sr.roc_auc,
    sr.pr_auc,
    sr.precision_at_top_decile,
    sr.recall_at_top_decile,
    sr.lift_vs_base_rate,

    fa.n_splits_for_model,
    fa.macro_roc_auc,
    fa.macro_pr_auc,
    fa.macro_precision_at_top_decile,
    fa.macro_recall_at_top_decile,
    fa.macro_lift_vs_base_rate,

    fa.pooled_validation_rows,
    fa.pooled_validation_positives,
    fa.pooled_top_decile_size,
    fa.pooled_top_decile_positive_count,
    fa.pooled_base_rate,
    fa.pooled_precision_at_top_decile,
    fa.pooled_recall_at_top_decile,
    CASE
        WHEN fa.pooled_base_rate IS NOT NULL AND fa.pooled_base_rate > 0
        THEN fa.pooled_precision_at_top_decile / fa.pooled_base_rate
        ELSE NULL
    END AS pooled_lift_vs_base_rate,
    CASE
        WHEN fa.pooled_recall_at_top_decile IS NOT NULL
         AND fa.pooled_base_rate IS NOT NULL
         AND fa.pooled_base_rate > 0
        THEN
            sr.model_name || ': top 10% highest-risk mines captured '
            || TO_CHAR(fa.pooled_recall_at_top_decile * 100, 'FM999990.0')
            || '% of next-quarter incidents at '
            || TO_CHAR((fa.pooled_precision_at_top_decile / fa.pooled_base_rate), 'FM999990.00')
            || 'x the base rate.'
        WHEN fa.pooled_recall_at_top_decile IS NOT NULL
        THEN
            sr.model_name || ': top 10% highest-risk mines captured '
            || TO_CHAR(fa.pooled_recall_at_top_decile * 100, 'FM999990.0')
            || '% of next-quarter incidents.'
        ELSE
            sr.model_name || ': pooled capture rate could not be computed.'
    END AS business_claim_text
FROM split_rows sr
LEFT JOIN final_agg fa
    ON sr.model_name = fa.model_name;
