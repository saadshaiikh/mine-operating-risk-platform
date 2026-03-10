BEGIN;

CREATE TABLE fact_backtest_result (
    backtest_result_key         BIGSERIAL PRIMARY KEY,
    backtest_run_id             TEXT NOT NULL,
    model_name                  TEXT NOT NULL,
    model_version               TEXT NOT NULL,
    train_start_year            INTEGER NOT NULL,
    train_end_year              INTEGER NOT NULL,
    validation_year             INTEGER NOT NULL,
    n_train_rows                INTEGER NOT NULL,
    n_validation_rows           INTEGER NOT NULL,
    n_validation_positives      INTEGER,
    base_rate                   NUMERIC(10,6),
    top_decile_size             INTEGER NOT NULL,
    top_decile_positive_count   INTEGER,
    roc_auc                     NUMERIC(10,6),
    pr_auc                      NUMERIC(10,6),
    precision_at_top_decile     NUMERIC(10,6),
    recall_at_top_decile        NUMERIC(10,6),
    lift_vs_base_rate           NUMERIC(10,6),
    created_at                  TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (model_version, validation_year)
);

CREATE INDEX idx_backtest_model_name
    ON fact_backtest_result(model_name);

CREATE INDEX idx_backtest_model_version
    ON fact_backtest_result(model_version);

CREATE INDEX idx_backtest_validation_year
    ON fact_backtest_result(validation_year);

CREATE INDEX idx_backtest_run_id
    ON fact_backtest_result(backtest_run_id);

COMMIT;
