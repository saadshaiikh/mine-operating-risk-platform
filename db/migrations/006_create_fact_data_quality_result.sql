BEGIN;

CREATE TABLE fact_data_quality_result (
    dq_key               BIGSERIAL PRIMARY KEY,
    run_id               TEXT NOT NULL,
    test_name            TEXT NOT NULL,
    entity_name          TEXT NOT NULL,
    status               TEXT NOT NULL,
    failure_count        INTEGER,
    severity             TEXT,
    checked_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE fact_data_quality_result
    ADD CONSTRAINT chk_dq_status_valid
    CHECK (status IN ('PASS', 'FAIL', 'WARN'));

ALTER TABLE fact_data_quality_result
    ADD CONSTRAINT chk_dq_failure_count_nonnegative
    CHECK (failure_count IS NULL OR failure_count >= 0);

ALTER TABLE fact_data_quality_result
    ADD CONSTRAINT chk_dq_severity_valid
    CHECK (severity IS NULL OR severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL'));

CREATE INDEX idx_dq_run_id ON fact_data_quality_result(run_id);
CREATE INDEX idx_dq_entity_name ON fact_data_quality_result(entity_name);
CREATE INDEX idx_dq_status ON fact_data_quality_result(status);
CREATE INDEX idx_dq_checked_at ON fact_data_quality_result(checked_at);

COMMIT;
