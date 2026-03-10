BEGIN;

CREATE TABLE fact_msha_violation (
    violation_key           BIGSERIAL PRIMARY KEY,
    mine_key                BIGINT NOT NULL REFERENCES dim_mine(mine_key),
    inspection_event_no     TEXT,
    violation_date          DATE NOT NULL,
    violation_quarter       TEXT NOT NULL,
    section_code            TEXT,
    likelihood_code         TEXT,
    negligence_code         TEXT,
    significant_substantial BOOLEAN,
    citation_order_flag     TEXT,
    source_record_id        TEXT NOT NULL,
    load_ts                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_record_id)
);

ALTER TABLE fact_msha_violation
    ADD CONSTRAINT chk_violation_quarter_format
    CHECK (violation_quarter ~ '^[0-9]{4}Q[1-4]$');

CREATE INDEX idx_violation_mine_key ON fact_msha_violation(mine_key);
CREATE INDEX idx_violation_date ON fact_msha_violation(violation_date);
CREATE INDEX idx_violation_quarter ON fact_msha_violation(violation_quarter);
CREATE INDEX idx_violation_mine_quarter ON fact_msha_violation(mine_key, violation_quarter);
CREATE INDEX idx_violation_ss_flag ON fact_msha_violation(significant_substantial);

COMMIT;
