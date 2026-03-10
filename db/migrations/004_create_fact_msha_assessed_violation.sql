BEGIN;

CREATE TABLE fact_msha_assessed_violation (
    assessed_violation_key  BIGSERIAL PRIMARY KEY,
    mine_key                BIGINT NOT NULL REFERENCES dim_mine(mine_key),
    assessed_date           DATE,
    assessed_quarter        TEXT,
    penalty_amount          NUMERIC(14,2),
    violation_reference_id  TEXT,
    source_record_id        TEXT NOT NULL,
    load_ts                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_record_id)
);

ALTER TABLE fact_msha_assessed_violation
    ADD CONSTRAINT chk_assessed_penalty_nonnegative
    CHECK (penalty_amount IS NULL OR penalty_amount >= 0);

ALTER TABLE fact_msha_assessed_violation
    ADD CONSTRAINT chk_assessed_quarter_format
    CHECK (assessed_quarter IS NULL OR assessed_quarter ~ '^[0-9]{4}Q[1-4]$');

CREATE INDEX idx_assessed_mine_key ON fact_msha_assessed_violation(mine_key);
CREATE INDEX idx_assessed_quarter ON fact_msha_assessed_violation(assessed_quarter);
CREATE INDEX idx_assessed_mine_quarter ON fact_msha_assessed_violation(mine_key, assessed_quarter);
CREATE INDEX idx_assessed_penalty_amount ON fact_msha_assessed_violation(penalty_amount);

COMMIT;
