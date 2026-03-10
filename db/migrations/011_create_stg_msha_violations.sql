BEGIN;

CREATE TABLE stg_msha_violations (
    run_id                  TEXT NOT NULL,
    source_record_id        TEXT NOT NULL,
    mine_id                 TEXT,
    event_number            TEXT,
    violation_no_raw        TEXT,
    violation_date_raw      TEXT,
    violation_date          DATE,
    violation_quarter       TEXT,
    section_code            TEXT,
    likelihood_code         TEXT,
    negligence_code         TEXT,
    significant_substantial BOOLEAN,
    citation_order_flag     TEXT,
    row_hash                TEXT NOT NULL,
    source_file_name        TEXT NOT NULL,
    ingested_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, source_record_id)
);

CREATE INDEX idx_stg_violations_run_id ON stg_msha_violations(run_id);
CREATE INDEX idx_stg_violations_source_record_id ON stg_msha_violations(source_record_id);
CREATE INDEX idx_stg_violations_mine_id ON stg_msha_violations(mine_id);
CREATE INDEX idx_stg_violations_quarter ON stg_msha_violations(violation_quarter);

COMMIT;
