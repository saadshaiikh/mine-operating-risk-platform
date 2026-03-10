BEGIN;

CREATE TABLE stg_msha_assessed_violations (
    run_id                  TEXT NOT NULL,
    source_record_id        TEXT NOT NULL,
    mine_id                 TEXT,
    assessment_case_no      TEXT,
    assessed_date_raw       TEXT,
    assessed_date           DATE,
    assessed_quarter        TEXT,
    section_code            TEXT,
    proposed_penalty_amount NUMERIC(14,2),
    assessment_amount       NUMERIC(14,2),
    interest_amount         NUMERIC(14,2),
    case_status_raw         TEXT,
    violation_reference_raw TEXT,
    row_hash                TEXT NOT NULL,
    source_file_name        TEXT NOT NULL,
    ingested_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, source_record_id)
);

CREATE INDEX idx_stg_assessed_run_id ON stg_msha_assessed_violations(run_id);
CREATE INDEX idx_stg_assessed_source_record_id ON stg_msha_assessed_violations(source_record_id);
CREATE INDEX idx_stg_assessed_mine_id ON stg_msha_assessed_violations(mine_id);
CREATE INDEX idx_stg_assessed_quarter ON stg_msha_assessed_violations(assessed_quarter);

COMMIT;
