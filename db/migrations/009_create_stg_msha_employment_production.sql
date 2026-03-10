BEGIN;

CREATE TABLE stg_msha_employment_production (
    run_id              TEXT NOT NULL,
    source_record_id    TEXT NOT NULL,
    mine_id             TEXT,
    year                INTEGER,
    quarter             INTEGER,
    period_key          TEXT,
    subunit_code        TEXT,
    avg_employees       NUMERIC(12,2),
    employee_hours      NUMERIC(14,2),
    production_volume   NUMERIC(16,2),
    production_unit_raw TEXT,
    row_hash            TEXT NOT NULL,
    source_file_name    TEXT NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, source_record_id)
);

CREATE INDEX idx_stg_ep_run_id ON stg_msha_employment_production(run_id);
CREATE INDEX idx_stg_ep_source_record_id ON stg_msha_employment_production(source_record_id);
CREATE INDEX idx_stg_ep_mine_id ON stg_msha_employment_production(mine_id);
CREATE INDEX idx_stg_ep_period_key ON stg_msha_employment_production(period_key);

COMMIT;
