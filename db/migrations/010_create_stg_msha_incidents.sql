BEGIN;

CREATE TABLE stg_msha_incidents (
    run_id              TEXT NOT NULL,
    source_record_id    TEXT NOT NULL,
    document_number     TEXT,
    mine_id             TEXT,
    incident_date_raw   TEXT,
    incident_date       DATE,
    incident_quarter    TEXT,
    incident_type_raw   TEXT,
    severity_class_raw  TEXT,
    lost_days           INTEGER,
    days_restricted     INTEGER,
    hours_worked_basis  NUMERIC(14,2),
    row_hash            TEXT NOT NULL,
    source_file_name    TEXT NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, source_record_id)
);

CREATE INDEX idx_stg_incidents_run_id ON stg_msha_incidents(run_id);
CREATE INDEX idx_stg_incidents_source_record_id ON stg_msha_incidents(source_record_id);
CREATE INDEX idx_stg_incidents_mine_id ON stg_msha_incidents(mine_id);
CREATE INDEX idx_stg_incidents_quarter ON stg_msha_incidents(incident_quarter);

COMMIT;
