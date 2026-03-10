BEGIN;

CREATE TABLE fact_msha_incident (
    incident_key        BIGSERIAL PRIMARY KEY,
    mine_key            BIGINT NOT NULL REFERENCES dim_mine(mine_key),
    incident_date       DATE NOT NULL,
    incident_quarter    TEXT NOT NULL,
    incident_type       TEXT,
    severity_class      TEXT,
    lost_days           INTEGER,
    days_restricted     INTEGER,
    hours_worked_basis  NUMERIC(14,2),
    source_record_id    TEXT NOT NULL,
    load_ts             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_record_id)
);

ALTER TABLE fact_msha_incident
    ADD CONSTRAINT chk_incident_lost_days_nonnegative
    CHECK (lost_days IS NULL OR lost_days >= 0);

ALTER TABLE fact_msha_incident
    ADD CONSTRAINT chk_incident_days_restricted_nonnegative
    CHECK (days_restricted IS NULL OR days_restricted >= 0);

ALTER TABLE fact_msha_incident
    ADD CONSTRAINT chk_incident_hours_nonnegative
    CHECK (hours_worked_basis IS NULL OR hours_worked_basis >= 0);

ALTER TABLE fact_msha_incident
    ADD CONSTRAINT chk_incident_quarter_format
    CHECK (incident_quarter ~ '^[0-9]{4}Q[1-4]$');

CREATE INDEX idx_incident_mine_key ON fact_msha_incident(mine_key);
CREATE INDEX idx_incident_date ON fact_msha_incident(incident_date);
CREATE INDEX idx_incident_quarter ON fact_msha_incident(incident_quarter);
CREATE INDEX idx_incident_mine_quarter ON fact_msha_incident(mine_key, incident_quarter);

COMMIT;
