BEGIN;

CREATE TABLE fact_msha_employment_production (
    ep_key                  BIGSERIAL PRIMARY KEY,
    mine_key                BIGINT NOT NULL REFERENCES dim_mine(mine_key),
    year                    INTEGER NOT NULL,
    quarter                 INTEGER NOT NULL,
    period_key              TEXT NOT NULL,
    avg_employees           NUMERIC(12,2),
    employee_hours          NUMERIC(14,2),
    production_volume       NUMERIC(16,2),
    production_unit         TEXT,
    source_record_id        TEXT NOT NULL,
    load_ts                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (mine_key, year, quarter, source_record_id)
);

ALTER TABLE fact_msha_employment_production
    ADD CONSTRAINT chk_ep_quarter_valid
    CHECK (quarter IN (1,2,3,4));

ALTER TABLE fact_msha_employment_production
    ADD CONSTRAINT chk_ep_year_reasonable
    CHECK (year >= 2000 AND year <= 2100);

ALTER TABLE fact_msha_employment_production
    ADD CONSTRAINT chk_ep_avg_employees_nonnegative
    CHECK (avg_employees IS NULL OR avg_employees >= 0);

ALTER TABLE fact_msha_employment_production
    ADD CONSTRAINT chk_ep_employee_hours_nonnegative
    CHECK (employee_hours IS NULL OR employee_hours >= 0);

ALTER TABLE fact_msha_employment_production
    ADD CONSTRAINT chk_ep_production_volume_nonnegative
    CHECK (production_volume IS NULL OR production_volume >= 0);

ALTER TABLE fact_msha_employment_production
    ADD CONSTRAINT chk_ep_period_key_format
    CHECK (period_key = (year::TEXT || 'Q' || quarter::TEXT));

CREATE INDEX idx_ep_mine_key ON fact_msha_employment_production(mine_key);
CREATE INDEX idx_ep_period_key ON fact_msha_employment_production(period_key);
CREATE INDEX idx_ep_mine_period ON fact_msha_employment_production(mine_key, period_key);
CREATE INDEX idx_ep_year_quarter ON fact_msha_employment_production(year, quarter);

COMMIT;
