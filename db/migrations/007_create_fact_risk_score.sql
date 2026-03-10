BEGIN;

CREATE TABLE fact_risk_score (
    risk_score_key          BIGSERIAL PRIMARY KEY,
    mine_key                BIGINT NOT NULL REFERENCES dim_mine(mine_key),
    period_key              TEXT NOT NULL,
    model_version           TEXT NOT NULL,
    risk_score              NUMERIC(8,5) NOT NULL,
    risk_band               TEXT NOT NULL,
    top_driver_1            TEXT,
    top_driver_2            TEXT,
    top_driver_3            TEXT,
    label_next_period       INTEGER,
    prediction_ts           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (mine_key, period_key, model_version)
);

ALTER TABLE fact_risk_score
    ADD CONSTRAINT chk_risk_score_range
    CHECK (risk_score >= 0 AND risk_score <= 1);

ALTER TABLE fact_risk_score
    ADD CONSTRAINT chk_risk_band_valid
    CHECK (risk_band IN ('Low', 'Medium', 'High', 'Critical'));

ALTER TABLE fact_risk_score
    ADD CONSTRAINT chk_label_next_period_binary
    CHECK (label_next_period IS NULL OR label_next_period IN (0,1));

ALTER TABLE fact_risk_score
    ADD CONSTRAINT chk_risk_score_period_format
    CHECK (period_key ~ '^[0-9]{4}Q[1-4]$');

CREATE INDEX idx_risk_score_mine_key ON fact_risk_score(mine_key);
CREATE INDEX idx_risk_score_period_key ON fact_risk_score(period_key);
CREATE INDEX idx_risk_score_model_version ON fact_risk_score(model_version);
CREATE INDEX idx_risk_score_mine_period ON fact_risk_score(mine_key, period_key);
CREATE INDEX idx_risk_score_score ON fact_risk_score(risk_score DESC);

COMMIT;
