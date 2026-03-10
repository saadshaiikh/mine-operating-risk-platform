BEGIN;

CREATE TABLE dim_mine (
    mine_key            BIGSERIAL PRIMARY KEY,
    mine_id             TEXT NOT NULL,
    source_system       TEXT NOT NULL,
    mine_name           TEXT,
    country_code        TEXT,
    province_state      TEXT,
    commodity_group     TEXT,
    mine_type           TEXT,
    latitude            NUMERIC(10,6),
    longitude           NUMERIC(10,6),
    active_flag         BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (mine_id, source_system)
);

CREATE INDEX idx_dim_mine_source_system ON dim_mine(source_system);
CREATE INDEX idx_dim_mine_mine_id ON dim_mine(mine_id);
CREATE INDEX idx_dim_mine_state ON dim_mine(province_state);

COMMIT;
