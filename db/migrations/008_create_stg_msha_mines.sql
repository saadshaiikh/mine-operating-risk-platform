BEGIN;

CREATE TABLE stg_msha_mines (
    run_id          TEXT NOT NULL,
    source_record_id TEXT NOT NULL,
    mine_id_raw     TEXT,
    mine_name_raw   TEXT,
    status_raw      TEXT,
    state_raw       TEXT,
    commodity_raw   TEXT,
    mine_type_raw   TEXT,
    latitude_raw    TEXT,
    longitude_raw   TEXT,
    row_hash        TEXT NOT NULL,
    source_file_name TEXT NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    mine_id         TEXT,
    mine_name       TEXT,
    status          TEXT,
    province_state  TEXT,
    commodity_group TEXT,
    mine_type       TEXT,
    latitude        NUMERIC(10,6),
    longitude       NUMERIC(10,6),
    UNIQUE (run_id, source_record_id)
);

CREATE INDEX idx_stg_msha_mines_run_id ON stg_msha_mines(run_id);
CREATE INDEX idx_stg_msha_mines_source_record_id ON stg_msha_mines(source_record_id);
CREATE INDEX idx_stg_msha_mines_mine_id ON stg_msha_mines(mine_id);

COMMIT;
