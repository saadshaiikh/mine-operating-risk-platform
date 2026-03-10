from __future__ import annotations

import time
from decimal import Decimal
from pathlib import Path
from typing import Any

from src.common.db import delete_run_rows, insert_rows
from src.common.hash import row_hash
from src.transforms.staging.helpers import (
    ensure_required_columns,
    get_value,
    parse_decimal,
    read_pipe_rows,
)


TABLE_NAME = "stg_msha_mines"
DATASET_NAME = "msha_mines"


CANDIDATES = {
    "mine_id": ["mine_id", "mineid"],
    "mine_name": ["mine_name", "mine_name_raw", "mine_name_txt", "mine_name"],
    "status": ["current_status", "status", "mine_status", "current_status_cd"],
    "state": ["state", "state_abbr", "state_cd", "province_state"],
    "commodity": ["primary_commodity", "commodity", "primary_commodity_cd", "commodity_cd"],
    "mine_type": ["mine_type", "mine_type_cd", "mine_type_desc", "mine_type_raw"],
    "latitude": ["latitude", "lat"],
    "longitude": ["longitude", "lon", "long"],
}


def _to_decimal(value: str | None) -> Decimal | None:
    if value is None:
        return None
    return parse_decimal(value)


def stage_dataset(
    *,
    run_id: str,
    txt_path: Path,
    source_file_name: str,
    delimiter: str,
    encoding: str,
    batch_size: int,
    required_columns: list[str],
    conn,
) -> dict[str, Any]:
    start = time.monotonic()

    headers, rows = read_pipe_rows(txt_path, delimiter=delimiter, encoding=encoding)
    ensure_required_columns(headers, required_columns, CANDIDATES)

    delete_run_rows(conn, TABLE_NAME, run_id)

    columns = [
        "run_id",
        "source_record_id",
        "mine_id_raw",
        "mine_name_raw",
        "status_raw",
        "state_raw",
        "commodity_raw",
        "mine_type_raw",
        "latitude_raw",
        "longitude_raw",
        "row_hash",
        "source_file_name",
        "mine_id",
        "mine_name",
        "status",
        "province_state",
        "commodity_group",
        "mine_type",
        "latitude",
        "longitude",
    ]

    batch: list[tuple[Any, ...]] = []
    rows_read = 0
    rows_loaded = 0
    rows_rejected = 0

    for raw_row in rows:
        rows_read += 1
        try:
            mine_id_raw = get_value(raw_row, CANDIDATES["mine_id"])
            if not mine_id_raw:
                raise ValueError("Missing mine_id")

            mine_name_raw = get_value(raw_row, CANDIDATES["mine_name"])
            status_raw = get_value(raw_row, CANDIDATES["status"])
            state_raw = get_value(raw_row, CANDIDATES["state"])
            commodity_raw = get_value(raw_row, CANDIDATES["commodity"])
            mine_type_raw = get_value(raw_row, CANDIDATES["mine_type"])
            latitude_raw = get_value(raw_row, CANDIDATES["latitude"])
            longitude_raw = get_value(raw_row, CANDIDATES["longitude"])

            latitude = _to_decimal(latitude_raw)
            longitude = _to_decimal(longitude_raw)

            source_record_id = mine_id_raw
            row_hash_value = row_hash(raw_row)

            record = (
                run_id,
                source_record_id,
                mine_id_raw,
                mine_name_raw,
                status_raw,
                state_raw,
                commodity_raw,
                mine_type_raw,
                latitude_raw,
                longitude_raw,
                row_hash_value,
                source_file_name,
                mine_id_raw,
                mine_name_raw,
                status_raw,
                state_raw,
                commodity_raw,
                mine_type_raw,
                latitude,
                longitude,
            )
            batch.append(record)
        except Exception:
            rows_rejected += 1
            continue

        if len(batch) >= batch_size:
            insert_rows(conn, TABLE_NAME, columns, batch, page_size=batch_size)
            rows_loaded += len(batch)
            batch.clear()

    if batch:
        insert_rows(conn, TABLE_NAME, columns, batch, page_size=batch_size)
        rows_loaded += len(batch)
        batch.clear()

    duration = time.monotonic() - start

    return {
        "run_id": run_id,
        "dataset_name": DATASET_NAME,
        "stage_table": TABLE_NAME,
        "source_file_name": source_file_name,
        "rows_read": rows_read,
        "rows_loaded": rows_loaded,
        "rows_rejected": rows_rejected,
        "duration_seconds": round(duration, 3),
        "status": "SUCCESS" if rows_rejected == 0 else "WARN",
    }
