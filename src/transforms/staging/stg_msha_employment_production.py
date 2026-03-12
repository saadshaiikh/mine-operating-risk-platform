from __future__ import annotations

import time
from decimal import Decimal
from pathlib import Path
from typing import Any

from src.common.db import delete_run_rows, insert_rows
from src.common.logging import get_logger, log_event
from src.common.hash import row_hash, sha256_text
from src.transforms.staging.helpers import (
    ensure_required_columns,
    get_value,
    parse_decimal,
    parse_int,
    read_pipe_rows,
)


TABLE_NAME = "stg_msha_employment_production"
DATASET_NAME = "msha_employment_production_quarterly"


CANDIDATES = {
    "mine_id": ["mine_id", "mineid"],
    "year": ["cal_yr", "year"],
    "quarter": ["cal_qtr", "quarter"],
    "subunit_code": ["subunit_cd", "subunit_code", "subunit"],
    "avg_employees": ["avg_employees", "avg_employee", "avg_emp", "avg_employment"],
    "employee_hours": ["employee_hours", "emp_hours", "hours_worked"],
    "production_volume": ["production", "production_volume", "prod_tons", "total_production"],
    "production_unit": ["production_unit", "prod_unit", "unit"],
}


def _nonnegative(value: Decimal | None) -> bool:
    if value is None:
        return True
    return value >= 0


def _build_source_record_id(
    mine_id: str,
    year: int,
    quarter: int,
    subunit: str | None,
    avg_employees: Decimal | None,
    employee_hours: Decimal | None,
    production_volume: Decimal | None,
    production_unit: str | None,
) -> str:
    parts = [
        mine_id,
        str(year),
        str(quarter),
        subunit or "",
        str(avg_employees or ""),
        str(employee_hours or ""),
        str(production_volume or ""),
        production_unit or "",
    ]
    return sha256_text("|".join(parts))


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
    logger = get_logger("stg_msha_employment_production")
    effective_batch_size = min(batch_size, 1000)
    progress_interval = 50000

    headers, rows = read_pipe_rows(txt_path, delimiter=delimiter, encoding=encoding)
    ensure_required_columns(headers, required_columns, CANDIDATES)

    delete_run_rows(conn, TABLE_NAME, run_id)
    conn.commit()

    columns = [
        "run_id",
        "source_record_id",
        "mine_id",
        "year",
        "quarter",
        "period_key",
        "subunit_code",
        "avg_employees",
        "employee_hours",
        "production_volume",
        "production_unit_raw",
        "row_hash",
        "source_file_name",
    ]

    batch: list[tuple[Any, ...]] = []
    rows_read = 0
    rows_loaded = 0
    rows_rejected = 0
    next_log_threshold = progress_interval

    for raw_row in rows:
        rows_read += 1
        try:
            mine_id = get_value(raw_row, CANDIDATES["mine_id"])
            if not mine_id:
                raise ValueError("Missing mine_id")

            year_val = get_value(raw_row, CANDIDATES["year"])
            quarter_val = get_value(raw_row, CANDIDATES["quarter"])
            year = parse_int(year_val, required=True)
            quarter = parse_int(quarter_val, required=True)
            if quarter not in (1, 2, 3, 4):
                raise ValueError("Invalid quarter")

            subunit = get_value(raw_row, CANDIDATES["subunit_code"])

            avg_employees = parse_decimal(get_value(raw_row, CANDIDATES["avg_employees"]))
            employee_hours = parse_decimal(get_value(raw_row, CANDIDATES["employee_hours"]))
            production_volume = parse_decimal(get_value(raw_row, CANDIDATES["production_volume"]))
            production_unit_raw = get_value(raw_row, CANDIDATES["production_unit"])

            if not _nonnegative(avg_employees):
                raise ValueError("Negative avg_employees")
            if not _nonnegative(employee_hours):
                raise ValueError("Negative employee_hours")
            if not _nonnegative(production_volume):
                raise ValueError("Negative production_volume")

            period_key = f"{year}Q{quarter}"

            source_record_id = _build_source_record_id(
                mine_id,
                year,
                quarter,
                subunit,
                avg_employees,
                employee_hours,
                production_volume,
                production_unit_raw,
            )

            record = (
                run_id,
                source_record_id,
                mine_id,
                year,
                quarter,
                period_key,
                subunit,
                avg_employees,
                employee_hours,
                production_volume,
                production_unit_raw,
                row_hash(raw_row),
                source_file_name,
            )
            batch.append(record)
        except Exception:
            rows_rejected += 1
            continue

        if len(batch) >= effective_batch_size:
            insert_rows(conn, TABLE_NAME, columns, batch, page_size=effective_batch_size)
            rows_loaded += len(batch)
            conn.commit()
            batch.clear()

        if rows_read >= next_log_threshold:
            log_event(
                logger,
                {
                    "event": "stage_progress",
                    "run_id": run_id,
                    "stage_table": TABLE_NAME,
                    "rows_read": rows_read,
                    "rows_loaded": rows_loaded,
                    "rows_rejected": rows_rejected,
                },
            )
            next_log_threshold += progress_interval

    if batch:
        insert_rows(conn, TABLE_NAME, columns, batch, page_size=effective_batch_size)
        rows_loaded += len(batch)
        conn.commit()
        batch.clear()

    duration = time.monotonic() - start

    log_event(
        logger,
        {
            "event": "stage_complete",
            "run_id": run_id,
            "stage_table": TABLE_NAME,
            "rows_read": rows_read,
            "rows_loaded": rows_loaded,
            "rows_rejected": rows_rejected,
        },
    )

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
