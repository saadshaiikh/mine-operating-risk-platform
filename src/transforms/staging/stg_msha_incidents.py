from __future__ import annotations

import time
from decimal import Decimal
from pathlib import Path
from typing import Any

from src.common.db import delete_run_rows, insert_rows
from src.common.dates import derive_quarter
from src.common.hash import row_hash
from src.transforms.staging.helpers import (
    ensure_required_columns,
    get_value,
    parse_date_field,
    parse_decimal,
    parse_int,
    read_pipe_rows,
)


TABLE_NAME = "stg_msha_incidents"
DATASET_NAME = "msha_incidents"


CANDIDATES = {
    "document_number": ["document_number", "document_no", "document_num"],
    "mine_id": ["mine_id", "mineid"],
    "incident_date": ["accident_dt", "incident_date", "accident_date"],
    "incident_type": ["accident_class", "incident_type", "accident_type", "occurrence_type"],
    "severity_class": ["severity", "injury_severity", "degree_injury", "severity_class"],
    "lost_days": ["lost_days", "lost_workdays", "lost_workday_cnt"],
    "days_restricted": ["days_restricted", "restricted_workdays", "restricted_workday_cnt"],
    "hours_worked_basis": ["hours_worked", "hours_worked_basis", "employee_hours"],
}


def _nonnegative(value: Decimal | int | None) -> bool:
    if value is None:
        return True
    return value >= 0


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
        "document_number",
        "mine_id",
        "incident_date_raw",
        "incident_date",
        "incident_quarter",
        "incident_type_raw",
        "severity_class_raw",
        "lost_days",
        "days_restricted",
        "hours_worked_basis",
        "row_hash",
        "source_file_name",
    ]

    batch: list[tuple[Any, ...]] = []
    rows_read = 0
    rows_loaded = 0
    rows_rejected = 0

    for raw_row in rows:
        rows_read += 1
        try:
            document_number = get_value(raw_row, CANDIDATES["document_number"])
            mine_id = get_value(raw_row, CANDIDATES["mine_id"])
            incident_date_raw = get_value(raw_row, CANDIDATES["incident_date"])
            incident_date = parse_date_field(incident_date_raw, required=True)
            if not document_number or not mine_id:
                raise ValueError("Missing required keys")

            incident_quarter = derive_quarter(incident_date)
            incident_type_raw = get_value(raw_row, CANDIDATES["incident_type"])
            severity_class_raw = get_value(raw_row, CANDIDATES["severity_class"])

            lost_days = parse_int(get_value(raw_row, CANDIDATES["lost_days"]))
            days_restricted = parse_int(get_value(raw_row, CANDIDATES["days_restricted"]))
            hours_worked_basis = parse_decimal(get_value(raw_row, CANDIDATES["hours_worked_basis"]))

            if not _nonnegative(lost_days):
                raise ValueError("Negative lost_days")
            if not _nonnegative(days_restricted):
                raise ValueError("Negative days_restricted")
            if not _nonnegative(hours_worked_basis):
                raise ValueError("Negative hours_worked_basis")

            record = (
                run_id,
                document_number,
                document_number,
                mine_id,
                incident_date_raw,
                incident_date,
                incident_quarter,
                incident_type_raw,
                severity_class_raw,
                lost_days,
                days_restricted,
                hours_worked_basis,
                row_hash(raw_row),
                source_file_name,
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
