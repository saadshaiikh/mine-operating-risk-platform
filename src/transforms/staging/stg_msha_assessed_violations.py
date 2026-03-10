from __future__ import annotations

import time
from decimal import Decimal
from pathlib import Path
from typing import Any

from src.common.db import delete_run_rows, insert_rows
from src.common.dates import derive_quarter
from src.common.hash import row_hash, sha256_text
from src.transforms.staging.helpers import (
    ensure_required_columns,
    get_value,
    parse_date_field,
    parse_decimal,
    read_pipe_rows,
)


TABLE_NAME = "stg_msha_assessed_violations"
DATASET_NAME = "msha_assessed_violations"


CANDIDATES = {
    "mine_id": ["mine_id", "mineid"],
    "assessment_case_no": ["assessment_case_no", "assessment_case_number", "case_no", "case_number"],
    "assessed_date": [
        "assessed_date",
        "assessment_date",
        "assess_case_status_dt",
        "issue_dt",
        "occurrence_dt",
        "final_order_dt",
        "bill_print_dt",
    ],
    "section_code": ["section_code", "section", "section_cd", "mine_act_section"],
    "proposed_penalty_amount": [
        "proposed_penalty_amount",
        "proposed_penalty",
        "proposed_penalty_amt",
    ],
    "assessment_amount": ["assessment_amount", "assessed_amount", "assessment_amt"],
    "interest_amount": ["interest_amount", "interest_amt"],
    "case_status": ["case_status", "case_status_cd", "assess_case_status"],
    "violation_reference": ["violation_no", "violation_number", "violation_reference", "violation_id"],
}


def _nonnegative(value: Decimal | None) -> bool:
    if value is None:
        return True
    return value >= 0


def _build_source_record_id(
    mine_id: str,
    assessment_case_no: str | None,
    section_code: str | None,
    assessed_date: str | None,
    proposed_penalty_amount: Decimal | None,
) -> str:
    parts = [
        mine_id,
        assessment_case_no or "",
        section_code or "",
        assessed_date or "",
        str(proposed_penalty_amount or ""),
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

    headers, rows = read_pipe_rows(txt_path, delimiter=delimiter, encoding=encoding)
    ensure_required_columns(headers, required_columns, CANDIDATES)

    delete_run_rows(conn, TABLE_NAME, run_id)

    columns = [
        "run_id",
        "source_record_id",
        "mine_id",
        "assessment_case_no",
        "assessed_date_raw",
        "assessed_date",
        "assessed_quarter",
        "section_code",
        "proposed_penalty_amount",
        "assessment_amount",
        "interest_amount",
        "case_status_raw",
        "violation_reference_raw",
        "row_hash",
        "source_file_name",
    ]

    batch: list[tuple[Any, ...]] = []
    rows_read = 0
    rows_loaded = 0
    rows_rejected = 0

    require_assessed_date = "assessed_date" in required_columns

    for raw_row in rows:
        rows_read += 1
        try:
            mine_id = get_value(raw_row, CANDIDATES["mine_id"])
            if not mine_id:
                raise ValueError("Missing mine_id")

            assessment_case_no = get_value(raw_row, CANDIDATES["assessment_case_no"])
            assessed_date_raw = get_value(raw_row, CANDIDATES["assessed_date"])
            assessed_date = parse_date_field(assessed_date_raw, required=require_assessed_date)
            assessed_quarter = derive_quarter(assessed_date) if assessed_date else None

            section_code = get_value(raw_row, CANDIDATES["section_code"])
            proposed_penalty_amount = parse_decimal(get_value(raw_row, CANDIDATES["proposed_penalty_amount"]))
            assessment_amount = parse_decimal(get_value(raw_row, CANDIDATES["assessment_amount"]))
            interest_amount = parse_decimal(get_value(raw_row, CANDIDATES["interest_amount"]))
            case_status_raw = get_value(raw_row, CANDIDATES["case_status"])
            violation_reference_raw = get_value(raw_row, CANDIDATES["violation_reference"])

            if not _nonnegative(proposed_penalty_amount):
                raise ValueError("Negative proposed_penalty_amount")
            if not _nonnegative(assessment_amount):
                raise ValueError("Negative assessment_amount")
            if not _nonnegative(interest_amount):
                raise ValueError("Negative interest_amount")

            source_record_id = _build_source_record_id(
                mine_id,
                assessment_case_no,
                section_code,
                assessed_date_raw,
                proposed_penalty_amount,
            )

            record = (
                run_id,
                source_record_id,
                mine_id,
                assessment_case_no,
                assessed_date_raw,
                assessed_date,
                assessed_quarter,
                section_code,
                proposed_penalty_amount,
                assessment_amount,
                interest_amount,
                case_status_raw,
                violation_reference_raw,
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
