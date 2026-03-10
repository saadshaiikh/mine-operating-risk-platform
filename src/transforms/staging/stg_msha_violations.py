from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from psycopg2.extras import execute_values

from src.common.db import delete_run_rows
from src.common.dates import derive_quarter
from src.common.hash import row_hash, sha256_text
from src.transforms.staging.helpers import (
    ensure_required_columns,
    get_value,
    normalize_bool,
    parse_date_field,
    read_pipe_rows,
)


TABLE_NAME = "stg_msha_violations"
DATASET_NAME = "msha_violations"


CANDIDATES = {
    "mine_id": ["mine_id", "mineid"],
    "event_number": ["event_no", "event_number", "inspection_event_no"],
    "violation_no": ["violation_no", "violation_number", "violation_id"],
    "violation_date": [
        "violation_issue_dt",
        "violation_date",
        "violation_dt",
        "violation_occur_dt",
    ],
    "section_code": ["section_code", "section", "section_citation", "section_cd"],
    "likelihood_code": ["likelihood", "likelihood_code", "likelihood_cd"],
    "negligence_code": ["negligence", "negligence_code", "negligence_cd"],
    "significant_substantial": ["s_and_s", "significant_substantial", "sig_sub"],
    "citation_order_flag": ["citation_order", "citation_order_flag", "citation_or_order"],
}


def _build_source_record_id(
    mine_id: str,
    event_number: str | None,
    violation_no_raw: str | None,
    violation_date: str,
    section_code: str | None,
    citation_order_flag: str | None,
) -> str:
    parts = [
        mine_id,
        event_number or "",
        violation_no_raw or "",
        violation_date,
        section_code or "",
        citation_order_flag or "",
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
        "event_number",
        "violation_no_raw",
        "violation_date_raw",
        "violation_date",
        "violation_quarter",
        "section_code",
        "likelihood_code",
        "negligence_code",
        "significant_substantial",
        "citation_order_flag",
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
            mine_id = get_value(raw_row, CANDIDATES["mine_id"])
            if not mine_id:
                raise ValueError("Missing mine_id")

            event_number = get_value(raw_row, CANDIDATES["event_number"])
            violation_no_raw = get_value(raw_row, CANDIDATES["violation_no"])
            violation_date_raw = get_value(raw_row, CANDIDATES["violation_date"])
            violation_date = parse_date_field(violation_date_raw, required=True)
            violation_quarter = derive_quarter(violation_date)

            section_code = get_value(raw_row, CANDIDATES["section_code"])
            likelihood_code = get_value(raw_row, CANDIDATES["likelihood_code"])
            negligence_code = get_value(raw_row, CANDIDATES["negligence_code"])
            significant_substantial = normalize_bool(get_value(raw_row, CANDIDATES["significant_substantial"]))
            citation_order_flag = get_value(raw_row, CANDIDATES["citation_order_flag"])

            source_record_id = _build_source_record_id(
                mine_id,
                event_number,
                violation_no_raw,
                violation_date.isoformat(),
                section_code,
                citation_order_flag,
            )

            record = (
                run_id,
                source_record_id,
                mine_id,
                event_number,
                violation_no_raw,
                violation_date_raw,
                violation_date,
                violation_quarter,
                section_code,
                likelihood_code,
                negligence_code,
                significant_substantial,
                citation_order_flag,
                row_hash(raw_row),
                source_file_name,
            )
            batch.append(record)
        except Exception:
            rows_rejected += 1
            continue

        if len(batch) >= batch_size:
            rows_loaded += _insert_rows_dedup(conn, TABLE_NAME, columns, batch, page_size=batch_size)
            batch.clear()

    if batch:
        rows_loaded += _insert_rows_dedup(conn, TABLE_NAME, columns, batch, page_size=batch_size)
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


def _insert_rows_dedup(
    conn,
    table: str,
    columns: list[str],
    rows: list[tuple[Any, ...]],
    page_size: int,
) -> int:
    if not rows:
        return 0
    values_template = "(" + ",".join(["%s"] * len(columns)) + ")"
    sql = (
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s "
        "ON CONFLICT (run_id, source_record_id) DO NOTHING"
    )
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, template=values_template, page_size=page_size)
        return cur.rowcount or 0
