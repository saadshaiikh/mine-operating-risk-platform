from __future__ import annotations

import csv
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable

from src.common.dates import parse_date
from src.common.io import normalize_headers, normalize_row_keys, normalize_str


def read_pipe_rows(txt_path: Path, delimiter: str, encoding: str) -> tuple[list[str], Iterable[dict[str, Any]]]:
    handle = txt_path.open("r", encoding=encoding, newline="")
    reader = csv.DictReader(handle, delimiter=delimiter)
    if reader.fieldnames is None:
        handle.close()
        raise ValueError("Missing header row")
    normalized = normalize_headers(reader.fieldnames)
    reader.fieldnames = normalized

    def _iter():
        for row in reader:
            yield normalize_row_keys(row)
        handle.close()

    return normalized, _iter()


def resolve_column(headers: list[str], candidates: list[str]) -> str | None:
    for name in candidates:
        if name in headers:
            return name
    return None


def get_value(row: dict[str, Any], candidates: list[str]) -> str | None:
    for name in candidates:
        if name in row:
            return normalize_str(row.get(name))
    return None


def normalize_bool(value: str | None) -> bool | None:
    if value is None:
        return None
    raw = value.strip().lower()
    if raw in {"y", "yes", "true", "1"}:
        return True
    if raw in {"n", "no", "false", "0"}:
        return False
    return None


def parse_int(value: str | None, required: bool = False) -> int | None:
    if value is None:
        if required:
            raise ValueError("Missing required integer")
        return None
    raw = value.strip()
    if not raw:
        if required:
            raise ValueError("Missing required integer")
        return None
    try:
        return int(raw)
    except ValueError as exc:
        if required:
            raise
        raise ValueError(f"Invalid integer: {value}") from exc


def parse_decimal(value: str | None, required: bool = False) -> Decimal | None:
    if value is None:
        if required:
            raise ValueError("Missing required decimal")
        return None
    raw = value.strip()
    if not raw:
        if required:
            raise ValueError("Missing required decimal")
        return None
    raw = raw.replace(",", "")
    try:
        return Decimal(raw)
    except (InvalidOperation, ValueError) as exc:
        if required:
            raise
        raise ValueError(f"Invalid decimal: {value}") from exc


def parse_date_field(value: str | None, required: bool = False):
    parsed = parse_date(value)
    if parsed is None and required:
        raise ValueError("Missing or invalid required date")
    return parsed


def ensure_required_columns(
    headers: list[str],
    required_fields: list[str],
    candidate_map: dict[str, list[str]],
) -> None:
    missing: list[str] = []
    for field in required_fields:
        candidates = candidate_map.get(field, [field])
        if not any(name in headers for name in candidates):
            missing.append(field)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
