from __future__ import annotations

from datetime import date, datetime
from typing import Iterable


_DATE_FORMATS: list[str] = [
    "%Y-%m-%d",
    "%m/%d/%Y",
    "%Y%m%d",
    "%d-%b-%Y",
    "%Y/%m/%d",
]


def parse_date(value: str | None, formats: Iterable[str] | None = None) -> date | None:
    if value is None:
        return None
    raw = value.strip()
    if not raw:
        return None
    for fmt in formats or _DATE_FORMATS:
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def derive_quarter(value: date) -> str:
    quarter = (value.month - 1) // 3 + 1
    return f"{value.year}Q{quarter}"


def parse_int(value: str | None) -> int | None:
    if value is None:
        return None
    raw = value.strip()
    if not raw:
        return None
    return int(raw)
