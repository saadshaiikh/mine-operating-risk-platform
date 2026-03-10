from __future__ import annotations

import hashlib
import json
from typing import Any


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def row_hash(row: dict[str, Any]) -> str:
    canonical = json.dumps(row, sort_keys=True, ensure_ascii=True, default=str)
    return sha256_text(canonical)
