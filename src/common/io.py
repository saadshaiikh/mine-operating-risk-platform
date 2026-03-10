from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import yaml


_HEADER_CLEAN_RE = re.compile(r"[^a-z0-9_]+")
_MULTISCORE_RE = re.compile(r"_+")


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def read_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def normalize_header(header: str) -> str:
    value = header.strip().lower().replace(" ", "_")
    value = _HEADER_CLEAN_RE.sub("_", value)
    value = _MULTISCORE_RE.sub("_", value).strip("_")
    return value


def normalize_headers(headers: list[str]) -> list[str]:
    return [normalize_header(h) for h in headers]


def normalize_row_keys(row: dict[str, Any]) -> dict[str, Any]:
    return {normalize_header(k): v for k, v in row.items()}


def normalize_str(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        value = str(value)
    cleaned = value.strip()
    return cleaned if cleaned else None


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
