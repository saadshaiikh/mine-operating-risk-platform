from __future__ import annotations

from typing import Any

from src.common.io import read_yaml, repo_root


_SOURCES_PATH = repo_root() / "configs" / "sources.yaml"


def _load_sources() -> dict[str, dict[str, Any]]:
    payload = read_yaml(_SOURCES_PATH)
    msha = payload.get("msha", {})
    datasets = msha.get("datasets", {})
    return datasets


MSHA_SOURCES: dict[str, dict[str, Any]] = _load_sources()


def get_source(name: str) -> dict[str, Any]:
    if name not in MSHA_SOURCES:
        raise KeyError(f"Unknown MSHA source: {name}")
    return MSHA_SOURCES[name]
