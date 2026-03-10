from __future__ import annotations

from pathlib import Path
from typing import Any

from src.common.db import get_connection
from src.common.exceptions import DataValidationError
from src.common.io import read_yaml, repo_root
from src.common.logging import get_logger, log_event
from src.connectors.msha.catalog import MSHA_SOURCES
from src.connectors.msha.manifest import read_manifest
from src.transforms.staging import (
    stg_msha_assessed_violations,
    stg_msha_employment_production,
    stg_msha_incidents,
    stg_msha_mines,
    stg_msha_violations,
)


RAW_BASE_DIR = repo_root() / "data" / "raw" / "msha"
CONFIG_PATH = repo_root() / "configs" / "ingestion.yaml"


STAGE_DISPATCH = {
    "msha_mines": stg_msha_mines.stage_dataset,
    "msha_employment_production_quarterly": stg_msha_employment_production.stage_dataset,
    "msha_incidents": stg_msha_incidents.stage_dataset,
    "msha_violations": stg_msha_violations.stage_dataset,
    "msha_assessed_violations": stg_msha_assessed_violations.stage_dataset,
}


def _load_config() -> dict[str, Any]:
    return read_yaml(CONFIG_PATH)


def get_load_order() -> list[str]:
    cfg = _load_config()
    return cfg.get("msha", {}).get("load_order", [])


def assert_required_files(manifest: dict[str, Any]) -> None:
    datasets = {entry["dataset_name"]: entry for entry in manifest.get("datasets", [])}
    missing = []
    for name, cfg in MSHA_SOURCES.items():
        if cfg.get("required") and name not in datasets:
            missing.append(name)
    if missing:
        raise DataValidationError(f"Missing required datasets in manifest: {missing}")


def _get_dataset_entry(manifest: dict[str, Any], dataset_name: str) -> dict[str, Any] | None:
    for entry in manifest.get("datasets", []):
        if entry.get("dataset_name") == dataset_name:
            return entry
    return None


def load_dataset(
    dataset_name: str,
    txt_path: Path,
    run_id: str,
    settings: dict[str, Any],
    conn,
) -> dict[str, Any]:
    if dataset_name not in STAGE_DISPATCH:
        raise DataValidationError(f"No staging module for {dataset_name}")
    stage_fn = STAGE_DISPATCH[dataset_name]
    return stage_fn(
        run_id=run_id,
        txt_path=txt_path,
        source_file_name=txt_path.name,
        delimiter=settings["delimiter"],
        encoding=settings["encoding"],
        batch_size=settings["batch_size"],
        required_columns=settings.get("required_columns", {}).get(dataset_name, []),
        conn=conn,
    )


def load_run(run_id: str) -> None:
    logger = get_logger("msha_stage")
    cfg = _load_config().get("msha", {})
    settings = {
        "delimiter": cfg.get("delimiter", "|"),
        "encoding": cfg.get("encoding", "utf-8"),
        "batch_size": int(cfg.get("batch_size", 5000)),
        "required_columns": cfg.get("required_columns", {}),
    }

    run_dir = RAW_BASE_DIR / "runs" / run_id
    manifest = read_manifest(run_dir)
    assert_required_files(manifest)

    load_order = get_load_order()
    datasets = {entry["dataset_name"]: entry for entry in manifest.get("datasets", [])}

    with get_connection() as conn:
        for dataset_name in load_order:
            entry = datasets.get(dataset_name)
            if not entry:
                if MSHA_SOURCES.get(dataset_name, {}).get("required"):
                    raise DataValidationError(f"Required dataset missing from manifest: {dataset_name}")
                log_event(
                    logger,
                    {
                        "event": "dataset_skipped",
                        "run_id": run_id,
                        "dataset_name": dataset_name,
                        "reason": "missing_manifest_entry",
                    },
                )
                continue

            txt_path = run_dir / entry["txt_filename"]
            if not txt_path.exists():
                raise DataValidationError(f"Missing text file for {dataset_name}: {txt_path}")

            log_event(
                logger,
                {
                    "event": "stage_dataset_start",
                    "run_id": run_id,
                    "dataset_name": dataset_name,
                    "source_file": str(txt_path),
                },
            )
            try:
                result = load_dataset(dataset_name, txt_path, run_id, settings, conn)
                conn.commit()
                log_event(logger, {"event": "stage_dataset_complete", **result})
            except Exception as exc:
                conn.rollback()
                log_event(
                    logger,
                    {
                        "event": "stage_dataset_failed",
                        "run_id": run_id,
                        "dataset_name": dataset_name,
                        "error": str(exc),
                    },
                )
                if MSHA_SOURCES.get(dataset_name, {}).get("required"):
                    raise
