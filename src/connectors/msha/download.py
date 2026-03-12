from __future__ import annotations

import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import zipfile

import requests

from src.common.exceptions import SourceDownloadError
from src.common.io import ensure_dir, ensure_parent, repo_root
from src.common.logging import get_logger, log_event
from src.connectors.msha.catalog import MSHA_SOURCES
from src.connectors.msha.manifest import write_manifest


RAW_BASE_DIR = repo_root() / "data" / "raw" / "msha"


def create_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")


def get_run_dir(base_dir: Path, run_id: str) -> Path:
    return base_dir / "runs" / run_id


def compute_sha256(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _download_to_path(url: str, output_path: Path) -> None:
    ensure_parent(output_path)
    with requests.get(url, stream=True, timeout=120) as resp:
        resp.raise_for_status()
        with output_path.open("wb") as handle:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handle.write(chunk)


def unzip_source(zip_path: Path, output_dir: Path, target_name: str) -> Path:
    if not zip_path.exists():
        raise SourceDownloadError(f"Zip not found: {zip_path}")
    ensure_dir(output_dir)
    with zipfile.ZipFile(zip_path, "r") as archive:
        txt_members = [m for m in archive.infolist() if m.filename.lower().endswith(".txt")]
        if not txt_members:
            raise SourceDownloadError(f"No .txt file found in {zip_path}")
        txt_members.sort(key=lambda m: m.file_size, reverse=True)
        member = txt_members[0]
        archive.extract(member, output_dir)

    extracted = output_dir / member.filename
    target_path = output_dir / f"{target_name}.txt"
    if extracted.name != target_path.name:
        if extracted.name.lower() == target_path.name.lower():
            temp_path = output_dir / f".{target_name}.tmp"
            if temp_path.exists():
                temp_path.unlink()
            shutil.move(str(extracted), str(temp_path))
            shutil.move(str(temp_path), str(target_path))
        else:
            if target_path.exists():
                target_path.unlink()
            shutil.move(str(extracted), str(target_path))
    return target_path


def build_manifest(run_id: str, entries: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "source_system": "MSHA",
        "datasets": entries,
    }


def download_source(source_name: str, run_dir: Path) -> dict[str, Any]:
    if source_name not in MSHA_SOURCES:
        raise SourceDownloadError(f"Unknown dataset: {source_name}")
    cfg = MSHA_SOURCES[source_name]
    url = cfg["source_url"]
    raw_filename = cfg["raw_filename"]

    zip_path = run_dir / f"{raw_filename}.zip"
    _download_to_path(url, zip_path)

    txt_path = unzip_source(zip_path, run_dir, raw_filename)

    sha256 = compute_sha256(zip_path)
    size_bytes = zip_path.stat().st_size

    return {
        "dataset_name": source_name,
        "label": cfg.get("label"),
        "source_url": url,
        "zip_filename": zip_path.name,
        "txt_filename": txt_path.name,
        "sha256": sha256,
        "size_bytes": size_bytes,
        "downloaded_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "parser_status": "PENDING",
    }


def _write_latest_pointer(run_dir: Path, run_id: str) -> None:
    latest = RAW_BASE_DIR / "latest"
    try:
        if latest.exists() or latest.is_symlink():
            latest.unlink()
        latest.symlink_to(run_dir, target_is_directory=True)
    except OSError:
        pointer = RAW_BASE_DIR / "latest.txt"
        pointer.write_text(run_id + "\n", encoding="utf-8")


def download_all(sources: list[str] | None = None, run_id: str | None = None) -> dict[str, Any]:
    logger = get_logger("msha_download")
    run_id = run_id or create_run_id()
    run_dir = get_run_dir(RAW_BASE_DIR, run_id)
    ensure_dir(run_dir)

    datasets = sources or list(MSHA_SOURCES.keys())
    entries: list[dict[str, Any]] = []

    for name in datasets:
        log_event(
            logger,
            {"event": "download_start", "run_id": run_id, "dataset_name": name},
        )
        entry = download_source(name, run_dir)
        entries.append(entry)
        log_event(
            logger,
            {
                "event": "download_complete",
                "run_id": run_id,
                "dataset_name": name,
                "zip_filename": entry["zip_filename"],
                "txt_filename": entry["txt_filename"],
                "size_bytes": entry["size_bytes"],
                "sha256": entry["sha256"],
            },
        )

    manifest = build_manifest(run_id, entries)
    write_manifest(run_dir, manifest)
    _write_latest_pointer(run_dir, run_id)
    log_event(logger, {"event": "manifest_written", "run_id": run_id, "count": len(entries)})
    return manifest
