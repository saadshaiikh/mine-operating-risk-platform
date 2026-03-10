from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import joblib

from src.common.io import ensure_dir, repo_root


MODELS_DIR = repo_root() / "artifacts" / "models"
PREDICTIONS_DIR = repo_root() / "artifacts" / "predictions"


def save_json(path: Path, payload: dict[str, Any]) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def save_joblib(path: Path, obj: Any) -> None:
    ensure_dir(path.parent)
    joblib.dump(obj, path)


def model_artifact_path(filename: str) -> Path:
    return MODELS_DIR / filename


def prediction_artifact_path(filename: str) -> Path:
    return PREDICTIONS_DIR / filename
