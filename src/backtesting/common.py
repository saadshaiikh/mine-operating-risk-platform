from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.common.io import read_yaml, repo_root, ensure_dir


CONFIG_PATH = repo_root() / "configs" / "backtesting.yaml"
ARTIFACTS_BASE = repo_root() / "artifacts" / "backtests"


def load_backtest_config() -> dict[str, Any]:
    return read_yaml(CONFIG_PATH)


def make_backtest_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")


def prepare_run_dirs(run_id: str) -> dict[str, Path]:
    run_dir = ARTIFACTS_BASE / run_id
    predictions_dir = run_dir / "predictions"
    ensure_dir(run_dir)
    ensure_dir(predictions_dir)
    return {"run_dir": run_dir, "predictions_dir": predictions_dir}
