from __future__ import annotations

import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Tuple

import pandas as pd

from src.common.db import get_connection
from src.common.io import read_yaml, repo_root


CONFIG_PATH = repo_root() / "configs" / "modeling.yaml"


def load_modeling_config() -> dict[str, Any]:
    return read_yaml(CONFIG_PATH)


def get_git_commit() -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(repo_root()),
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip()
    except Exception:
        return None


def build_model_version(model_name: str, train_end_year: int, validation_year: int) -> str:
    return f"{model_name}_v1_train_to_{train_end_year}_validate_{validation_year}"


def load_training_window(validation_year: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    cfg = load_modeling_config()
    view = cfg.get("data", {}).get("training_view")
    if not view:
        raise RuntimeError("training_view is not configured")

    feature_cols = cfg.get("feature_columns", [])
    target_col = cfg.get("target_column", "had_incident_next_qtr")

    select_cols = [
        "mine_key",
        "period_key",
        "year",
        "quarter",
        target_col,
        *feature_cols,
    ]

    sql = f"SELECT {', '.join(select_cols)} FROM {view}"

    with get_connection() as conn:
        df = pd.read_sql(sql, conn)

    train_df = df[df["year"] < validation_year].copy()
    valid_df = df[df["year"] == validation_year].copy()

    return train_df, valid_df


def build_metadata_base(
    *,
    model_name: str,
    model_version: str,
    train_end_year: int,
    validation_year: int,
    target_column: str,
    feature_columns: list[str],
    row_count_train: int,
    row_count_validation: int,
    positive_rate_train: float,
    positive_rate_validation: float,
) -> dict[str, Any]:
    return {
        "model_version": model_version,
        "model_name": model_name,
        "train_end_year": train_end_year,
        "validation_year": validation_year,
        "target_column": target_column,
        "feature_columns": feature_columns,
        "row_count_train": row_count_train,
        "row_count_validation": row_count_validation,
        "positive_rate_train": positive_rate_train,
        "positive_rate_validation": positive_rate_validation,
        "built_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "git_commit": get_git_commit(),
        "config_version": 1,
    }
