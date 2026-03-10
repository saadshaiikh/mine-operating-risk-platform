from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from src.common.db import get_connection
from src.common.io import ensure_dir


def save_csv(df: pd.DataFrame, path: Path) -> None:
    ensure_dir(path.parent)
    df.to_csv(path, index=False)


def save_claims(claims: list[str], path: Path) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as handle:
        for claim in claims:
            handle.write(f"- {claim}\n")


def upsert_backtest_metric_row(metric_row: dict[str, Any]) -> None:
    sql = """
    INSERT INTO fact_backtest_result (
        backtest_run_id,
        model_name,
        model_version,
        train_start_year,
        train_end_year,
        validation_year,
        n_train_rows,
        n_validation_rows,
        n_validation_positives,
        base_rate,
        top_decile_size,
        top_decile_positive_count,
        roc_auc,
        pr_auc,
        precision_at_top_decile,
        recall_at_top_decile,
        lift_vs_base_rate
    )
    VALUES (
        %(backtest_run_id)s,
        %(model_name)s,
        %(model_version)s,
        %(train_start_year)s,
        %(train_end_year)s,
        %(validation_year)s,
        %(n_train_rows)s,
        %(n_validation_rows)s,
        %(n_validation_positives)s,
        %(base_rate)s,
        %(top_decile_size)s,
        %(top_decile_positive_count)s,
        %(roc_auc)s,
        %(pr_auc)s,
        %(precision_at_top_decile)s,
        %(recall_at_top_decile)s,
        %(lift_vs_base_rate)s
    )
    ON CONFLICT (model_version, validation_year)
    DO UPDATE SET
        backtest_run_id = EXCLUDED.backtest_run_id,
        model_name = EXCLUDED.model_name,
        train_start_year = EXCLUDED.train_start_year,
        train_end_year = EXCLUDED.train_end_year,
        n_train_rows = EXCLUDED.n_train_rows,
        n_validation_rows = EXCLUDED.n_validation_rows,
        n_validation_positives = EXCLUDED.n_validation_positives,
        base_rate = EXCLUDED.base_rate,
        top_decile_size = EXCLUDED.top_decile_size,
        top_decile_positive_count = EXCLUDED.top_decile_positive_count,
        roc_auc = EXCLUDED.roc_auc,
        pr_auc = EXCLUDED.pr_auc,
        precision_at_top_decile = EXCLUDED.precision_at_top_decile,
        recall_at_top_decile = EXCLUDED.recall_at_top_decile,
        lift_vs_base_rate = EXCLUDED.lift_vs_base_rate,
        created_at = NOW();
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, metric_row)
        conn.commit()
