from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, roc_auc_score


REQUIRED_COLUMNS = {
    "mine_key",
    "period_key",
    "year",
    "risk_score",
    "label_next_period",
    "model_version",
}


def validate_scored_df(scored_df: pd.DataFrame) -> None:
    missing = REQUIRED_COLUMNS - set(scored_df.columns)
    if missing:
        raise ValueError(f"Missing required scored_df columns: {sorted(missing)}")

    if scored_df["label_next_period"].isna().any():
        raise ValueError("Backtesting scored_df contains null labels.")

    bad_labels = ~scored_df["label_next_period"].isin([0, 1])
    if bad_labels.any():
        raise ValueError("Backtesting scored_df contains non-binary labels.")

    bad_scores = (
        scored_df["risk_score"].isna()
        | (scored_df["risk_score"] < 0)
        | (scored_df["risk_score"] > 1)
    )
    if bad_scores.any():
        raise ValueError("Backtesting scored_df contains invalid risk scores.")


def sort_validation_rows(scored_df: pd.DataFrame) -> pd.DataFrame:
    return scored_df.sort_values(
        by=["risk_score", "mine_key", "period_key"],
        ascending=[False, True, True],
        kind="mergesort",
    ).reset_index(drop=True)


def safe_roc_auc(y_true: np.ndarray, y_score: np.ndarray) -> float | None:
    if len(np.unique(y_true)) < 2:
        return None
    return float(roc_auc_score(y_true, y_score))


def safe_pr_auc(y_true: np.ndarray, y_score: np.ndarray) -> float | None:
    if len(np.unique(y_true)) < 2:
        return None
    return float(average_precision_score(y_true, y_score))


def compute_split_metrics(
    scored_df: pd.DataFrame,
    model_name: str,
    model_version: str,
    train_start_year: int,
    train_end_year: int,
    validation_year: int,
    n_train_rows: int,
    top_fraction: float = 0.10,
) -> dict[str, Any]:
    validate_scored_df(scored_df)

    if scored_df.empty:
        raise ValueError("Validation scored_df is empty.")

    ranked = sort_validation_rows(scored_df)

    y_true = ranked["label_next_period"].to_numpy(dtype=int)
    y_score = ranked["risk_score"].to_numpy(dtype=float)

    n_validation_rows = int(len(ranked))
    n_validation_positives = int(y_true.sum())
    base_rate = float(n_validation_positives / n_validation_rows) if n_validation_rows > 0 else None

    top_n = max(1, int(math.ceil(top_fraction * n_validation_rows)))
    top_df = ranked.head(top_n)
    top_positive_count = int(top_df["label_next_period"].sum())

    precision_at_top_decile = float(top_positive_count / top_n)
    recall_at_top_decile = (
        float(top_positive_count / n_validation_positives)
        if n_validation_positives > 0
        else None
    )
    lift_vs_base_rate = (
        float(precision_at_top_decile / base_rate)
        if base_rate is not None and base_rate > 0
        else None
    )

    metrics = {
        "model_name": model_name,
        "model_version": model_version,
        "train_start_year": int(train_start_year),
        "train_end_year": int(train_end_year),
        "validation_year": int(validation_year),
        "n_train_rows": int(n_train_rows),
        "n_validation_rows": n_validation_rows,
        "n_validation_positives": n_validation_positives,
        "base_rate": base_rate,
        "top_decile_size": int(top_n),
        "top_decile_positive_count": int(top_positive_count),
        "roc_auc": safe_roc_auc(y_true, y_score),
        "pr_auc": safe_pr_auc(y_true, y_score),
        "precision_at_top_decile": precision_at_top_decile,
        "recall_at_top_decile": recall_at_top_decile,
        "lift_vs_base_rate": lift_vs_base_rate,
    }
    return metrics


def aggregate_model_summary(split_metrics_df: pd.DataFrame) -> pd.DataFrame:
    if split_metrics_df.empty:
        return pd.DataFrame()

    rows = []
    for model_name, grp in split_metrics_df.groupby("model_name", sort=True):
        pooled_validation_rows = int(grp["n_validation_rows"].sum())
        pooled_validation_positives = int(grp["n_validation_positives"].sum())
        pooled_top_decile_size = int(grp["top_decile_size"].sum())
        pooled_top_decile_positive_count = int(grp["top_decile_positive_count"].sum())

        pooled_base_rate = (
            pooled_validation_positives / pooled_validation_rows
            if pooled_validation_rows > 0
            else None
        )
        pooled_precision_at_top_decile = (
            pooled_top_decile_positive_count / pooled_top_decile_size
            if pooled_top_decile_size > 0
            else None
        )
        pooled_recall_at_top_decile = (
            pooled_top_decile_positive_count / pooled_validation_positives
            if pooled_validation_positives > 0
            else None
        )
        pooled_lift_vs_base_rate = (
            pooled_precision_at_top_decile / pooled_base_rate
            if pooled_base_rate is not None and pooled_base_rate > 0
            else None
        )

        rows.append({
            "model_name": model_name,
            "n_splits": int(len(grp)),
            "macro_roc_auc": grp["roc_auc"].dropna().mean() if grp["roc_auc"].notna().any() else None,
            "macro_pr_auc": grp["pr_auc"].dropna().mean() if grp["pr_auc"].notna().any() else None,
            "macro_precision_at_top_decile": grp["precision_at_top_decile"].dropna().mean()
            if grp["precision_at_top_decile"].notna().any() else None,
            "macro_recall_at_top_decile": grp["recall_at_top_decile"].dropna().mean()
            if grp["recall_at_top_decile"].notna().any() else None,
            "macro_lift_vs_base_rate": grp["lift_vs_base_rate"].dropna().mean()
            if grp["lift_vs_base_rate"].notna().any() else None,
            "pooled_validation_rows": pooled_validation_rows,
            "pooled_validation_positives": pooled_validation_positives,
            "pooled_top_decile_size": pooled_top_decile_size,
            "pooled_top_decile_positive_count": pooled_top_decile_positive_count,
            "pooled_base_rate": pooled_base_rate,
            "pooled_precision_at_top_decile": pooled_precision_at_top_decile,
            "pooled_recall_at_top_decile": pooled_recall_at_top_decile,
            "pooled_lift_vs_base_rate": pooled_lift_vs_base_rate,
        })

    return pd.DataFrame(rows)


def generate_business_claims(summary_df: pd.DataFrame) -> list[str]:
    claims: list[str] = []
    if summary_df.empty:
        return claims

    for _, row in summary_df.iterrows():
        model_name = row["model_name"]
        capture = row["pooled_recall_at_top_decile"]
        lift = row["pooled_lift_vs_base_rate"]

        if capture is None or pd.isna(capture):
            claims.append(f"{model_name}: capture rate at top decile could not be computed.")
            continue

        if lift is None or pd.isna(lift):
            claims.append(
                f"{model_name}: top 10% highest-risk mines captured {capture:.1%} of next-quarter incidents."
            )
        else:
            claims.append(
                f"{model_name}: top 10% highest-risk mines captured {capture:.1%} of next-quarter incidents at {lift:.2f}x the base rate."
            )

    return claims
