from __future__ import annotations

import argparse
import logging
import sys

import pandas as pd
from sklearn.linear_model import LogisticRegression

from src.common.db import get_connection
from src.modeling.artifacts import model_artifact_path, prediction_artifact_path, save_json, save_joblib
from src.modeling.common import build_metadata_base, build_model_version, load_modeling_config, load_training_window
from src.modeling.explainability import LOGREG_DRIVER_LABELS, build_driver_columns
from src.modeling.preprocessing import fit_scaler, impute_zero, transform_with_scaler
from src.modeling.score_mine_quarter import prepare_fact_risk_score_rows, upsert_fact_risk_score


logger = logging.getLogger(__name__)


def train_logistic_regression(
    validation_year: int,
    *,
    write_to_db: bool = True,
    save_artifacts: bool = True,
) -> pd.DataFrame:
    cfg = load_modeling_config()
    logreg_cfg = cfg["logistic_regression"]
    target_col = cfg["target_column"]
    feature_cols = cfg["feature_columns"]

    train_df, valid_df = load_training_window(validation_year)

    if train_df.empty or valid_df.empty:
        raise RuntimeError("Training or validation set is empty. Check validation_year and data availability.")

    if train_df[target_col].nunique() < 2:
        raise RuntimeError("Training data contains only one class; logistic regression requires both classes.")

    X_train = impute_zero(train_df, feature_cols)
    X_valid = impute_zero(valid_df, feature_cols)

    scaler = fit_scaler(X_train)
    X_train_scaled = transform_with_scaler(scaler, X_train)
    X_valid_scaled = transform_with_scaler(scaler, X_valid)

    model = LogisticRegression(
        penalty=logreg_cfg["penalty"],
        C=float(logreg_cfg["C"]),
        solver=logreg_cfg["solver"],
        max_iter=int(logreg_cfg["max_iter"]),
        class_weight=logreg_cfg["class_weight"],
        random_state=int(logreg_cfg["random_state"]),
    )
    model.fit(X_train_scaled, train_df[target_col].values)

    probs = model.predict_proba(X_valid_scaled)[:, 1]

    coef = model.coef_[0]
    contrib = pd.DataFrame(X_valid_scaled * coef, columns=feature_cols, index=valid_df.index)

    drivers = build_driver_columns(contrib, LOGREG_DRIVER_LABELS, top_n=3, prefer_positive=True)

    scored = pd.DataFrame(
        {
            "mine_key": valid_df["mine_key"].values,
            "period_key": valid_df["period_key"].values,
            "risk_score": probs,
            "top_driver_1": drivers["top_driver_1"].values,
            "top_driver_2": drivers["top_driver_2"].values,
            "top_driver_3": drivers["top_driver_3"].values,
            target_col: valid_df[target_col].values,
            "year": valid_df["year"].values,
        }
    )

    train_end_year = validation_year - 1
    model_version = build_model_version(
        logreg_cfg["model_name"],
        train_end_year,
        validation_year,
        version_tag=logreg_cfg.get("version_tag"),
    )

    output_rows = prepare_fact_risk_score_rows(scored, model_version, label_col=target_col)

    _validate_outputs(output_rows, scored)

    if write_to_db:
        upsert_fact_risk_score(output_rows)
        _validate_db_rows(model_version, validation_year, len(output_rows))

    metadata = build_metadata_base(
        model_name=logreg_cfg["model_name"],
        model_version=model_version,
        train_end_year=train_end_year,
        validation_year=validation_year,
        target_column=target_col,
        feature_columns=feature_cols,
        row_count_train=len(train_df),
        row_count_validation=len(valid_df),
        positive_rate_train=float(train_df[target_col].mean()),
        positive_rate_validation=float(valid_df[target_col].mean()),
    )
    metadata["risk_band_thresholds"] = cfg["risk_band_thresholds"]
    metadata["scaler_mean"] = scaler.mean_.tolist()
    metadata["scaler_scale"] = scaler.scale_.tolist()
    metadata["coefficients"] = coef.tolist()
    metadata["intercept"] = float(model.intercept_[0])

    if save_artifacts:
        save_joblib(model_artifact_path(f"{model_version}.joblib"), {"model": model, "scaler": scaler, "features": feature_cols})
        save_json(model_artifact_path(f"{model_version}_metadata.json"), metadata)
        scored.to_csv(prediction_artifact_path(f"{model_version}_validation.csv"), index=False)

    logger.info("Logistic regression training complete: %s", model_version)
    scored_backtest = output_rows.copy()
    scored_backtest["year"] = scored["year"].values
    scored_backtest["model_version"] = model_version
    scored_backtest["risk_score"] = output_rows["risk_score"].values
    scored_backtest["risk_band"] = output_rows["risk_band"].values
    scored_backtest["label_next_period"] = output_rows["label_next_period"].values
    scored_backtest["top_driver_1"] = output_rows["top_driver_1"].values
    scored_backtest["top_driver_2"] = output_rows["top_driver_2"].values
    scored_backtest["top_driver_3"] = output_rows["top_driver_3"].values

    scored_backtest.attrs["n_train_rows"] = len(train_df)
    scored_backtest.attrs["train_start_year"] = int(train_df["year"].min())
    scored_backtest.attrs["train_end_year"] = int(train_df["year"].max())
    scored_backtest.attrs["model_version"] = model_version

    return scored_backtest


def _validate_outputs(output_rows: pd.DataFrame, scored: pd.DataFrame) -> None:
    if output_rows["risk_score"].isnull().any():
        raise RuntimeError("Null risk_score detected")
    if ((output_rows["risk_score"] < 0) | (output_rows["risk_score"] > 1)).any():
        raise RuntimeError("Risk score out of [0,1] range")
    if not output_rows["risk_band"].isin({"Low", "Medium", "High", "Critical"}).all():
        raise RuntimeError("Invalid risk_band detected")
    if output_rows[["mine_key", "period_key", "model_version"]].isnull().any().any():
        raise RuntimeError("Null identifiers detected")
    if output_rows["top_driver_1"].isnull().any():
        raise RuntimeError("Missing top_driver_1")
    if len(output_rows) != len(scored):
        raise RuntimeError("Output row count does not match validation row count")


def _validate_db_rows(model_version: str, validation_year: int, expected_rows: int) -> None:
    sql = """
        SELECT COUNT(*)
        FROM fact_risk_score rs
        JOIN vw_mine_quarter_mvp_training t
          ON rs.mine_key = t.mine_key
         AND rs.period_key = t.period_key
        WHERE rs.model_version = %s
          AND t.year = %s;
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (model_version, validation_year))
            count = cur.fetchone()[0]
    if count != expected_rows:
        raise RuntimeError(f"DB row count mismatch: expected {expected_rows}, got {count}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="Train logistic regression risk model")
    parser.add_argument("--validation-year", type=int, required=True)
    args = parser.parse_args()

    try:
        train_logistic_regression(args.validation_year)
    except Exception as exc:
        logger.exception("Logistic regression training failed: %s", exc)
        sys.exit(1)
