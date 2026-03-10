from __future__ import annotations

import argparse
import logging
from typing import Any

import pandas as pd

from src.backtesting.common import load_backtest_config, make_backtest_run_id, prepare_run_dirs
from src.backtesting.evaluate_metrics import (
    aggregate_model_summary,
    compute_split_metrics,
    generate_business_claims,
)
from src.backtesting.persistence import save_claims, save_csv, upsert_backtest_metric_row
from src.modeling.train_logistic_regression import train_logistic_regression
from src.modeling.train_rule_score import train_rule_score

logger = logging.getLogger(__name__)


MODEL_RUNNERS = {
    "rule_score": train_rule_score,
    "logreg": train_logistic_regression,
}


def _validate_scored_df(scored_df: pd.DataFrame, validation_year: int) -> None:
    if scored_df.empty:
        raise RuntimeError("Scored validation DataFrame is empty")
    if scored_df["year"].nunique() != 1 or int(scored_df["year"].iloc[0]) != validation_year:
        raise RuntimeError("Scored DataFrame contains unexpected validation years")
    if scored_df[["mine_key", "period_key", "model_version"]].isnull().any().any():
        raise RuntimeError("Scored DataFrame has null identifiers")
    if scored_df["risk_score"].isnull().any() or ((scored_df["risk_score"] < 0) | (scored_df["risk_score"] > 1)).any():
        raise RuntimeError("Scored DataFrame has invalid risk scores")
    if not scored_df["label_next_period"].isin([0, 1]).all():
        raise RuntimeError("Scored DataFrame has invalid labels")


def _check_minimums(scored_df: pd.DataFrame, n_train_rows: int, min_train: int, min_valid: int) -> bool:
    if n_train_rows < min_train:
        logger.warning("Skipping split: n_train_rows=%s below minimum %s", n_train_rows, min_train)
        return False
    if len(scored_df) < min_valid:
        logger.warning("Skipping split: n_validation_rows=%s below minimum %s", len(scored_df), min_valid)
        return False
    return True


def run_rolling_backtest(validation_years: list[int] | None = None) -> None:
    config = load_backtest_config()
    backtest_run_id = make_backtest_run_id()

    dirs = prepare_run_dirs(backtest_run_id)
    run_dir = dirs["run_dir"]
    predictions_dir = dirs["predictions_dir"]

    top_fraction = float(config["top_fraction"])
    models = list(config["models"])
    validation_years = validation_years or list(config["validation_years"])

    min_train = int(config["minimums"]["min_train_rows"])
    min_valid = int(config["minimums"]["min_validation_rows"])
    require_both_classes = bool(config["minimums"].get("require_train_both_classes", True))

    metric_rows: list[dict[str, Any]] = []

    for model_name in models:
        if model_name not in MODEL_RUNNERS:
            raise ValueError(f"Unsupported model_name: {model_name}")

        for validation_year in validation_years:
            logger.info("Running %s for validation year %s", model_name, validation_year)
            try:
                scored_df = MODEL_RUNNERS[model_name](
                    validation_year=validation_year,
                    write_to_db=True,
                    save_artifacts=True,
                )
            except RuntimeError as exc:
                if require_both_classes and "only one class" in str(exc).lower():
                    logger.warning(
                        "Skipping %s %s due to single-class training data.",
                        model_name,
                        validation_year,
                    )
                    continue
                raise

            n_train_rows = int(scored_df.attrs.get("n_train_rows", 0))
            if not _check_minimums(scored_df, n_train_rows, min_train, min_valid):
                continue

            _validate_scored_df(scored_df, validation_year)

            model_version = str(scored_df["model_version"].iloc[0])
            train_start_year = int(scored_df.attrs.get("train_start_year", validation_year - 1))
            train_end_year = int(scored_df.attrs.get("train_end_year", validation_year - 1))

            metric_row = compute_split_metrics(
                scored_df=scored_df,
                model_name=model_name,
                model_version=model_version,
                train_start_year=train_start_year,
                train_end_year=train_end_year,
                validation_year=validation_year,
                n_train_rows=n_train_rows,
                top_fraction=top_fraction,
            )
            metric_row["backtest_run_id"] = backtest_run_id

            upsert_backtest_metric_row(metric_row)
            metric_rows.append(metric_row)

            if config["artifacts"].get("save_predictions_csv", True):
                save_csv(scored_df, predictions_dir / f"{model_version}.csv")

    split_metrics_df = pd.DataFrame(metric_rows)

    if config["artifacts"].get("save_split_metrics_csv", True):
        save_csv(split_metrics_df, run_dir / "split_metrics.csv")

    summary_df = aggregate_model_summary(split_metrics_df)
    if config["artifacts"].get("save_model_summary_csv", True):
        save_csv(summary_df, run_dir / "model_summary.csv")

    claims = generate_business_claims(summary_df)
    if config["artifacts"].get("save_business_claims_md", True):
        save_claims(claims, run_dir / "business_claims.md")

    logger.info("Backtesting complete. Results saved to %s", run_dir)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run rolling backtests")
    parser.add_argument("--validation-years", nargs="*", type=int, default=None)
    args = parser.parse_args()

    run_rolling_backtest(args.validation_years)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
