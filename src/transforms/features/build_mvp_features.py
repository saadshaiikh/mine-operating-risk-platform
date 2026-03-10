from __future__ import annotations

import logging
import sys
from pathlib import Path

from src.common.db import get_connection
from src.common.io import repo_root


logger = logging.getLogger(__name__)

FEATURE_VIEW_SQL_PATH = repo_root() / "db" / "views" / "vw_mine_quarter_mvp_features.sql"
TRAINING_VIEW_SQL_PATH = repo_root() / "db" / "views" / "vw_mine_quarter_mvp_training.sql"

VALIDATION_QUERIES = {
    "duplicate_grain": """
        SELECT COUNT(*) FROM (
            SELECT mine_key, period_key
            FROM vw_mine_quarter_mvp_features
            GROUP BY mine_key, period_key
            HAVING COUNT(*) > 1
        ) t;
    """,
    "null_keys": """
        SELECT COUNT(*)
        FROM vw_mine_quarter_mvp_features
        WHERE mine_key IS NULL
           OR period_key IS NULL;
    """,
    "row_count_delta_vs_labels": """
        SELECT
            (SELECT COUNT(*) FROM vw_mine_quarter_labels)
            - (SELECT COUNT(*) FROM vw_mine_quarter_mvp_features);
    """,
    "invalid_ss_share": """
        SELECT COUNT(*)
        FROM vw_mine_quarter_mvp_features
        WHERE feat_ss_share < 0
           OR feat_ss_share > 1;
    """,
    "negative_rolling_counts": """
        SELECT COUNT(*)
        FROM vw_mine_quarter_mvp_features
        WHERE feat_rolling_4q_incident_count < 0
           OR feat_rolling_4q_violation_count < 0;
    """,
    "negative_streak_count": """
        SELECT COUNT(*)
        FROM vw_mine_quarter_mvp_features
        WHERE feat_deterioration_streak_count < 0;
    """,
    "prior_incident_mismatch": """
        SELECT COUNT(*)
        FROM vw_mine_quarter_mvp_features
        WHERE feat_prior_incident_count IS DISTINCT FROM incident_count_prior_qtr;
    """,
    "prior_violation_mismatch": """
        SELECT COUNT(*)
        FROM vw_mine_quarter_mvp_features
        WHERE feat_prior_violation_count IS DISTINCT FROM violation_count_prior_qtr;
    """,
    "penalty_lag_mismatch": """
        SELECT COUNT(*)
        FROM vw_mine_quarter_mvp_features
        WHERE feat_assessed_penalty_amount_lag1 IS DISTINCT FROM assessed_penalty_amount_prior_qtr;
    """,
    "training_null_label_count": """
        SELECT COUNT(*)
        FROM vw_mine_quarter_mvp_training
        WHERE had_incident_next_qtr IS NULL;
    """,
    "feature_row_count": """
        SELECT COUNT(*) FROM vw_mine_quarter_mvp_features;
    """,
    "training_row_count": """
        SELECT COUNT(*) FROM vw_mine_quarter_mvp_training;
    """,
}


def read_sql(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"SQL file not found: {path}")
    return path.read_text(encoding="utf-8")


def run_scalar_query(conn, sql: str) -> int:
    with conn.cursor() as cur:
        cur.execute(sql)
        result = cur.fetchone()
        if result is None:
            raise RuntimeError("Expected scalar result, got none.")
        return int(result[0])


def build_mvp_features() -> None:
    feature_sql = read_sql(FEATURE_VIEW_SQL_PATH)
    training_sql = read_sql(TRAINING_VIEW_SQL_PATH)

    with get_connection() as conn:
        with conn.cursor() as cur:
            logger.info("Creating or replacing vw_mine_quarter_mvp_features")
            cur.execute(feature_sql)
            logger.info("Creating or replacing vw_mine_quarter_mvp_training")
            cur.execute(training_sql)
        conn.commit()

        failures: dict[str, int] = {}
        stats: dict[str, int] = {}

        for name, query in VALIDATION_QUERIES.items():
            value = run_scalar_query(conn, query)
            logger.info("Check %s = %s", name, value)

            if name in {"feature_row_count", "training_row_count"}:
                stats[name] = value
                continue

            if value != 0:
                failures[name] = value

        logger.info("Feature build stats: %s", stats)

        if failures:
            logger.error("MVP feature validation failed: %s", failures)
            raise RuntimeError(f"MVP feature validation failures: {failures}")

        logger.info("vw_mine_quarter_mvp_features built and validated successfully.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        build_mvp_features()
    except Exception as exc:
        logger.exception("Build failed: %s", exc)
        sys.exit(1)
