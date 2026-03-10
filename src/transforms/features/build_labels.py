from __future__ import annotations

import logging
import sys
from pathlib import Path

from src.common.db import get_connection
from src.common.io import repo_root


logger = logging.getLogger(__name__)

VIEW_SQL_PATH = repo_root() / "db" / "views" / "vw_mine_quarter_labels.sql"

VALIDATION_QUERIES = {
    "duplicate_grain": """
        SELECT COUNT(*) FROM (
            SELECT mine_key, period_key
            FROM vw_mine_quarter_labels
            GROUP BY mine_key, period_key
            HAVING COUNT(*) > 1
        ) t;
    """,
    "null_keys": """
        SELECT COUNT(*)
        FROM vw_mine_quarter_labels
        WHERE mine_key IS NULL
           OR period_key IS NULL;
    """,
    "invalid_label_values": """
        SELECT COUNT(*)
        FROM vw_mine_quarter_labels
        WHERE had_incident_next_qtr IS NOT NULL
          AND had_incident_next_qtr NOT IN (0,1);
    """,
    "invalid_exists_flag": """
        SELECT COUNT(*)
        FROM vw_mine_quarter_labels
        WHERE next_quarter_exists_flag NOT IN (0,1);
    """,
    "missing_next_but_nonnull_label": """
        SELECT COUNT(*)
        FROM vw_mine_quarter_labels
        WHERE next_quarter_exists_flag = 0
          AND had_incident_next_qtr IS NOT NULL;
    """,
    "positive_logic_mismatch": """
        SELECT COUNT(*)
        FROM vw_mine_quarter_labels
        WHERE next_quarter_exists_flag = 1
          AND COALESCE(next_quarter_incident_count, 0) >= 1
          AND had_incident_next_qtr <> 1;
    """,
    "negative_logic_mismatch": """
        SELECT COUNT(*)
        FROM vw_mine_quarter_labels
        WHERE next_quarter_exists_flag = 1
          AND COALESCE(next_quarter_incident_count, 0) = 0
          AND had_incident_next_qtr <> 0;
    """,
    "invalid_next_period_key": """
        SELECT COUNT(*)
        FROM vw_mine_quarter_labels
        WHERE next_period_key IS NULL
           OR next_period_key !~ '^[0-9]{4}Q[1-4]$';
    """,
    "row_count_delta_vs_base": """
        SELECT
            (SELECT COUNT(*) FROM vw_mine_quarter_base) -
            (SELECT COUNT(*) FROM vw_mine_quarter_labels);
    """,
    "label_null_count": """
        SELECT COUNT(*)
        FROM vw_mine_quarter_labels
        WHERE had_incident_next_qtr IS NULL;
    """,
    "label_zero_count": """
        SELECT COUNT(*)
        FROM vw_mine_quarter_labels
        WHERE had_incident_next_qtr = 0;
    """,
    "label_one_count": """
        SELECT COUNT(*)
        FROM vw_mine_quarter_labels
        WHERE had_incident_next_qtr = 1;
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


def build_labels() -> None:
    sql = read_sql(VIEW_SQL_PATH)

    with get_connection() as conn:
        with conn.cursor() as cur:
            logger.info("Creating or replacing vw_mine_quarter_labels")
            cur.execute(sql)
        conn.commit()

        failures: dict[str, int] = {}
        stats: dict[str, int] = {}

        for name, query in VALIDATION_QUERIES.items():
            value = run_scalar_query(conn, query)
            logger.info("Check %s = %s", name, value)

            if name in {"label_null_count", "label_zero_count", "label_one_count"}:
                stats[name] = value
                continue

            if value != 0:
                failures[name] = value

        logger.info("Label distribution stats: %s", stats)

        if failures:
            logger.error("vw_mine_quarter_labels validation failed: %s", failures)
            raise RuntimeError(f"Label validation failures: {failures}")

        logger.info("vw_mine_quarter_labels built and validated successfully.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        build_labels()
    except Exception as exc:
        logger.exception("Build failed: %s", exc)
        sys.exit(1)
