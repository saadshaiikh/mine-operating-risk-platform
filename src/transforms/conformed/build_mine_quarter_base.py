from __future__ import annotations

import logging
import sys
from pathlib import Path

from src.common.db import get_connection
from src.common.io import repo_root


logger = logging.getLogger(__name__)

VIEW_SQL_PATH = repo_root() / "db" / "views" / "vw_mine_quarter_base.sql"

VALIDATION_QUERIES = {
    "duplicate_grain": """
        SELECT COUNT(*) FROM (
            SELECT mine_key, period_key
            FROM vw_mine_quarter_base
            GROUP BY mine_key, period_key
            HAVING COUNT(*) > 1
        ) t;
    """,
    "null_grain": """
        SELECT COUNT(*)
        FROM vw_mine_quarter_base
        WHERE mine_key IS NULL
           OR period_key IS NULL;
    """,
    "invalid_quarter": """
        SELECT COUNT(*)
        FROM vw_mine_quarter_base
        WHERE quarter NOT IN (1,2,3,4);
    """,
    "period_mismatch": """
        SELECT COUNT(*)
        FROM vw_mine_quarter_base
        WHERE period_key <> CAST(year AS TEXT) || 'Q' || CAST(quarter AS TEXT);
    """,
    "negative_denominator": """
        SELECT COUNT(*)
        FROM vw_mine_quarter_base
        WHERE avg_employees < 0
           OR employee_hours < 0
           OR production_volume < 0;
    """,
    "negative_prior_values": """
        SELECT COUNT(*)
        FROM vw_mine_quarter_base
        WHERE incident_count_prior_qtr < 0
           OR violation_count_prior_qtr < 0
           OR assessed_penalty_amount_prior_qtr < 0;
    """,
    "row_count": """
        SELECT COUNT(*) FROM vw_mine_quarter_base;
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


def build_mine_quarter_base() -> None:
    sql = read_sql(VIEW_SQL_PATH)

    with get_connection() as conn:
        with conn.cursor() as cur:
            logger.info("Creating or replacing vw_mine_quarter_base")
            cur.execute(sql)
        conn.commit()

        failures: dict[str, int] = {}

        for check_name, query in VALIDATION_QUERIES.items():
            value = run_scalar_query(conn, query)
            logger.info("Validation check %s = %s", check_name, value)

            if check_name == "row_count":
                continue

            if value != 0:
                failures[check_name] = value

        if failures:
            logger.error("vw_mine_quarter_base validation failed: %s", failures)
            raise RuntimeError(f"Validation failures: {failures}")

        logger.info("vw_mine_quarter_base built and validated successfully.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        build_mine_quarter_base()
    except Exception as exc:
        logger.exception("Build failed: %s", exc)
        sys.exit(1)
