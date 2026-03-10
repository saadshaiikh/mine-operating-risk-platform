from __future__ import annotations

import argparse
import logging

from src.common.db import get_connection
from src.transforms.conformed.load_dim_mine import load_dim_mine
from src.transforms.conformed.load_fact_msha_assessed_violation import load_fact_msha_assessed_violation
from src.transforms.conformed.load_fact_msha_employment_production import load_fact_msha_employment_production
from src.transforms.conformed.load_fact_msha_incident import load_fact_msha_incident
from src.transforms.conformed.load_fact_msha_violation import load_fact_msha_violation

logger = logging.getLogger(__name__)


def _get_latest_run_id() -> str:
    sql = """
        SELECT run_id
        FROM stg_msha_mines
        ORDER BY ingested_at DESC
        LIMIT 1;
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
    if not row:
        raise RuntimeError("No staging runs found in stg_msha_mines")
    return row[0]


def run_warehouse_load(run_id: str | None = None) -> None:
    run_id = run_id or _get_latest_run_id()
    logger.info("Using run_id: %s", run_id)

    dim_count = load_dim_mine(run_id)
    logger.info("dim_mine upserted rows: %s", dim_count)

    ep_count = load_fact_msha_employment_production(run_id)
    logger.info("fact_msha_employment_production inserted rows: %s", ep_count)

    incident_count = load_fact_msha_incident(run_id)
    logger.info("fact_msha_incident inserted rows: %s", incident_count)

    violation_count = load_fact_msha_violation(run_id)
    logger.info("fact_msha_violation inserted rows: %s", violation_count)

    assessed_count = load_fact_msha_assessed_violation(run_id)
    logger.info("fact_msha_assessed_violation inserted rows: %s", assessed_count)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="Load warehouse facts from staging")
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args()

    run_warehouse_load(args.run_id)
