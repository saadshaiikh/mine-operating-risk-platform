from __future__ import annotations

import logging
import sys
from pathlib import Path

from src.common.db import get_connection
from src.common.io import repo_root


logger = logging.getLogger(__name__)

VIEW_FILES = [
    repo_root() / "db" / "views" / "vw_kpi_summary.sql",
    repo_root() / "db" / "views" / "vw_top_risk_mines.sql",
    repo_root() / "db" / "views" / "vw_mine_detail.sql",
    repo_root() / "db" / "views" / "vw_backtest_summary.sql",
    repo_root() / "db" / "views" / "vw_governance_source_freshness.sql",
]


def build_bi_views() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            for path in VIEW_FILES:
                logger.info("Building BI view from %s", path)
                sql = path.read_text(encoding="utf-8")
                cur.execute(sql)
        conn.commit()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        build_bi_views()
    except Exception:
        logger.exception("Failed to build BI views")
        sys.exit(1)
