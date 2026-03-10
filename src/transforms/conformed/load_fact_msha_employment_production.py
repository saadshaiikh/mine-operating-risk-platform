from __future__ import annotations

from src.common.db import get_connection


def load_fact_msha_employment_production(run_id: str) -> int:
    sql = """
        INSERT INTO fact_msha_employment_production (
            mine_key,
            year,
            quarter,
            period_key,
            avg_employees,
            employee_hours,
            production_volume,
            production_unit,
            source_record_id
        )
        SELECT
            dm.mine_key,
            ep.year,
            ep.quarter,
            ep.period_key,
            ep.avg_employees,
            ep.employee_hours,
            ep.production_volume,
            ep.production_unit_raw,
            ep.source_record_id
        FROM stg_msha_employment_production ep
        JOIN dim_mine dm
          ON dm.mine_id = ep.mine_id
         AND dm.source_system = 'MSHA'
        WHERE ep.run_id = %s
          AND ep.mine_id IS NOT NULL
          AND ep.year IS NOT NULL
          AND ep.quarter IS NOT NULL
        ON CONFLICT (mine_key, year, quarter, source_record_id)
        DO NOTHING;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (run_id,))
            rowcount = cur.rowcount
        conn.commit()

    return rowcount
