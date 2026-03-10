from __future__ import annotations

from src.common.db import get_connection


def load_fact_msha_incident(run_id: str) -> int:
    sql = """
        INSERT INTO fact_msha_incident (
            mine_key,
            incident_date,
            incident_quarter,
            incident_type,
            severity_class,
            lost_days,
            days_restricted,
            hours_worked_basis,
            source_record_id
        )
        SELECT
            dm.mine_key,
            i.incident_date,
            i.incident_quarter,
            i.incident_type_raw,
            i.severity_class_raw,
            i.lost_days,
            i.days_restricted,
            i.hours_worked_basis,
            i.source_record_id
        FROM stg_msha_incidents i
        JOIN dim_mine dm
          ON dm.mine_id = i.mine_id
         AND dm.source_system = 'MSHA'
        WHERE i.run_id = %s
          AND i.mine_id IS NOT NULL
          AND i.incident_date IS NOT NULL
          AND i.incident_quarter IS NOT NULL
        ON CONFLICT (source_record_id)
        DO NOTHING;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (run_id,))
            rowcount = cur.rowcount
        conn.commit()

    return rowcount
