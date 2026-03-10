from __future__ import annotations

from src.common.db import get_connection


def load_fact_msha_violation(run_id: str) -> int:
    sql = """
        INSERT INTO fact_msha_violation (
            mine_key,
            inspection_event_no,
            violation_date,
            violation_quarter,
            section_code,
            likelihood_code,
            negligence_code,
            significant_substantial,
            citation_order_flag,
            source_record_id
        )
        SELECT
            dm.mine_key,
            v.event_number,
            v.violation_date,
            v.violation_quarter,
            v.section_code,
            v.likelihood_code,
            v.negligence_code,
            v.significant_substantial,
            v.citation_order_flag,
            v.source_record_id
        FROM stg_msha_violations v
        JOIN dim_mine dm
          ON dm.mine_id = v.mine_id
         AND dm.source_system = 'MSHA'
        WHERE v.run_id = %s
          AND v.mine_id IS NOT NULL
          AND v.violation_date IS NOT NULL
          AND v.violation_quarter IS NOT NULL
        ON CONFLICT (source_record_id)
        DO NOTHING;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (run_id,))
            rowcount = cur.rowcount
        conn.commit()

    return rowcount
