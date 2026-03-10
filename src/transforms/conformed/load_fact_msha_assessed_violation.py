from __future__ import annotations

from src.common.db import get_connection


def load_fact_msha_assessed_violation(run_id: str) -> int:
    sql = """
        INSERT INTO fact_msha_assessed_violation (
            mine_key,
            assessed_date,
            assessed_quarter,
            penalty_amount,
            violation_reference_id,
            source_record_id
        )
        SELECT
            dm.mine_key,
            av.assessed_date,
            av.assessed_quarter,
            COALESCE(av.assessment_amount, av.proposed_penalty_amount) AS penalty_amount,
            av.violation_reference_raw,
            av.source_record_id
        FROM stg_msha_assessed_violations av
        JOIN dim_mine dm
          ON dm.mine_id = av.mine_id
         AND dm.source_system = 'MSHA'
        WHERE av.run_id = %s
          AND av.mine_id IS NOT NULL
        ON CONFLICT (source_record_id)
        DO NOTHING;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (run_id,))
            rowcount = cur.rowcount
        conn.commit()

    return rowcount
