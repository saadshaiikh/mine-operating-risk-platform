from __future__ import annotations

from typing import Any

from src.common.db import get_connection


def load_dim_mine(run_id: str) -> int:
    sql = """
        INSERT INTO dim_mine (
            mine_id,
            source_system,
            mine_name,
            country_code,
            province_state,
            commodity_group,
            mine_type,
            latitude,
            longitude,
            active_flag
        )
        SELECT
            sm.mine_id,
            'MSHA' AS source_system,
            sm.mine_name,
            'US' AS country_code,
            sm.province_state,
            sm.commodity_group,
            sm.mine_type,
            sm.latitude,
            sm.longitude,
            TRUE AS active_flag
        FROM stg_msha_mines sm
        WHERE sm.run_id = %s
          AND sm.mine_id IS NOT NULL
        ON CONFLICT (mine_id, source_system)
        DO UPDATE SET
            mine_name = EXCLUDED.mine_name,
            country_code = EXCLUDED.country_code,
            province_state = EXCLUDED.province_state,
            commodity_group = EXCLUDED.commodity_group,
            mine_type = EXCLUDED.mine_type,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            active_flag = EXCLUDED.active_flag,
            updated_at = NOW();
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (run_id,))
            rowcount = cur.rowcount
        conn.commit()

    return rowcount
