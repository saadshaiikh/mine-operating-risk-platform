from __future__ import annotations

from typing import Iterable

import pandas as pd
from psycopg2.extras import execute_values

from src.common.db import get_connection
from src.modeling.common import load_modeling_config


def assign_risk_band(score: float, thresholds: dict[str, float]) -> str:
    if score < thresholds["low"]:
        return "Low"
    if score < thresholds["medium"]:
        return "Medium"
    if score < thresholds["high"]:
        return "High"
    return "Critical"


def prepare_fact_risk_score_rows(
    df: pd.DataFrame,
    model_version: str,
    label_col: str = "had_incident_next_qtr",
) -> pd.DataFrame:
    cfg = load_modeling_config()
    thresholds = cfg.get("risk_band_thresholds", {"low": 0.25, "medium": 0.5, "high": 0.75})

    out = df.copy()
    out["model_version"] = model_version
    out["risk_band"] = out["risk_score"].apply(lambda x: assign_risk_band(float(x), thresholds))
    out["label_next_period"] = out[label_col]

    return out[[
        "mine_key",
        "period_key",
        "model_version",
        "risk_score",
        "risk_band",
        "top_driver_1",
        "top_driver_2",
        "top_driver_3",
        "label_next_period",
    ]]


def upsert_fact_risk_score(rows: pd.DataFrame) -> None:
    if rows.empty:
        return

    sql = """
        INSERT INTO fact_risk_score (
            mine_key,
            period_key,
            model_version,
            risk_score,
            risk_band,
            top_driver_1,
            top_driver_2,
            top_driver_3,
            label_next_period
        )
        VALUES %s
        ON CONFLICT (mine_key, period_key, model_version)
        DO UPDATE SET
            risk_score = EXCLUDED.risk_score,
            risk_band = EXCLUDED.risk_band,
            top_driver_1 = EXCLUDED.top_driver_1,
            top_driver_2 = EXCLUDED.top_driver_2,
            top_driver_3 = EXCLUDED.top_driver_3,
            label_next_period = EXCLUDED.label_next_period,
            prediction_ts = NOW();
    """

    values = [
        (
            int(row.mine_key),
            row.period_key,
            row.model_version,
            float(row.risk_score),
            row.risk_band,
            row.top_driver_1,
            row.top_driver_2,
            row.top_driver_3,
            row.label_next_period,
        )
        for row in rows.itertuples(index=False)
    ]

    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, values, page_size=5000)
        conn.commit()
