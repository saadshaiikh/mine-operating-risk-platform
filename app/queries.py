import os
from typing import Dict, Optional

import pandas as pd
from sqlalchemy import create_engine, text


def _get_streamlit_secret(key: str) -> Optional[str]:
    try:
        import streamlit as st

        if key in st.secrets:
            return st.secrets[key]
    except Exception:
        return None
    return None


def get_db_url() -> str:
    secret_url = _get_streamlit_secret("DATABASE_URL")
    if secret_url:
        return secret_url
    env_url = os.getenv("DATABASE_URL")
    if env_url:
        return env_url
    raise RuntimeError("DATABASE_URL is not set in st.secrets or environment")


def get_engine():
    return create_engine(get_db_url())


def fetch_df(sql: str, params: Optional[Dict] = None) -> pd.DataFrame:
    engine = get_engine()
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


def get_kpi_summary() -> pd.DataFrame:
    return fetch_df(
        """
        SELECT
            model_name,
            model_version,
            period_key,
            year,
            quarter,
            is_latest_period_for_model_flag,
            total_mines_scored,
            avg_risk_score,
            median_risk_score,
            max_risk_score,
            high_risk_mine_count,
            critical_risk_mine_count,
            high_or_critical_risk_mine_count,
            high_or_critical_risk_share,
            actual_positive_count,
            flagged_positive_count,
            flagged_positive_precision
        FROM vw_kpi_summary
        ORDER BY model_version, year, quarter
        """
    )


def get_top_risk_filters() -> Dict[str, pd.DataFrame]:
    return {
        "model_versions": fetch_df("SELECT DISTINCT model_version FROM vw_top_risk_mines ORDER BY model_version"),
        "periods": fetch_df("SELECT DISTINCT period_key FROM vw_top_risk_mines ORDER BY period_key"),
        "states": fetch_df("SELECT DISTINCT province_state FROM vw_top_risk_mines WHERE province_state IS NOT NULL ORDER BY province_state"),
        "commodities": fetch_df("SELECT DISTINCT commodity_group FROM vw_top_risk_mines WHERE commodity_group IS NOT NULL ORDER BY commodity_group"),
        "risk_bands": fetch_df("SELECT DISTINCT risk_band FROM vw_top_risk_mines WHERE risk_band IS NOT NULL ORDER BY risk_band"),
    }


def get_top_risk_mines(filters: Dict, limit: int = 500) -> pd.DataFrame:
    clauses = []
    params = {}
    if filters.get("model_version"):
        clauses.append("model_version = :model_version")
        params["model_version"] = filters["model_version"]
    if filters.get("period_key"):
        clauses.append("period_key = :period_key")
        params["period_key"] = filters["period_key"]
    if filters.get("province_state"):
        clauses.append("province_state = :province_state")
        params["province_state"] = filters["province_state"]
    if filters.get("commodity_group"):
        clauses.append("commodity_group = :commodity_group")
        params["commodity_group"] = filters["commodity_group"]
    if filters.get("risk_band"):
        clauses.append("risk_band = :risk_band")
        params["risk_band"] = filters["risk_band"]

    where_sql = "WHERE " + " AND ".join(clauses) if clauses else ""
    sql = f"""
        SELECT
            model_version,
            period_key,
            mine_key,
            mine_id,
            mine_name,
            province_state,
            commodity_group,
            mine_type,
            risk_rank,
            risk_score,
            risk_band,
            top_driver_1,
            top_driver_2,
            top_driver_3,
            employee_hours,
            production_volume,
            incident_count_current_qtr,
            violation_count_current_qtr,
            had_incident_next_qtr
        FROM vw_top_risk_mines
        {where_sql}
        ORDER BY risk_score DESC, mine_key ASC
        LIMIT {limit}
    """
    return fetch_df(sql, params)


def get_mine_detail_filters() -> Dict[str, pd.DataFrame]:
    return {
        "model_versions": fetch_df("SELECT DISTINCT model_version FROM vw_mine_detail ORDER BY model_version"),
        "periods": fetch_df("SELECT DISTINCT period_key FROM vw_mine_detail ORDER BY period_key"),
        "mines": fetch_df("SELECT DISTINCT mine_id, mine_name FROM vw_mine_detail WHERE mine_id IS NOT NULL ORDER BY mine_id"),
    }


def get_mine_detail_row(model_version: str, period_key: str, mine_id: str) -> pd.DataFrame:
    return fetch_df(
        """
        SELECT *
        FROM vw_mine_detail
        WHERE model_version = :model_version
          AND period_key = :period_key
          AND mine_id = :mine_id
        LIMIT 1
        """,
        {
            "model_version": model_version,
            "period_key": period_key,
            "mine_id": mine_id,
        },
    )


def get_mine_history(model_version: str, mine_id: str) -> pd.DataFrame:
    return fetch_df(
        """
        SELECT
            period_key,
            year,
            quarter,
            incident_count_current_qtr,
            violation_count_current_qtr,
            production_per_employee_hour_current_qtr
        FROM vw_mine_detail
        WHERE model_version = :model_version
          AND mine_id = :mine_id
        ORDER BY year, quarter
        """,
        {
            "model_version": model_version,
            "mine_id": mine_id,
        },
    )


def get_backtest_summary() -> pd.DataFrame:
    return fetch_df(
        """
        SELECT
            model_name,
            model_version,
            validation_year,
            train_start_year,
            train_end_year,
            n_train_rows,
            n_validation_rows,
            n_validation_positives,
            base_rate,
            top_decile_size,
            top_decile_positive_count,
            roc_auc,
            pr_auc,
            precision_at_top_decile,
            recall_at_top_decile,
            lift_vs_base_rate,
            n_splits_for_model,
            macro_roc_auc,
            macro_pr_auc,
            macro_precision_at_top_decile,
            macro_recall_at_top_decile,
            macro_lift_vs_base_rate,
            pooled_validation_rows,
            pooled_validation_positives,
            pooled_top_decile_size,
            pooled_top_decile_positive_count,
            pooled_base_rate,
            pooled_precision_at_top_decile,
            pooled_recall_at_top_decile,
            pooled_lift_vs_base_rate,
            business_claim_text
        FROM vw_backtest_summary
        ORDER BY model_version, validation_year
        """
    )


def get_governance() -> pd.DataFrame:
    return fetch_df(
        """
        SELECT
            source_system,
            entity_name,
            row_count,
            latest_data_ts,
            latest_quality_check_ts,
            latest_quality_status,
            latest_quality_severity,
            freshness_age_hours,
            freshness_status
        FROM vw_governance_source_freshness
        ORDER BY source_system, entity_name
        """
    )
