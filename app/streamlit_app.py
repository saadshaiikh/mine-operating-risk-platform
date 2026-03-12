import streamlit as st
import pandas as pd

from queries import (
    get_kpi_summary,
    get_top_risk_filters,
    get_top_risk_mines,
    get_mine_detail_filters,
    get_mine_detail_row,
    get_mine_history,
    get_backtest_summary,
    get_governance,
)


st.set_page_config(page_title="Mine Risk MVP Dashboard", layout="wide")


@st.cache_data(ttl=300)
def load_kpi():
    return get_kpi_summary()


@st.cache_data(ttl=300)
def load_top_risk_filters():
    return get_top_risk_filters()


@st.cache_data(ttl=300)
def load_top_risk_mines(filters, limit):
    return get_top_risk_mines(filters, limit=limit)


@st.cache_data(ttl=300)
def load_mine_filters():
    return get_mine_detail_filters()


@st.cache_data(ttl=300)
def load_mine_detail(model_version, period_key, mine_id):
    return get_mine_detail_row(model_version, period_key, mine_id)


@st.cache_data(ttl=300)
def load_mine_history(model_version, mine_id):
    return get_mine_history(model_version, mine_id)


@st.cache_data(ttl=300)
def load_backtest():
    return get_backtest_summary()


@st.cache_data(ttl=300)
def load_governance():
    return get_governance()


def format_period(period_key: str) -> str:
    return period_key


st.title("Mine Operating Risk MVP")

page = st.sidebar.radio(
    "Navigate",
    [
        "Executive Overview",
        "Top-Risk Mines",
        "Mine Detail Drill-Through",
        "Backtest Performance",
        "Governance & Freshness",
    ],
)


if page == "Executive Overview":
    st.header("Executive Overview")
    kpi = load_kpi()
    if kpi.empty:
        st.warning("No KPI data found.")
        st.stop()

    latest = kpi[kpi["is_latest_period_for_model_flag"] == 1].copy()
    if latest.empty:
        latest = kpi.copy()
    latest = latest.sort_values(["model_version", "year", "quarter"], ascending=[True, False, False])
    latest_row = latest.iloc[0]

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total Scored Mine-Quarters", f"{int(latest_row['total_mines_scored']):,}")
    col2.metric("High/Critical Count", f"{int(latest_row['high_or_critical_risk_mine_count']):,}")
    col3.metric("Average Risk Score", f"{latest_row['avg_risk_score']:.3f}")
    col4.metric("Latest Model", str(latest_row["model_version"]))
    col5.metric("Latest Period", format_period(str(latest_row["period_key"])))

    st.subheader("Risk Score Trend Over Time")
    model_versions = kpi["model_version"].dropna().unique().tolist()
    model_version = st.selectbox("Model Version", model_versions, index=0)
    trend = kpi[kpi["model_version"] == model_version].copy()
    trend = trend.sort_values(["year", "quarter"])
    trend["period_sort"] = trend["year"].astype(str) + "Q" + trend["quarter"].astype(str)
    trend = trend.set_index("period_sort")
    st.line_chart(trend["avg_risk_score"], height=220)

    st.subheader("High/Critical Share Over Time")
    st.line_chart(trend["high_or_critical_risk_share"], height=220)


elif page == "Top-Risk Mines":
    st.header("Top-Risk Mines")
    filters = load_top_risk_filters()

    model_versions = filters["model_versions"]["model_version"].tolist()
    periods = filters["periods"]["period_key"].tolist()
    states = ["All"] + filters["states"]["province_state"].tolist()
    commodities = ["All"] + filters["commodities"]["commodity_group"].tolist()
    risk_bands = ["All"] + filters["risk_bands"]["risk_band"].tolist()

    selected_model = st.sidebar.selectbox("Model Version", model_versions)
    selected_period = st.sidebar.selectbox("Period", periods)
    selected_state = st.sidebar.selectbox("Province/State", states)
    selected_commodity = st.sidebar.selectbox("Commodity Group", commodities)
    selected_risk_band = st.sidebar.selectbox("Risk Band", risk_bands)

    limit = st.sidebar.slider("Max Rows", min_value=50, max_value=1000, value=300, step=50)

    query_filters = {
        "model_version": selected_model,
        "period_key": selected_period,
        "province_state": None if selected_state == "All" else selected_state,
        "commodity_group": None if selected_commodity == "All" else selected_commodity,
        "risk_band": None if selected_risk_band == "All" else selected_risk_band,
    }

    top_risk = load_top_risk_mines(query_filters, limit)
    if top_risk.empty:
        st.info("No rows matched the current filters.")
    else:
        st.dataframe(top_risk, use_container_width=True, height=450)

        st.subheader("Risk Band Distribution")
        band_counts = top_risk["risk_band"].value_counts().sort_index()
        st.bar_chart(band_counts)

        st.subheader("Top Driver Summary")
        driver_counts = top_risk["top_driver_1"].value_counts().head(10)
        st.bar_chart(driver_counts)


elif page == "Mine Detail Drill-Through":
    st.header("Mine Detail Drill-Through")
    filters = load_mine_filters()

    model_versions = filters["model_versions"]["model_version"].tolist()
    periods = filters["periods"]["period_key"].tolist()
    mines_df = filters["mines"].copy()
    mines_df["display"] = mines_df["mine_id"].astype(str) + " - " + mines_df["mine_name"].fillna("")

    selected_model = st.sidebar.selectbox("Model Version", model_versions, index=0)
    selected_period = st.sidebar.selectbox("Period", periods, index=0)
    selected_mine_display = st.sidebar.selectbox("Mine", mines_df["display"].tolist(), index=0)
    selected_mine_id = selected_mine_display.split(" - ")[0]

    detail = load_mine_detail(selected_model, selected_period, selected_mine_id)
    if detail.empty:
        st.warning("No data for the selected mine-period-model.")
    else:
        row = detail.iloc[0]
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Risk Score", f"{row['risk_score']:.3f}")
        col2.metric("Risk Band", row["risk_band"])
        col3.metric("Top Driver 1", row["top_driver_1"])
        col4.metric("Top Driver 2", row["top_driver_2"])

        st.subheader("Incident / Violation / Production Trends")
        history = load_mine_history(selected_model, selected_mine_id)
        if not history.empty:
            history = history.sort_values(["year", "quarter"])
            history["period"] = history["year"].astype(str) + "Q" + history["quarter"].astype(str)
            history = history.set_index("period")
            st.line_chart(history[["incident_count_current_qtr", "violation_count_current_qtr"]], height=220)
            st.line_chart(history[["production_per_employee_hour_current_qtr"]], height=220)

        st.subheader("Mine Context")
        context_cols = [
            "mine_id",
            "mine_name",
            "province_state",
            "commodity_group",
            "mine_type",
            "employee_hours",
            "production_volume",
        ]
        st.dataframe(detail[context_cols].T.rename(columns={row.name: "value"}), use_container_width=True)

        st.subheader("Feature Values")
        feature_cols = [col for col in detail.columns if col.startswith("feat_")]
        feature_df = detail[feature_cols].T.rename(columns={row.name: "value"})
        st.dataframe(feature_df, use_container_width=True)


elif page == "Backtest Performance":
    st.header("Backtest Performance")
    backtest = load_backtest()
    if backtest.empty:
        st.warning("No backtest data found.")
        st.stop()

    per_year = backtest[[
        "validation_year",
        "roc_auc",
        "pr_auc",
        "precision_at_top_decile",
        "recall_at_top_decile",
        "lift_vs_base_rate",
        "business_claim_text",
    ]].drop_duplicates()

    st.subheader("Per-Year Metrics")
    st.dataframe(per_year, use_container_width=True)

    st.subheader("Performance Charts")
    chart_df = per_year.set_index("validation_year")
    st.line_chart(chart_df[["roc_auc", "pr_auc"]], height=220)
    st.line_chart(chart_df[["precision_at_top_decile", "recall_at_top_decile", "lift_vs_base_rate"]], height=220)

    st.subheader("Pooled Summary")
    pooled = backtest[[
        "pooled_precision_at_top_decile",
        "pooled_recall_at_top_decile",
        "pooled_lift_vs_base_rate",
    ]].dropna().head(1)
    st.dataframe(pooled, use_container_width=True)

    st.subheader("Naive Baseline Comparison (Static)")
    naive_rows = pd.DataFrame(
        [
            {"validation_year": 2019, "precision_at_top_decile": 0.393, "recall_at_top_decile": 0.582, "lift_vs_base_rate": 5.816},
            {"validation_year": 2020, "precision_at_top_decile": 0.354, "recall_at_top_decile": 0.595, "lift_vs_base_rate": 5.954},
            {"validation_year": 2021, "precision_at_top_decile": 0.349, "recall_at_top_decile": 0.566, "lift_vs_base_rate": 5.663},
            {"validation_year": "pooled", "precision_at_top_decile": 0.366, "recall_at_top_decile": 0.581, "lift_vs_base_rate": 5.810},
        ]
    )
    st.dataframe(naive_rows, use_container_width=True)


elif page == "Governance & Freshness":
    st.header("Governance & Freshness")
    gov = load_governance()
    if gov.empty:
        st.warning("No governance data found.")
        st.stop()

    st.dataframe(gov, use_container_width=True)

    st.subheader("Freshness Status Counts")
    status_counts = gov["freshness_status"].value_counts()
    st.bar_chart(status_counts)
