from __future__ import annotations

from typing import Iterable

import pandas as pd


RULE_DRIVER_LABELS = {
    "feat_incident_rate_per_200k_hours": "Elevated incident rate",
    "feat_prior_incident_count": "Recent incident count remained elevated",
    "feat_rolling_4q_incident_count": "Sustained 4-quarter incident history",
    "feat_prior_violation_count": "Recent violation count remained elevated",
    "feat_rolling_4q_violation_count": "Sustained 4-quarter violation burden",
    "feat_ss_share": "High S&S violation share",
    "feat_assessed_penalty_amount_lag1": "Large prior penalty burden",
    "feat_production_efficiency_qoq_delta": "Production efficiency deteriorated",
    "feat_violation_burden_qoq_delta": "Violation burden worsened QoQ",
    "feat_deterioration_streak_count": "Multi-quarter deterioration streak",
}

LOGREG_DRIVER_LABELS = {
    "feat_prior_incident_count": "Recent incident history increased risk",
    "feat_rolling_4q_incident_count": "Sustained incident history increased risk",
    "feat_prior_lost_day_incident_count": "Lost-day incident history increased risk",
    "feat_incident_rate_per_200k_hours": "Current incident rate increased risk",
    "feat_prior_violation_count": "Recent violation history increased risk",
    "feat_rolling_4q_violation_count": "Sustained violation burden increased risk",
    "feat_ss_share": "S&S share increased risk",
    "feat_assessed_penalty_amount_lag1": "Penalty burden increased risk",
    "feat_production_per_employee_hour": "Low production efficiency increased risk",
    "feat_production_efficiency_qoq_delta": "Production efficiency trend weakened",
    "feat_violation_burden_qoq_delta": "Violation burden trend worsened",
    "feat_deterioration_streak_count": "Deterioration streak increased risk",
}


def _rank_drivers(values: pd.Series, mapping: dict[str, str], top_n: int, prefer_positive: bool) -> list[str]:
    if prefer_positive:
        positives = values[values > 0].sort_values(ascending=False)
        if len(positives) >= top_n:
            return [mapping.get(k, k) for k in positives.index[:top_n]]
        remaining = values.drop(positives.index)
        combined = pd.concat([positives, remaining.reindex(remaining.abs().sort_values(ascending=False).index)])
        return [mapping.get(k, k) for k in combined.index[:top_n]]

    ordered = values.sort_values(ascending=False)
    return [mapping.get(k, k) for k in ordered.index[:top_n]]


def build_driver_columns(
    contributions: pd.DataFrame,
    mapping: dict[str, str],
    top_n: int = 3,
    prefer_positive: bool = False,
) -> pd.DataFrame:
    drivers = []
    for _, row in contributions.iterrows():
        top = _rank_drivers(row, mapping, top_n=top_n, prefer_positive=prefer_positive)
        drivers.append(top + [None] * (top_n - len(top)))
    return pd.DataFrame(drivers, columns=["top_driver_1", "top_driver_2", "top_driver_3"])
