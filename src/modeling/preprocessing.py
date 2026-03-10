from __future__ import annotations

from typing import Tuple

import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler


def impute_zero(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    return df[columns].fillna(0.0)


def fit_scaler(train_features: pd.DataFrame) -> StandardScaler:
    scaler = StandardScaler()
    scaler.fit(train_features)
    return scaler


def transform_with_scaler(scaler: StandardScaler, features: pd.DataFrame) -> np.ndarray:
    return scaler.transform(features)
