"""
Shared feature engineering helpers for the new model pipeline.
"""

from __future__ import annotations

from typing import List

import pandas as pd


EXTRA_GAME_FEATURE_COLS: List[str] = [
    "home_court",
    "rest_days_diff",
    "back_to_back_diff",
    "inj_out_diff",
    "pace_diff",
]


def _col(df: pd.DataFrame, name: str) -> pd.Series:
    if name in df.columns:
        return df[name]
    return pd.Series([0] * len(df), index=df.index, dtype="float64")


def add_game_deltas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add simple matchup/condition deltas using existing team feature columns.
    Safe even when columns are missing (fills with 0).
    """
    df = df.copy()
    df["home_court"] = 1.0
    df["rest_days_diff"] = _col(df, "rest_days_home") - _col(df, "rest_days_away")
    df["back_to_back_diff"] = _col(df, "back_to_back_home") - _col(df, "back_to_back_away")
    df["inj_out_diff"] = _col(df, "inj_out_home") - _col(df, "inj_out_away")
    df["pace_diff"] = _col(df, "pace_home") - _col(df, "pace_away")
    return df
