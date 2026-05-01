"""
feature_engineering.py — new_ml_4
-----------------------------------
Validates and extracts the 15-column integrated feature set that satisfies both:

  (A) PDF Framework requirements:    null_pct, dup_pct, mismatch_count, missingcol_count
  (B) DOCX Specification requirements: temporal lag, rolling stats, cyclical time encoding
"""

import pandas as pd
import numpy as np


# ────────────────────────────────────────────────────────────────────────────────
# Integrated Feature Schema (PDF + DOCX merged)
# ────────────────────────────────────────────────────────────────────────────────
VOLUME_TEMPORAL_FEATURES = [
    "current_volume",      # raw row count for the hour
    "pct_change",          # % change from previous hour (DOCX + PDF)
    "lag_1h",              # volume 1 hour ago (DOCX)
    "lag_2h",              # volume 2 hours ago (DOCX)
    "lag_3h",              # volume 3 hours ago (DOCX)
    "rolling_mean_3h",     # 3-hour moving average (DOCX)
    "rolling_mean_6h",     # 6-hour moving average (DOCX)
    "rolling_std_6h",      # 6-hour rolling std deviation (DOCX)
    "hour_sin",            # cyclical hour encoding — sin (DOCX)
    "hour_cos",            # cyclical hour encoding — cos (DOCX)
    "is_weekend",          # binary weekend flag (DOCX)
]

SCHEMA_QUALITY_FEATURES = [
    "null_pct",            # % null rows in Bronze (PDF)
    "dup_pct",             # % duplicate rows in Bronze (PDF)
    "mismatch_count",      # rows failing Silver quality constraints (PDF)
    "missingcol_count",    # columns missing vs. expected schema (PDF)
]

ALL_FEATURES = VOLUME_TEMPORAL_FEATURES + SCHEMA_QUALITY_FEATURES


def validate_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates that the input dataframe contains all required integrated features.
    
    Args:
        df (pd.DataFrame): Raw monitoring metrics from the pipeline_monitoring_metrics table.
        
    Returns:
        pd.DataFrame: Feature-only dataframe with all 15 required columns, with NaN filled.
        
    Raises:
        ValueError: If any required feature column is missing from the dataframe.
    """
    missing = [col for col in ALL_FEATURES if col not in df.columns]
    if missing:
        raise ValueError(
            f"Input dataframe is missing mandatory feature columns: {missing}\n"
            f"Please ensure telemetry_generation.py has been run first."
        )
    
    feature_df = df[ALL_FEATURES].copy()
    
    # Fill any NaN values with column medians to avoid prediction errors
    for col in ALL_FEATURES:
        if feature_df[col].isna().any():
            fill_val = feature_df[col].median()
            feature_df[col] = feature_df[col].fillna(fill_val)
    
    return feature_df


def get_feature_groups() -> dict:
    """
    Returns the feature group definitions used for Softmax probability attribution.
    
    Group A (Volumetric): Captures time-series volume patterns.
    Group B (Schema):     Captures data quality degradation.
    
    Returns:
        dict: {"volume": [...], "schema": [...]}
    """
    return {
        "volume": VOLUME_TEMPORAL_FEATURES,
        "schema": SCHEMA_QUALITY_FEATURES
    }


def get_feature_weights() -> dict:
    """
    Returns the Z-score weighting for each feature within its group.
    Higher weight = more impactful signal for anomaly attribution.
    
    Returns:
        dict: Feature weights for the attribution calculation.
    """
    return {
        # Volume Group Weights (total = 1.0)
        "current_volume":   0.25,
        "pct_change":       0.20,
        "lag_1h":           0.15,
        "lag_2h":           0.10,
        "lag_3h":           0.08,
        "rolling_mean_3h":  0.08,
        "rolling_mean_6h":  0.05,
        "rolling_std_6h":   0.05,
        "hour_sin":         0.02,
        "hour_cos":         0.01,
        "is_weekend":       0.01,

        # Schema Group Weights (total = 1.0)
        "null_pct":         0.30,
        "dup_pct":          0.20,
        "mismatch_count":   0.35,
        "missingcol_count": 0.15,
    }
