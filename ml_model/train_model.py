"""
train_model.py — new_ml_4
----------------------------
Trains the integrated Medallion Anomaly Detector (PDF + DOCX features merged)
and registers it to Unity Catalog via MLflow.

Features used are defined in feature_engineering.py.
"""

import mlflow
import mlflow.sklearn
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import joblib
import logging

from feature_engineering import validate_features, get_feature_groups, get_feature_weights, ALL_FEATURES

logger = logging.getLogger(__name__)


# ────────────────────────────────────────────────────────────────────────────────
# The Integrated Anomaly Detector
# ────────────────────────────────────────────────────────────────────────────────
class IntegratedAnomalyDetector:
    """
    Two-Stage Anomaly Detector combining PDF and DOCX feature sets.
    
    Stage 1: IsolationForest on all 15 Integrated features — detects global outliers.
    Stage 2: Z-Score Weighted Softmax Attribution — tells if, volume or schema caused it.
    """

    def __init__(self, contamination: float = 0.05, temp: float = 3.0):
        """
        Args:
            contamination: Expected fraction of outliers in training data.
            temp: Softmax temperature. Higher = smoother probability distribution.
        """
        self.contamination = contamination
        self.temp = temp
        self.classifier = IsolationForest(contamination=contamination, random_state=42, n_estimators=150)
        self.feature_groups = get_feature_groups()
        self.feature_weights = get_feature_weights()
        self.historical_mean = None
        self.historical_std = None
        self.is_trained = False

    def train(self, df: pd.DataFrame):
        """
        Trains the Isolation Forest on the integrated feature set.
        
        Args:
            df: Historical monitoring metrics dataframe.
        
        Returns:
            self (for chaining)
        """
        feature_df = validate_features(df)
        self.classifier.fit(feature_df)
        self.historical_mean = feature_df.mean().to_dict()
        self.historical_std = feature_df.std().to_dict()
        
        # Avoid zero-division in Z-score calculation
        for col in self.historical_std:
            if self.historical_std[col] == 0 or pd.isna(self.historical_std[col]):
                self.historical_std[col] = 1e-9
        
        self.is_trained = True
        logger.info(f"Model trained on {len(feature_df)} rows using {len(ALL_FEATURES)} features.")
        return self

    def predict(self, metrics: dict) -> dict:
        """
        Predicts anomaly status and computes probability attribution.

        Mathematical Pipeline (mirrors the PDF Framework exactly):
        ──────────────────────────────────────────────────────────
        STAGE 1 — Isolation Forest
            Input vector X: all 15 Integrated features.
            pred_label = IsolationForest.predict(X)   → +1 (normal) or -1 (anomaly)
            Severity   = -IsolationForest.score_samples(X)    [higher = more anomalous]

        STAGE 2 — Z-Score Weighted Softmax Attribution (only if pred_label == -1)
            STEP 1 — Z-Score per feature:
                Z(f) = (X(f) - μ(f)) / σ(f)
                μ, σ learned from historical training data.

            STEP 2 — Weighted Deviation per Group:
                D_volume = Σ [ W(f) × |Z(f)| ]  for f in Volume_Temporal_Group
                D_schema = Σ [ W(f) × |Z(f)| ]  for f in Schema_Quality_Group
                Weights W(f) defined in feature_engineering.get_feature_weights()

            STEP 3 — Score Compression (Log Scale):
                L_volume = log(1 + D_volume)     → prevents extreme values dominating
                L_schema = log(1 + D_schema)

            STEP 4 — Temperature Scaling:
                S_volume = L_volume / T           → T=3.0 controls sharpness
                S_schema = L_schema / T

            STEP 5 — Softmax Probability Layer:
                P_volume = exp(S_volume)  / [exp(S_volume) + exp(S_schema)]
                P_schema = exp(S_schema) / [exp(S_volume) + exp(S_schema)]
                Note: P_volume + P_schema = 1.0  (always)
        ──────────────────────────────────────────────────────────

        Args:
            metrics: Dict with values for all features in ALL_FEATURES.

        Returns:
            dict: {Anomaly_Detected, Severity_Score, Volume_Spike_Probability, Schema_Quality_Probability}
        """
        if not self.is_trained:
            raise RuntimeError("Model must be trained first. Call train().")

        # ── STAGE 1: Isolation Forest ─────────────────────────────────────────
        input_arr  = np.array([[metrics.get(col, 0.0) for col in ALL_FEATURES]])
        pred_label = self.classifier.predict(input_arr)[0]       # +1 or -1
        raw_score  = -self.classifier.score_samples(input_arr)[0]
        severity   = round(float(raw_score), 4)

        # Early exit: row is normal
        if pred_label == 1:
            return {
                "Anomaly_Detected":          "NO",
                "Severity_Score":             severity,
                "Volume_Spike_Probability":   0.0,
                "Schema_Quality_Probability": 0.0
            }

        # ── STAGE 2: Z-Score Weighted Softmax Attribution ─────────────────────
        weights = self.feature_weights

        # STEP 1 — Z-scores and STEP 2 — Weighted group deviation (D)
        def compute_group_deviation(feature_list: list) -> float:
            """D_group = Σ W(f) × |Z(f)|"""
            D = 0.0
            for feat in feature_list:
                X_f     = metrics.get(feat, 0.0)
                mu_f    = self.historical_mean.get(feat, 0.0)
                sigma_f = self.historical_std.get(feat, 1e-9)
                Z_f     = (X_f - mu_f) / sigma_f          # Z-Score: STEP 1
                D      += weights.get(feat, 0.0) * abs(Z_f)  # Weighted sum: STEP 2
            return D

        D_volume = compute_group_deviation(self.feature_groups["volume"])
        D_schema = compute_group_deviation(self.feature_groups["schema"])

        # STEP 3 — Score Compression Layer (Log Scale)
        L_volume = np.log1p(D_volume)   # log(1 + D_volume)
        L_schema = np.log1p(D_schema)   # log(1 + D_schema)

        # STEP 4 — Temperature Scaling
        T        = self.temp            # default = 3.0
        S_volume = L_volume / T
        S_schema = L_schema / T

        # STEP 5 — Softmax Probability Layer
        exp_S_volume = np.exp(S_volume)
        exp_S_schema = np.exp(S_schema)
        denominator  = exp_S_volume + exp_S_schema

        P_volume = exp_S_volume / denominator   # Volume_Spike_Probability
        P_schema = exp_S_schema / denominator   # Schema_Quality_Probability
        # Note: P_volume + P_schema = 1.0 always

        return {
            "Anomaly_Detected":          "YES",
            "Severity_Score":             severity,
            "Volume_Spike_Probability":   round(float(P_volume), 3),
            "Schema_Quality_Probability": round(float(P_schema), 3),
        }


# ────────────────────────────────────────────────────────────────────────────────
# Training Entry Point
# ────────────────────────────────────────────────────────────────────────────────
def run_training_pipeline(spark, metrics_table: str, model_name: str) -> str:
    """
    Full training + MLflow Unity Catalog registration.
    
    Args:
        spark: Active SparkSession.
        metrics_table: Full table name (catalog.schema.table).
        model_name: Full Unity Catalog model name (catalog.schema.model_name).
    
    Returns:
        str: The MLflow run_id of the registered experiment.
    """
    logger.info(f"Loading data from {metrics_table}...")
    df = spark.table(metrics_table).toPandas()
    
    if len(df) < 15:
        raise ValueError(f"Only {len(df)} rows found. Need at least 15 to train reliably.")
    
    detector = IntegratedAnomalyDetector(contamination=0.05)
    detector.train(df)
    
    mlflow.set_registry_uri("databricks-uc")
    
    with mlflow.start_run() as run:
        mlflow.log_param("contamination",    detector.contamination)
        mlflow.log_param("temperature",      detector.temp)
        mlflow.log_param("n_features",       len(ALL_FEATURES))
        mlflow.log_param("n_training_rows",  len(df))
        mlflow.log_param("feature_list",     str(ALL_FEATURES))
        
        mlflow.sklearn.log_model(
            sk_model=detector,
            artifact_path="model",
            registered_model_name=model_name
        )
        
        run_id = run.info.run_id
        logger.info(f"Model registered as '{model_name}' (MLflow run: {run_id})")
        return run_id
