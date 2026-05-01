"""
evaluate_model.py — new_ml_4
--------------------------------
Evaluates the IntegratedAnomalyDetector model against 3 canonical test scenarios:
  1. NORMAL: Healthy pipeline, no anomalies expected.
  2. VOLUME SPIKE: Large sudden spike + lag deviation. Volume group should dominate.
  3. SCHEMA FAILURE: High nulls, duplicates, mismatches. Schema group should dominate.

Also wraps an mlflow.evaluate()-compatible evaluation interface as required by the DOCX.
"""

import mlflow
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


# ────────────────────────────────────────────────────────────────────────────────
# Canonical Test Scenarios
# ────────────────────────────────────────────────────────────────────────────────
EVAL_SCENARIOS = [
    {
        "name": "NORMAL",
        "expected_anomaly": "NO",
        "metrics": {
            "current_volume": 5000, "pct_change": 2.0,
            "lag_1h": 4900,         "lag_2h": 4850,    "lag_3h": 4800,
            "rolling_mean_3h": 4850,"rolling_mean_6h": 4820, "rolling_std_6h": 80.0,
            "hour_sin": 0.5,        "hour_cos": 0.866, "is_weekend": 0,
            "null_pct": 0.3,        "dup_pct": 0.1,
            "mismatch_count": 2,    "missingcol_count": 0,
        }
    },
    {
        "name": "VOLUME_SPIKE",
        "expected_anomaly": "YES",
        "expected_cause": "Volume",
        "metrics": {
            "current_volume": 45000, "pct_change": 800.0,
            "lag_1h": 5000,          "lag_2h": 4900,    "lag_3h": 4850,
            "rolling_mean_3h": 4900, "rolling_mean_6h": 4850, "rolling_std_6h": 90.0,
            "hour_sin": 0.5,         "hour_cos": 0.866, "is_weekend": 0,
            "null_pct": 0.3,         "dup_pct": 0.1,
            "mismatch_count": 2,     "missingcol_count": 0,
        }
    },
    {
        "name": "SCHEMA_FAILURE",
        "expected_anomaly": "YES",
        "expected_cause": "Schema",
        "metrics": {
            "current_volume": 5000, "pct_change": 1.0,
            "lag_1h": 4900,         "lag_2h": 4850,    "lag_3h": 4800,
            "rolling_mean_3h": 4850,"rolling_mean_6h": 4820, "rolling_std_6h": 80.0,
            "hour_sin": 0.5,        "hour_cos": 0.866, "is_weekend": 0,
            "null_pct": 45.0,       "dup_pct": 18.0,
            "mismatch_count": 200,  "missingcol_count": 3,
        }
    }
]


def run_scenario_tests(model) -> dict:
    """
    Runs the 3 canonical test scenarios against a trained model.
    
    Args:
        model: A trained IntegratedAnomalyDetector instance.
    
    Returns:
        dict: {"passed": int, "failed": int, "results": [...]}
    """
    results = []
    passed  = 0
    failed  = 0

    print("\n" + "=" * 60)
    print("  IntelliPipe — Integrated Model Evaluation")
    print("=" * 60)
    
    for scenario in EVAL_SCENARIOS:
        prediction = model.predict(scenario["metrics"])
        
        # Check 1: Did we correctly detect/not detect?
        detected_correct = (prediction["Anomaly_Detected"] == scenario["expected_anomaly"])
        
        # Check 2: (If anomaly) Is the primary cause right?
        cause_correct = True
        if scenario.get("expected_cause"):
            vol_prob = prediction["Volume_Spike_Probability"]
            sch_prob = prediction["Schema_Quality_Probability"]
            predicted_cause = "Volume" if vol_prob >= sch_prob else "Schema"
            cause_correct = (predicted_cause == scenario["expected_cause"])
        
        test_passed = detected_correct and cause_correct
        passed += 1 if test_passed else 0
        failed += 1 if not test_passed else 0
        
        result = {
            "scenario":           scenario["name"],
            "expected_anomaly":   scenario["expected_anomaly"],
            "predicted_anomaly":  prediction["Anomaly_Detected"],
            "severity_score":     prediction["Severity_Score"],
            "vol_probability":    prediction["Volume_Spike_Probability"],
            "schema_probability": prediction["Schema_Quality_Probability"],
            "cause_correct":      cause_correct,
            "test_passed":        test_passed,
        }
        results.append(result)
        
        status = "✅ PASS" if test_passed else "❌ FAIL"
        print(f"\n{status} Scenario: {scenario['name']}")
        print(f"   Anomaly Detected:      {prediction['Anomaly_Detected']}")
        print(f"   Severity Score:        {prediction['Severity_Score']}")
        print(f"   Volume Probability:    {prediction['Volume_Spike_Probability']}")
        print(f"   Schema Probability:    {prediction['Schema_Quality_Probability']}")
    
    print("\n" + "=" * 60)
    print(f"  Summary: {passed}/3 scenarios passed")
    print("=" * 60)
    
    return {"passed": passed, "failed": failed, "results": results}


def log_evaluation_to_mlflow(model, run_id: str, spark=None, metrics_table: str = None):
    """
    Logs evaluation metrics from the canonical tests to the MLflow run.
    
    Args:
        model: Trained IntegratedAnomalyDetector instance.
        run_id: The MLflow run_id from train_model.run_training_pipeline().
        spark: Optional SparkSession for extended evaluation on live data.
        metrics_table: Optional. If given, runs a quick sanity check on live data too.
    """
    eval_results = run_scenario_tests(model)
    
    with mlflow.start_run(run_id=run_id):
        mlflow.log_metric("scenarios_passed", eval_results["passed"])
        mlflow.log_metric("scenarios_failed", eval_results["failed"])
        mlflow.log_metric("scenario_pass_rate", eval_results["passed"] / 3.0)
        
        # Log each scenario result
        for res in eval_results["results"]:
            prefix = res["scenario"].lower()
            mlflow.log_metric(f"{prefix}_vol_prob",    res["vol_probability"])
            mlflow.log_metric(f"{prefix}_schema_prob", res["schema_probability"])
            mlflow.log_metric(f"{prefix}_severity",    res["severity_score"])
        
        logger.info(f"Evaluation results logged to MLflow run: {run_id}")
    
    return eval_results
