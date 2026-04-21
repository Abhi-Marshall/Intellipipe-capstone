import sys
import os
# Adds the current directory to the search path
sys.path.append(os.path.abspath('.'))

import mlflow
import mlflow.sklearn
from mlflow.models import infer_signature
from sklearn.ensemble import GradientBoostingRegressor, IsolationForest

# 1. Import from your other two files
from feature_engineering import engineer_features, get_train_test_split
from evaluate_model import calculate_volume_metrics, calculate_anomaly_f1, register_best_model

print("Starting IntelliPipe ML Training Job...\n")

# ==============================================================================
# STEP 1: PREP THE DATA
# ==============================================================================
print("--- STEP 1: Prepping Data ---")
pdf = spark.read.table("capstone_project.capstone_schema.hourly_order_metrics").toPandas()
df_clean = engineer_features(pdf)
X_train, y_train, X_test, y_test = get_train_test_split(df_clean)
print(f"Data prepped! Training on {len(X_train)} records.\n")


# ==============================================================================
# STEP 2: VOLUME PREDICTOR TOURNAMENT
# ==============================================================================
print("--- STEP 2: Volume Predictor (RMSE Optimization) ---")
volume_param_grid = [
    {'n_estimators': 50, 'learning_rate': 0.1, 'max_depth': 3},
    {'n_estimators': 100, 'learning_rate': 0.1, 'max_depth': 3},
    {'n_estimators': 100, 'learning_rate': 0.05, 'max_depth': 5}
]

best_volume_rmse = float('inf')
best_volume_run_id = None

for params in volume_param_grid:
    with mlflow.start_run(run_name="Volume_Tuning") as run:
        # Train
        gbm = GradientBoostingRegressor(**params, random_state=42)
        gbm.fit(X_train, y_train)
        preds = gbm.predict(X_test)
        
        # Evaluate using evaluate_model.py
        rmse, mae = calculate_volume_metrics(y_test, preds)
        
        # Log to MLflow
        mlflow.log_params(params)
        mlflow.log_metrics({"RMSE": rmse, "MAE": mae})
        signature = infer_signature(X_train, preds)
        mlflow.sklearn.log_model(gbm, "model", signature=signature)
        
        print(f"Tested {params} -> RMSE: {rmse:.2f}")
        
        # Track the winner
        if rmse < best_volume_rmse:
            best_volume_rmse = rmse
            best_volume_run_id = run.info.run_id


# ==============================================================================
# STEP 3: ANOMALY DETECTOR TOURNAMENT
# ==============================================================================
print("\n--- STEP 3: Anomaly Detector (F1 Score Optimization) ---")
anomaly_param_grid = [0.01, 0.05, 0.10]

best_anomaly_f1 = -1
best_anomaly_run_id = None

for contamination in anomaly_param_grid:
    with mlflow.start_run(run_name="Anomaly_Tuning") as run:
        # Train (Unsupervised)
        iso = IsolationForest(n_estimators=100, contamination=contamination, random_state=42)
        iso.fit(X_train)
        preds = iso.predict(X_test)
        
        # Evaluate using evaluate_model.py
        f1 = calculate_anomaly_f1(X_test, y_test, preds)
        
        # Log to MLflow
        mlflow.log_param("contamination", contamination)
        mlflow.log_metric("anomaly_F1", f1)
        signature = infer_signature(X_train, preds)
        mlflow.sklearn.log_model(iso, "model", signature=signature)
        
        print(f"Tested contamination {contamination} -> F1 Score: {f1:.3f}")
        
        # Track the winner
        if f1 > best_anomaly_f1:
            best_anomaly_f1 = f1
            best_anomaly_run_id = run.info.run_id


# ==============================================================================
# STEP 4: REGISTER THE WINNERS
# ==============================================================================
print("\n--- STEP 4: Registering Best Models to Unity Catalog ---")
register_best_model(best_volume_run_id, "capstone_project.capstone_schema.volume_predictor")
register_best_model(best_anomaly_run_id, "capstone_project.capstone_schema.anomaly_detector")

print("\nPipeline Complete! Both models are live in Unity Catalog.")