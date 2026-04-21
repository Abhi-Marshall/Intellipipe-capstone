import numpy as np
import mlflow
from sklearn.metrics import mean_squared_error, mean_absolute_error, f1_score

def calculate_volume_metrics(y_true, y_pred):
    """Calculates RMSE and MAE for the regression model."""
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    mae = mean_absolute_error(y_true, y_pred)
    return rmse, mae

def calculate_anomaly_f1(X_test, y_true_volume, anomaly_preds):
    """Calculates the F1 score against a synthetic statistical baseline."""
    # Create the baseline: true anomaly is > 2.5 std devs from the 6h mean
    y_test_true_anomaly = np.where(
        (y_true_volume > X_test['rolling_mean_6h'] + 2.5 * X_test['rolling_std_6h']) |
        (y_true_volume < X_test['rolling_mean_6h'] - 2.5 * X_test['rolling_std_6h']),
        -1, 1
    )
    
    # Calculate F1 specifically for the anomaly class (-1)
    f1 = f1_score(y_test_true_anomaly, anomaly_preds, pos_label=-1, zero_division=0)
    return f1

def register_best_model(run_id: str, model_name: str):
    """Registers a specific MLflow run to the Unity Catalog."""
    print(f"Registering Run {run_id} to {model_name}...")
    mlflow.set_registry_uri("databricks-uc")
    mlflow.register_model(f"runs:/{run_id}/model", model_name)