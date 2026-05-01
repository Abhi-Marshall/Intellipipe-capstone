from databricks.sdk import WorkspaceClient
import pandas as pd
import logging
import json
import sys
import os

# Setup: Ensure the project root is visible from the MCP server context
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../')))

from src.tools.email_tool import send_status_email

logger = logging.getLogger(__name__)

# Configuration
CATALOG = "capstone_project"
SCHEMA  = "capstone_schema"
MONITORING_TABLE = f"{CATALOG}.{SCHEMA}.pipeline_monitoring_metrics"
WAREHOUSE_ID = "f0007797a3f24edc"
ENDPOINT_NAME = "anomaly-detector-endpoint"


def fetch_latest_medallion_metrics() -> dict:
    """
    Fetches the most recent row from the `pipeline_monitoring_metrics` table
    using the app's default identity (same pattern as other working tools).
    
    Returns:
        dict: Latest telemetry metrics row with all 14 features required by the integrated model.
    """
    try:
        w = WorkspaceClient()
        
        # Query ALL 14 features that the integrated model requires
        query = f"""
            SELECT 
                run_id,
                run_timestamp,
                current_volume,
                pct_change,
                lag_1h,
                lag_2h,
                lag_3h,
                rolling_mean_3h,
                rolling_mean_6h,
                rolling_std_6h,
                hour_sin,
                hour_cos,
                is_weekend,
                null_pct,
                dup_pct,
                mismatch_count,
                missingcol_count
            FROM {MONITORING_TABLE}
            ORDER BY run_timestamp DESC
            LIMIT 1
        """
        
        res = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=query,
            wait_timeout="30s"
        )
        
        if res.status.state != "SUCCEEDED":
            raise Exception(f"SQL query failed: {res.status}")
        
        # Parse results
        if res.result and res.result.data_array and len(res.result.data_array) > 0:
            columns = [col.name for col in res.manifest.schema.columns]
            row = res.result.data_array[0]
            latest = dict(zip(columns, row))
            
            logger.info(f"Fetched latest telemetry row: run_id={latest.get('run_id')}")
            return latest
        else:
            raise ValueError("No telemetry data found in pipeline_monitoring_metrics table.")
            
    except Exception as e:
        logger.error(f"Error fetching medallion metrics: {e}")
        # Return baseline features as fallback (all 14 required features)
        return {
            "run_id": "fallback",
            "run_timestamp": "2024-01-01 00:00:00",
            "current_volume": 1000.0,
            "pct_change": 0.0,
            "lag_1h": 1000,
            "lag_2h": 1000,
            "lag_3h": 1000,
            "rolling_mean_3h": 1000.0,
            "rolling_mean_6h": 1000.0,
            "rolling_std_6h": 50.0,
            "hour_sin": 0.0,
            "hour_cos": 1.0,
            "is_weekend": 0,
            "null_pct": 0.0,
            "dup_pct": 0.0,
            "mismatch_count": 0,
            "missingcol_count": 0
        }


def get_anomaly_prediction() -> dict:
    """
    Zero-input MCP tool for the Supervisor Agent.
    Uses the app's default identity (same as other working tools).

    Automatically:
      1. Fetches the latest telemetry from the monitoring table.
      2. Calls the Databricks Model Serving Endpoint using WorkspaceClient.
      3. Sends an email alert if an anomaly is detected.

    Returns:
        dict: {
            Anomaly_Detected, Severity_Score,
            Volume_Spike_Probability, Schema_Quality_Probability,
            run_id, timestamp, status
        }
    """
    try:
        # Step 1: Fetch latest integrated telemetry
        latest_metrics = fetch_latest_medallion_metrics()

        # Step 2: Prepare payload - ALL 14 features the integrated model expects
        feature_keys = [
            "current_volume", "pct_change",
            "lag_1h", "lag_2h", "lag_3h",
            "rolling_mean_3h", "rolling_mean_6h", "rolling_std_6h",
            "hour_sin", "hour_cos", "is_weekend",
            "null_pct", "dup_pct", "mismatch_count", "missingcol_count"
        ]
        feature_payload = {k: latest_metrics[k] for k in feature_keys if k in latest_metrics}

        # Step 3: Call the Model Serving Endpoint using WorkspaceClient (same as other tools)
        w = WorkspaceClient()
        
        response = w.serving_endpoints.query(
            name=ENDPOINT_NAME,
            dataframe_records=[feature_payload]
        )

        # Extract the prediction from the endpoint response
        predictions = response.predictions if hasattr(response, 'predictions') else response.get('predictions', [])
        prediction = predictions[0] if (isinstance(predictions, list) and len(predictions) > 0) else predictions

        # Step 4: Build response
        response_data = {
            "status": "success",
            "run_id": latest_metrics.get("run_id"),
            "timestamp": str(latest_metrics.get("run_timestamp")),
            "Anomaly_Detected": prediction.get("Anomaly_Detected"),
            "Severity_Score": prediction.get("Severity_Score"),
            "Volume_Spike_Probability": prediction.get("Volume_Spike_Probability"),
            "Schema_Quality_Probability": prediction.get("Schema_Quality_Probability"),
        }

        # Step 5: Email alert if anomaly detected
        if prediction.get("Anomaly_Detected") == "YES":
            try:
                vol_prob  = prediction.get("Volume_Spike_Probability", 0)
                sch_prob  = prediction.get("Schema_Quality_Probability", 0)
                primary   = "Volume Spike" if vol_prob >= sch_prob else "Schema Quality"

                email_body = f"""
                <h2>⚠️ IntelliPipe ML Anomaly Alert</h2>
                <p><b>Run ID:</b> {latest_metrics.get('run_id')}</p>
                <p><b>Timestamp:</b> {latest_metrics.get('run_timestamp')}</p>
                <p><b>Anomaly Detected:</b> <span style="color:red; font-weight:bold;">YES</span></p>
                <p><b>Severity Score:</b> {prediction.get('Severity_Score')}</p>
                <p><b>Primary Cause:</b> {primary}</p>
                <p><b>Volume Spike Probability:</b> {round(vol_prob * 100, 1)}%</p>
                <p><b>Schema Quality Probability:</b> {round(sch_prob * 100, 1)}%</p>
                <h3>Integrated Features Used</h3>
                <pre style="background:#f4f4f4;padding:10px;border-radius:5px;">
{json.dumps(feature_payload, indent=2)}
                </pre>
                <h3>Full System State</h3>
                <pre style="background:#f4f4f4;padding:10px;border-radius:5px;">
{json.dumps({k: v for k, v in latest_metrics.items() if k not in ['run_id', 'run_timestamp']}, indent=2)}
                </pre>
                """

                send_status_email(
                    subject=f"🚨 IntelliPipe Anomaly Alert - {primary}",
                    body=email_body,
                    to_email="capstoneproject196@gmail.com"
                )
                logger.info("Anomaly email alert sent successfully.")
            except Exception as email_error:
                logger.error(f"Failed to send anomaly email: {email_error}")

        return response_data

    except Exception as e:
        logger.error(f"Error getting anomaly prediction: {e}")
        return {"status": "failed", "error": str(e)}
