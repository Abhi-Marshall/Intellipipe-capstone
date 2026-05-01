from databricks.sdk import WorkspaceClient
from typing import Dict, Any, List
import logging
from datetime import datetime, timezone # ADDED FOR PROPER TIMESTAMPS
from dateutil import parser # ADD THIS (Standard in Databricks environments)
from src.tools.email_tool import send_status_email
import json

# ----- for trigger pipeline email handling part --------- 
import threading
import time
from src.tools.email_tool import send_status_email

logger = logging.getLogger(__name__)

WAREHOUSE_ID = "f0007797a3f24edc"
DEFAULT_PIPELINE_ID = "b5b42690-b884-4945-8da6-74b19aa93553"


def execute_sql(query: str):
    try:
        w = WorkspaceClient()

        res = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=query,
            wait_timeout="30s"
        )

        print("SQL STATUS:", res.status.state)

        if res.status.state != "SUCCEEDED":
            print("❌ SQL FAILED:", res.status)
        else:
            print("✅ SQL SUCCESS")

        return res

    except Exception as e:
        print("SQL ERROR:", str(e))


def get_pipeline_health(pipeline_id: str = DEFAULT_PIPELINE_ID) -> Dict[str, Any]:
    """
    Returns DLT pipeline status, last successful run time, and row counts per layer.
    """
    try:
        w = WorkspaceClient()
        pipeline_info = w.pipelines.get(pipeline_id)
        
        # ── 1. Basic pipeline info ────────────────────────────────────────────
        api_health = pipeline_info.health.value if pipeline_info.health else "UNKNOWN"
        
        health_response = {
            "pipeline_id":  pipeline_id,
            "name":         pipeline_info.name,
            "state":        pipeline_info.state.value if pipeline_info.state else "UNKNOWN",
            "health":       api_health,
            "last_updated": datetime.fromtimestamp(
                pipeline_info.last_modified / 1000.0, tz=timezone.utc
            ).strftime('%Y-%m-%dT%H:%M:%S.%fZ') if pipeline_info.last_modified else None
        }

        # ── 2. Latest update details ──────────────────────────────────────────
        if hasattr(pipeline_info, 'latest_updates') and pipeline_info.latest_updates:
            latest     = pipeline_info.latest_updates[0]
            update_id  = latest.update_id
            
            update_details = w.pipelines.get_update(pipeline_id, update_id)
            update_info    = update_details.update
            
            if isinstance(update_info.creation_time, int):
                start_ts = update_info.creation_time / 1000.0
            else:
                start_ts = parser.parse(str(update_info.creation_time)).timestamp()

            health_response["latest_update"] = {
                "update_id":  update_id,
                "state":      update_info.state.value,
                "start_time": datetime.fromtimestamp(
                    start_ts, tz=timezone.utc
                ).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            }

            # ── Health override ───────────────────────────────────────────────
            if health_response["health"] == "UNKNOWN":
                if update_info.state.value == "COMPLETED":
                    health_response["health"] = "HEALTHY (Operational)"
                elif update_info.state.value in ["RUNNING", "INITIALIZING", "SETTING_UP"]:
                    health_response["health"] = "HEALTHY (Update in progress)"
                elif update_info.state.value == "FAILED":
                    health_response["health"] = "UNHEALTHY (Last run failed)"

        # ── 3. Row counts per layer via Delta history ─────────────────────────
        try:
            LAYER_TABLES = {
                "bronze": [
                    "capstone_project.capstone_schema.bronze_orders",
                ],
                "silver": [
                    "capstone_project.capstone_schema.clean_orders",
                ],
                "gold": [
                    "capstone_project.capstone_schema.category_daily_metrics",
                    "capstone_project.capstone_schema.category_summary",
                    "capstone_project.capstone_schema.customer_360",
                    "capstone_project.capstone_schema.daily_business_metrics",
                    "capstone_project.capstone_schema.hourly_order_metrics",
                    "capstone_project.capstone_schema.product_daily_metrics",
                    "capstone_project.capstone_schema.product_performance",
                ]
            }

            layer_counts = {
                "bronze": {"total_current_rows": 0, "recent_rows_written": 0, "recent_rows_updated": 0, "recent_rows_deleted": 0, "tables_updated": []},
                "silver": {"total_current_rows": 0, "recent_rows_written": 0, "recent_rows_updated": 0, "recent_rows_deleted": 0, "tables_updated": []},
                "gold":   {"total_current_rows": 0, "recent_rows_written": 0, "recent_rows_updated": 0, "recent_rows_deleted": 0, "tables_updated": []}
            }

            for layer, tables in LAYER_TABLES.items():
                for full_table_name in tables:
                    try:
                        history_query = f"""
                        WITH latest_operation AS (
                            SELECT
                                operationMetrics['numOutputRows']::bigint AS rows_written,
                                operationMetrics['numUpdatedRows']::bigint AS rows_updated,
                                operationMetrics['numDeletedRows']::bigint AS rows_deleted
                            FROM (DESCRIBE HISTORY {full_table_name})
                            WHERE operation IN ('WRITE', 'MERGE', 'CREATE TABLE AS SELECT', 'REPLACE TABLE AS SELECT', 'STREAMING UPDATE', 'DELETE', 'UPDATE')
                            ORDER BY timestamp DESC
                            LIMIT 1
                        ),
                        current_count AS (
                            SELECT COUNT(*) AS total_rows
                            FROM {full_table_name}
                        )
                        SELECT 
                            cc.total_rows,
                            lo.rows_written,
                            lo.rows_updated,
                            lo.rows_deleted
                        FROM current_count cc
                        CROSS JOIN latest_operation lo
                        """

                        res = w.statement_execution.execute_statement(
                            warehouse_id=WAREHOUSE_ID,
                            statement=history_query,
                            wait_timeout="30s"
                        )

                        if res.result and res.result.data_array:
                            row          = res.result.data_array[0]
                            # Handle potential Nulls from the SQL query safely
                            total_rows   = int(row[0] if row[0] is not None else 0)
                            rows_written = int(row[1] if row[1] is not None else 0)
                            rows_updated = int(row[2] if row[2] is not None else 0)
                            rows_deleted = int(row[3] if row[3] is not None else 0)
                            
                            short_name   = full_table_name.split(".")[-1]

                            layer_counts[layer]["total_current_rows"]  += total_rows
                            layer_counts[layer]["recent_rows_written"] += rows_written
                            layer_counts[layer]["recent_rows_updated"] += rows_updated
                            layer_counts[layer]["recent_rows_deleted"] += rows_deleted

                            if short_name not in layer_counts[layer]["tables_updated"]:
                                layer_counts[layer]["tables_updated"].append(short_name)

                    except Exception as table_error:
                        logger.warning(f"Could not fetch Delta history for {full_table_name}: {table_error}")

            health_response["row_counts_per_layer"] = layer_counts

        except Exception as query_error:
            logger.warning(f"Could not fetch row count metrics: {query_error}")
            health_response["row_counts_per_layer"] = []
            health_response["metrics_note"] = "Row count metrics unavailable"

        # ─── NEW EMAIL TRIGGER LOGIC STARTS HERE ──────────────────────────────
        try:
            email_body = f"""
            <h2>Pipeline Health Report</h2>
            <p><b>Pipeline Name:</b> {health_response.get('name', 'Unknown')}</p>
            <p><b>Status:</b> {health_response.get('health', 'Unknown')}</p>
            <p><b>State:</b> {health_response.get('state', 'Unknown')}</p>
            <p><b>Last Updated:</b> {health_response.get('last_updated', 'Unknown')}</p>
            <h3>Row Counts Per Layer</h3>
            <pre style="background-color: #f4f4f4; padding: 10px; border-radius: 5px;">
{json.dumps(health_response.get('row_counts_per_layer', {}), indent=2)}
            </pre>
            """
            
            # Fire off the email silently in the background
            send_status_email(
                subject=f"Databricks Pipeline Alert: {health_response.get('health', 'UNKNOWN')}",
                body=email_body,
                to_email="capstoneproject196@gmail.com"
            )
            logger.info("Successfully triggered auto-email for pipeline health.")
        except Exception as email_error:
            logger.error(f"Failed to auto-send pipeline health email: {email_error}")
        # ─── NEW EMAIL TRIGGER LOGIC ENDS HERE ────────────────────────────────

        return health_response
        
    except Exception as e:
        logger.error(f"Error getting pipeline health: {e}")
        return {"error": str(e), "pipeline_id": pipeline_id}


# ---- watcher function for email handling part in trigger pipeline ---------

def _watch_pipeline_and_email(pipeline_id: str, update_id: str, run_type: str):
    """
    Background task that polls Databricks every 30 seconds to check if the pipeline finished.
    Once it hits a terminal state (COMPLETED, FAILED, CANCELED), it sends the final email.
    """
    try:
        w = WorkspaceClient()
        logger.info(f"Started background watcher for pipeline {pipeline_id}, update {update_id}")
        
        start_time = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

        # INSERT RUNNING STATUS
        running_query = f"""
        INSERT INTO capstone_project.capstone_schema.pipeline_status
        (pipeline_id, update_id, run_type, status, start_time, end_time, error_message)
        VALUES (
            '{pipeline_id}',
            '{update_id}',
            '{run_type}',
            'RUNNING',
            current_timestamp(),
            NULL,
            ''
        )
        """
        print("🚀 ABOUT TO EXECUTE RUNNING QUERY")
        print(running_query)

        execute_sql(running_query)

        while True:
            # Check current status
            update_details = w.pipelines.get_update(pipeline_id, update_id)
            current_state = update_details.update.state.value
            
            # Terminal states that mean the pipeline has stopped running
            if current_state in ["COMPLETED", "FAILED", "CANCELED"]:
                status_color = "green" if current_state == "COMPLETED" else "red"

                end_time = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

                # sanitize error message (VERY IMPORTANT)
                error_message = error_message.replace("'", " ")[:500]

                final_query = f"""
                INSERT INTO capstone_project.capstone_schema.pipeline_status
                (pipeline_id, update_id, run_type, status, start_time, end_time, error_message)
                VALUES (
                    '{pipeline_id}',
                    '{update_id}',
                    '{run_type}',
                    '{current_state}',
                    NULL,
                    current_timestamp(),
                    '{error_message}'
                )
                """

                if current_state != "COMPLETED":
                    error_message = str(update_details.update)

                print("🚀 ABOUT TO EXECUTE FINAL QUERY")
                print(final_query)
                
                execute_sql(final_query)
                
                email_body = f"""
                <h2>Pipeline Run Finished</h2>
                <p><b>Pipeline ID:</b> {pipeline_id}</p>
                <p><b>Update ID:</b> {update_id}</p>
                <p><b>Run Type:</b> {run_type}</p>
                <p><b>Final Status:</b> <span style="color: {status_color}; font-weight: bold;">{current_state}</span></p>
                """
                
                send_status_email(
                    subject=f"Pipeline {current_state}: {pipeline_id}",
                    body=email_body,
                    to_email="capstoneproject196@gmail.com"
                )
                logger.info(f"Pipeline {update_id} finished with state {current_state}. Final email sent.")
                break # Exit the loop and kill the thread
                
            # If still running, sleep for 30 seconds before checking again
            time.sleep(30)
            
    except Exception as e:
        logger.error(f"Background watcher failed for pipeline {pipeline_id}: {e}")


#  ----- updated trigger pipeline part to handle the threading for status checking and keeping the conn alive------

def trigger_pipeline_run(pipeline_id: str = DEFAULT_PIPELINE_ID, full_refresh: bool = False) -> Dict[str, Any]:
    """
    Triggers a DLT pipeline via Databricks Pipelines API.
    Sends an initial email, and spawns a background thread to send a final email upon completion.
    """
    try:
        w = WorkspaceClient()
        
        # Trigger the pipeline
        update_response = w.pipelines.start_update(
            pipeline_id=pipeline_id, 
            full_refresh=full_refresh
        )
        
        run_type = "Full Refresh (Wipe and recompute)" if full_refresh else "Incremental Update"
        update_id = update_response.update_id
        
        # ─── 1. SEND "STARTED" EMAIL ──────────────────────────────────────────
        try:
            start_email_body = f"""
            <h2>Pipeline Triggered Successfully</h2>
            <p><b>Pipeline ID:</b> {pipeline_id}</p>
            <p><b>Update ID:</b> {update_id}</p>
            <p><b>Run Type:</b> {run_type}</p>
            <p><b>Status:</b> <span style="color: blue; font-weight: bold;">RUNNING / INITIALIZING</span></p>
            <p><i>You will receive another email when this run completes or fails.</i></p>
            """
            send_status_email(
                subject=f"Pipeline Triggered: {pipeline_id}",
                body=start_email_body,
                to_email="capstoneproject196@gmail.com"
            )
        except Exception as email_error:
            logger.error(f"Failed to send initial trigger email: {email_error}")

        # ─── 2. START BACKGROUND WATCHER THREAD ───────────────────────────────
        # This allows the AI agent to get an immediate response while the server waits in the background
        watcher_thread = threading.Thread(
            target=_watch_pipeline_and_email, 
            args=(pipeline_id, update_id, run_type)
        )
        watcher_thread.daemon = False # Daemon means it won't prevent the server from shutting down
        watcher_thread.start()

        # ─── 3. RETURN RESPONSE TO AI AGENT IMMEDIATELY ───────────────────────
        agent_note = f"Successfully triggered a {run_type} for pipeline {pipeline_id}. An email was sent to the admin, and a background task is tracking its completion."
        
        return {
            "status": "triggered",
            "pipeline_id": pipeline_id,
            "update_id": update_id,
            "run_type": run_type,
            "agent_note": agent_note
        }
        
    except Exception as e:
        logger.error(f"Error triggering pipeline run: {e}")
        return {"error": str(e), "pipeline_id": pipeline_id}
