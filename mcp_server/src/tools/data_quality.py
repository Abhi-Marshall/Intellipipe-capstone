from databricks.sdk import WorkspaceClient
from typing import Dict, Any, List
import logging
import json
from src.tools.email_tool import send_status_email

logger = logging.getLogger(__name__)

WAREHOUSE_ID = "f0007797a3f24edc"
DEFAULT_PIPELINE_ID = "b5b42690-b884-4945-8da6-74b19aa93553"

def get_data_quality_report(table_name: str = "capstone_project.capstone_schema.clean_orders") -> Dict[str, Any]:
    try:
        w = WorkspaceClient()

        # ── 1. Unified CTE Query: Live Scan + Delta History ──
        unified_query = f"""
        WITH latest_operation AS (
            SELECT
                timestamp AS last_run_timestamp,
                operation AS last_operation,
                operationMetrics['numOutputRows']::bigint AS rows_passed,
                operationMetrics['numDeletedRows']::bigint AS rows_dropped
            FROM (DESCRIBE HISTORY {table_name})
            WHERE operation IN ('WRITE', 'MERGE', 'CREATE TABLE AS SELECT', 'REPLACE TABLE AS SELECT', 'STREAMING UPDATE')
            ORDER BY timestamp DESC
            LIMIT 1
        ),
        quality_scan AS (
            SELECT
                COUNT(*) AS total_rows,
                COUNT(CASE WHEN order_id IS NULL THEN 1 END) AS null_order_ids,
                COUNT(CASE WHEN unit_price < 0.0 OR unit_price IS NULL THEN 1 END) AS invalid_unit_price,
                COUNT(CASE WHEN quantity <= 0 OR quantity IS NULL THEN 1 END) AS invalid_quantity,
                COUNT(CASE WHEN customer_id IS NULL THEN 1 END) AS null_customer_ids,
                COUNT(CASE WHEN discount < 0 THEN 1 END) AS invalid_discount
            FROM {table_name}
        )
        SELECT 
            qs.*,
            lo.last_run_timestamp,
            lo.last_operation,
            lo.rows_passed,
            lo.rows_dropped
        FROM quality_scan qs
        CROSS JOIN latest_operation lo
        """

        res = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=unified_query,
            wait_timeout="30s"
        )

        # ── 2. Parse the Unified Results ──
        total_rows = 0
        rows_passed = 0
        rows_dropped = 0
        last_run_ts = None
        last_op = None
        live_violations = {}

        if res.result and res.result.data_array:
            cols = [c.name for c in res.manifest.schema.columns]
            row_dict = dict(zip(cols, res.result.data_array[0]))
            
            # Extract basic metrics
            total_rows   = int(row_dict.get("total_rows") or 0)
            rows_passed  = int(row_dict.get("rows_passed") or 0)
            rows_dropped = int(row_dict.get("rows_dropped") or 0)
            last_run_ts  = row_dict.get("last_run_timestamp")
            last_op      = row_dict.get("last_operation")
            
            # Extract violations dynamically
            violation_keys = ["null_order_ids", "invalid_unit_price", "invalid_quantity", "null_customer_ids", "invalid_discount"]
            for k in violation_keys:
                live_violations[k] = int(row_dict.get(k) or 0)

        total_processed   = rows_passed + rows_dropped
        pass_rate         = round((rows_passed / total_processed * 100), 2) if total_processed > 0 else 0.0
        total_violations  = sum(live_violations.values())
        quality_status    = "PASS" if total_violations == 0 else "FAIL"

        # ── 3. Build the final response dictionary ──
        report_data = {
            "pipeline_id":   DEFAULT_PIPELINE_ID,
            "target_table":  table_name,
            "status":        "success",

            "last_run_summary": {
                "timestamp":        last_run_ts,
                "operation":        last_op,
                "rows_passed_dlt":  rows_passed,   
                "rows_dropped_dlt": rows_dropped,  
                "total_processed":  total_processed,
                "pass_rate_pct":    pass_rate,
            },

            "live_quality_check": {
                "total_rows":       total_rows,
                "violations":       live_violations,
                "total_violations": total_violations,
                "quality_status":   quality_status,
                "expectations_applied": [
                    "order_id IS NOT NULL",
                    "unit_price >= 0.0",
                    "quantity > 0"
                ]
            }
        }

        # ─── 4. NEW EMAIL TRIGGER LOGIC STARTS HERE ──────────────────────────────
        try:
            status_color = "green" if quality_status == "PASS" else "red"
            
            email_body = f"""
            <h2>Data Quality Report</h2>
            <p><b>Target Table:</b> {table_name}</p>
            <p><b>Quality Status:</b> <span style="color: {status_color}; font-weight: bold;">{quality_status}</span></p>
            <p><b>Pass Rate (Last Run):</b> {pass_rate}%</p>
            <p><b>Total Live Violations:</b> {total_violations}</p>
            
            <h3>Live Quality Violations</h3>
            <pre style="background-color: #f4f4f4; padding: 10px; border-radius: 5px;">
{json.dumps(live_violations, indent=2)}
            </pre>
            
            <h3>Last Run Summary</h3>
            <pre style="background-color: #f4f4f4; padding: 10px; border-radius: 5px;">
{json.dumps(report_data['last_run_summary'], indent=2)}
            </pre>
            """
            
            # Fire off the email silently in the background
            send_status_email(
                subject=f"Data Quality Alert: {table_name} is {quality_status}",
                body=email_body,
                to_email="capstoneproject196@gmail.com"
            )
            logger.info(f"Successfully triggered auto-email for data quality on {table_name}.")
        except Exception as email_error:
            logger.error(f"Failed to auto-send data quality email: {email_error}")
        # ─── NEW EMAIL TRIGGER LOGIC ENDS HERE ────────────────────────────────

        return report_data

    except Exception as e:
        logger.error(f"Error getting data quality report: {e}")
        return {"error": str(e), "table_name": table_name}


def get_table_lineage(table_name: str) -> Dict[str, Any]:
    """
    Lineage tool optimized for Databricks Community Edition.
    Uses a Metadata Map since System Tables are unavailable in Free Tier.
    """
    try:
        # 1. Clean the input
        t_name = table_name.lower().split(".")[-1].strip()

        # 2. Define your Medallion Architecture Map
        # This acts as your 'Truth' since system tables are disabled.
        # 2. Define your Medallion Architecture Map
        # Reflecting your actual DLT graph where Silver is the nexus.
        project_lineage = {
            "bronze_orders": {
                "upstream": [], 
                "downstream": ["clean_orders"]
            },
            "clean_orders": {
                "upstream": ["bronze_orders"], 
                "downstream": [
                    "hourly_order_metrics", 
                    "customer_360", 
                    "category_daily_metrics", 
                    "category_summary", 
                    "daily_business_metrics", 
                    "product_daily_metrics",
                    "product_performance"
                ]
            },
            # --- GOLD LAYER TABLES (All sourced from clean_orders) ---
            "hourly_order_metrics": {
                "upstream": ["clean_orders"], 
                "downstream": []
            },
            "customer_360": {
                "upstream": ["clean_orders"], 
                "downstream": []
            },
            "category_daily_metrics": {
                "upstream": ["clean_orders"], 
                "downstream": []
            },
            "category_summary": {
                "upstream": ["clean_orders"], 
                "downstream": []
            },
            "daily_business_metrics": {
                "upstream": ["clean_orders"], 
                "downstream": []
            },
            "product_daily_metrics": {
                "upstream": ["clean_orders"], 
                "downstream": []
            }, 
            "product_performance": {
                "upstream": ["clean_orders"], 
                "downstream": []
            }, 
        }

        # 3. Fetch from map
        lineage_info = project_lineage.get(t_name, {"upstream": [], "downstream": []})

        return {
            "table_name": table_name,
            "upstream": lineage_info["upstream"],
            "downstream": lineage_info["downstream"],
            "source": "internal_metadata",
            "agent_note": "Lineage retrieved via internal project metadata (Community Edition Mode)."
        }

    except Exception as e:
        return {"error": str(e), "table_name": table_name}