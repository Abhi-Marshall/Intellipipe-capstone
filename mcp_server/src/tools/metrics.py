from databricks.sdk import WorkspaceClient
from typing import Dict, Any, List
import logging

logger = logging.getLogger(__name__)

WAREHOUSE_ID = "f0007797a3f24edc"

def get_hourly_metrics(hours: int = 24) -> List[Dict[str, Any]]:
    """
    Returns the last N hours of capstone_project.capstone_schema.hourly_order_metrics as JSON.
    """
    try:
        w = WorkspaceClient()
        
        # 🛑 FIX: Changed "hour" to "hour_start" to match your Gold table schema!
        query = f"""
        SELECT *
        FROM capstone_project.capstone_schema.hourly_order_metrics
        ORDER BY hour_start DESC
        LIMIT {hours}
        """
        
        res = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=query,
            wait_timeout="30s" 
        )
        
        metrics = []
        if res.result and res.result.data_array:
            columns = [col.name for col in res.manifest.schema.columns]
            for row in res.result.data_array:
                metrics.append(dict(zip(columns, row)))
                
        # If the table is actually empty, add a helpful note for the AI Agent
        if len(metrics) == 0:
            return [{"agent_note": f"The query executed successfully, but the table returned 0 rows. The hourly_order_metrics table might be empty."}]
            
        return metrics
        
    except Exception as e:
        logger.error(f"Error getting hourly metrics: {e}")
        # Explicitly return the SQL error so the Agent (and you) can see it!
        return [{"error": str(e), "sql_executed": query}]