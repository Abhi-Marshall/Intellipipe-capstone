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
        
        query = f"""
        SELECT *
        FROM capstone_project.capstone_schema.hourly_order_metrics
        ORDER BY hour DESC
        LIMIT {hours}
        """
        
        res = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=query,
            wait_timeout="120s"
        )
        
        metrics = []
        if res.result and res.result.data_array:
            columns = [col.name for col in res.manifest.schema.columns]
            for row in res.result.data_array:
                metrics.append(dict(zip(columns, row)))
                
        return metrics
    except Exception as e:
        logger.error(f"Error getting hourly metrics: {e}")
        return [{"error": str(e)}]
