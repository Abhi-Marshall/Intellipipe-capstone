from databricks.sdk import WorkspaceClient
from typing import Dict, Any, List
import logging

logger = logging.getLogger(__name__)

def get_hourly_metrics(hours: int = 24) -> List[Dict[str, Any]]:
    """
    Returns the last N hours of gold.hourly_order_metrics as JSON.
    """
    try:
        w = WorkspaceClient()
        
        # Typically this would involve a SQL execution using Databricks SQL 
        # w.statement_execution.execute_statement(warehouse_id=..., statement="SELECT * FROM gold.hourly_order_metrics ORDER BY hour DESC LIMIT X")
        
        # Returning mocked data for the server layout structure
        metrics = []
        for i in range(hours):
            metrics.append({
                "hour": f"T-{i}",
                "total_orders": 100 + i * 10,
                "revenue": 5000.0 + i * 100,
                "anomalous": False
            })
            
        return metrics
    except Exception as e:
        logger.error(f"Error getting hourly metrics: {e}")
        return [{"error": str(e)}]
