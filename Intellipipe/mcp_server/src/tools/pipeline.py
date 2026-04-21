from databricks.sdk import WorkspaceClient
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

def get_pipeline_health(pipeline_id: str) -> Dict[str, Any]:
    """
    Returns DLT pipeline run status, last successful run time, row counts per layer.
    """
    try:
        w = WorkspaceClient()
        pipeline_info = w.pipelines.get(pipeline_id)
        
        # In a real scenario, you would fetch pipeline events to get row counts per layer.
        # This is a simplified version returning basic health info.
        
        return {
            "pipeline_id": pipeline_id,
            "name": pipeline_info.name,
            "state": pipeline_info.state.value if pipeline_info.state else "UNKNOWN",
            "cluster_id": pipeline_info.cluster_id,
            "health": pipeline_info.health.value if pipeline_info.health else "UNKNOWN",
            "last_updated": str(pipeline_info.last_modified) if pipeline_info.last_modified else None
        }
    except Exception as e:
        logger.error(f"Error getting pipeline health: {e}")
        return {"error": str(e), "pipeline_id": pipeline_id}

def trigger_pipeline_run(pipeline_id: str) -> Dict[str, Any]:
    """
    Triggers a DLT pipeline via Databricks Jobs API (or pipelines start update).
    """
    try:
        w = WorkspaceClient()
        # For DLT, you typically start an update
        update_response = w.pipelines.start_update(pipeline_id)
        
        return {
            "status": "triggered",
            "pipeline_id": pipeline_id,
            "update_id": update_response.update_id
        }
    except Exception as e:
        logger.error(f"Error triggering pipeline run: {e}")
        return {"error": str(e), "pipeline_id": pipeline_id}
