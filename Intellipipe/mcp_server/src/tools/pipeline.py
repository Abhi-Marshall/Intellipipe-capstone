from databricks.sdk import WorkspaceClient
from typing import Dict, Any, List
import logging

logger = logging.getLogger(__name__)

WAREHOUSE_ID = "f0007797a3f24edc"
DEFAULT_PIPELINE_ID = "b5b42690-b884-4945-8da6-74b19aa93553"

def get_pipeline_health(pipeline_id: str = DEFAULT_PIPELINE_ID) -> Dict[str, Any]:
    """
    Returns DLT pipeline run status, last successful run time, row counts per layer.
    """
    try:
        w = WorkspaceClient()
        pipeline_info = w.pipelines.get(pipeline_id)
        
        # Query event log to get actual row counts per layer
        query = f"""
        SELECT 
            timestamp,
            details:flow_progress.metrics.num_output_rows as output_rows,
            details:flow_progress.data_quality.dropped_records as dropped_rows,
            details:flow_definition.output_dataset as dataset
        FROM event_log('{pipeline_id}')
        WHERE event_type = 'flow_progress' 
          AND details:flow_progress.metrics.num_output_rows IS NOT NULL
        ORDER BY timestamp DESC
        LIMIT 20
        """
        
        res = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=query,
            wait_timeout="120s"
        )
        
        row_counts = []
        if res.result and res.result.data_array:
            columns = [col.name for col in res.manifest.schema.columns]
            for row in res.result.data_array:
                row_counts.append(dict(zip(columns, row)))
        
        return {
            "pipeline_id": pipeline_id,
            "name": pipeline_info.name,
            "state": pipeline_info.state.value if pipeline_info.state else "UNKNOWN",
            "health": pipeline_info.health.value if pipeline_info.health else "UNKNOWN",
            "last_updated": str(pipeline_info.last_modified) if pipeline_info.last_modified else None,
            "row_counts_per_layer": row_counts
        }
    except Exception as e:
        logger.error(f"Error getting pipeline health: {e}")
        return {"error": str(e), "pipeline_id": pipeline_id}

def trigger_pipeline_run(pipeline_id: str = DEFAULT_PIPELINE_ID) -> Dict[str, Any]:
    """
    Triggers a DLT pipeline via Databricks Jobs API.
    """
    try:
        w = WorkspaceClient()
        update_response = w.pipelines.start_update(pipeline_id, full_refresh=False)
        
        return {
            "status": "triggered",
            "pipeline_id": pipeline_id,
            "update_id": update_response.update_id
        }
    except Exception as e:
        logger.error(f"Error triggering pipeline run: {e}")
        return {"error": str(e), "pipeline_id": pipeline_id}
