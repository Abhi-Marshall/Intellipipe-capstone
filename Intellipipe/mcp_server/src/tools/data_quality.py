from databricks.sdk import WorkspaceClient
from typing import Dict, Any, List
import logging

logger = logging.getLogger(__name__)

WAREHOUSE_ID = "f0007797a3f24edc"
DEFAULT_PIPELINE_ID = "f6590e60-90b1-4bd8-8065-25e1a173a659"

def get_data_quality_report(table_name: str = None) -> Dict[str, Any]:
    """
    Queries Delta table constraint violations and returns a structured quality summary.
    Using DLT event log for data quality expectations.
    """
    try:
        w = WorkspaceClient()
        
        query = f"""
        SELECT 
            timestamp,
            details:flow_progress.data_quality.expectations as expectations
        FROM event_log('{DEFAULT_PIPELINE_ID}')
        WHERE event_type = 'flow_progress' 
        AND details:flow_progress.data_quality.expectations IS NOT NULL
        ORDER BY timestamp DESC
        LIMIT 10
        """
        
        res = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=query,
            wait_timeout="120s"
        )
        
        violations = []
        if res.result and res.result.data_array:
            columns = [col.name for col in res.manifest.schema.columns]
            for row in res.result.data_array:
                violations.append(dict(zip(columns, row)))
                
        return {
            "pipeline_id": DEFAULT_PIPELINE_ID,
            "status": "Success",
            "recent_data_quality_metrics": violations
        }
    except Exception as e:
        logger.error(f"Error getting data quality report: {e}")
        return {"error": str(e)}

def get_table_lineage(table_name: str) -> Dict[str, Any]:
    """
    Calls Unity Catalog lineage APIs (via system tables) to return upstream/downstream dependencies.
    """
    try:
        w = WorkspaceClient()
        
        query = f"""
        SELECT 
            source_table_catalog, source_table_schema, source_table_name,
            target_table_catalog, target_table_schema, target_table_name,
            lineage_type
        FROM system.access.table_lineage
        WHERE 
            (target_table_catalog || '.' || target_table_schema || '.' || target_table_name) = '{table_name}'
            OR
            (source_table_catalog || '.' || source_table_schema || '.' || source_table_name) = '{table_name}'
        ORDER BY event_time DESC
        LIMIT 50
        """
        
        res = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=query,
            wait_timeout="120s"
        )
        
        lineage_records = []
        if res.result and res.result.data_array:
            columns = [col.name for col in res.manifest.schema.columns]
            for row in res.result.data_array:
                lineage_records.append(dict(zip(columns, row)))
                
        # Group by upstream/downstream
        upstream = []
        downstream = []
        for record in lineage_records:
            source = f"{record['source_table_catalog']}.{record['source_table_schema']}.{record['source_table_name']}"
            target = f"{record['target_table_catalog']}.{record['target_table_schema']}.{record['target_table_name']}"
            if target == table_name and source not in upstream:
                upstream.append(source)
            if source == table_name and target not in downstream:
                downstream.append(target)
                
        return {
            "table_name": table_name,
            "upstream": upstream,
            "downstream": downstream,
            "raw_lineage": lineage_records
        }
    except Exception as e:
        logger.error(f"Error getting table lineage: {e}")
        return {"error": str(e), "table_name": table_name}
