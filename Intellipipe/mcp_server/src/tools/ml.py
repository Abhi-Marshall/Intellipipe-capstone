from databricks.sdk import WorkspaceClient
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

def get_anomaly_prediction(endpoint_name: str, features: Dict[str, Any]) -> Dict[str, Any]:
    """
    Calls the deployed ML model endpoint to return next-hour anomaly probability.
    """
    try:
        w = WorkspaceClient()
        
        # Querying the model endpoint
        response = w.serving_endpoints.query(
            name=endpoint_name,
            dataframe_records=[features]
        )
        
        # `response.predictions` contains the prediction results
        predictions = response.predictions if hasattr(response, 'predictions') else response.get('predictions', [])
            
        return {
            "endpoint_name": endpoint_name,
            "anomaly_probability": predictions[0] if isinstance(predictions, list) and len(predictions) > 0 else predictions,
            "status": "success"
        }
    except Exception as e:
        logger.error(f"Error getting anomaly prediction: {e}")
        return {
            "endpoint_name": endpoint_name,
            "error": str(e),
            "status": "failed"
        }
