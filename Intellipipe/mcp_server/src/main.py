import os
from fastapi import FastAPI, Request
from starlette.responses import JSONResponse
import mcp.types as types
from mcp.server import Server
from mcp.server.sse import SseServerTransport
from src.tools.pipeline import get_pipeline_health, trigger_pipeline_run
from src.tools.data_quality import get_data_quality_report, get_table_lineage
from src.tools.metrics import get_hourly_metrics
from src.tools.ml import get_anomaly_prediction
import json

# Setup FastAPI App
app = FastAPI(title="Databricks MCP Server", description="MCP Server exposing Databricks pipeline state and operations.")

# Setup MCP Server
mcp_server = Server("databricks-mcp-server")

# Define tools
@mcp_server.list_tools()
async def list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="get_pipeline_health",
            description="Returns DLT pipeline run status, last successful run time, row counts per layer.",
            inputSchema={
                "type": "object",
                "properties": {
                    "pipeline_id": {"type": "string", "description": "The ID of the DLT pipeline", "default": "f6590e60-90b1-4bd8-8065-25e1a173a659"}
                }
            }
        ),
        types.Tool(
            name="get_data_quality_report",
            description="Queries Delta table constraint violations and returns a structured quality summary.",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "The full name of the Delta table (catalog.schema.table)", "default": "capstone_project.capstone_schema.hourly_order_metrics"}
                }
            }
        ),
        types.Tool(
            name="get_table_lineage",
            description="Calls Unity Catalog lineage APIs to return upstream/downstream table dependencies.",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "The full name of the table", "default": "capstone_project.capstone_schema.hourly_order_metrics"}
                },
                "required": ["table_name"]
            }
        ),
        types.Tool(
            name="get_hourly_metrics",
            description="Returns the last N hours of capstone_project.capstone_schema.hourly_order_metrics as JSON.",
            inputSchema={
                "type": "object",
                "properties": {
                    "hours": {"type": "integer", "description": "Number of hours to retrieve", "default": 24}
                }
            }
        ),
        types.Tool(
            name="trigger_pipeline_run",
            description="Triggers a DLT pipeline via Databricks Jobs API.",
            inputSchema={
                "type": "object",
                "properties": {
                    "pipeline_id": {"type": "string", "description": "The ID of the DLT pipeline", "default": "f6590e60-90b1-4bd8-8065-25e1a173a659"}
                }
            }
        ),
        types.Tool(
            name="get_anomaly_prediction",
            description="Calls the deployed ML model endpoint to return next-hour anomaly probability.",
            inputSchema={
                "type": "object",
                "properties": {
                    "endpoint_name": {"type": "string", "description": "Name of the ML model serving endpoint", "default": "anomaly_detector"},
                    "features": {"type": "object", "description": "JSON object containing feature values for prediction"}
                },
                "required": ["features"]
            }
        )
    ]

@mcp_server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    try:
        if name == "get_pipeline_health":
            result = get_pipeline_health(arguments.get("pipeline_id", "f6590e60-90b1-4bd8-8065-25e1a173a659"))
        elif name == "get_data_quality_report":
            result = get_data_quality_report(arguments.get("table_name", "capstone_project.capstone_schema.hourly_order_metrics"))
        elif name == "get_table_lineage":
            result = get_table_lineage(arguments.get("table_name", "capstone_project.capstone_schema.hourly_order_metrics"))
        elif name == "get_hourly_metrics":
            result = get_hourly_metrics(arguments.get("hours", 24))
        elif name == "trigger_pipeline_run":
            result = trigger_pipeline_run(arguments.get("pipeline_id", "f6590e60-90b1-4bd8-8065-25e1a173a659"))
        elif name == "get_anomaly_prediction":
            result = get_anomaly_prediction(arguments.get("endpoint_name", "anomaly_detector"), arguments["features"])
        else:
            raise ValueError(f"Unknown tool: {name}")

        return [types.TextContent(type="text", text=json.dumps(result))]
    except Exception as e:
        return [types.TextContent(type="text", text=f"Error executing tool {name}: {str(e)}")]

# Simple SSE Transport endpoint for FastAPI
# In production with modern mcp SDK, this uses starlette app lifecycle.
# This code provides the required bridging for Databricks apps.

# A global transport can be tricky, typically we'd use something provided by `mcp.server.fastapi` like `app.mount("/mcp", create_fastapi_server(mcp_server))`
# Let's add basic routes to check server status since Databricks App requires a health check usually
@app.get("/health")
def health_check():
    return {"status": "ok", "message": "MCP Server is running"}

# Integrating the MCP server using SSE
try:
    from mcp.server.sse import SseServerTransport
    # Implementation depends on exact SDK version, keeping a placeholder or using provided fastAPI utils if available
    # Often it looks like this for manual wiring:
    sse = SseServerTransport("/messages")
    
    @app.get("/sse")
    async def handle_sse(request: Request):
        async with sse.connect_sse(request.scope, request.receive, request._send) as streams:
            await mcp_server.run(streams[0], streams[1], mcp_server.create_initialization_options())
            
except ImportError:
    pass

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
