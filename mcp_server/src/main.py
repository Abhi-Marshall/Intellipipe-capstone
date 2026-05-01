import os
import json
import uuid
import asyncio
import time
from fastapi import FastAPI, Request
from starlette.responses import JSONResponse, StreamingResponse
import mcp.types as types
from mcp.server import Server
from mcp.server.sse import SseServerTransport
from src.tools.pipeline import get_pipeline_health, trigger_pipeline_run
from src.tools.data_quality import get_data_quality_report, get_table_lineage
from src.tools.metrics import get_hourly_metrics
from src.tools.ml import get_anomaly_prediction

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
                    "pipeline_id": {"type": "string", "description": "The ID of the DLT pipeline", "default": "b5b42690-b884-4945-8da6-74b19aa93553"}
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
            description="Triggers a DLT pipeline update. Can do incremental updates (default) or full refresh (wipe and recompute all data).",
            inputSchema={
                "type": "object",
                "properties": {
                    "pipeline_id": {"type": "string", "description": "The ID of the DLT pipeline", "default": "b5b42690-b884-4945-8da6-74b19aa93553"},
                    "full_refresh": {"type": "boolean", "description": "If true, performs a full refresh (wipes and recomputes all tables). If false (default), performs incremental update.", "default": False}
                }
            }
        ), 
        types.Tool(
            name="get_anomaly_prediction",
            description="Checks if the current system state is anomalous. Requires NO inputs. It automatically fetches the latest pipeline metrics and runs them against the ML model.",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        )
    ]

@mcp_server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    try:
        if name == "get_pipeline_health":
            result = get_pipeline_health(arguments.get("pipeline_id", "b5b42690-b884-4945-8da6-74b19aa93553"))
        elif name == "get_data_quality_report":
            result = get_data_quality_report(arguments.get("table_name", "capstone_project.capstone_schema.hourly_order_metrics"))
        elif name == "get_table_lineage":
            result = get_table_lineage(arguments.get("table_name", "capstone_project.capstone_schema.hourly_order_metrics"))
        elif name == "get_hourly_metrics":
            result = get_hourly_metrics(arguments.get("hours", 24))
        elif name == "trigger_pipeline_run":
            result = trigger_pipeline_run(
                arguments.get("pipeline_id", "b5b42690-b884-4945-8da6-74b19aa93553"),
                arguments.get("full_refresh", False)
            )
        elif name == "get_anomaly_prediction":
            # result = get_anomaly_prediction(arguments.get("endpoint_name", "anomaly_detector"), arguments["features"])
            # No arguments needed! The function fetches the data itself.
            result = get_anomaly_prediction()
        else:
            raise ValueError(f"Unknown tool: {name}")

        return [types.TextContent(type="text", text=json.dumps(result))]
    except Exception as e:
        return [types.TextContent(type="text", text=f"Error executing tool {name}: {str(e)}")]

# Health check endpoint
@app.get("/health")
def health_check():
    return {"status": "ok", "message": "MCP Server is running", "tools": 6}

# ──────────────────────────────────────────────────────────────────────────────
# MCP SSE Implementation  
# ──────────────────────────────────────────────────────────────────────────────

# Session management - keep sessions alive for 1 hour
sse_sessions = {}
SESSION_TIMEOUT = 3600  # 1 hour in seconds

def cleanup_expired_sessions():
    """Remove sessions older than SESSION_TIMEOUT."""
    current_time = time.time()
    expired = [sid for sid, data in sse_sessions.items() 
               if current_time - data['created'] > SESSION_TIMEOUT]
    for sid in expired:
        sse_sessions.pop(sid, None)

def get_public_base_url(request: Request) -> str:
    """
    Get the public-facing base URL for the app.
    Handles Databricks Apps proxy headers correctly.
    """
    # Try X-Forwarded-Host first (set by Databricks Apps proxy)
    forwarded_host = request.headers.get("x-forwarded-host")
    if forwarded_host:
        # X-Forwarded-Proto tells us if it's http or https
        forwarded_proto = request.headers.get("x-forwarded-proto", "https")
        return f"{forwarded_proto}://{forwarded_host}"
    
    # Fallback to Host header
    host = request.headers.get("host")
    if host:
        # Assume https for Databricks Apps
        return f"https://{host}"
    
    # Last resort: use request.base_url but this might be localhost
    return str(request.base_url).rstrip('/')

@app.get("/sse")
async def handle_sse(request: Request):
    """
    MCP SSE Endpoint - Emits 'event: endpoint' with session URL.
    
    Per MCP spec:
      1. Client does GET /sse
      2. Server emits:  event: endpoint\ndata: <session_url>\n\n
      3. Client extracts URL and POSTs to /messages
    """
    # Cleanup old sessions
    cleanup_expired_sessions()
    
    # Generate unique session ID
    session_id = str(uuid.uuid4())
    
    # Construct session URL using public-facing base URL
    base_url = get_public_base_url(request)
    session_url = f"{base_url}/messages?session_id={session_id}"
    
    # Store session - keep it alive even after SSE connection closes
    queue = asyncio.Queue()
    sse_sessions[session_id] = {
        "created": time.time(),
        "active": True,
        "queue": queue
    }
    
    async def event_generator():
        # Emit the endpoint event as per MCP protocol
        yield f"event: endpoint\n"
        yield f"data: {session_url}\n\n"
        
        # Keep connection alive with heartbeats and messages
        try:
            while True:
                try:
                    # Wait for a message with a timeout to send heartbeats
                    message = await asyncio.wait_for(queue.get(), timeout=30.0)
                    yield f"event: message\n"
                    yield f"data: {message}\n\n"
                except asyncio.TimeoutError:
                    yield f": heartbeat\n\n"
        except asyncio.CancelledError:
            # Don't delete session immediately - it might reconnect
            pass

    return StreamingResponse(
    event_generator(),
    media_type="text/event-stream",
    headers={
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",  # <--- CRITICAL for Databricks/Nginx
    }
)

@app.post("/messages")
async def handle_messages(request: Request):
    """
    Handle JSON-RPC POST messages from MCP clients.
    Implements JSON-RPC 2.0 protocol.
    """
    session_id = request.query_params.get("session_id")
    
    # Cleanup old sessions
    cleanup_expired_sessions()
    
    # Validate session
    if not session_id or session_id not in sse_sessions:
        return JSONResponse(
            status_code=404,
            content={"error": "Could not find session"}
        )
    
    # Update session last activity
    sse_sessions[session_id]["last_activity"] = time.time()
    
    # Parse JSON-RPC request
    try:
        body = await request.json()
    except Exception as e:
        return JSONResponse(
            status_code=400,
            content={
                "jsonrpc": "2.0",
                "id": None,
                "error": {
                    "code": -32700,
                    "message": "Parse error",
                    "data": str(e)
                }
            }
        )
    
    # Extract JSON-RPC components
    method = body.get("method")
    params = body.get("params", {})
    request_id = body.get("id")
    
    # Handle different MCP methods
    try:
        # changed from here :
        if method == "initialize":
            result = {
                "protocolVersion": "2024-11-05",
                "capabilities": {
                    "tools": {}
                },
                "serverInfo": {
                    "name": "intellipipe-mcp-server",
                    "version": "1.0.0"
                }
            }
            
        elif method == "notifications/initialized":
            # Notifications do not require a JSON-RPC response object
            return JSONResponse(status_code=202, content="Accepted")
            
        elif method == "ping":
            result = {}
        
        # changed till here :
        elif method == "tools/list":
            # List all available tools
            tools = await list_tools()
            result = {
                "tools": [
                    {
                        "name": tool.name,
                        "description": tool.description,
                        "inputSchema": tool.inputSchema
                    }
                    for tool in tools
                ]
            }
            
        elif method == "tools/call":
            # Call a specific tool
            tool_name = params.get("name")
            tool_arguments = params.get("arguments", {})
            
            if not tool_name:
                raise ValueError("Missing tool name")
            
            # Execute the tool
            tool_result = await call_tool(tool_name, tool_arguments)
            
            # Format result
            result = {
                "content": [
                    {
                        "type": content.type,
                        "text": content.text
                    }
                    for content in tool_result
                ]
            }
            
        else:
            # Unknown method
            raise ValueError(f"Unknown method: {method}")
    
        # Return the result as a proper JSON-RPC response
        return JSONResponse({
            "jsonrpc": "2.0",
            "id": body.get("id"),
            "result": result
        })
        
    except Exception as e:
        # Return JSON-RPC error
        return JSONResponse({
            "jsonrpc": "2.0",
            "id": body.get("id"),
            "error": {
                "code": -32603,
                "message": str(e)
            }
        }, status_code=500)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)