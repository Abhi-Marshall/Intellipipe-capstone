"""
agent_integrated.py
────────────────────────────
IntelliPipe Supervisor Agent — Integrated Version

Combines:
  1. Fixed MCP tool integration (from agent.py)
  2. Genie Space integration (from supervisor_agent_model3.py)
  3. Keyword-based routing logic

Routing Logic
─────────────
  MCP   → pipeline / data-engineering questions  →  MCP server via UC Connection
  Genie → business / analytics questions         →  Genie Space REST API
  Direct→ everything else                        →  Polite clarification message
"""

import ast
import json
import os
import time
from typing import Any

import pandas as pd
import requests
import mlflow
from mlflow.pyfunc import PythonModel, PythonModelContext
from databricks_langchain import ChatDatabricks

# ══════════════════════════════════════════════════════════════════════════════
# GENIE CLIENT
# ══════════════════════════════════════════════════════════════════════════════

class GenieClient:
    """
    Thin wrapper around the Databricks Genie Space REST API.
    Handles conversation lifecycle: start → poll → fetch rows.
    """

    def __init__(self, workspace_url: str, space_id: str, token: str):
        self.base    = workspace_url.rstrip("/")
        self.space   = space_id
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type":  "application/json",
        }

    def _url(self, path: str) -> str:
        return f"{self.base}/api/2.0/genie/spaces/{self.space}{path}"

    def start_conversation(self, question: str) -> tuple[str, str]:
        resp = requests.post(
            self._url("/start-conversation"),
            headers=self.headers,
            json={"content": question},
            timeout=30,
        )
        resp.raise_for_status()
        d = resp.json()
        return d["conversation_id"], d["message_id"]

    def poll_message(self, conv_id: str, msg_id: str, max_sec: int = 120) -> dict:
        url   = self._url(f"/conversations/{conv_id}/messages/{msg_id}")
        start = time.time()
        while True:
            resp = requests.get(url, headers=self.headers, timeout=30)
            resp.raise_for_status()
            data   = resp.json()
            status = data.get("status", "")
            if status == "COMPLETED":
                return data
            if status in ("FAILED", "CANCELLED", "QUERY_RESULT_EXPIRED"):
                raise RuntimeError(f"Genie message ended with status: {status}")
            if time.time() - start > max_sec:
                raise TimeoutError("Genie polling timed out after 120 s")
            time.sleep(3)

    def fetch_query_result(self, conv_id: str, msg_id: str) -> dict:
        url  = self._url(f"/conversations/{conv_id}/messages/{msg_id}/query-result")
        resp = requests.get(url, headers=self.headers, timeout=30)
        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def _extract_val(cell: Any) -> str:
        if isinstance(cell, dict):
            for k in ("str", "i64", "f64", "bool", "date", "timestamp"):
                if k in cell:
                    return str(cell[k])
            return str(cell)
        return str(cell) if cell is not None else "NULL"

    @staticmethod
    def _rows_to_markdown(columns: list, rows: list, max_rows: int = 50) -> str:
        if not rows:
            return "_No rows returned._"
        col_names = [c.get("name", f"col{i}") for i, c in enumerate(columns)]
        header = "| " + " | ".join(col_names) + " |"
        sep    = "| " + " | ".join(["---"] * len(col_names)) + " |"
        lines  = [header, sep]
        for row in rows[:max_rows]:
            vals = [GenieClient._extract_val(v) for v in row.get("values", [])]
            lines.append("| " + " | ".join(vals) + " |")
        if len(rows) > max_rows:
            lines.append(f"_... {len(rows) - max_rows} more rows not shown_")
        return "\n".join(lines)

    def ask(self, question: str) -> dict:
        """
        Ask a natural-language question against the Genie Space.

        Returns
        -------
        dict with keys:
            nl_answer       – natural-language answer from Genie
            sql_query       – SQL that was executed
            result_markdown – tabular results as a Markdown table
            row_count       – number of rows returned
        """
        conv_id, msg_id = self.start_conversation(question)
        message         = self.poll_message(conv_id, msg_id)

        nl_answer = ""
        sql_query = ""
        result_md = ""
        row_count = 0

        for att in message.get("attachments", []):
            if "text"  in att:
                nl_answer = att["text"].get("content", "")
            if "query" in att:
                sql_query = att["query"].get("query", "")

        if sql_query:
            try:
                qr        = self.fetch_query_result(conv_id, msg_id)
                sr        = qr.get("statement_response", {})
                columns   = sr.get("manifest", {}).get("schema", {}).get("columns", [])
                rows      = sr.get("result", {}).get("data_typed_array", [])
                row_count = len(rows)
                result_md = self._rows_to_markdown(columns, rows)
            except Exception as exc:
                result_md = f"_Could not fetch rows: {exc}_"

        return {
            "nl_answer":       nl_answer,
            "sql_query":       sql_query,
            "result_markdown": result_md,
            "row_count":       row_count,
        }

# ══════════════════════════════════════════════════════════════════════════════
# MCP TOOL INTEGRATION (FIXED VERSION FROM agent.py)
# ══════════════════════════════════════════════════════════════════════════════

def get_auth_token(client_id: str, client_secret: str, host: str):
    """Get authentication token for MCP server calls using service principal."""
    try:
        # Use OAuth M2M flow to get token
        token_url = f"https://{host}/oidc/v1/token"
        
        response = requests.post(
            token_url,
            data={
                "grant_type": "client_credentials",
                "scope": "all-apis"
            },
            auth=(client_id, client_secret),
            timeout=10
        )
        
        if response.status_code == 200:
            token_data = response.json()
            return token_data["access_token"]
        else:
            print(f"Failed to get token: {response.status_code} - {response.text}")
            return None
            
    except Exception as e:
        print(f"Error getting auth token: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def get_mcp_connection_url(connection_name: str, client_id: str, client_secret: str, host: str):
    """Get MCP server URL from Unity Catalog connection."""
    try:
        from databricks.sdk import WorkspaceClient
        
        # Initialize with credentials
        w = WorkspaceClient(
            host=f"https://{host}",
            client_id=client_id,
            client_secret=client_secret
        )
        
        # Get the UC connection (FIXED: using name= instead of name_arg=)
        connection = w.connections.get(name=connection_name)
        
        # Extract URL from connection options
        mcp_url = connection.options.get("host") or connection.options.get("url")
        
        if mcp_url:
            print(f"✅ Found MCP server URL: {mcp_url}")
            return mcp_url
        else:
            print(f"No URL found in connection options: {connection.options}")
            return None
            
    except Exception as e:
        print(f"Error getting MCP connection: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def fetch_mcp_tools(mcp_server_url: str, auth_token: str):
    """
    Fetch available tools from the MCP server via Unity Catalog connection.
    Converts them to OpenAI function format for LangChain.
    """
    try:
        headers = {
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/json"
        }
        
        # Establish SSE connection to get session URL
        print(f"🔌 Connecting to MCP server: {mcp_server_url}/sse")
        sse_response = requests.get(
            f"{mcp_server_url}/sse",
            headers=headers,
            stream=True,
            timeout=10
        )
        
        if sse_response.status_code != 200:
            print(f"SSE connection failed: {sse_response.status_code} - {sse_response.text}")
            return []
        
        # Parse SSE response to get session URL
        session_url = None
        for line in sse_response.iter_lines():
            if line:
                decoded = line.decode('utf-8')
                if decoded.startswith('data: '):
                    session_url = decoded[6:].strip()
                    break
        
        sse_response.close()
        
        if not session_url:
            print("Could not get session URL from SSE endpoint")
            return []
        
        print(f"✅ Got session URL: {session_url}")
        
        # Send tools/list request via JSON-RPC
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/list",
            "params": {}
        }
        
        print(f"📡 Requesting tools list...")
        rpc_response = requests.post(
            session_url,
            headers=headers,
            json=payload,
            timeout=30
        )
        
        if rpc_response.status_code != 200:
            print(f"Tools list request failed: {rpc_response.status_code} - {rpc_response.text}")
            return []
        
        rpc_data = rpc_response.json()
        
        if "result" not in rpc_data or "tools" not in rpc_data["result"]:
            print(f"Invalid MCP response format: {rpc_data}")
            return []
        
        # Convert MCP tools to OpenAI function format
        openai_tools = []
        for tool in rpc_data["result"]["tools"]:
            openai_tool = {
                "type": "function",
                "function": {
                    "name": tool["name"],
                    "description": tool.get("description", ""),
                    "parameters": tool.get("inputSchema", {
                        "type": "object",
                        "properties": {},
                        "required": []
                    })
                }
            }
            openai_tools.append(openai_tool)
        
        print(f"✅ Loaded {len(openai_tools)} MCP tools: {[t['function']['name'] for t in openai_tools]}")
        return openai_tools
        
    except Exception as e:
        print(f"Failed to fetch MCP tools: {str(e)}")
        import traceback
        traceback.print_exc()
        return []

def call_mcp_tool(tool_name: str, arguments: dict, mcp_server_url: str, auth_token: str):
    """
    Execute an MCP tool via the Unity Catalog connection.
    """
    try:
        headers = {
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/json"
        }
        
        # Establish SSE session
        sse_response = requests.get(
            f"{mcp_server_url}/sse",
            headers=headers,
            stream=True,
            timeout=10
        )
        
        session_url = None
        for line in sse_response.iter_lines():
            if line:
                decoded = line.decode('utf-8')
                if decoded.startswith('data: '):
                    session_url = decoded[6:].strip()
                    break
        
        sse_response.close()
        
        if not session_url:
            return "Error: Could not establish MCP session"
        
        # Call the tool
        payload = {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": tool_name,
                "arguments": arguments
            }
        }
        
        print(f"🔧 Calling MCP tool: {tool_name} with args: {arguments}")
        
        # FIXED: Use longer timeout for anomaly prediction to handle ML endpoint cold starts
        timeout_seconds = 120 if tool_name == "get_anomaly_prediction" else 60
        print(f"⏱️  Using {timeout_seconds}s timeout for {tool_name}")
        
        rpc_response = requests.post(
            session_url,
            headers=headers,
            json=payload,
            timeout=timeout_seconds
        )
        
        rpc_data = rpc_response.json()
        
        if "result" in rpc_data:
            # Extract text content from MCP response
            if "content" in rpc_data["result"]:
                text_parts = [c.get("text", "") for c in rpc_data["result"]["content"] if c.get("type") == "text"]
                result = "\n".join(text_parts)
                print(f"✅ Tool result: {result[:200]}...")
                return result
            return json.dumps(rpc_data["result"])
        elif "error" in rpc_data:
            error_msg = f"MCP Error: {rpc_data['error'].get('message', str(rpc_data['error']))}"
            print(f"{error_msg}")
            return error_msg
        else:
            return json.dumps(rpc_data)
            
    except requests.exceptions.Timeout:
        # FIXED: Specific timeout exception handling with retry guidance
        if tool_name == "get_anomaly_prediction":
            timeout_msg = (
                "⏱️ Anomaly prediction timed out. This is likely due to ML endpoint cold start (first request after idle period).\n\n"
                "The ML model endpoint needs to spin up compute resources, which can take 30-60 seconds.\n\n"
                "✅ SOLUTION: Please try your request again - the endpoint should now be warm and respond quickly."
            )
        else:
            timeout_msg = f"⏱️ Tool '{tool_name}' timed out. The operation took longer than expected. Please try again."
        print(f"Timeout: {timeout_msg}")
        return timeout_msg
    except Exception as e:
        error_msg = f"Error calling MCP tool {tool_name}: {str(e)}"
        print(f"{error_msg}")
        import traceback
        traceback.print_exc()
        return error_msg

# ══════════════════════════════════════════════════════════════════════════════
# ROUTING LOGIC
# ══════════════════════════════════════════════════════════════════════════════

# Keyword sets for routing
MCP_KEYWORDS = frozenset([
    # Pipeline operations
    "pipeline", "dlt", "delta live", "trigger", "refresh", "run",
    "job", "workflow", "schedule", "orchestrat",
    # Data quality / observability
    "health", "quality", "anomaly", "alert", "expectation", "check",
    "sla", "freshness", "stale", "latency",
    # Lineage / metadata
    "lineage", "upstream", "downstream", "dependency", "dependencies",
    "schema", "column", "partition", "table metadata",
    # Engineering operations
    "ingest", "bronze", "silver", "transform", "backfill", "replay",
])

GENIE_KEYWORDS = frozenset([
    # Business analytics
    "revenue", "sales", "customer", "product", "order", "profit",
    "growth", "kpi", "top", "bottom", "average", "total", "count",
    "trend", "category", "region", "metric", "forecast", "ytd", "mtd",
    # Question patterns
    "how many", "which", "what is", "show me", "list", "compare",
    "rank", "breakdown", "split", "by",
])

def route_question(question: str) -> str:
    """
    Classify the question into one of three routes:
        'mcp'    – data-engineering / pipeline questions
        'genie'  – business analytics / KPI questions
        'direct' – general questions answered without external tools
    """
    q_lower = question.lower()

    # MCP takes priority (engineering > analytics)
    if any(kw in q_lower for kw in MCP_KEYWORDS):
        return "mcp"

    if any(kw in q_lower for kw in GENIE_KEYWORDS):
        return "genie"

    return "direct"

def format_genie_response(genie_result: dict) -> str:
    """Format Genie response with markdown sections."""
    parts = []

    if genie_result["nl_answer"]:
        parts.append(f"### 💬 Answer\n{genie_result['nl_answer']}")

    if genie_result["sql_query"]:
        parts.append(
            f"### 📝 SQL Query Executed\n"
            f"sql\n{genie_result['sql_query'].strip()}\n"
        )

    if genie_result["result_markdown"]:
        parts.append(
            f"### 📊 Data Results ({genie_result['row_count']} rows)\n"
            + genie_result["result_markdown"]
        )

    return (
        "\n\n".join(parts)
        if parts
        else "_Genie returned no content for this question._"
    )

# ══════════════════════════════════════════════════════════════════════════════
# INTEGRATED SUPERVISOR AGENT (MLFLOW PYFUNC MODEL)
# ══════════════════════════════════════════════════════════════════════════════

MCP_SYSTEM_PROMPT = """You are the IntelliPipe Supervisor Agent.
You are connected to an MCP server through Databricks Unity Catalog.

Your available tools let you investigate:
1. Pipeline Health & Runs
2. Data Quality & Constraints
3. Table Lineage & Dependencies
4. Hourly Metrics & ML Anomaly Detection

When a user asks a question, intelligently determine which tools to call.
Once you have the data, summarize it clearly and provide insights."""

class IntegratedSupervisorAgent(PythonModel):
    """
    Integrated Supervisor Agent — MLflow PythonModel
    
    Combines MCP tools and Genie Space with intelligent routing.
    """
    
    def load_context(self, context: PythonModelContext) -> None:
        """Called once when the model endpoint starts up."""
        import logging
        logger = logging.getLogger(__name__)
        
        # ── Get configuration from model_config ───────────────────────────────
        self.workspace_url = context.model_config["WORKSPACE_URL"]
        self.genie_space_id = context.model_config.get(
            "GENIE_SPACE_ID",
            "01f133e2518a1c609256e4485119b82d"
        )
        self.llm_endpoint = context.model_config.get(
            "LLM_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct"
        )
        self.mcp_connection_name = context.model_config.get(
            "MCP_CONNECTION_NAME", "intellipipe_mcp_connection"
        )
        
        # Extract Databricks host from workspace URL
        self.databricks_host = self.workspace_url.replace("https://", "").replace("http://", "")
        
        # ══════════════════════════════════════════════════════════════════════
        # Get credentials - keep them separate, don't pollute os.environ
        # ══════════════════════════════════════════════════════════════════════
        
        # Genie uses PAT token (stored as variable, NOT in os.environ)
        genie_token = (
            context.model_config.get("DATABRICKS_TOKEN")
            or os.environ.get("DATABRICKS_TOKEN")
        )
        
        # MCP uses service principal OAuth
        sp_client_id = (
            context.model_config.get("DATABRICKS_CLIENT_ID")
            or os.environ.get("DATABRICKS_CLIENT_ID")
        )
        sp_client_secret = (
            context.model_config.get("DATABRICKS_CLIENT_SECRET")
            or os.environ.get("DATABRICKS_CLIENT_SECRET")
        )
        
        # ══════════════════════════════════════════════════════════════════════
        # GENIE INITIALIZATION (uses PAT token directly, not through SDK)
        # ══════════════════════════════════════════════════════════════════════
        
        if not genie_token:
            logger.warning("DATABRICKS_TOKEN not found - Genie features will be disabled")
            self.genie = None
        else:
            try:
                self.genie = GenieClient(
                    workspace_url=self.workspace_url,
                    space_id=self.genie_space_id,
                    token=genie_token,  # Genie uses token directly in API calls
                )
                logger.info("✅ GenieClient initialized successfully")
            except Exception as e:
                logger.error(f"❌ Failed to initialize Genie client: {e}")
                self.genie = None
        
        # ══════════════════════════════════════════════════════════════════════
        # MCP INITIALIZATION (uses service principal OAuth)
        # Set OAuth credentials in os.environ for SDK auto-detection
        # ══════════════════════════════════════════════════════════════════════
        
        if not sp_client_id or not sp_client_secret:
            logger.warning(
                "Service principal credentials not found (DATABRICKS_CLIENT_ID/SECRET) - "
                "MCP features will be disabled"
            )
            self.mcp_tools = []
            self.llm = None
            self.llm_with_tools = None
            self.mcp_auth_token = None
            self.mcp_server_url = None
        else:
            try:
                # Set OAuth credentials in os.environ for SDK auto-detection
                # This is what agent.py does - see lines 16-18
                os.environ["DATABRICKS_HOST"] = self.databricks_host
                os.environ["DATABRICKS_CLIENT_ID"] = sp_client_id
                os.environ["DATABRICKS_CLIENT_SECRET"] = sp_client_secret
                
                # CRITICAL: Remove DATABRICKS_TOKEN from os.environ to avoid conflict
                # The SDK will see both OAuth and PAT if TOKEN is set
                if "DATABRICKS_TOKEN" in os.environ:
                    del os.environ["DATABRICKS_TOKEN"]
                
                logger.info("🔐 Getting OAuth token for MCP...")
                self.mcp_auth_token = get_auth_token(
                    sp_client_id,
                    sp_client_secret,
                    self.databricks_host
                )
                
                if not self.mcp_auth_token:
                    raise RuntimeError("Failed to get OAuth token from service principal")
                
                logger.info("✅ OAuth token obtained successfully")
                
                # Get MCP server URL from UC connection
                logger.info(f"🔍 Getting MCP server URL from connection: {self.mcp_connection_name}")
                self.mcp_server_url = get_mcp_connection_url(
                    self.mcp_connection_name,
                    sp_client_id,
                    sp_client_secret,
                    self.databricks_host
                )
                
                if not self.mcp_server_url:
                    raise RuntimeError(
                        f"Failed to get MCP server URL from connection '{self.mcp_connection_name}'"
                    )
                
                logger.info(f"✅ MCP server URL retrieved: {self.mcp_server_url}")
                
                # Fetch MCP tools
                logger.info("📡 Fetching MCP tools...")
                self.mcp_tools = fetch_mcp_tools(self.mcp_server_url, self.mcp_auth_token)
                
                if not self.mcp_tools:
                    logger.warning("⚠️ No MCP tools found")
                    self.mcp_tools = []
                else:
                    logger.info(f"✅ Loaded {len(self.mcp_tools)} MCP tools")
                
                # Initialize LLM with tools
                # ChatDatabricks will now auto-detect OAuth credentials from os.environ
                self.llm = ChatDatabricks(endpoint=self.llm_endpoint, max_tokens=2048)
                self.llm_with_tools = self.llm.bind(tools=self.mcp_tools) if self.mcp_tools else self.llm
                
                logger.info("✅ MCP client initialized successfully")
                
            except Exception as e:
                logger.error(f"❌ Failed to initialize MCP: {e}")
                import traceback
                traceback.print_exc()
                # Set fallback values
                self.mcp_auth_token = None
                self.mcp_server_url = None
                self.mcp_tools = []
                self.llm = None
                self.llm_with_tools = None
    
    def _handle_mcp_question(self, question: str) -> str:
        """Handle data-engineering questions using MCP tools."""
        if not self.llm_with_tools:
            return (
                "⚠️ **MCP tools are not available.**\n\n"
                "The MCP service failed to initialize. Check the serving endpoint logs for details."
            )
        
        # Prepare messages
        messages = [
            {"role": "system", "content": MCP_SYSTEM_PROMPT},
            {"role": "user", "content": question}
        ]
        
        # Invoke LLM with tools
        response = self.llm_with_tools.invoke(messages)
        
        # FIXED: Handle tool calls if present (proper iteration outside tool call loop)
        max_iterations = 5
        iteration = 0
        
        while hasattr(response, "tool_calls") and response.tool_calls and iteration < max_iterations:
            iteration += 1
            print(f"🔄 Tool calling iteration {iteration}")
            
            # Execute each tool call
            tool_messages = []
            for tool_call in response.tool_calls:
                tool_name = tool_call["name"]
                tool_args = tool_call.get("args", {})
                tool_id = tool_call.get("id", f"call_{iteration}")
                
                # Execute the MCP tool
                tool_result = call_mcp_tool(
                    tool_name,
                    tool_args,
                    self.mcp_server_url,
                    self.mcp_auth_token
                )
                
                # Add tool result to messages
                tool_messages.append({
                    "role": "tool",
                    "content": tool_result,
                    "tool_call_id": tool_id
                })
            
            # Add assistant message with tool calls
            messages.append({
                "role": "assistant",
                "content": response.content or "",
                "tool_calls": response.tool_calls
            })
            
            # Add tool results
            messages.extend(tool_messages)
            
            # FIXED: Get next response AFTER all tools are executed (moved outside for loop)
            response = self.llm_with_tools.invoke(messages)
        
        final_response = response.content if hasattr(response, "content") else str(response)
        print(f"✅ Final response: {final_response[:200]}...")
        return final_response
    
    def _handle_genie_question(self, question: str) -> str:
        """Handle business analytics questions using Genie Space."""
        if self.genie is None:
            return (
                "⚠️ **Genie Space is not available.**\n\n"
                "DATABRICKS_TOKEN is missing. Please ensure it's configured in the endpoint settings."
            )
        
        try:
            genie_result = self.genie.ask(question)
            return format_genie_response(genie_result)
        except Exception as exc:
            return (
                f"❌ Genie Space error: {exc}\n\n"
                "_Please verify the Genie Space ID and that the space is trained on the Gold tables._"
            )
    
    @staticmethod
    def _parse_input(model_input: Any) -> str:
        """Parse input to extract question string."""
        def _deserialise_messages(raw: Any) -> list[dict]:
            if isinstance(raw, list):
                return raw
            if isinstance(raw, str):
                for loader in (json.loads, ast.literal_eval):
                    try:
                        result = loader(raw)
                        if isinstance(result, list):
                            return result
                    except Exception:
                        pass
            return []

        if isinstance(model_input, dict):
            messages = _deserialise_messages(model_input.get("messages", []))
            if messages:
                return messages[-1].get("content", "")
            return model_input.get("question", "")

        if hasattr(model_input, "to_dict"):  # pandas DataFrame
            row = model_input.to_dict(orient="records")[0]
            messages = _deserialise_messages(row.get("messages", []))
            if messages:
                return messages[-1].get("content", "")
            return row.get("question", "")

        return str(model_input)
    
    def predict(self, context: PythonModelContext, model_input: Any) -> dict:
        """
        Main inference entrypoint with intelligent routing.
        """
        question = self._parse_input(model_input).strip()

        if not question:
            content = "Please ask a business or data-engineering question."
        else:
            route = route_question(question)
            print(f"📍 Routing '{question}' to: {route}")
            
            if route == "mcp":
                answer = self._handle_mcp_question(question)
                content = f"### 🛠️ Data Engineering Agent\n\n{answer}"
            
            elif route == "genie":
                content = self._handle_genie_question(question)
            
            else:
                content = (
                    "I'm the IntelliPipe Supervisor Agent. I specialise in:\n\n"
                    "- **📊 Business Analytics** — ask me about revenue, sales, customers, "
                    "KPIs, trends, or any Gold-table data.\n"
                    "- **🛠️ Data Engineering** — ask me about pipeline health, DLT runs, "
                    "data lineage, quality checks, or table metadata.\n\n"
                    f"Your question — _\"{question}\"_ — doesn't clearly match either category. "
                    "Could you rephrase it as a data or engineering question?"
                )
        
        return {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": content,
                    }
                }
            ]
        }

# Inform MLflow about the model interface
mlflow.models.set_model(IntegratedSupervisorAgent())