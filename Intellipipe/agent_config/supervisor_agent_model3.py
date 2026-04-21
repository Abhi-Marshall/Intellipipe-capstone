"""
supervisor_agent_model3.py
──────────────────────────
IntelliPipe Supervisor Agent — Model v3

Routing logic
─────────────
  MCP   → pipeline / data-engineering questions  →  Databricks LLM + UC Connection (MCP server)
  Genie → business / analytics questions         →  Genie Space REST API
  Direct→ everything else                        →  Polite clarification message

This file is referenced in supervisor_config.py Cell 4 via:
    code_paths = ["...supervisor_agent_model3.py"]
"""

from __future__ import annotations

import ast
import json
import os
import time
from typing import Any

import requests
# from databricks_langchain import ChatDatabricks, UCFunctionToolkit
from databricks_langchain import ChatDatabricks
from langchain_core.messages import HumanMessage, SystemMessage
from mlflow.pyfunc import PythonModel, PythonModelContext


# ══════════════════════════════════════════════════════════════════════════════
# 1.  GENIE CLIENT
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

    # ── URL helpers ───────────────────────────────────────────────────────────

    def _url(self, path: str) -> str:
        return f"{self.base}/api/2.0/genie/spaces/{self.space}{path}"

    # ── REST primitives ───────────────────────────────────────────────────────

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

    def poll_message(
        self, conv_id: str, msg_id: str, max_sec: int = 120
    ) -> dict:
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

    # ── Formatting helpers ────────────────────────────────────────────────────

    @staticmethod
    def _extract_val(cell: Any) -> str:
        if isinstance(cell, dict):
            for k in ("str", "i64", "f64", "bool", "date", "timestamp"):
                if k in cell:
                    return str(cell[k])
            return str(cell)
        return str(cell) if cell is not None else "NULL"

    @staticmethod
    def _rows_to_markdown(
        columns: list, rows: list, max_rows: int = 50
    ) -> str:
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

    # ── Main entry ────────────────────────────────────────────────────────────

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
# 2.  MCP CLIENT
# ══════════════════════════════════════════════════════════════════════════════

# class MCPClient:
#     """
#     Wraps the Databricks LLM endpoint that is bound to the MCP server
#     via a Unity Catalog Connection (uc_connection).

#     IMPORTANT: The current ChatDatabricks extra_params format with 
#     {"type": "uc_connection", ...} is NOT supported by foundation model endpoints.
    
#     This implementation provides graceful degradation until proper MCP integration
#     is implemented using the databricks-mcp library.
#     """

#     # MCP-aware system prompt
#     SYSTEM_PROMPT = (
#         "You are the IntelliPipe Data-Engineering Agent. "
#         "You have access to a suite of MCP tools deployed on Databricks. "
#         "Use these tools to answer questions about:\n"
#         "  • Pipeline health and run status\n"
#         "  • Data lineage and dependencies\n"
#         "  • Data quality checks and anomaly detection\n"
#         "  • DLT (Delta Live Tables) operations\n"
#         "  • Table metadata and schema inspection\n"
#         "  • Triggering or refreshing pipelines\n\n"
#         "Always explain which tool you called and summarise the result clearly."
#     )

#     def __init__(
#         self,
#         llm_endpoint: str,
#         mcp_connection_name: str,
#     ):
#         self.llm_endpoint        = llm_endpoint
#         self.mcp_connection_name = mcp_connection_name
#         self._llm: ChatDatabricks | None = None
#         self._initialization_error: str | None = None

#     # ── Lazy initialisation with error handling ───────────────────────────────

#     # def _get_llm(self) -> ChatDatabricks:
#     #     """
#     #     Attempt to initialize ChatDatabricks with MCP tools.
        
#     #     Raises:
#     #         RuntimeError: If initialization failed previously or fails now
#     #     """
#     #     if self._initialization_error:
#     #         raise RuntimeError(self._initialization_error)
            
#     #     if self._llm is None:
#     #         try:
#     #             # Attempt to initialize with UC connection format
#     #             # NOTE: This format is currently NOT supported by foundation model endpoints
#     #             self._llm = ChatDatabricks(
#     #                 endpoint=self.llm_endpoint,
#     #                 extra_params={
#     #                     "tools": [
#     #                         {
#     #                             "type": "uc_connection",
#     #                             "uc_connection": {"name": self.mcp_connection_name},
#     #                         }
#     #                     ]
#     #                 },
#     #             )
#     #         except Exception as e:
#     #             error_msg = (
#     #                 f"MCP initialization failed: {str(e)}\n\n"
#     #                 "The ChatDatabricks extra_params format with UC connections is not "
#     #                 "currently supported by foundation model endpoints. To enable MCP "
#     #                 "tool calling, the implementation needs to use the databricks-mcp "
#     #                 "library or the UC connections proxy format."
#     #             )
#     #             self._initialization_error = error_msg
#     #             raise RuntimeError(error_msg)
                
#     #     return self._llm


#     def _get_llm(self) -> ChatDatabricks:
#         """
#         Initialize ChatDatabricks and bind tools from Unity Catalog connections.
#         """
#         if self._initialization_error:
#             raise RuntimeError(self._initialization_error)
            
#         if self._llm is None:
#             try:
#                 # 1. Initialize the base ChatDatabricks model
#                 # Ensure your endpoint supports function calling
#                 base_llm = ChatDatabricks(
#                     endpoint=self.llm_endpoint,
#                     # Do not pass 'tools' via extra_params here
#                 )

#                 # 2. Use UCFunctionToolkit to fetch your UC-managed tools.
#                 # Replace {catalog}.{schema}.{function_name} with your actual 
#                 # UC function that utilizes the mcp_connection.
#                 toolkit = UCFunctionToolkit(
#                     function_names=[f"capstone_project.capstone_schema.{self.mcp_connection_name}"]
#                 )
#                 tools = toolkit.tools

#                 # 3. Bind the tools to the LLM
#                 # This is the correct way to register tools in LangChain
#                 self._llm = base_llm.bind_tools(tools)
                
#             except Exception as e:
#                 error_msg = (
#                     f"MCP initialization failed: {str(e)}\n\n"
#                     "To enable tool calling, ensure your UC Connection is wrapped in "
#                     "a Unity Catalog function and use UCFunctionToolkit."
#                 )
#                 self._initialization_error = error_msg
#                 raise RuntimeError(error_msg)
                
#         return self._llm


#     # ── Main entry ────────────────────────────────────────────────────────────

#     def ask(self, question: str) -> str:
#         """
#         Send the user question to the LLM+MCP pipeline.

#         Returns the raw content string from the LLM response.
        
#         Raises:
#             RuntimeError: If MCP integration is not properly configured
#         """
#         llm      = self._get_llm()
#         messages = [
#             SystemMessage(content=self.SYSTEM_PROMPT),
#             HumanMessage(content=question),
#         ]
#         response = llm.invoke(messages)
#         return response.content

class MCPClient:
    """
    MCP integration using DatabricksMCPClient for direct tool calling.
    
    Architecture:
    1. Connects to MCP server via UC Connection
    2. Discovers available MCP tools dynamically
    3. Wraps tools in LangChain-compatible format
    4. Binds tools to ChatDatabricks for function calling
    """

    # MCP-aware system prompt
    SYSTEM_PROMPT = (
        "You are the IntelliPipe Data-Engineering Agent. "
        "You have access to MCP tools for analyzing data pipelines and infrastructure.\n\n"
        "Available capabilities:\n"
        "• Pipeline health monitoring and run status\n"
        "• Data lineage and dependency tracking\n"
        "• Data quality checks and anomaly detection\n"
        "• DLT (Delta Live Tables) operations\n"
        "• Table metadata and schema inspection\n"
        "• Pipeline triggering and orchestration\n\n"
        "When answering:\n"
        "1. Call relevant MCP tools to gather data\n"
        "2. Analyze the tool results\n"
        "3. Provide clear, actionable insights\n"
        "4. Highlight any issues or important findings\n"
        "5. Use markdown formatting for readability"
    )

    def __init__(
        self,
        llm_endpoint: str,
        mcp_connection_name: str,
        workspace_url: str | None = None,
        token: str | None = None,
    ):
        """
        Initialize MCP client.
        
        Parameters
        ----------
        llm_endpoint : str
            Foundation model endpoint name
        mcp_connection_name : str
            UC connection name pointing to MCP server
        workspace_url : str | None
            Databricks workspace URL
        token : str | None
            Databricks token
        """
        self.llm_endpoint = llm_endpoint
        self.mcp_connection_name = mcp_connection_name
        self.workspace_url = workspace_url
        self.token = token
        
        self._llm: ChatDatabricks | None = None
        self._mcp_client = None
        self._tools_cache = None
        self._initialization_error: str | None = None

    def _initialize_mcp_client(self):
        """Initialize the MCP client and discover available tools."""
        try:
            from databricks.sdk import WorkspaceClient
            from databricks_mcp import DatabricksMCPClient
            
            # Initialize WorkspaceClient
            if self.token:
                workspace_client = WorkspaceClient(
                    host=self.workspace_url,
                    token=self.token
                )
            else:
                workspace_client = WorkspaceClient(host=self.workspace_url)
            
            # Get UC Connection to find MCP server URL
            connection = workspace_client.connections.get(self.mcp_connection_name)
            
            if not connection.url:
                raise ValueError(
                    f"UC Connection '{self.mcp_connection_name}' has no URL configured. "
                    "Ensure the connection points to a running MCP server."
                )
            
            mcp_server_url = connection.url
            
            # Initialize MCP client
            self._mcp_client = DatabricksMCPClient(
                server_url=mcp_server_url,
                workspace_client=workspace_client
            )
            
            # List available tools
            mcp_tools = self._mcp_client.list_tools()
            
            if not mcp_tools:
                raise ValueError(
                    f"No tools found on MCP server at {mcp_server_url}. "
                    "Ensure your MCP server is running and exposes tools."
                )
            
            return mcp_tools
            
        except Exception as e:
            error_msg = (
                f"Failed to initialize MCP client: {str(e)}\n\n"
                f"Checklist:\n"
                f"1. UC Connection '{self.mcp_connection_name}' exists\n"
                f"2. Connection URL points to running MCP server\n"
                f"3. MCP server is accessible from serving endpoint\n"
                f"4. MCP server implements MCP protocol correctly\n"
                f"5. Endpoint has permission to use UC Connection"
            )
            self._initialization_error = error_msg
            raise RuntimeError(error_msg)

    def _convert_mcp_tools_to_langchain(self, mcp_tools: list) -> list:
        """Convert MCP tools to LangChain-compatible Tool objects."""
        from langchain_core.tools import StructuredTool
        from pydantic import BaseModel, Field
        
        langchain_tools = []
        
        for mcp_tool in mcp_tools:
            tool_name = mcp_tool.name
            tool_description = (
                mcp_tool.description 
                if hasattr(mcp_tool, 'description') and mcp_tool.description
                else f"MCP tool: {tool_name}"
            )
            
            # Parse input schema if available
            input_schema = getattr(mcp_tool, 'inputSchema', None)
            
            # Create a dynamic Pydantic model for tool arguments
            if input_schema and isinstance(input_schema, dict):
                properties = input_schema.get('properties', {})
                field_defs = {}
                
                for prop_name, prop_info in properties.items():
                    field_type = str
                    field_description = prop_info.get('description', '')
                    field_default = ... if prop_name in input_schema.get('required', []) else None
                    field_defs[prop_name] = (
                        field_type,
                        Field(default=field_default, description=field_description)
                    )
                
                if field_defs:
                    ArgsModel = type(
                        f"{tool_name}_args",
                        (BaseModel,),
                        {
                            '__annotations__': {k: v[0] for k, v in field_defs.items()},
                            **{k: v[1] for k, v in field_defs.items()}
                        }
                    )
                else:
                    ArgsModel = type(f"{tool_name}_args", (BaseModel,), {})
            else:
                ArgsModel = type(f"{tool_name}_args", (BaseModel,), {})
            
            # Create the tool function
            def make_tool_func(tool_name_capture):
                def tool_func(**kwargs) -> str:
                    """Execute MCP tool and return results."""
                    try:
                        result = self._mcp_client.call_tool(tool_name_capture, kwargs)
                        if hasattr(result, 'content'):
                            if isinstance(result.content, list):
                                return "\n".join(str(block) for block in result.content)
                            else:
                                return str(result.content)
                        return str(result)
                    except Exception as e:
                        return f"Error calling tool {tool_name_capture}: {str(e)}"
                return tool_func
            
            # Create LangChain StructuredTool
            langchain_tool = StructuredTool(
                name=tool_name,
                description=tool_description,
                func=make_tool_func(tool_name),
                args_schema=ArgsModel
            )
            langchain_tools.append(langchain_tool)
        
        return langchain_tools

    def _get_llm(self) -> ChatDatabricks:
        """Get initialized ChatDatabricks with MCP tools bound."""
        if self._initialization_error:
            raise RuntimeError(self._initialization_error)
            
        if self._llm is None:
            try:
                # Initialize MCP client and get tools
                mcp_tools = self._initialize_mcp_client()
                
                # Convert to LangChain tools
                langchain_tools = self._convert_mcp_tools_to_langchain(mcp_tools)
                
                # Cache tools
                self._tools_cache = {tool.name: tool for tool in langchain_tools}
                
                # Initialize base ChatDatabricks
                base_llm = ChatDatabricks(endpoint=self.llm_endpoint)
                
                # Bind tools to LLM
                self._llm = base_llm.bind_tools(langchain_tools)
                
                import logging
                logger = logging.getLogger(__name__)
                logger.info(
                    f"MCPClient initialized with {len(langchain_tools)} tools: "
                    f"{', '.join(tool.name for tool in langchain_tools)}"
                )
                
            except Exception as e:
                if not self._initialization_error:
                    self._initialization_error = str(e)
                raise RuntimeError(self._initialization_error)
                
        return self._llm

    def ask(self, question: str) -> str:
        """
        Answer a question using MCP tools and LLM synthesis.
        
        The LLM will automatically decide which tools to call.
        """
        llm = self._get_llm()
        
        messages = [
            SystemMessage(content=self.SYSTEM_PROMPT),
            HumanMessage(content=question),
        ]
        
        # Invoke LLM with tool calling enabled
        response = llm.invoke(messages)
        
        # Extract content from response
        if hasattr(response, 'content'):
            return response.content
        else:
            return str(response)


# ══════════════════════════════════════════════════════════════════════════════
# 3.  SUPERVISOR AGENT  (MLflow PyFunc model)
# ══════════════════════════════════════════════════════════════════════════════

class SupervisorAgent(PythonModel):
    """
    Mosaic AI Supervisor Agent — v3

    Registered as an MLflow PyFunc model in Unity Catalog.

    Input  : {"messages": [{"role": "user", "content": "<question>"}]}
    Output : {"choices": [{"message": {"role": "assistant", "content": "<answer>"}}]}

    Routing
    -------
    MCP keywords   →  MCPClient   (pipeline/engineering tools)
    Genie keywords →  GenieClient (Gold-table analytics)
    Fallback       →  Polite clarification
    """

    # ── Keyword sets ──────────────────────────────────────────────────────────

    _MCP_KEYWORDS: frozenset[str] = frozenset([
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

    _GENIE_KEYWORDS: frozenset[str] = frozenset([
        # Business analytics
        "revenue", "sales", "customer", "product", "order", "profit",
        "growth", "kpi", "top", "bottom", "average", "total", "count",
        "trend", "category", "region", "metric", "forecast", "ytd", "mtd",
        # Question patterns
        "how many", "which", "what is", "show me", "list", "compare",
        "rank", "breakdown", "split", "by",
    ])

    # ── MLflow lifecycle ──────────────────────────────────────────────────────

    def load_context(self, context: PythonModelContext) -> None:
        """Called once when the model endpoint starts up."""
        import logging
        logger = logging.getLogger(__name__)
        
        token = (
            context.model_config.get("DATABRICKS_TOKEN")
            or os.environ.get("DATABRICKS_TOKEN")
        )
        
        workspace_url       = context.model_config["WORKSPACE_URL"]
        genie_space_id      = context.model_config["GENIE_SPACE_ID"]
        llm_endpoint        = context.model_config.get(
            "LLM_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct"
        )
        mcp_connection_name = context.model_config.get(
            "MCP_CONNECTION_NAME", "intellipipe_mcp_connection"
        )
        
        # Initialize Genie client
        if not token:
            logger.warning("DATABRICKS_TOKEN not found - Genie features will be disabled")
            self.genie = None
        else:
            self.genie = GenieClient(
                workspace_url=workspace_url,
                space_id=genie_space_id,
                token=token,
            )
            logger.info("GenieClient initialized successfully")
        
        # Initialize MCP client (always create, errors handled lazily on first use)
        # self.mcp = MCPClient(
        #     llm_endpoint=llm_endpoint,
        #     mcp_connection_name=mcp_connection_name,
        # )
        # logger.info(f"MCPClient configured with endpoint: {llm_endpoint}, connection: {mcp_connection_name}")

        # Initialize MCP client
        if not token:
            logger.warning("DATABRICKS_TOKEN not found - MCP features will be disabled")
            self.mcp = None
        else:
            try:
                self.mcp = MCPClient(
                    llm_endpoint=llm_endpoint,
                    mcp_connection_name=mcp_connection_name,
                    workspace_url=workspace_url,
                    token=token,
                )
                logger.info(f"MCPClient configured: endpoint={llm_endpoint}, connection={mcp_connection_name}")
            except Exception as e:
                logger.error(f"MCPClient initialization deferred: {e}")
                # Don't fail here - errors will surface on first use
                self.mcp = MCPClient(
                    llm_endpoint=llm_endpoint,
                    mcp_connection_name=mcp_connection_name,
                    workspace_url=workspace_url,
                    token=token,
                )

    # ── Routing ───────────────────────────────────────────────────────────────

    def _route(self, question: str) -> str:
        """
        Classify the question into one of three routes:
            'mcp'    – data-engineering / pipeline questions
            'genie'  – business analytics / KPI questions
            'direct' – general questions answered without external tools
        """
        q_lower = question.lower()

        # MCP takes priority (engineering > analytics)
        if any(kw in q_lower for kw in self._MCP_KEYWORDS):
            return "mcp"

        if any(kw in q_lower for kw in self._GENIE_KEYWORDS):
            return "genie"

        return "direct"

    # ── Response formatters ───────────────────────────────────────────────────

    @staticmethod
    def _format_genie_response(genie_result: dict) -> str:
        parts: list[str] = []

        if genie_result["nl_answer"]:
            parts.append(f"### 💬 Answer\n{genie_result['nl_answer']}")

        if genie_result["sql_query"]:
            parts.append(
                f"### 📝 SQL Query Executed\n"
                f"```sql\n{genie_result['sql_query'].strip()}\n```"
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

    # ── Input parsing ─────────────────────────────────────────────────────────

    @staticmethod
    def _parse_input(model_input: Any) -> str:
        """
        Normalise model_input to a plain question string.
        Handles dict, pandas DataFrame row, and raw string inputs.
        Also handles the case where 'messages' is a JSON/Python-literal string.
        """

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

        if hasattr(model_input, "to_dict"):          # pandas DataFrame
            row      = model_input.to_dict(orient="records")[0]
            messages = _deserialise_messages(row.get("messages", []))
            if messages:
                return messages[-1].get("content", "")
            return row.get("question", "")

        return str(model_input)

    # ── predict (main entry point) ────────────────────────────────────────────

    def predict(self, context: PythonModelContext, model_input: Any) -> dict:
        """
        Main inference entrypoint.

        Parameters
        ----------
        context     : MLflow model context (injected by serving runtime)
        model_input : OpenAI-compatible chat dict  OR  pandas DataFrame row

        Returns
        -------
        OpenAI-compatible response dict:
            {"choices": [{"message": {"role": "assistant", "content": "..."}}]}
        """
        question = self._parse_input(model_input).strip()

        if not question:
            return self._build_response(
                "Please ask a business or data-engineering question."
            )

        route = self._route(question)

        # ── MCP branch ────────────────────────────────────────────────────────
        # ── MCP branch ────────────────────────────────────────────────────
        if route == "mcp":
            if self.mcp is None:
                answer = (
                    "⚠️ **Data engineering queries are currently unavailable.**\n\n"
                    "The MCP service is not configured because DATABRICKS_TOKEN "
                    "is missing. Please configure the token as an environment variable "
                    "in the endpoint settings to enable data engineering queries."
                )
            else:
                try:
                    raw_answer = self.mcp.ask(question)
                    answer     = f"### 🛠️ Data Engineering Agent\n\n{raw_answer}"
                except Exception as exc:
                    # Check if it's an initialization error
                    error_detail = str(exc)[:400]
                    if "UC Connection" in error_detail or "does not exist" in error_detail:
                        answer = (
                            f"⚠️ **MCP service configuration error.**\n\n"
                            f"{error_detail}\n\n"
                            "**Common causes:**\n"
                            "• UC Connection doesn't exist or has wrong name\n"
                            "• MCP server (Databricks App) is not running\n"
                            "• Connection URL is incorrect or inaccessible\n\n"
                            "_For business analytics, try asking about revenue, sales, or customers._"
                        )
                    else:
                        answer = (
                            f"⚠️ **Data engineering query failed.**\n\n"
                            f"Technical details: {error_detail}\n\n"
                            "_For business analytics, try asking about revenue, sales, or customers._"
                        )

        # ── Genie branch ──────────────────────────────────────────────────────
        elif route == "genie":
            if self.genie is None:
                answer = (
                    "⚠️ **Data queries are currently unavailable.**\n\n"
                    "The Genie service is not configured because DATABRICKS_TOKEN "
                    "is missing. Please configure the token as an environment variable "
                    "in the endpoint settings to enable business analytics queries."
                )
            else:
                try:
                    genie_result = self.genie.ask(question)
                    answer       = self._format_genie_response(genie_result)
                except Exception as exc:
                    answer = (
                        f"❌ Genie Space error: {exc}\n\n"
                        "_Please verify the Genie Space ID and that the space is trained "
                        "on the Gold tables._"
                    )

        # ── Direct / fallback branch ──────────────────────────────────────────
        else:
            answer = (
                "I'm the IntelliPipe Supervisor Agent. I specialise in:\n\n"
                "- **📊 Business Analytics** — ask me about revenue, sales, customers, "
                "KPIs, trends, or any Gold-table data.\n"
                "- **🛠️ Data Engineering** — ask me about pipeline health, DLT runs, "
                "data lineage, quality checks, or table metadata.\n\n"
                f"Your question — _\"{question}\"_ — doesn't clearly match either category. "
                "Could you rephrase it as a data or engineering question?"
            )

        return self._build_response(answer)

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _build_response(content: str) -> dict:
        return {
            "choices": [
                {
                    "message": {
                        "role":    "assistant",
                        "content": content,
                    }
                }
            ]
        }
