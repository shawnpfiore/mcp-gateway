import os
from typing import Any, Dict, Optional

import httpx
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

app = FastAPI(
    title="Gameplay MCP Server",
    version="0.1.0",
    description="MCP gateway for Gameplay tools (P4Diff, SprintInsights, etc.)",
)

# ---- Config from environment ----

P4DIFF_BASE_URL = os.getenv("P4DIFF_BASE_URL", "http://localhost:9001")
SPRINT_INSIGHTS_BASE_URL = os.getenv("SPRINT_INSIGHTS_BASE_URL", "http://localhost:9002")
MCP_AUTH_TOKEN = os.getenv("MCP_AUTH_TOKEN")  # optional – if not set, auth is disabled


# ---- Simple auth helper ----

def verify_auth(authorization: Optional[str]) -> None:
    """
    If MCP_AUTH_TOKEN is set, require Authorization: Bearer <token>.
    If not set, auth is effectively disabled (for local dev).
    """
    if not MCP_AUTH_TOKEN:
        return

    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    expected = f"Bearer {MCP_AUTH_TOKEN}"
    if authorization != expected:
        raise HTTPException(status_code=403, detail="Invalid token")


# ---- Models for MCP-like tool API ----

class MCPToolRequest(BaseModel):
    tool: str
    arguments: Dict[str, Any] = {}


class MCPToolResponse(BaseModel):
    ok: bool
    result: Optional[Any] = None
    error: Optional[str] = None


# ---- Health check ----

@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


# ---- Tool dispatcher ----

@app.post("/mcp/tools", response_model=MCPToolResponse)
async def call_tool(
    payload: MCPToolRequest,
    authorization: Optional[str] = Header(default=None),
):
    """
    Generic MCP-style endpoint.
    Body shape:
    {
      "tool": "get_p4_diff",
      "arguments": { ... }
    }
    """
    verify_auth(authorization)

    tool = payload.tool

    if tool == "get_p4_diff":
        return await tool_get_p4_diff(payload.arguments)
    elif tool == "get_sprint_metrics":
        return await tool_get_sprint_metrics(payload.arguments)
    else:
        return MCPToolResponse(ok=False, error=f"Unknown tool: {tool}")


# ---- Tool implementations ----

async def tool_get_p4_diff(args: Dict[str, Any]) -> MCPToolResponse:
    """
    Calls your P4Diff API to compare two streams or branches.
    Expected args example:
      { "stream_a": "//Game/College/26/DL", "stream_b": "//Game/Football/26/DL" }
    """
    stream_a = args.get("stream_a")
    stream_b = args.get("stream_b")

    if not stream_a or not stream_b:
        return MCPToolResponse(ok=False, error="stream_a and stream_b are required")

    # TODO: update this URL/path to match your actual p4diff API
    url = f"{P4DIFF_BASE_URL}/api/compare_streams"

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(url, params={"stream_a": stream_a, "stream_b": stream_b})
            resp.raise_for_status()
            data = resp.json()
        return MCPToolResponse(ok=True, result=data)
    except Exception as e:
        return MCPToolResponse(ok=False, error=f"p4diff call failed: {e}")


async def tool_get_sprint_metrics(args: Dict[str, Any]) -> MCPToolResponse:
    """
    Calls SprintInsights to get metrics for a sprint.
    Expected args example:
      { "sprint_name": "Gameplay Sprint 42" }
    """
    sprint_name = args.get("sprint_name")

    if not sprint_name:
        return MCPToolResponse(ok=False, error="sprint_name is required")

    # TODO: update this URL/path to match your actual SprintInsights API
    url = f"{SPRINT_INSIGHTS_BASE_URL}/api/sprint_metrics"

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(url, params={"sprint_name": sprint_name})
            resp.raise_for_status()
            data = resp.json()
        return MCPToolResponse(ok=True, result=data)
    except Exception as e:
        return MCPToolResponse(ok=False, error=f"SprintInsights call failed: {e}")


# Local dev: uvicorn server:app --host 0.0.0.0 --port 8000
