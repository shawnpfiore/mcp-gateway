import os
from typing import Dict, Any

import httpx
from mcp.server.fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import JSONResponse

# -------------------------------------------------------------------
# Config from environment
# -------------------------------------------------------------------

P4DIFF_BASE_URL = os.getenv("P4DIFF_BASE_URL", "https://p4streamdiff.tib.ad.ea.com")
SPRINT_INSIGHTS_BASE_URL = os.getenv("SPRINT_INSIGHTS_BASE_URL", "https://sprintinsightsapp.tib.ad.ea.com")

# -------------------------------------------------------------------
# MCP server definition
# -------------------------------------------------------------------

# json_response=True => tools return plain JSON
# stateless_http=True => good for K8s / scaling
mcp = FastMCP(
    "Gameplay MCP Gateway",
    json_response=True,
    stateless_http=True,
)

# ---- Health check route for Kubernetes ----
# This is a normal HTTP endpoint, not an MCP tool.
@mcp.custom_route("/healthz", ["GET"])
async def healthz(request: Request):
    return JSONResponse({"status": "ok"})


# ---- Tool 1: get_p4_stream_summary --------------------------------
@mcp.tool()
async def get_p4_stream_summary(stream: str) -> Dict[str, Any]:
    """
    Get summary stats for a single Perforce stream from P4StreamDiff.

    Args:
        stream: Perforce depot path, e.g. "//Game/Football/dev/ML"

    Returns:
        {
          "ok": True/False,
          "data": { ... } or
          "error": "message"
        }
    """
    url = f"{P4DIFF_BASE_URL}/api/stream-summary/"

    try:
        async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
            resp = await client.get(url, params={"stream": stream})
            if resp.status_code == 404:
                return {
                    "ok": False,
                    "error": f"Stream not found in P4StreamDiff: {stream}",
                }

            resp.raise_for_status()
            data = resp.json()
            return {
                "ok": True,
                "data": data,
            }
    except Exception as e:
        return {
            "ok": False,
            "error": f"p4streamdiff call failed: {e}",
        }


# ---- Tool 2: get_sprint_metrics -----------------------------------
@mcp.tool()
async def get_sprint_metrics(sprint_name: str) -> Dict[str, Any]:
    """
    Get metrics for a sprint from SprintInsights.

    Args:
        sprint_name: Human-readable sprint name, e.g. "Gameplay Sprint 42"

    Returns:
        {
          "ok": True/False,
          "data": { ... } or
          "error": "message"
        }
    """
    url = f"{SPRINT_INSIGHTS_BASE_URL}/api/sprint-summary/"

    try:
        async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
            resp = await client.get(url, params={"sprint_name": sprint_name})
            if resp.status_code == 404:
                return {
                    "ok": False,
                    "error": f"Sprint not found in SprintInsights: {sprint_name}",
                }

            resp.raise_for_status()
            data = resp.json()
            return {
                "ok": True,
                "data": data,
            }
    except Exception as e:
        return {
            "ok": False,
            "error": f"SprintInsights call failed: {e}",
        }


# -------------------------------------------------------------------
# ASGI app for uvicorn / K8s (Streamable HTTP MCP)
# -------------------------------------------------------------------

# This exposes a Streamable HTTP MCP server with:
#   - MCP endpoint at /mcp
#   - Health check at /healthz
app = mcp.streamable_http_app()

# For local dev:
#   uvicorn server:app --host 0.0.0.0 --port 8000
