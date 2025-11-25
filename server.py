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


# -------------------------------------------------------------------
# Health check route for Kubernetes (not an MCP tool)
# -------------------------------------------------------------------

@mcp.custom_route("/healthz", ["GET"])
async def healthz(request: Request):
    return JSONResponse({"status": "ok"})


# -------------------------------------------------------------------
# MCP TOOL A — stream summary
# -------------------------------------------------------------------

@mcp.tool()
async def get_p4_stream_summary(stream: str) -> Dict[str, Any]:
    """
    Get summary stats for a single Perforce stream from P4StreamDiff.

    Args:
        stream: Perforce depot path, e.g. "//Game/Football/dev/ML"

    Returns:
        {
          "ok": True/False,
          "data": { ... } or None
          "error": "message" (optional)
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
            return {
                "ok": True,
                "data": resp.json(),
            }
    except Exception as e:
        return {
            "ok": False,
            "error": f"stream-summary call failed: {e}",
        }


# -------------------------------------------------------------------
# MCP TOOL B — check CL membership in a stream
# -------------------------------------------------------------------

@mcp.tool()
async def check_stream_changelists(stream: str, changelists: list[int]) -> Dict[str, Any]:
    """
    Check whether specific changelist numbers belong to a given Perforce stream.

    Args:
        stream: Perforce depot path, e.g. "//Game/College/26/DL"
        changelists: List of changelist numbers to check.

    Returns:
        {
          "ok": True/False,
          "data": {
            "stream": "...",
            "results": [
              {"cl_num": 1234, "in_stream": true/false},
              ...
            ]
          } or None
          "error": "message" (optional)
        }
    """
    url = f"{P4DIFF_BASE_URL}/api/stream-changelists-membership/"
    cls_param = ",".join(str(c) for c in changelists)

    try:
        async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
            resp = await client.get(url, params={"stream": stream, "cls": cls_param})
            if resp.status_code == 404:
                return {
                    "ok": False,
                    "error": f"Stream not found in P4StreamDiff: {stream}",
                }

            resp.raise_for_status()
            return {
                "ok": True,
                "data": resp.json(),
            }
    except Exception as e:
        return {
            "ok": False,
            "error": f"stream-changelists-membership call failed: {e}",
        }


# -------------------------------------------------------------------
# MCP TOOL C — list all changelists for a stream
# -------------------------------------------------------------------

@mcp.tool()
async def get_stream_changelists(stream: str) -> Dict[str, Any]:
    """
    List all known changelists for a given stream from P4StreamDiff.

    Args:
        stream: Perforce depot path, e.g. "//Game/College/26/DL"

    Returns:
        {
          "ok": True/False,
          "data": {
            "stream": "...",
            "total": N,
            "changelists": [...]
          } or None
          "error": "message" (optional)
        }
    """
    url = f"{P4DIFF_BASE_URL}/api/stream-changelists/"

    try:
        async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
            resp = await client.get(url, params={"stream": stream})
            if resp.status_code == 404:
                return {
                    "ok": False,
                    "error": f"Stream not found in P4StreamDiff: {stream}",
                }

            resp.raise_for_status()
            return {
                "ok": True,
                "data": resp.json(),
            }
    except Exception as e:
        return {
            "ok": False,
            "error": f"stream-changelists call failed: {e}",
        }


# -------------------------------------------------------------------
# MCP TOOL D — list latest file revisions for a stream
# -------------------------------------------------------------------

@mcp.tool()
async def get_stream_files(stream: str) -> Dict[str, Any]:
    """
    List latest file revisions for a given stream.

    Args:
        stream: Perforce depot path, e.g. "//Game/College/26/DL"

    Returns:
        {
          "ok": True/False,
          "data": {
            "stream": "...",
            "total_files": N,
            "files": [...]
          } or None
          "error": "message" (optional)
        }
    """
    url = f"{P4DIFF_BASE_URL}/api/stream-files/"

    try:
        async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
            resp = await client.get(url, params={"stream": stream})
            if resp.status_code == 404:
                return {
                    "ok": False,
                    "error": f"Stream not found in P4StreamDiff: {stream}",
                }

            resp.raise_for_status()
            return {
                "ok": True,
                "data": resp.json(),
            }
    except Exception as e:
        return {
            "ok": False,
            "error": f"stream-files call failed: {e}",
        }


# -------------------------------------------------------------------
# MCP TOOL E — get details for a single changelist
# -------------------------------------------------------------------

@mcp.tool()
async def get_changelist_details(changelist: int) -> Dict[str, Any]:
    """
    Get detailed metadata for a single changelist.

    Args:
        changelist: Changelist number.

    Returns:
        {
          "ok": True/False,
          "data": { ... } or None
          "error": "message" (optional)
        }
    """
    url = f"{P4DIFF_BASE_URL}/api/changelist-details/"

    try:
        async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
            resp = await client.get(url, params={"cl": changelist})
            if resp.status_code == 404:
                return {
                    "ok": False,
                    "error": f"Changelist not found in P4StreamDiff: {changelist}",
                }

            resp.raise_for_status()
            return {
                "ok": True,
                "data": resp.json(),
            }
    except Exception as e:
        return {
            "ok": False,
            "error": f"changelist-details call failed: {e}",
        }


# -------------------------------------------------------------------
# MCP TOOL F — find which streams changelists belong to
# -------------------------------------------------------------------

@mcp.tool()
async def find_changelist_streams(changelists: list[int]) -> Dict[str, Any]:
    """
    Given a list of changelist numbers, find which streams they belong to.

    Args:
        changelists: List of CL numbers, e.g. [8402852, 8402853]

    Returns:
        {
          "ok": True/False,
          "data": {
            "results": [
              {
                "cl_num": 8402852,
                "streams": [
                  {
                    "stream": "//Game/College/26/DL",
                    "commit_date_time": "...",
                    "tags": [...],
                    "title": [...]
                  },
                  ...
                ]
              },
              {
                "cl_num": 9999999,
                "streams": []
              }
            ]
          } or None
          "error": "message" (optional)
        }
    """
    url = f"{P4DIFF_BASE_URL}/api/changelists-streams/"
    cls_param = ",".join(str(c) for c in changelists)

    try:
        async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
            resp = await client.get(url, params={"cls": cls_param})
            if resp.status_code == 404:
                return {
                    "ok": False,
                    "error": "Endpoint /api/changelists-streams/ not found on P4Diff",
                }

            resp.raise_for_status()
            return {
                "ok": True,
                "data": resp.json(),
            }
    except Exception as e:
        return {
            "ok": False,
            "error": f"changelists-streams call failed: {e}",
        }


# -------------------------------------------------------------------
# MCP TOOL G — SprintInsights metrics
# -------------------------------------------------------------------

@mcp.tool()
async def get_sprint_metrics(sprint_name: str) -> Dict[str, Any]:
    """
    Get metrics for a sprint from SprintInsights.

    Args:
        sprint_name: Human-readable sprint name, e.g. "Gameplay Sprint 42"

    Returns:
        {
          "ok": True/False,
          "data": { ... } or None
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
            return {
                "ok": True,
                "data": resp.json(),
            }
    except Exception as e:
        return {
            "ok": False,
            "error": f"SprintInsights call failed: {e}",
        }


# -------------------------------------------------------------------
# ASGI app for uvicorn / K8s (SSE MCP)
# -------------------------------------------------------------------

# This exposes an SSE MCP server with:
#   - SSE endpoint at /sse
#   - Messages endpoint at /messages/
#   - Health check at /healthz
app = mcp.sse_app()
# In this FastMCP version, sse_app() defaults to /sse and /messages/
# We don't pass path/message_path to avoid version mismatch issues.

# For local dev:
#   uvicorn server:app --host 0.0.0.0 --port 8000
