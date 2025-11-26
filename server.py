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
# MCP TOOL H — list active Jira sprints
# -------------------------------------------------------------------

@mcp.tool()
async def get_active_jira_sprints() -> Dict[str, Any]:
    """
    List active Jira sprints (as known by the Jira tasks service).
    """
    # ✅ include /tasks/ prefix to match Django
    url = f"{SPRINT_INSIGHTS_BASE_URL}/tasks/api/active-sprints/"

    try:
        async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
            resp = await client.get(url)
            if resp.status_code == 404:
                return {
                    "ok": False,
                    "error": "active-sprints endpoint not found on Jira tasks service",
                }

            resp.raise_for_status()
            return {"ok": True, "data": resp.json()}
    except Exception as e:
        return {
            "ok": False,
            "error": f"active-sprints call failed: {e}",
        }


# -------------------------------------------------------------------
# MCP TOOL I — get tasks for a Jira sprint
# -------------------------------------------------------------------

@mcp.tool()
async def get_jira_sprint_tasks(
    sprint_id: str,
    status_filter: str = ""
) -> Dict[str, Any]:
    """
    Get tasks for a given Jira sprint from the Jira tasks service.
    """
    # ✅ include /tasks/ prefix here as well
    url = f"{SPRINT_INSIGHTS_BASE_URL}/tasks/api/sprint-tasks/"
    params = {"sprint_id": sprint_id}
    if status_filter:
        params["status"] = status_filter

    try:
        async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
            resp = await client.get(url, params=params)
            if resp.status_code == 404:
                return {
                    "ok": False,
                    "error": f"Sprint {sprint_id} not found in Jira tasks service",
                }

            resp.raise_for_status()
            return {"ok": True, "data": resp.json()}
    except Exception as e:
        return {
            "ok": False,
            "error": f"sprint-tasks call failed: {e}",
        }

@mcp.tool()
async def get_user_sprint_tasks(
    sprint_id: str,
    user_name: str,
    status_filter: str = ""
) -> Dict[str, Any]:
    """
    Get all tasks and summary stats for a given user in a given sprint.

    Args:
        sprint_id: Jira sprint id (as string).
        user_name: Display name / username as stored in Task.user.username.
        status_filter: Optional comma-separated list of statuses
                       (e.g. "IN PROGRESS,CLOSED").

    Returns:
        {
          "ok": True/False,
          "data": { ... } or None,
          "error": "message" (optional)
        }
    """
    url = f"{SPRINT_INSIGHTS_BASE_URL}/api/sprint-user-tasks/"
    params = {"sprint_id": sprint_id, "user": user_name}
    if status_filter:
        params["status"] = status_filter

    try:
        async with httpx.AsyncClient(timeout=60.0, verify=False) as client:
            resp = await client.get(url, params=params)
            if resp.status_code == 404:
                return {
                    "ok": False,
                    "error": f"Sprint {sprint_id} or user {user_name} not found in Jira tasks service",
                }

            resp.raise_for_status()
            return {"ok": True, "data": resp.json()}
    except Exception as e:
        return {
            "ok": False,
            "error": f"sprint-user-tasks call failed: {e}",
        }
@mcp.tool()
async def get_user_sprint_hours(
    sprint_id: str,
    user_name: str,
) -> Dict[str, Any]:
    """
    Get a summary of estimated hours for a user in a sprint.

    Uses Task.story_points as hours (your existing convention).

    Returns:
        {
          "ok": True/False,
          "data": {
            "sprint_id": "...",
            "sprint_name": "...",
            "user": "...",
            "total_estimated_hours": ...,
            "completed_estimated_hours": ...,
            "open_estimated_hours": ...,
            "total_tasks": ...,
            "completed_tasks": ...,
            "open_tasks": ...,
            "unestimated_tasks": ...
          } or None,
          "error": "message" (optional)
        }
    """
    url = f"{SPRINT_INSIGHTS_BASE_URL}/api/sprint-user-tasks/"
    params = {"sprint_id": sprint_id, "user": user_name}

    try:
        async with httpx.AsyncClient(timeout=60.0, verify=False) as client:
            resp = await client.get(url, params=params)
            if resp.status_code == 404:
                return {
                    "ok": False,
                    "error": f"Sprint {sprint_id} or user {user_name} not found in Jira tasks service",
                }

            resp.raise_for_status()
            full = resp.json()
            # pick out just the summary fields
            summary = {
                "sprint_id": full.get("sprint_id"),
                "sprint_name": full.get("sprint_name"),
                "user": full.get("user"),
                "total_tasks": full.get("total_tasks"),
                "completed_tasks": full.get("completed_tasks"),
                "open_tasks": full.get("open_tasks"),
                "unestimated_tasks": full.get("unestimated_tasks"),
                "total_estimated_hours": full.get("total_estimated_hours"),
                "completed_estimated_hours": full.get("completed_estimated_hours"),
                "open_estimated_hours": full.get("open_estimated_hours"),
            }
            return {"ok": True, "data": summary}
    except Exception as e:
        return {
            "ok": False,
            "error": f"sprint-user-tasks (hours view) call failed: {e}",
        }
@mcp.tool()
async def search_jira_tasks(
    query: str,
    sprint_id: str = "",
    user_name: str = "",
    status_filter: str = "",
) -> Dict[str, Any]:
    """
    Search Jira tasks across sprints using the Jira tasks service database.

    Args:
        query: Free-text search term (issue key, title, sprint name, fix version, product year).
        sprint_id: Optional Sprint.sprint_id filter.
        user_name: Optional user name filter (contains match).
        status_filter: Optional comma-separated statuses, e.g. "IN PROGRESS,CLOSED".

    Returns:
        {
          "ok": True/False,
          "data": {
            "total": N,
            "tasks": [ ... ]
          } or None,
          "error": "message" (optional)
        }
    """
    url = f"{SPRINT_INSIGHTS_BASE_URL}/api/search-tasks/"
    params = {"q": query}
    if sprint_id:
        params["sprint_id"] = sprint_id
    if user_name:
        params["user"] = user_name
    if status_filter:
        params["status"] = status_filter

    try:
        async with httpx.AsyncClient(timeout=60.0, verify=False) as client:
            resp = await client.get(url, params=params)
            if resp.status_code == 404:
                return {
                    "ok": False,
                    "error": "search-tasks endpoint not found on Jira tasks service",
                }

            resp.raise_for_status()
            return {"ok": True, "data": resp.json()}
    except Exception as e:
        return {
            "ok": False,
            "error": f"search-tasks call failed: {e}",
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
