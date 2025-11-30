import os
from typing import Dict, Any, List

import httpx
from mcp.server.fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import JSONResponse
from prometheus_client.parser import text_string_to_metric_families

# -------------------------------------------------------------------
# Config from environment
# -------------------------------------------------------------------

P4DIFF_BASE_URL = os.getenv("P4DIFF_BASE_URL", "https://p4streamdiff.tib.ad.ea.com")
SPRINT_INSIGHTS_BASE_URL = os.getenv("SPRINT_INSIGHTS_BASE_URL", "https://sprintinsightsapp.tib.ad.ea.com")
SKILL_MATRIX_BASE_URL = os.getenv(
    "SKILL_MATRIX_BASE_URL",
    "http://maddenskillmatrix-app.ea.svc.cluster.local:9100",
)
SWARM_METRICS_BASE_URL = os.getenv(
    "SWARM_METRICS_BASE_URL",
    "http://thehiveapp-app.ea.svc.cluster.local:5020",
)

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
async def get_jira_sprint_tasks(sprint_id: str, status_filter: str = "") -> Dict[str, Any]:
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
async def get_user_sprint_tasks(sprint_id: str, user_name: str, status_filter: str = "") -> Dict[str, Any]:
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
async def get_user_sprint_hours(sprint_id: str, user_name: str,) -> Dict[str, Any]:
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
async def search_jira_tasks(query: str, sprint_id: str = "", user_name: str = "", status_filter: str = "",) -> Dict[str, Any]:
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


@mcp.tool()
async def get_skill_matrix_metrics_raw() -> Dict[str, Any]:
    """
    Get the raw Prometheus metrics text from the Skill Matrix service.

    Useful for debugging or ad-hoc inspection.
    """
    url = f"{SKILL_MATRIX_BASE_URL}/metrics"

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(url)
            if resp.status_code == 404:
                return {
                    "ok": False,
                    "error": "Skill Matrix /metrics endpoint not found",
                }
            resp.raise_for_status()
            return {
                "ok": True,
                "data": resp.text,
            }
    except Exception as e:
        return {
            "ok": False,
            "error": f"Skill Matrix /metrics call failed: {e}",
        }


@mcp.tool()
async def get_submitter_skill_profile(submitter: str) -> Dict[str, Any]:
    """
    Get Skill Matrix proficiency metrics for a given submitter.

    Uses Prometheus gauges:
      - proficiency_by_user_initiative
      - proficiency_by_user_epic
    """
    metrics_url = f"{SKILL_MATRIX_BASE_URL}/metrics"

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.get(metrics_url)
            resp.raise_for_status()
            metrics_text = resp.text
    except Exception as e:
        return {
            "ok": False,
            "error": f"Failed to fetch Skill Matrix metrics: {e}",
        }

    by_initiative: List[Dict[str, Any]] = []
    by_epic: List[Dict[str, Any]] = []

    try:
        for family in text_string_to_metric_families(metrics_text):
            # proficiency_by_user_initiative
            if family.name == "proficiency_by_user_initiative":
                for sample in family.samples:
                    labels = sample.labels
                    if labels.get("submitter") == submitter:
                        by_initiative.append({
                            "initiative": labels.get("initiative"),
                            "epic_team": labels.get("epic_team"),
                            "proficiency_level": sample.value,  # 0-4 from your thresholds
                        })

            # proficiency_by_user_epic
            if family.name == "proficiency_by_user_epic":
                for sample in family.samples:
                    labels = sample.labels
                    if labels.get("submitter") == submitter:
                        by_epic.append({
                            "epic": labels.get("epic"),
                            "epic_team": labels.get("epic_team"),
                            "proficiency_level": sample.value,
                        })

        return {
            "ok": True,
            "data": {
                "submitter": submitter,
                "by_initiative": by_initiative,
                "by_epic": by_epic,
            },
        }
    except Exception as e:
        return {
            "ok": False,
            "error": f"Error parsing Skill Matrix metrics: {e}",
        }


@mcp.tool()
async def get_initiative_coverage(initiative: str, epic_team: str = "") -> Dict[str, Any]:
    """
    Get coverage for an initiative: how many submitters have experience
    in it, optionally filtered by epic_team.

    Uses:
      - submitter_experience_by_initiative
      - initiative_coverage
    """
    metrics_url = f"{SKILL_MATRIX_BASE_URL}/metrics"

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.get(metrics_url)
            resp.raise_for_status()
            metrics_text = resp.text
    except Exception as e:
        return {
            "ok": False,
            "error": f"Failed to fetch Skill Matrix metrics: {e}",
        }

    total_submitters_by_team: Dict[str, float] = {}
    coverage_pct_by_submitter: List[Dict[str, Any]] = []
    normalized_initiative = initiative.strip()

    try:
        for family in text_string_to_metric_families(metrics_text):
            # submitter_experience_by_initiative: count of unique submitters per initiative+team
            if family.name == "submitter_experience_by_initiative":
                for sample in family.samples:
                    labels = sample.labels
                    if labels.get("initiative") != normalized_initiative:
                        continue

                    team = labels.get("epic_team")
                    if epic_team and team != epic_team:
                        continue

                    total_submitters_by_team[team] = sample.value

            # initiative_coverage: pct per submitter
            if family.name == "initiative_coverage":
                for sample in family.samples:
                    labels = sample.labels
                    if labels.get("initiative") != normalized_initiative:
                        continue

                    team = labels.get("epic_team")
                    if epic_team and team != epic_team:
                        continue

                    coverage_pct_by_submitter.append({
                        "submitter": labels.get("submitter"),
                        "initiative": labels.get("initiative"),
                        "epic_team": team,
                        "coverage_pct": sample.value,
                    })

        return {
            "ok": True,
            "data": {
                "initiative": normalized_initiative,
                "total_submitters_by_team": total_submitters_by_team,
                "coverage_by_submitter": coverage_pct_by_submitter,
            },
        }
    except Exception as e:
        return {
            "ok": False,
            "error": f"Error parsing Skill Matrix initiative metrics: {e}",
        }


@mcp.tool()
async def find_best_submitters_for_initiative( initiative: str, min_level: int = 3) -> Dict[str, Any]:
    """
    Find submitters with proficiency >= min_level for a given initiative
    across all epic teams.

    Proficiency levels are the 0-4 mapping from your Skill Matrix.
    """
    metrics_url = f"{SKILL_MATRIX_BASE_URL}/metrics"

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.get(metrics_url)
            resp.raise_for_status()
            metrics_text = resp.text
    except Exception as e:
        return {
            "ok": False,
            "error": f"Failed to fetch Skill Matrix metrics: {e}",
        }

    normalized_initiative = initiative.strip()
    candidates: List[Dict[str, Any]] = []

    try:
        for family in text_string_to_metric_families(metrics_text):
            if family.name != "proficiency_by_user_initiative":
                continue

            for sample in family.samples:
                labels = sample.labels
                if labels.get("initiative") != normalized_initiative:
                    continue

                level = sample.value
                if level >= min_level:
                    candidates.append({
                        "submitter": labels.get("submitter"),
                        "initiative": labels.get("initiative"),
                        "epic_team": labels.get("epic_team"),
                        "proficiency_level": level,
                    })

        # sort by proficiency desc
        candidates.sort(key=lambda c: c["proficiency_level"], reverse=True)

        return {
            "ok": True,
            "data": {
                "initiative": normalized_initiative,
                "min_level": min_level,
                "candidates": candidates,
            },
        }
    except Exception as e:
        return {
            "ok": False,
            "error": f"Error parsing proficiency metrics: {e}",
        }


@mcp.tool()
async def get_epic_expertise(epic: str) -> Dict[str, Any]:
    """
    Summarize who has experience in a given epic and their proficiency levels.

    Uses:
      - submitter_experience_by_epic (unique submitters per epic)
      - proficiency_by_user_epic (per submitter+epic+team)
    """
    metrics_url = f"{SKILL_MATRIX_BASE_URL}/metrics"

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.get(metrics_url)
            resp.raise_for_status()
            metrics_text = resp.text
    except Exception as e:
        return {
            "ok": False,
            "error": f"Failed to fetch Skill Matrix metrics: {e}",
        }

    target_epic = epic or "Unknown"
    submitter_count = 0
    submitter_levels: List[Dict[str, Any]] = []

    try:
        for family in text_string_to_metric_families(metrics_text):
            if family.name == "submitter_experience_by_epic":
                for sample in family.samples:
                    labels = sample.labels
                    if labels.get("epic") == target_epic:
                        submitter_count = sample.value

            if family.name == "proficiency_by_user_epic":
                for sample in family.samples:
                    labels = sample.labels
                    if labels.get("epic") == target_epic:
                        submitter_levels.append({
                            "submitter": labels.get("submitter"),
                            "epic_team": labels.get("epic_team"),
                            "proficiency_level": sample.value,
                        })

        submitter_levels.sort(key=lambda x: x["proficiency_level"], reverse=True)

        return {
            "ok": True,
            "data": {
                "epic": target_epic,
                "total_submitters": submitter_count,
                "submitter_levels": submitter_levels,
            },
        }
    except Exception as e:
        return {
            "ok": False,
            "error": f"Error parsing epic expertise metrics: {e}",
        }

@mcp.tool()
async def get_swarm_metrics_raw() -> Dict[str, Any]:
    """
    Get the raw Prometheus metrics text from the Swarm Metrics service.

    Useful for debugging or ad-hoc inspection.
    """
    # your Swarm app exposes metrics at /metrics/
    url = f"{SWARM_METRICS_BASE_URL}/metrics/"

    try:
        async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
            resp = await client.get(url)
            if resp.status_code == 404:
                return {"ok": False, "error": "Swarm /metrics endpoint not found"}
            resp.raise_for_status()
            return {"ok": True, "data": resp.text}
    except Exception as e:
        return {"ok": False, "error": f"Swarm metrics call failed: {e}"}

@mcp.tool()
async def get_swarm_group_summary(group: str) -> Dict[str, Any]:
    """
    Get a summary of Swarm review metrics for a given group.

    Includes:
      - engagement rate
      - time to first vote
      - vote-up duration
      - total reviews
      - avg review duration
      - engagement counts
      - needsReview count
    """
    metrics_url = f"{SWARM_METRICS_BASE_URL}/metrics/"

    try:
        async with httpx.AsyncClient(timeout=60.0, verify=False) as client:
            resp = await client.get(metrics_url)
            resp.raise_for_status()
            metrics_text = resp.text
    except Exception as e:
        return {"ok": False, "error": f"Failed to fetch Swarm metrics: {e}"}

    target_group = group
    summary = {
        "group": target_group,
        "engagement_rate": None,
        "avg_time_to_first_vote_hours": None,
        "avg_vote_up_duration_hours": None,
        "total_reviews": None,
        "avg_review_duration_hours": None,
        "fully_engaged_reviews": None,
        "partially_engaged_reviews": None,
        "not_engaged_reviews": None,
        "needs_review_count": None,
    }

    try:
        for family in text_string_to_metric_families(metrics_text):
            for sample in family.samples:
                labels = sample.labels
                if labels.get("group") != target_group:
                    continue

                name = family.name
                val = sample.value

                if name == "group_engagement_rate":
                    summary["engagement_rate"] = val
                elif name == "group_avg_time_to_first_vote":
                    summary["avg_time_to_first_vote_hours"] = val
                elif name == "group_avg_vote_up_duration":
                    summary["avg_vote_up_duration_hours"] = val
                elif name == "group_total_reviews":
                    summary["total_reviews"] = val
                elif name == "group_avg_review_duration":
                    summary["avg_review_duration_hours"] = val
                elif name == "group_fully_engaged_reviews":
                    summary["fully_engaged_reviews"] = val
                elif name == "group_partially_engaged_reviews":
                    summary["partially_engaged_reviews"] = val
                elif name == "group_not_engaged_reviews":
                    summary["not_engaged_reviews"] = val
                elif name == "group_needs_review_count":
                    summary["needs_review_count"] = val

        return {"ok": True, "data": summary}
    except Exception as e:
        return {"ok": False, "error": f"Error parsing Swarm metrics: {e}"}

@mcp.tool()
async def get_swarm_group_top_contributors(group: str, limit: int = 5) -> Dict[str, Any]:
    """
    Get top contributors for a Swarm group, based on activity counts
    (once per review per user, as computed in your metrics).

    Uses the group_top_contributor gauge.
    """
    metrics_url = f"{SWARM_METRICS_BASE_URL}/metrics/"

    try:
        async with httpx.AsyncClient(timeout=60.0, verify=False) as client:
            resp = await client.get(metrics_url)
            resp.raise_for_status()
            metrics_text = resp.text
    except Exception as e:
        return {"ok": False, "error": f"Failed to fetch Swarm metrics: {e}"}

    target_group = group
    contributors: List[Dict[str, Any]] = []

    try:
        for family in text_string_to_metric_families(metrics_text):
            if family.name != "group_top_contributor":
                continue

            for sample in family.samples:
                labels = sample.labels
                if labels.get("group") != target_group:
                    continue

                contributors.append({
                    "group": labels.get("group"),
                    "contributor": labels.get("contributor"),
                    "activity_count": sample.value,
                })

        contributors.sort(key=lambda c: c["activity_count"], reverse=True)
        return {
            "ok": True,
            "data": {
                "group": target_group,
                "top_contributors": contributors[:limit],
            },
        }
    except Exception as e:
        return {"ok": False, "error": f"Error parsing top contributors: {e}"}

@mcp.tool()
async def get_swarm_group_daily_snapshot(group: str) -> Dict[str, Any]:
    """
    Get today's Swarm review metrics for a group (daily gauges).

    Includes today's:
      - engagement rate
      - time to first vote
      - vote-up duration
      - total reviews
      - avg review duration
      - needsReview count
    """
    metrics_url = f"{SWARM_METRICS_BASE_URL}/metrics/"

    try:
        async with httpx.AsyncClient(timeout=60.0, verify=False) as client:
            resp = await client.get(metrics_url)
            resp.raise_for_status()
            metrics_text = resp.text
    except Exception as e:
        return {"ok": False, "error": f"Failed to fetch Swarm metrics: {e}"}

    target_group = group
    snapshot = {
        "group": target_group,
        "daily_engagement_rate": None,
        "daily_avg_time_to_first_vote_hours": None,
        "daily_avg_vote_up_duration_hours": None,
        "daily_total_reviews": None,
        "daily_avg_review_duration_hours": None,
        "daily_needs_review_count": None,
    }

    try:
        for family in text_string_to_metric_families(metrics_text):
            for sample in family.samples:
                labels = sample.labels
                if labels.get("group") != target_group:
                    continue

                name = family.name
                val = sample.value

                if name == "group_daily_engagement_rate":
                    snapshot["daily_engagement_rate"] = val
                elif name == "group_daily_avg_time_to_first_vote":
                    snapshot["daily_avg_time_to_first_vote_hours"] = val
                elif name == "group_daily_avg_vote_up_duration":
                    snapshot["daily_avg_vote_up_duration_hours"] = val
                elif name == "group_daily_total_reviews":
                    snapshot["daily_total_reviews"] = val
                elif name == "group_daily_avg_review_duration":
                    snapshot["daily_avg_review_duration_hours"] = val
                elif name == "group_daily_needs_review_count":
                    snapshot["daily_needs_review_count"] = val

        return {"ok": True, "data": snapshot}
    except Exception as e:
        return {"ok": False, "error": f"Error parsing Swarm daily metrics: {e}"}

@mcp.tool()
async def get_swarm_group_history(
    group: str,
    start_date: str,
    end_date: str,
) -> Dict[str, Any]:
    """
    Get per-day Swarm metrics for a group between start_date and end_date.

    Args:
        group: Swarm group name (e.g. "2A-Franchise-Gameplay").
        start_date: Start date in YYYY-MM-DD.
        end_date: End date in YYYY-MM-DD.

    Returns:
        {
          "ok": True/False,
          "data": {
            "group": "...",
            "start": "YYYY-MM-DD",
            "end": "YYYY-MM-DD",
            "days": [
              {
                "date": "YYYY-MM-DD",
                "engagement_rate": ...,
                "average_time_to_first_vote": ...,
                "average_vote_up_duration": ...,
                "total_reviews": ...,
                "average_review_duration": ...,
                "needs_review_count": ...
              },
              ...
            ]
          } or None,
          "error": "message" (optional)
        }
    """
    url = f"{SWARM_METRICS_BASE_URL}/api/group-metrics-range/"
    params = {
        "group": group,
        "start": start_date,
        "end": end_date,
    }

    try:
        async with httpx.AsyncClient(timeout=60.0, verify=False) as client:
            resp = await client.get(url, params=params)
            if resp.status_code == 400:
                # Bad input from caller
                return {
                    "ok": False,
                    "error": f"Bad request to Swarm app: {resp.text}",
                }
            if resp.status_code == 404:
                return {
                    "ok": False,
                    "error": "group-metrics-range endpoint not found on Swarm app",
                }

            resp.raise_for_status()
            return {
                "ok": True,
                "data": resp.json(),
            }
    except Exception as e:
        return {
            "ok": False,
            "error": f"Swarm date-range metrics call failed: {e}",
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
