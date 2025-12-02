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
# Helper functions for Skill Matrix metrics
# -------------------------------------------------------------------

def _norm(v: str) -> str:
    """Normalize a string for comparison (strip + lowercase)."""
    return (v or "").strip().lower()


def _filter_skillmatrix_metrics(metrics_text: str) -> str:
    """
    Strip the /metrics payload down to only the lines relevant
    to the custom Skill Matrix metrics we care about.

    This massively reduces the size before Prometheus parses it.
    """
    relevant_names = (
        "proficiency_by_user_initiative",
        "proficiency_by_user_initiative_combined",
        "proficiency_by_user_epic",
        "submitter_experience_by_initiative",
        "submitter_experience_by_epic",
        "initiative_coverage",
        "total_epic_team_count",
        "total_epic_team_count_by_submitter",
        "total_initiative_count_by_submitter",
    )

    lines_out: List[str] = []
    for line in metrics_text.splitlines():
        # Keep HELP/TYPE lines for relevant metrics
        if line.startswith("# HELP") or line.startswith("# TYPE"):
            if any(name in line for name in relevant_names):
                lines_out.append(line)
            continue

        # Keep sample lines that start with one of our metric names
        if any(line.startswith(name) for name in relevant_names):
            lines_out.append(line)

    return "\n".join(lines_out)


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
async def get_sprint_metrics(
    sprint_id: str | None = None,
    sprint_name: str | None = None,
) -> Dict[str, Any]:
    """
    Get metrics for a sprint from SprintInsights.

    You can pass either sprint_id (preferred) or sprint_name.
    """
    if not sprint_id and not sprint_name:
        return {
            "ok": False,
            "error": "You must provide sprint_id or sprint_name",
        }

    url = f"{SPRINT_INSIGHTS_BASE_URL}/tasks/api/sprint-summary/"
    params: Dict[str, str] = {}
    if sprint_id:
        params["sprint_id"] = sprint_id
    if sprint_name:
        params["sprint_name"] = sprint_name

    try:
        async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
            resp = await client.get(url, params=params)

            if resp.status_code == 404:
                return {
                    "ok": False,
                    "error": f"Sprint or endpoint not found (status=404, body={resp.text!r})",
                }

            resp.raise_for_status()
            return {"ok": True, "data": resp.json()}
    except Exception as e:
        return {"ok": False, "error": f"SprintInsights call failed: {e}"}


# -------------------------------------------------------------------
# MCP TOOL H — list active Jira sprints
# -------------------------------------------------------------------

@mcp.tool()
async def get_active_jira_sprints() -> Dict[str, Any]:
    """
    List active Jira sprints (as known by the Jira tasks service).
    """
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
    status_filter: str = "",
    limit: int = 200,
) -> Dict[str, Any]:
    """
    Get tasks for a given Jira sprint from the Jira tasks service.

    limit: max number of tasks to return (default 200, max enforced server-side)
    """
    url = f"{SPRINT_INSIGHTS_BASE_URL}/tasks/api/sprint-tasks/"
    params: Dict[str, str] = {"sprint_id": sprint_id, "limit": str(limit)}
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
    status_filter: str = "",
    limit: int = 200,
) -> Dict[str, Any]:
    """
    Get all tasks and summary stats for a given user in a given sprint.
    """
    url = f"{SPRINT_INSIGHTS_BASE_URL}/tasks/api/sprint-user-tasks/"
    params: Dict[str, str] = {
        "sprint_id": sprint_id,
        "user": user_name,
        "limit": str(limit),
    }
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
async def get_user_sprint_hours(sprint_id: str, user_name: str) -> Dict[str, Any]:
    """
    Get a summary of estimated hours for a user in a sprint.

    Uses Task.story_points as hours (your existing convention).
    """
    url = f"{SPRINT_INSIGHTS_BASE_URL}/tasks/api/sprint-user-tasks/"
    # Only need summary, not the full tasks list: ask for 1 row max
    params = {"sprint_id": sprint_id, "user": user_name, "limit": "1"}

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
    limit: int = 200,
    offset: int = 0,
) -> Dict[str, Any]:
    """
    Search Jira tasks across sprints using the Jira tasks service database.
    """
    url = f"{SPRINT_INSIGHTS_BASE_URL}/tasks/api/search-tasks/"
    params: Dict[str, str] = {
        "q": query,
        "limit": str(limit),
        "offset": str(offset),
    }
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
# Skill Matrix tools
# -------------------------------------------------------------------

@mcp.tool()
async def get_skill_matrix_metrics_raw() -> Dict[str, Any]:
    """
    Return the full /metrics text for Skill Matrix (debug only).
    """
    url = f"{SKILL_MATRIX_BASE_URL}/metrics"

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(url)
            print(
                "[SkillMatrix] GET", url,
                "status=", resp.status_code,
                "len=", len(resp.text),
            )
            if resp.status_code == 404:
                return {"ok": False, "error": "Skill Matrix /metrics endpoint not found"}
            resp.raise_for_status()
            print("[SkillMatrix] first line:", resp.text.splitlines()[0:3])
            return {"ok": True, "data": resp.text}
    except Exception as e:
        print("[SkillMatrix] ERROR calling /metrics:", repr(e))
        return {"ok": False, "error": f"Skill Matrix /metrics call failed: {e}"}


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
            full_text = resp.text
    except Exception as e:
        return {
            "ok": False,
            "error": f"Failed to fetch Skill Matrix metrics: {e}",
        }

    # Filter down to just the relevant custom metrics
    metrics_text = _filter_skillmatrix_metrics(full_text)

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
                            "proficiency_level": sample.value,
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
    """
    metrics_url = f"{SKILL_MATRIX_BASE_URL}/metrics"

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.get(metrics_url)
            resp.raise_for_status()
            full_text = resp.text
    except Exception as e:
        return {
            "ok": False,
            "error": f"Failed to fetch Skill Matrix metrics: {e}",
        }

    metrics_text = _filter_skillmatrix_metrics(full_text)

    total_submitters_by_team: Dict[str, float] = {}
    coverage_pct_by_submitter: List[Dict[str, Any]] = []
    target_norm = _norm(initiative)

    try:
        for family in text_string_to_metric_families(metrics_text):
            # submitter_experience_by_initiative
            if family.name == "submitter_experience_by_initiative":
                for sample in family.samples:
                    labels = sample.labels
                    if _norm(labels.get("initiative")) != target_norm:
                        continue

                    team = labels.get("epic_team")
                    if epic_team and team != epic_team:
                        continue

                    total_submitters_by_team[team] = sample.value

            # initiative_coverage
            if family.name == "initiative_coverage":
                for sample in family.samples:
                    labels = sample.labels
                    if _norm(labels.get("initiative")) != target_norm:
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
                "initiative": initiative,
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
async def find_best_submitters_for_initiative(initiative: str, min_level: int = 3) -> Dict[str, Any]:
    """
    Find submitters with proficiency >= min_level for a given initiative
    across all epic teams.
    """
    metrics_url = f"{SKILL_MATRIX_BASE_URL}/metrics"

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.get(metrics_url)
            resp.raise_for_status()
            full_text = resp.text
    except Exception as e:
        return {
            "ok": False,
            "error": f"Failed to fetch Skill Matrix metrics: {e}",
        }

    metrics_text = _filter_skillmatrix_metrics(full_text)

    target_norm = _norm(initiative)
    candidates: List[Dict[str, Any]] = []

    try:
        for family in text_string_to_metric_families(metrics_text):
            if family.name != "proficiency_by_user_initiative":
                continue

            for sample in family.samples:
                labels = sample.labels
                if _norm(labels.get("initiative")) != target_norm:
                    continue

                level = sample.value
                if level >= min_level:
                    candidates.append({
                        "submitter": labels.get("submitter"),
                        "initiative": labels.get("initiative"),
                        "epic_team": labels.get("epic_team"),
                        "proficiency_level": level,
                    })

        candidates.sort(key=lambda c: c["proficiency_level"], reverse=True)

        return {
            "ok": True,
            "data": {
                "initiative": initiative,
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
    """
    metrics_url = f"{SKILL_MATRIX_BASE_URL}/metrics"

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.get(metrics_url)
            resp.raise_for_status()
            full_text = resp.text
    except Exception as e:
        return {
            "ok": False,
            "error": f"Failed to fetch Skill Matrix metrics: {e}",
        }

    metrics_text = _filter_skillmatrix_metrics(full_text)

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


# -------------------------------------------------------------------
# Swarm metrics tools
# -------------------------------------------------------------------

@mcp.tool()
async def get_swarm_metrics_raw() -> Dict[str, Any]:
    """
    Get the raw Prometheus metrics text from the Swarm Metrics service.
    """
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
    Get top contributors for a Swarm group, based on activity counts.
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
async def get_swarm_group_history(group: str, start_date: str, end_date: str) -> Dict[str, Any]:
    """
    Get per-day Swarm metrics for a group between start_date and end_date.
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

app = mcp.sse_app()
# For local dev:
#   uvicorn server:app --host 0.0.0.0 --port 8000
