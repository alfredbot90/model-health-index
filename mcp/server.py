"""
Power BI Model Health MCP Server

FastMCP server for Claude Code integration.
Provides tools to query and analyze Power BI models via natural language.
"""

import json
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from fastmcp import FastMCP
from core.auth import get_token
from core.fabric_client import FabricClient
from core.powerbi_client import PowerBIClient
from core.orchestrator import ModelHealthOrchestrator

# Create MCP server
mcp = FastMCP("powerbi-health")

# Initialize clients (will be set on first use)
_fabric_client: Optional[FabricClient] = None
_powerbi_client: Optional[PowerBIClient] = None
_orchestrators: Dict[str, ModelHealthOrchestrator] = {}


def get_fabric_client() -> FabricClient:
    """Get or create Fabric client"""
    global _fabric_client
    if _fabric_client is None:
        token = get_token()
        _fabric_client = FabricClient(token)
    return _fabric_client


def get_powerbi_client() -> PowerBIClient:
    """Get or create Power BI client"""
    global _powerbi_client
    if _powerbi_client is None:
        token = get_token()
        _powerbi_client = PowerBIClient(token)
    return _powerbi_client


def get_orchestrator(workspace_id: str) -> ModelHealthOrchestrator:
    """Get or create orchestrator for workspace"""
    if workspace_id not in _orchestrators:
        _orchestrators[workspace_id] = ModelHealthOrchestrator(workspace_id)
    return _orchestrators[workspace_id]


@mcp.tool()
async def list_workspaces() -> str:
    """
    List all Power BI workspaces accessible to the current user.

    Returns:
        JSON string with workspace list containing id, name, and type
    """
    try:
        client = get_fabric_client()
        workspaces = client.get_workspaces()

        result = [
            {
                "id": ws.id,
                "name": ws.name,
                "type": ws.type,
                "capacity_id": ws.capacity_id
            }
            for ws in workspaces
        ]

        return json.dumps({"workspaces": result}, indent=2)

    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
async def list_semantic_models(workspace_id: str) -> str:
    """
    List all semantic models in a workspace.

    Args:
        workspace_id: Workspace GUID

    Returns:
        JSON string with semantic model list
    """
    try:
        client = get_fabric_client()
        models = client.get_semantic_models(workspace_id)

        result = [
            {
                "id": model.id,
                "name": model.name,
                "description": model.description
            }
            for model in models
        ]

        return json.dumps({"models": result}, indent=2)

    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
async def analyze_model_health(
    workspace_id: str,
    model_id: str,
    include_reports: bool = True
) -> str:
    """
    Analyze a semantic model's health and get comprehensive scoring.

    This performs a full analysis including:
    - Semantic model structure analysis
    - M-Code performance analysis
    - DAX complexity analysis
    - Relationship pattern detection
    - Optional report binding analysis

    Args:
        workspace_id: Workspace GUID
        model_id: Semantic model GUID
        include_reports: Include report binding analysis (default: True)

    Returns:
        JSON string with complete analysis results including scores, issues, and recommendations
    """
    try:
        orchestrator = get_orchestrator(workspace_id)
        result = await orchestrator.analyze_model(
            model_id=model_id,
            include_reports=include_reports
        )

        return json.dumps(result.to_dict(), indent=2)

    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
async def get_model_statistics(workspace_id: str, model_id: str) -> str:
    """
    Get quick statistics about a semantic model without full analysis.

    Args:
        workspace_id: Workspace GUID
        model_id: Semantic model GUID

    Returns:
        JSON string with model statistics (tables, measures, columns, relationships)
    """
    try:
        orchestrator = get_orchestrator(workspace_id)

        # Try to get cached result first
        cached = orchestrator.get_cached_result(model_id)
        if cached:
            return json.dumps({
                "model_id": cached.model_id,
                "model_name": cached.model_name,
                "statistics": cached.statistics,
                "overall_score": cached.overall_score,
                "grade": cached.grade,
                "cached": True
            }, indent=2)

        # If not cached, run quick analysis
        result = await orchestrator.analyze_model(model_id, include_reports=False)

        return json.dumps({
            "model_id": result.model_id,
            "model_name": result.model_name,
            "statistics": result.statistics,
            "overall_score": result.overall_score,
            "grade": result.grade,
            "cached": False
        }, indent=2)

    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
async def get_model_issues(
    workspace_id: str,
    model_id: str,
    severity: Optional[str] = None
) -> str:
    """
    Get issues found in a semantic model, optionally filtered by severity.

    Args:
        workspace_id: Workspace GUID
        model_id: Semantic model GUID
        severity: Optional severity filter (Critical, High, Medium, Low, Info)

    Returns:
        JSON string with issues and recommendations
    """
    try:
        orchestrator = get_orchestrator(workspace_id)

        # Try cached result first
        result = orchestrator.get_cached_result(model_id)
        if not result:
            # Run analysis if not cached
            result = await orchestrator.analyze_model(model_id, include_reports=False)

        issues = result.detailed_issues

        # Filter by severity if specified
        if severity:
            issues = [i for i in issues if i.get("severity") == severity]

        return json.dumps({
            "model_id": result.model_id,
            "model_name": result.model_name,
            "total_issues": len(issues),
            "issues": issues,
            "top_recommendations": result.top_recommendations
        }, indent=2)

    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
async def execute_dax_query(
    workspace_id: str,
    dataset_id: str,
    dax_query: str
) -> str:
    """
    Execute a DAX query against a dataset.

    Args:
        workspace_id: Workspace GUID
        dataset_id: Dataset GUID
        dax_query: DAX query string (e.g., "EVALUATE VALUES('Table'[Column])")

    Returns:
        JSON string with query results

    Note:
        Requires XMLA endpoint access (Premium/PPU capacity)
    """
    try:
        client = get_powerbi_client()
        results = client.execute_dax_query(workspace_id, dataset_id, dax_query)

        return json.dumps({
            "query": dax_query,
            "row_count": len(results),
            "results": results
        }, indent=2)

    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
async def compare_models(
    workspace_id: str,
    model_id_1: str,
    model_id_2: str
) -> str:
    """
    Compare health scores of two semantic models.

    Args:
        workspace_id: Workspace GUID
        model_id_1: First model GUID
        model_id_2: Second model GUID

    Returns:
        JSON string with comparison results
    """
    try:
        orchestrator = get_orchestrator(workspace_id)

        # Analyze both models
        result1 = await orchestrator.analyze_model(model_id_1, include_reports=False)
        result2 = await orchestrator.analyze_model(model_id_2, include_reports=False)

        comparison = {
            "model_1": {
                "id": result1.model_id,
                "name": result1.model_name,
                "score": result1.overall_score,
                "grade": result1.grade,
                "statistics": result1.statistics
            },
            "model_2": {
                "id": result2.model_id,
                "name": result2.model_name,
                "score": result2.overall_score,
                "grade": result2.grade,
                "statistics": result2.statistics
            },
            "score_difference": result1.overall_score - result2.overall_score,
            "better_model": result1.model_name if result1.overall_score > result2.overall_score else result2.model_name
        }

        return json.dumps(comparison, indent=2)

    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
async def get_workspace_health_summary(workspace_id: str) -> str:
    """
    Get health summary for all models in a workspace.

    Args:
        workspace_id: Workspace GUID

    Returns:
        JSON string with workspace health summary
    """
    try:
        orchestrator = get_orchestrator(workspace_id)
        results = await orchestrator.analyze_workspace()

        summary = {
            "workspace_id": workspace_id,
            "workspace_name": orchestrator.workspace_name,
            "total_models": len(results),
            "average_score": sum(r.overall_score for r in results.values()) / len(results) if results else 0,
            "grade_distribution": {},
            "models": []
        }

        # Count grade distribution
        for result in results.values():
            grade = result.grade
            summary["grade_distribution"][grade] = summary["grade_distribution"].get(grade, 0) + 1

            summary["models"].append({
                "id": result.model_id,
                "name": result.model_name,
                "score": result.overall_score,
                "grade": result.grade,
                "critical_issues": result.issues_by_severity.get("Critical", 0),
                "high_issues": result.issues_by_severity.get("High", 0)
            })

        # Sort models by score
        summary["models"].sort(key=lambda x: x["score"], reverse=True)

        return json.dumps(summary, indent=2)

    except Exception as e:
        return json.dumps({"error": str(e)})


# Prompts for common tasks
@mcp.prompt()
async def analyze_model_prompt(workspace_id: str, model_name: str) -> str:
    """
    Generate a prompt to analyze a model by name.

    Args:
        workspace_id: Workspace GUID
        model_name: Model name to search for
    """
    return f"""Please analyze the Power BI model named "{model_name}" in workspace {workspace_id}.

1. First, list all models in the workspace to find the exact model ID
2. Then run a comprehensive health analysis
3. Provide a summary of:
   - Overall health score and grade
   - Top 5 critical or high severity issues
   - Key recommendations for improvement
   - Model statistics (tables, measures, relationships)
"""


@mcp.prompt()
async def workspace_report_prompt(workspace_id: str) -> str:
    """
    Generate a prompt to create a workspace health report.

    Args:
        workspace_id: Workspace GUID
    """
    return f"""Please create a comprehensive health report for workspace {workspace_id}.

Include:
1. List of all semantic models with their health scores
2. Overall workspace health average
3. Distribution of grades (A, B, C, D, F)
4. Top 10 most critical issues across all models
5. Models that need immediate attention (grade D or F)
6. Best performing models (grade A)
"""


if __name__ == "__main__":
    # Run the MCP server
    mcp.run()
