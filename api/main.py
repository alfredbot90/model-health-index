"""
Power BI Model Health API

FastAPI application providing RESTful endpoints for model analysis.
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
import asyncio
from pathlib import Path
import sys

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from core.orchestrator import ModelHealthOrchestrator, AnalysisResult
from core.fabric_client import FabricClient
from core.auth import get_token
from config.settings import CACHE_DIR
from tools.pbix_extractor import scan_cached_pbix, find_similar_reports

# Initialize FastAPI
app = FastAPI(
    title="Power BI Model Health API",
    description="Comprehensive Power BI semantic model analysis API",
    version="7.0.0"
)

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory cache for orchestrators
_orchestrators: Dict[str, ModelHealthOrchestrator] = {}


def get_orchestrator(workspace_id: str) -> ModelHealthOrchestrator:
    """Get or create orchestrator for workspace"""
    if workspace_id not in _orchestrators:
        _orchestrators[workspace_id] = ModelHealthOrchestrator(workspace_id)
    return _orchestrators[workspace_id]


# Request/Response Models
class AnalyzeModelRequest(BaseModel):
    """Request model for model analysis"""
    workspace_id: str = Field(..., description="Workspace GUID")
    model_id: str = Field(..., description="Semantic model GUID")
    include_reports: bool = Field(True, description="Include report binding analysis")
    force_refresh: bool = Field(False, description="Force fresh analysis")


class AnalyzeWorkspaceRequest(BaseModel):
    """Request model for workspace analysis"""
    workspace_id: str = Field(..., description="Workspace GUID")


class ModelHealthResponse(BaseModel):
    """Response model for model health"""
    model_id: str
    model_name: str
    workspace_id: str
    workspace_name: str
    overall_score: int
    grade: str
    grade_description: str
    total_issues: int
    issues_by_severity: Dict[str, int]
    statistics: Dict[str, int]
    analysis_timestamp: str


class WorkspaceHealthResponse(BaseModel):
    """Response model for workspace health"""
    workspace_id: str
    workspace_name: str
    total_models: int
    average_score: float
    grade_distribution: Dict[str, int]
    models: List[Dict[str, Any]]


# Health check endpoints
@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "Power BI Model Health API",
        "version": "7.0.0",
        "status": "running",
        "docs": "/docs"
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Test authentication
        token = get_token()
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "auth": "configured" if token else "not configured"
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Service unhealthy: {str(e)}")


# Workspace endpoints
@app.get("/api/v1/workspaces")
async def list_workspaces():
    """List all accessible workspaces"""
    try:
        client = FabricClient()
        workspaces = client.get_workspaces()

        return {
            "workspaces": [
                {
                    "id": ws.id,
                    "name": ws.name,
                    "type": ws.type,
                    "capacity_id": ws.capacity_id
                }
                for ws in workspaces
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/workspaces/{workspace_id}")
async def get_workspace(workspace_id: str):
    """Get workspace details"""
    try:
        client = FabricClient()
        workspace = client.get_workspace(workspace_id)

        return {
            "id": workspace.id,
            "name": workspace.name,
            "type": workspace.type,
            "capacity_id": workspace.capacity_id
        }
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))


# Model endpoints
@app.get("/api/v1/workspaces/{workspace_id}/models")
async def list_models(workspace_id: str):
    """List semantic models in workspace"""
    try:
        client = FabricClient()
        models = client.get_semantic_models(workspace_id)

        return {
            "models": [
                {
                    "id": model.id,
                    "name": model.name,
                    "description": model.description
                }
                for model in models
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/models/{workspace_id}/{model_id}")
async def get_model(workspace_id: str, model_id: str):
    """Get semantic model details"""
    try:
        client = FabricClient()
        model = client.get_semantic_model(workspace_id, model_id)

        return {
            "id": model.id,
            "name": model.name,
            "workspace_id": model.workspace_id,
            "description": model.description
        }
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))


# Analysis endpoints
@app.post("/api/v1/analyze/model")
async def analyze_model(request: AnalyzeModelRequest, background_tasks: BackgroundTasks):
    """
    Analyze a semantic model

    Returns analysis results including:
    - Overall health score (0-100)
    - Grade (A-F)
    - Detailed issues by category and severity
    - Recommendations
    - Model statistics
    """
    try:
        orchestrator = get_orchestrator(request.workspace_id)

        result = await orchestrator.analyze_model(
            model_id=request.model_id,
            include_reports=request.include_reports,
            force_refresh=request.force_refresh
        )

        return result.to_dict()

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/analyze/workspace")
async def analyze_workspace(request: AnalyzeWorkspaceRequest):
    """
    Analyze all models in a workspace

    Returns health summary for all models
    """
    try:
        orchestrator = get_orchestrator(request.workspace_id)
        results = await orchestrator.analyze_workspace()

        # Build summary
        models_list = []
        total_score = 0
        grade_dist = {}

        for model_id, result in results.items():
            total_score += result.overall_score
            grade = result.grade
            grade_dist[grade] = grade_dist.get(grade, 0) + 1

            models_list.append({
                "id": result.model_id,
                "name": result.model_name,
                "score": result.overall_score,
                "grade": result.grade,
                "critical_issues": result.issues_by_severity.get("Critical", 0),
                "high_issues": result.issues_by_severity.get("High", 0),
                "total_issues": result.total_issues
            })

        # Sort by score descending
        models_list.sort(key=lambda x: x["score"], reverse=True)

        return {
            "workspace_id": request.workspace_id,
            "workspace_name": orchestrator.workspace_name,
            "total_models": len(results),
            "average_score": total_score / len(results) if results else 0,
            "grade_distribution": grade_dist,
            "models": models_list,
            "analysis_timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/analysis/{workspace_id}/{model_id}")
async def get_analysis(workspace_id: str, model_id: str):
    """Get cached analysis result"""
    try:
        orchestrator = get_orchestrator(workspace_id)
        result = orchestrator.get_cached_result(model_id)

        if not result:
            raise HTTPException(
                status_code=404,
                detail="No cached analysis found. Run analysis first."
            )

        return result.to_dict()

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/analysis/{workspace_id}/{model_id}/issues")
async def get_model_issues(
    workspace_id: str,
    model_id: str,
    severity: Optional[str] = None
):
    """Get issues for a model, optionally filtered by severity"""
    try:
        orchestrator = get_orchestrator(workspace_id)
        result = orchestrator.get_cached_result(model_id)

        if not result:
            # Run analysis if not cached
            result = await orchestrator.analyze_model(model_id, include_reports=False)

        issues = result.detailed_issues

        # Filter by severity
        if severity:
            issues = [i for i in issues if i.get("severity") == severity]

        return {
            "model_id": result.model_id,
            "model_name": result.model_name,
            "total_issues": len(issues),
            "issues": issues,
            "top_recommendations": result.top_recommendations
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/analysis/{workspace_id}/{model_id}/statistics")
async def get_model_statistics(workspace_id: str, model_id: str):
    """Get model statistics"""
    try:
        orchestrator = get_orchestrator(workspace_id)
        result = orchestrator.get_cached_result(model_id)

        if not result:
            # Run quick analysis
            result = await orchestrator.analyze_model(model_id, include_reports=False)

        return {
            "model_id": result.model_id,
            "model_name": result.model_name,
            "statistics": result.statistics,
            "overall_score": result.overall_score,
            "grade": result.grade
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Report cache utilities
@app.get("/api/v1/reports/cached")
async def list_cached_reports():
    """List parsed structure of cached PBIX files in the cache directory."""
    try:
        reports = scan_cached_pbix(str(CACHE_DIR))
        return {
            "count": len(reports),
            "reports": [
                {
                    "report_id": r.get("report_id"),
                    "pbix_path": r.get("pbix_path"),
                    "pages": r.get("pages"),
                    "visuals": r.get("visuals"),
                    "unique_bindings": r.get("unique_bindings"),
                }
                for r in reports
            ],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/reports/{report_id}/similar")
async def get_similar_reports(report_id: str, top_k: int = 5):
    """Find similar reports in cache by layout bindings similarity (Jaccard)."""
    try:
        sims = find_similar_reports(str(CACHE_DIR), report_id, top_k)
        return {"report_id": report_id, "similar": sims}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Report generation endpoints
@app.get("/api/v1/reports/{workspace_id}/{model_id}/json")
async def get_json_report(workspace_id: str, model_id: str):
    """Get JSON report for model analysis"""
    try:
        orchestrator = get_orchestrator(workspace_id)
        result = orchestrator.get_cached_result(model_id)

        if not result:
            raise HTTPException(status_code=404, detail="Analysis not found")

        return result.to_dict()

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/reports/{workspace_id}/{model_id}/pdf")
async def get_pdf_report(workspace_id: str, model_id: str):
    """Get PDF report for model analysis"""
    try:
        # Import here to avoid circular dependency
        from reports.pdf_generator import PDFReportGenerator

        orchestrator = get_orchestrator(workspace_id)
        result = orchestrator.get_cached_result(model_id)

        if not result:
            raise HTTPException(status_code=404, detail="Analysis not found")

        # Generate PDF
        generator = PDFReportGenerator()
        pdf_path = generator.generate(result)

        return FileResponse(
            pdf_path,
            media_type="application/pdf",
            filename=f"{result.model_name}_analysis.pdf"
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Comparison endpoints
@app.post("/api/v1/compare/models")
async def compare_models(
    workspace_id: str,
    model_id_1: str,
    model_id_2: str
):
    """Compare two models"""
    try:
        orchestrator = get_orchestrator(workspace_id)

        result1 = await orchestrator.analyze_model(model_id_1, include_reports=False)
        result2 = await orchestrator.analyze_model(model_id_2, include_reports=False)

        return {
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

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
