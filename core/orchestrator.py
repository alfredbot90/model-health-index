"""
Model Health Orchestrator

Main orchestration service that coordinates all analysis components.
Uses TOM (Tabular Object Model) via .NET tools as the primary extraction method.
"""

import json
import asyncio
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, asdict
from datetime import datetime
import logging

from .fabric_client import FabricClient
from .powerbi_client import PowerBIClient
from .auth import get_token

# Import analyzers (these will use the extracted data)
import sys
sys.path.append(str(Path(__file__).parent.parent))

from analyzers.semantic_analyzer import EnhancedSemanticModelAnalyzer
from analyzers.mcode_analyzer import EnhancedMCodeAnalyzer
from analyzers.unified_analyzer import UnifiedPowerBIAnalyzer
from tools.tmdl_client import TmdlClient
from tools.pbix_extractor import extract_bindings_from_layout, read_layout_from_pbix
from config.settings import TMDL_TOOLS_PATH

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class AnalysisResult:
    """Complete analysis result"""
    model_id: str
    model_name: str
    workspace_id: str
    workspace_name: str

    # Overall scores
    overall_score: int
    grade: str
    grade_description: str

    # Category scores
    category_scores: Dict[str, Dict]

    # Issues
    total_issues: int
    issues_by_severity: Dict[str, int]
    detailed_issues: List[Dict[str, Any]]

    # Recommendations
    top_recommendations: List[str]

    # Model statistics
    statistics: Dict[str, int]

    # Additional metadata
    tmdl_path: Optional[str] = None
    report_bindings: Optional[List[Dict]] = None
    analysis_timestamp: str = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return asdict(self)


class ModelHealthOrchestrator:
    """
    Main orchestration service for model health analysis

    Workflow:
    1. Download TMDL using TOM (.NET tools) - PRIMARY METHOD
    2. Parse TMDL to extract model structure
    3. Run semantic model analysis
    4. Run M-Code analysis
    5. Optionally analyze report bindings (PBIX)
    6. Combine results into unified report
    """

    def __init__(
        self,
        workspace_id: str,
        cache_dir: str = "cache",
        tmdl_tools_path: Optional[str] = None
    ):
        """
        Initialize orchestrator

        Args:
            workspace_id: Workspace GUID
            cache_dir: Directory for caching analysis results
            tmdl_tools_path: Path to TmdlTools.exe (optional, will use default)
        """
        self.workspace_id = workspace_id
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)

        # Initialize clients
        token = get_token()
        self.fabric_client = FabricClient(token)
        self.powerbi_client = PowerBIClient(token)

        # Initialize TOM-based TMDL client (PRIMARY EXTRACTION METHOD)
        # Prefer explicit path argument, else config default
        resolved_tmdl_path = tmdl_tools_path or str(TMDL_TOOLS_PATH)
        self.tmdl_client = TmdlClient(exe_path=resolved_tmdl_path)

        # Initialize analyzers
        self.semantic_analyzer = EnhancedSemanticModelAnalyzer()
        self.mcode_analyzer = EnhancedMCodeAnalyzer()
        self.unified_analyzer = UnifiedPowerBIAnalyzer()

        # Get workspace info
        try:
            workspace = self.fabric_client.get_workspace(workspace_id)
            self.workspace_name = workspace.name
        except Exception as e:
            logger.warning(f"Could not get workspace name: {e}")
            self.workspace_name = workspace_id

    async def analyze_model(
        self,
        model_id: str,
        include_reports: bool = True,
        force_refresh: bool = False
    ) -> AnalysisResult:
        """
        Analyze a semantic model

        Args:
            model_id: Semantic model GUID
            include_reports: Include report binding analysis
            force_refresh: Force fresh download (ignore cache)

        Returns:
            Complete analysis result
        """
        logger.info(f"Starting analysis for model {model_id}")

        # Get model info
        try:
            model_info = self.fabric_client.get_semantic_model(
                self.workspace_id,
                model_id
            )
            model_name = model_info.name
        except Exception as e:
            logger.warning(f"Could not get model name: {e}")
            model_name = model_id

        # Step 1: Download TMDL using TOM (.NET tools)
        logger.info("Downloading TMDL using TOM...")
        tmdl_dir = self.cache_dir / f"{model_id}_tmdl"

        try:
            if force_refresh or not tmdl_dir.exists():
                tmdl_path = self.tmdl_client.download_tmdl(
                    workspace_id=self.workspace_id,
                    semantic_model_id=model_id,
                    out_dir=str(tmdl_dir)
                )
                logger.info(f"TMDL downloaded to: {tmdl_path}")
            else:
                logger.info(f"Using cached TMDL from: {tmdl_dir}")
                tmdl_path = str(tmdl_dir)
        except Exception as e:
            logger.error(f"TMDL download failed: {e}")
            raise RuntimeError(f"Could not download TMDL: {e}")

        # Step 2: Find and parse a rich model definition (prefer JSON when available)
        model_file = self._find_model_file(tmdl_dir)
        if not model_file:
            raise RuntimeError(f"No model file found in {tmdl_dir}")

        logger.info(f"Parsing model file: {model_file}")

        # Step 3: Run semantic model analysis on TMDL
        logger.info("Running semantic model analysis...")
        semantic_result = self.semantic_analyzer.analyze_tmdl_export(str(model_file))

        if "error" in semantic_result:
            raise RuntimeError(f"Semantic analysis failed: {semantic_result['error']}")

        # Step 4: Extract and analyze M-Code from model
        logger.info("Analyzing M-Code...")
        mcode_issues = []

        # Parse TMDL to extract M-Code expressions
        mcode_queries = self._extract_mcode_from_tmdl(model_file)

        for query_name, mcode in mcode_queries.items():
            analysis = self.mcode_analyzer.analyze_query(mcode, query_name)
            if analysis.get("issues"):
                mcode_issues.extend(analysis["issues"])

        # Step 5: Optionally analyze report bindings
        report_bindings = None
        if include_reports:
            logger.info("Analyzing report bindings...")
            report_bindings = await self._analyze_report_bindings(model_id)

        # Step 6: Combine results
        logger.info("Combining analysis results...")

        # Merge issues from all sources
        all_issues = semantic_result.get("issues", [])

        # Convert M-Code issues to standard format
        for issue in mcode_issues:
            all_issues.append({
                "category": "M-Code Performance",
                "severity": issue.get("severity", "Medium"),
                "title": issue.get("title", "M-Code Issue"),
                "description": issue.get("description", ""),
                "recommendation": issue.get("recommendation", ""),
                "impact_score": issue.get("impact_score", 3),
                "location": issue.get("location", "")
            })

        # Recalculate combined score
        combined_score = self._calculate_combined_score(semantic_result, mcode_issues)

        # Generate final result
        result = AnalysisResult(
            model_id=model_id,
            model_name=model_name,
            workspace_id=self.workspace_id,
            workspace_name=self.workspace_name,
            overall_score=combined_score["score"],
            grade=combined_score["grade"],
            grade_description=combined_score["description"],
            category_scores=semantic_result.get("category_scores", {}),
            total_issues=len(all_issues),
            issues_by_severity=self._count_by_severity(all_issues),
            detailed_issues=all_issues,
            top_recommendations=self._get_top_recommendations(all_issues),
            statistics=semantic_result.get("statistics", {}),
            tmdl_path=str(tmdl_path),
            report_bindings=report_bindings,
            analysis_timestamp=datetime.now().isoformat()
        )

        # Cache result
        self._cache_result(model_id, result)

        logger.info(f"Analysis complete. Score: {result.overall_score}/100 ({result.grade})")
        return result

    async def analyze_workspace(self) -> Dict[str, AnalysisResult]:
        """
        Analyze all semantic models in workspace

        Returns:
            Dictionary mapping model_id to AnalysisResult
        """
        logger.info(f"Analyzing workspace {self.workspace_id}")

        # Get all models
        models = self.fabric_client.get_semantic_models(self.workspace_id)
        logger.info(f"Found {len(models)} models to analyze")

        # Analyze each model
        results = {}
        for model in models:
            try:
                result = await self.analyze_model(model.id)
                results[model.id] = result
            except Exception as e:
                logger.error(f"Failed to analyze model {model.id}: {e}")
                continue

        return results

    def _find_model_file(self, tmdl_dir: Path) -> Optional[Path]:
        """Find the best available model definition inside the TMDL export.

        Preference order:
        1) definition.raw.json (full JSON definition if present)
        2) database.json (classic TMSL JSON)
        3) model.tmdl (may omit per-table details; handled by analyzer heuristics)
        4) Any *.json containing database/model
        5) Any *.tmdl containing model
        """
        # 1) Prefer full raw JSON if present
        raw_json = tmdl_dir / "definition.raw.json"
        if raw_json.exists():
            return raw_json

        # 2) Classic TMSL JSON
        database_json = tmdl_dir / "database.json"
        if database_json.exists():
            return database_json

        # 3) TMDL root file
        model_tmdl = tmdl_dir / "model.tmdl"
        if model_tmdl.exists():
            return model_tmdl

        # 4) Search JSONs
        for file in tmdl_dir.rglob("*.json"):
            if "database" in file.name.lower() or "model" in file.name.lower():
                return file

        # 5) Search TMDL files
        for file in tmdl_dir.rglob("*.tmdl"):
            if "model" in file.name.lower():
                return file

        return None

    def _extract_mcode_from_tmdl(self, model_file: Path) -> Dict[str, str]:
        """Extract M-Code queries from TMDL/TMSL file"""
        queries = {}

        if model_file.suffix == ".tmdl":
            # Parse TMDL format
            content = model_file.read_text(encoding="utf-8")

            # Extract partition expressions (simplified regex)
            import re
            partition_pattern = r'partition\s+([^\n]+).*?expression\s*=\s*```(.*?)```'
            matches = re.finditer(partition_pattern, content, re.DOTALL)

            for match in matches:
                partition_name = match.group(1).strip()
                expression = match.group(2).strip()
                queries[partition_name] = expression

        elif model_file.suffix == ".json":
            # Parse TMSL format
            with open(model_file, 'r', encoding='utf-8') as f:
                model = json.load(f)

            # Extract M-Code from partitions
            for table in model.get("model", {}).get("tables", []):
                table_name = table.get("name", "")
                for partition in table.get("partitions", []):
                    partition_name = partition.get("name", "")
                    source = partition.get("source", {})
                    if source.get("type") == "m":
                        expression = source.get("expression", "")
                        if expression:
                            queries[f"{table_name}.{partition_name}"] = expression

        return queries

    async def _analyze_report_bindings(
        self,
        model_id: str
    ) -> Optional[List[Dict]]:
        """Analyze report bindings (which visuals use which measures)"""
        try:
            # Get reports using this model
            reports = self.powerbi_client.get_reports(self.workspace_id)
            model_reports = [r for r in reports if r.dataset_id == model_id]

            if not model_reports:
                return None

            all_bindings = []

            for report in model_reports[:5]:  # Limit to 5 reports
                try:
                    # Download PBIX
                    pbix_path = self.cache_dir / f"{report.id}.pbix"
                    if not pbix_path.exists():
                        self.powerbi_client.export_report_pbix(
                            self.workspace_id,
                            report.id,
                            str(pbix_path)
                        )

                    # Extract bindings
                    layout = read_layout_from_pbix(str(pbix_path))
                    bindings = extract_bindings_from_layout(layout)

                    all_bindings.extend(bindings)

                except Exception as e:
                    logger.warning(f"Could not analyze report {report.id}: {e}")
                    continue

            return all_bindings if all_bindings else None

        except Exception as e:
            logger.warning(f"Report binding analysis failed: {e}")
            return None

    def _calculate_combined_score(
        self,
        semantic_result: Dict,
        mcode_issues: List[Dict]
    ) -> Dict[str, Any]:
        """Calculate combined score from all analysis dimensions"""
        base_score = semantic_result.get("score", 0)

        # Deduct points for M-Code issues
        mcode_penalty = sum(
            issue.get("impact_score", 3) * 0.1  # 10% weight for M-Code
            for issue in mcode_issues
        )

        combined_score = max(0, base_score - mcode_penalty)

        # Determine grade
        if combined_score >= 90:
            grade = "A"
            description = "Excellent"
        elif combined_score >= 80:
            grade = "B"
            description = "Good"
        elif combined_score >= 70:
            grade = "C"
            description = "Average"
        elif combined_score >= 60:
            grade = "D"
            description = "Below Average"
        else:
            grade = "F"
            description = "Poor"

        return {
            "score": int(combined_score),
            "grade": grade,
            "description": description
        }

    def _count_by_severity(self, issues: List[Dict]) -> Dict[str, int]:
        """Count issues by severity"""
        counts = {"Critical": 0, "High": 0, "Medium": 0, "Low": 0, "Info": 0}
        for issue in issues:
            severity = issue.get("severity", "Medium")
            if severity in counts:
                counts[severity] += 1
        return counts

    def _get_top_recommendations(
        self,
        issues: List[Dict],
        limit: int = 5
    ) -> List[str]:
        """Get top recommendations from issues"""
        # Sort by impact score
        sorted_issues = sorted(
            issues,
            key=lambda x: (
                {"Critical": 4, "High": 3, "Medium": 2, "Low": 1, "Info": 0}.get(
                    x.get("severity", "Medium"), 0
                ),
                x.get("impact_score", 0)
            ),
            reverse=True
        )

        recommendations = []
        for issue in sorted_issues[:limit]:
            rec = issue.get("recommendation", "")
            if rec and rec not in recommendations:
                recommendations.append(rec)

        return recommendations

    def _cache_result(self, model_id: str, result: AnalysisResult) -> None:
        """Cache analysis result"""
        cache_file = self.cache_dir / f"{model_id}_result.json"
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(result.to_dict(), f, indent=2)

    def get_cached_result(self, model_id: str) -> Optional[AnalysisResult]:
        """Get cached analysis result"""
        cache_file = self.cache_dir / f"{model_id}_result.json"
        if cache_file.exists():
            with open(cache_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return AnalysisResult(**data)
        return None
