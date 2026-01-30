#!/usr/bin/env python3
"""
Unified Power BI Model Analyzer
===============================

This module ties together the semantic model reviewer and the enhanced
M‑code analyzer to provide a single entry point for analysing an entire
Power BI dataset exported as JSON. It extracts all M‑code partitions from
the model and runs them through the ``EnhancedMCodeAnalyzer`` while
simultaneously running the ``JSONSemanticModelReviewer`` on the same
model. The results are merged into a unified report with a combined
score, grade and consolidated list of issues.

The scoring weights for each high‑level category are inspired by
community best practices: model design, relationships and DAX measures
each account for 15% of the overall score; M‑code performance, query
folding and security collectively contribute 40%; documentation and
naming conventions round out the remainder. These weights can be
adjusted as required.

Example usage::

    from unified_powerbi_analyzer import UnifiedPowerBIAnalyzer
    analyzer = UnifiedPowerBIAnalyzer()
    report = analyzer.analyze_model('model.json')
    # ``report`` contains combined scores, grade and issues
    # Pass it to the polished report generator to produce a PDF

"""

from __future__ import annotations

import json
import os
from typing import Dict, Any, List, Tuple

# Add current directory to Python path for imports
import sys
import os
import re

# Use package-relative imports to reference sibling analyzer modules
from .semantic_analyzer import EnhancedSemanticModelAnalyzer
from .mcode_analyzer import EnhancedMCodeAnalyzer


def extract_mcode_from_json(model: Dict[str, Any]) -> Dict[str, str]:
    """Extract M‑code expressions from a semantic model definition.

    The Fabric REST API includes M‑code in the ``source`` field of table
    partitions. This helper iterates through all tables and partitions to
    collect these expressions. The resulting dictionary maps a
    ``query_name`` of the form ``TableName.PartitionName`` to the M‑code
    expression itself. Only partitions whose ``source`` has ``type`` set
    to ``'m'`` are considered.

    Parameters
    ----------
    model : dict
        The loaded semantic model JSON.

    Returns
    -------
    dict
        A mapping of ``query_name`` → ``M‑code`` strings.
    """
    queries: Dict[str, str] = {}
    tables = model.get('tables', [])
    for table in tables:
        tname = table.get('name', '')
        for partition in table.get('partitions', []):
            pname = partition.get('name', '')
            source = partition.get('source', {}) or {}
            if source.get('type') == 'm':
                expr = source.get('expression', '') or ''
                if expr:
                    queries[f"{tname}.{pname}"] = expr
    return queries


class UnifiedPowerBIAnalyzer:
    """High‑level analyzer that combines semantic model and M‑code analysis."""

    # Default category weights for the unified score. These can be
    # overridden by passing a ``weights`` dict to the constructor.
    DEFAULT_WEIGHTS = {
        'Model Design': 0.15,
        'Relationships': 0.15,
        'DAX Measures': 0.15,
        'M‑Code Performance': 0.20,
        'Query Folding': 0.15,
        'Documentation': 0.10,
        'Naming': 0.05,
        'Security': 0.05
    }

    def __init__(self, weights: Dict[str, float] | None = None) -> None:
        self.json_reviewer = EnhancedSemanticModelAnalyzer()
        self.mcode_analyzer = EnhancedMCodeAnalyzer()
        # Normalise weights to sum to 1
        if weights:
            total = sum(weights.values())
            self.weights = {k: (v / total if total else 0) for k, v in weights.items()}
        else:
            self.weights = UnifiedPowerBIAnalyzer.DEFAULT_WEIGHTS.copy()

    def analyze_model(self, json_path: str) -> Dict[str, Any]:
        """Analyse a semantic model JSON file and return a unified report.

        Parameters
        ----------
        json_path : str
            Path to the model JSON file. If the file does not exist or
            cannot be parsed, an error entry is returned.

        Returns
        -------
        dict
            A unified report with combined scores, grade, lists of issues
            (including those from M‑code), statistics and top
            recommendations.
        """
        if not os.path.exists(json_path):
            return {'error': f'File not found: {json_path}'}
        # Load the model
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                model = json.load(f)
        except Exception as exc:
            return {'error': f'Failed to parse JSON: {exc}'}

        # Run semantic model analysis
        model_report = self.json_reviewer.analyze_tmdl_export(json_path)
        if 'error' in model_report:
            return model_report

        # Extract M‑code queries and analyse them
        mcode_queries = extract_mcode_from_json(model)
        query_analyses: Dict[str, Dict[str, Any]] = {}
        # Reset mcode analyzer state before analysis
        self.mcode_analyzer.issues = []
        for qname, mcode in mcode_queries.items():
            analysis = self.mcode_analyzer._analyze_query(qname, mcode)
            query_analyses[qname] = analysis
        # Compute M‑code report (score and category breakdown)
        mcode_score = self.mcode_analyzer._calculate_score()
        mcode_report = self.mcode_analyzer._generate_report(mcode_score, query_analyses)

        # Merge issues from both reports
        combined_issues: List[Dict[str, Any]] = []
        # Normalise JSON reviewer issue fields
        for iss in model_report.get('issues', []):
            combined_issues.append({
                'source': 'Model',
                'category': iss.get('category'),
                'severity': iss.get('severity'),
                'title': iss.get('title'),
                'description': iss.get('description'),
                'recommendation': iss.get('recommendation'),
                'impact_score': iss.get('impact_score', 0),
                'location': iss.get('location'),
                'context': iss.get('location')
            })
        # Normalize M‑code issues
        for iss in mcode_report.get('issues', []):
            combined_issues.append({
                'source': 'M‑Code',
                'category': iss.get('category'),
                'severity': iss.get('severity'),
                'title': iss.get('title'),
                'description': iss.get('description'),
                'recommendation': iss.get('recommendation'),
                'impact_score': iss.get('impact_score', 0),
                'location': iss.get('query_name'),
                'context': iss.get('query_name')
            })

        # Determine category percentages for the unified score
        # Semantic model categories
        model_categories = model_report.get('category_scores', {})
        # M‑code categories
        mcode_categories = mcode_report.get('category_scores', {})
        # Extract percentages for each unified category
        def pct(cat: str, default: int = 100) -> float:
            return model_categories.get(cat, {}).get('percentage', default)
        def m_pct(cat: str, default: int = 100) -> float:
            return mcode_categories.get(cat, {}).get('percentage', default)

        # Map unified categories to percentages from sub‑reports
        unified_pct: Dict[str, float] = {
            'Model Design': pct('Design'),
            'Relationships': pct('Relationships'),
            'DAX Measures': pct('Measures'),
            'Documentation': pct('Documentation'),
            'Naming': pct('Naming'),
            # M‑code categories: map performance, folding and security
            'M‑Code Performance': m_pct('Performance'),
            'Query Folding': m_pct('Query Folding'),
            'Security': m_pct('Security'),
        }

        # Compute weighted unified score (out of 100)
        unified_score = 0.0
        for cat, weight in self.weights.items():
            pct_value = unified_pct.get(cat, 100)
            unified_score += (pct_value / 100.0) * weight * 100
        unified_score = max(0, round(unified_score))

        # Determine grade from unified score
        if unified_score >= 90:
            grade, grade_desc = 'A', 'Excellent'
        elif unified_score >= 75:
            grade, grade_desc = 'B', 'Good'
        elif unified_score >= 60:
            grade, grade_desc = 'C', 'Fair'
        elif unified_score >= 50:
            grade, grade_desc = 'D', 'Poor'
        else:
            grade, grade_desc = 'F', 'Failing'

        # Combine statistics: use model stats and add M‑code stats
        stats = model_report.get('statistics', {}).copy()
        stats['mcode_queries'] = mcode_report.get('statistics', {}).get('total_queries', 0)
        stats['mcode_steps'] = mcode_report.get('statistics', {}).get('total_steps', 0)
        stats['mcode_folding_issues'] = mcode_report.get('statistics', {}).get('queries_with_folding_issues', 0)

        # Top recommendations: combine and de‑duplicate from both sources
        top_recs: List[str] = []
        for rec in model_report.get('top_recommendations', []):
            if rec not in top_recs:
                top_recs.append(rec)
        for rec in mcode_report.get('top_recommendations', []):
            if rec not in top_recs:
                top_recs.append(rec)
        # Limit to 10 recommendations
        top_recs = top_recs[:10]

        return {
            'score': unified_score,
            'max_score': 100,
            'grade': grade,
            'grade_description': grade_desc,
            'statistics': stats,
            'category_scores': unified_pct,
            'issues': combined_issues,
            'top_recommendations': top_recs,
            # Include sub‑reports for optional deeper inspection
            'model_report': model_report,
            'mcode_report': mcode_report
        }