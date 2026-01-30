"""
Copilot & AI Readiness Analyzer

Checks semantic models for optimization with Power BI Copilot and Fabric IQ.
Based on Microsoft's guidance for AI-optimized semantic models (2025).

References:
- https://community.fabric.microsoft.com/t5/Power-BI-Community-Blog/Optimizing-Semantic-Models-for-Copilot-Best-Practices-and-Why/ba-p/4850173
- Microsoft Fabric IQ (Ignite 2025)
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from enum import Enum


class AIReadinessCategory(Enum):
    NAVIGATION = "Navigation & Discovery"
    DOCUMENTATION = "Documentation & Descriptions"
    NAMING = "Naming Clarity"
    COMPLEXITY = "Model Complexity"
    DATA_QUALITY = "Data Quality"
    RELATIONSHIPS = "Relationship Clarity"


class AIReadinessSeverity(Enum):
    CRITICAL = "Critical"  # Copilot won't work well
    HIGH = "High"  # Significant impact on AI accuracy
    MEDIUM = "Medium"  # May cause confusion
    LOW = "Low"  # Minor improvement
    INFO = "Info"  # Suggestion


@dataclass
class AIReadinessIssue:
    category: AIReadinessCategory
    severity: AIReadinessSeverity
    title: str
    description: str
    recommendation: str
    impact_score: int
    object_type: str
    object_name: str
    table_name: Optional[str] = None


class CopilotAnalyzer:
    """
    Analyzes semantic models for Copilot and AI readiness.
    
    Key factors for Copilot success:
    1. Clear, descriptive names for all objects
    2. Comprehensive descriptions on tables, columns, measures
    3. Simple, navigable model structure
    4. Proper data categorization
    5. Minimal ambiguity in relationships
    6. Well-organized display folders
    """
    
    def __init__(self):
        self.issues: List[AIReadinessIssue] = []
        self.max_score = 100
    
    def analyze_model(self, model_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze a semantic model for Copilot/AI readiness.
        
        Args:
            model_data: Parsed model data with tables, relationships, measures, etc.
        
        Returns:
            Analysis report with score, issues, and recommendations.
        """
        self.issues = []
        
        # Run all checks
        self._check_table_documentation(model_data)
        self._check_column_documentation(model_data)
        self._check_measure_documentation(model_data)
        self._check_naming_clarity(model_data)
        self._check_model_complexity(model_data)
        self._check_relationship_clarity(model_data)
        self._check_display_folders(model_data)
        self._check_data_categories(model_data)
        self._check_hidden_objects(model_data)
        self._check_synonyms(model_data)
        self._check_aggregation_guidance(model_data)
        
        # Calculate score
        score = self._calculate_score()
        
        # Generate report
        return self._generate_report(score, model_data)
    
    def _check_table_documentation(self, model_data: Dict) -> None:
        """Check if tables have descriptions for Copilot understanding."""
        tables = model_data.get('tables', [])
        tables_without_desc = []
        
        for table in tables:
            if not table.get('is_hidden', False):
                name = table.get('name', '')
                description = table.get('description', '')
                
                if not description or len(description) < 10:
                    tables_without_desc.append(name)
        
        if tables_without_desc:
            ratio = len(tables_without_desc) / max(len(tables), 1)
            severity = AIReadinessSeverity.HIGH if ratio > 0.5 else AIReadinessSeverity.MEDIUM
            
            self.issues.append(AIReadinessIssue(
                category=AIReadinessCategory.DOCUMENTATION,
                severity=severity,
                title="Tables Missing Descriptions",
                description=f"{len(tables_without_desc)} of {len(tables)} tables lack descriptions. "
                           f"Copilot uses descriptions to understand table purpose.",
                recommendation="Add descriptions to all tables explaining what data they contain "
                             "and how they should be used in business context.",
                impact_score=6 if ratio > 0.5 else 4,
                object_type="Tables",
                object_name=", ".join(tables_without_desc[:5]) + ("..." if len(tables_without_desc) > 5 else "")
            ))
    
    def _check_column_documentation(self, model_data: Dict) -> None:
        """Check if key columns have descriptions."""
        total_columns = 0
        undocumented_columns = []
        
        for table in model_data.get('tables', []):
            table_name = table.get('name', '')
            for column in table.get('columns', []):
                if not column.get('is_hidden', False):
                    total_columns += 1
                    description = column.get('description', '')
                    
                    if not description:
                        undocumented_columns.append(f"{table_name}[{column.get('name', '')}]")
        
        if undocumented_columns and total_columns > 10:
            ratio = len(undocumented_columns) / total_columns
            
            if ratio > 0.7:  # More than 70% undocumented
                self.issues.append(AIReadinessIssue(
                    category=AIReadinessCategory.DOCUMENTATION,
                    severity=AIReadinessSeverity.HIGH,
                    title="Columns Missing Descriptions",
                    description=f"{len(undocumented_columns)} visible columns lack descriptions. "
                               f"Copilot needs column descriptions to generate accurate queries.",
                    recommendation="Add descriptions to columns explaining what data they contain, "
                                 "valid values, and business meaning.",
                    impact_score=6,
                    object_type="Columns",
                    object_name=f"{len(undocumented_columns)} columns"
                ))
    
    def _check_measure_documentation(self, model_data: Dict) -> None:
        """Check if measures have descriptions - critical for Copilot."""
        total_measures = 0
        undocumented_measures = []
        
        for table in model_data.get('tables', []):
            table_name = table.get('name', '')
            for measure in table.get('measures', []):
                total_measures += 1
                description = measure.get('description', '')
                
                # Also check for comments in the expression
                expression = measure.get('expression', '')
                has_inline_docs = '//' in expression or '/*' in expression
                
                if not description and not has_inline_docs:
                    undocumented_measures.append(f"{table_name}[{measure.get('name', '')}]")
        
        if undocumented_measures:
            ratio = len(undocumented_measures) / max(total_measures, 1)
            severity = AIReadinessSeverity.CRITICAL if ratio > 0.5 else AIReadinessSeverity.HIGH
            
            self.issues.append(AIReadinessIssue(
                category=AIReadinessCategory.DOCUMENTATION,
                severity=severity,
                title="Measures Missing Descriptions",
                description=f"{len(undocumented_measures)} of {total_measures} measures lack descriptions. "
                           f"This is CRITICAL for Copilot - it needs to understand what each measure calculates.",
                recommendation="Add business-friendly descriptions to all measures explaining what they calculate, "
                             "when to use them, and any important caveats.",
                impact_score=8 if ratio > 0.5 else 5,
                object_type="Measures",
                object_name=", ".join(undocumented_measures[:5]) + ("..." if len(undocumented_measures) > 5 else "")
            ))
    
    def _check_naming_clarity(self, model_data: Dict) -> None:
        """Check if object names are clear and descriptive."""
        unclear_names = []
        
        # Patterns that suggest unclear naming
        unclear_patterns = [
            ('_', 'underscore separators'),
            ('tbl', 'table prefix'),
            ('dim', 'dimension prefix'),
            ('fct', 'fact prefix'),
            ('Dim_', 'dimension prefix'),
            ('Fact_', 'fact prefix'),
            ('ID', 'technical ID suffix'),
            ('FK', 'foreign key suffix'),
            ('PK', 'primary key suffix'),
        ]
        
        # Check for abbreviations that Copilot might not understand
        common_abbreviations = ['qty', 'amt', 'num', 'cnt', 'val', 'pct', 'yr', 'mo', 'dt']
        
        for table in model_data.get('tables', []):
            table_name = table.get('name', '')
            
            # Check for abbreviations in table names
            for abbr in common_abbreviations:
                if abbr.lower() in table_name.lower():
                    unclear_names.append((table_name, 'Table', f"contains abbreviation '{abbr}'"))
            
            # Check measures for unclear names
            for measure in table.get('measures', []):
                measure_name = measure.get('name', '')
                if len(measure_name) < 4:  # Too short
                    unclear_names.append((measure_name, 'Measure', "name too short"))
                
                for abbr in common_abbreviations:
                    if abbr.lower() == measure_name.lower() or measure_name.lower().startswith(abbr.lower()):
                        unclear_names.append((measure_name, 'Measure', f"uses abbreviation '{abbr}'"))
        
        if unclear_names and len(unclear_names) > 3:
            self.issues.append(AIReadinessIssue(
                category=AIReadinessCategory.NAMING,
                severity=AIReadinessSeverity.MEDIUM,
                title="Objects With Unclear Names",
                description=f"Found {len(unclear_names)} objects with abbreviations or technical naming. "
                           f"Copilot works best with natural language names.",
                recommendation="Use full, descriptive names: 'Quantity' instead of 'Qty', "
                             "'Sales Amount' instead of 'SalesAmt'. Add synonyms for common abbreviations.",
                impact_score=4,
                object_type="Various",
                object_name=", ".join([f"{n[0]} ({n[2]})" for n in unclear_names[:5]])
            ))
    
    def _check_model_complexity(self, model_data: Dict) -> None:
        """Check if model is too complex for Copilot to navigate effectively."""
        tables = model_data.get('tables', [])
        relationships = model_data.get('relationships', [])
        
        table_count = len(tables)
        relationship_count = len(relationships)
        
        # Count total measures
        total_measures = sum(len(t.get('measures', [])) for t in tables)
        
        # Complexity thresholds
        if table_count > 50:
            self.issues.append(AIReadinessIssue(
                category=AIReadinessCategory.COMPLEXITY,
                severity=AIReadinessSeverity.HIGH,
                title="Very Large Model",
                description=f"Model has {table_count} tables. Large models can overwhelm Copilot's navigation.",
                recommendation="Consider splitting into multiple focused models, or ensure excellent documentation "
                             "and display folders to help Copilot navigate.",
                impact_score=5,
                object_type="Model",
                object_name="Model"
            ))
        
        if total_measures > 200:
            self.issues.append(AIReadinessIssue(
                category=AIReadinessCategory.COMPLEXITY,
                severity=AIReadinessSeverity.HIGH,
                title="Very Large Number of Measures",
                description=f"Model has {total_measures} measures. This can make it hard for Copilot to select the right one.",
                recommendation="Organize measures into display folders by business area. "
                             "Add clear descriptions to help Copilot differentiate between similar measures.",
                impact_score=5,
                object_type="Model",
                object_name="Model"
            ))
        
        # Check for snowflake complexity
        tables_as_both = set()
        for rel in relationships:
            from_t = rel.get('from_table', '')
            to_t = rel.get('to_table', '')
            if any(r.get('to_table') == from_t for r in relationships if r != rel):
                tables_as_both.add(from_t)
        
        if len(tables_as_both) > 3:
            self.issues.append(AIReadinessIssue(
                category=AIReadinessCategory.COMPLEXITY,
                severity=AIReadinessSeverity.MEDIUM,
                title="Complex Snowflake Schema",
                description=f"Model has chained dimension relationships (snowflake). "
                           f"This adds complexity for Copilot's relationship navigation.",
                recommendation="Document the relationship chain clearly. Consider flattening to star schema "
                             "or add detailed descriptions on how tables relate.",
                impact_score=3,
                object_type="Relationships",
                object_name=", ".join(list(tables_as_both)[:5])
            ))
    
    def _check_relationship_clarity(self, model_data: Dict) -> None:
        """Check if relationships are clear and unambiguous."""
        relationships = model_data.get('relationships', [])
        
        # Check for inactive relationships (can confuse Copilot)
        inactive_rels = [r for r in relationships if not r.get('is_active', True)]
        if inactive_rels:
            self.issues.append(AIReadinessIssue(
                category=AIReadinessCategory.RELATIONSHIPS,
                severity=AIReadinessSeverity.MEDIUM,
                title="Inactive Relationships Present",
                description=f"Model has {len(inactive_rels)} inactive relationships. "
                           f"Copilot may not correctly handle USERELATIONSHIP scenarios.",
                recommendation="Document when to use each inactive relationship. "
                             "Consider creating separate measures for each date context.",
                impact_score=3,
                object_type="Relationships",
                object_name=f"{len(inactive_rels)} inactive"
            ))
        
        # Check for many-to-many (ambiguous for Copilot)
        m2m_rels = [r for r in relationships if r.get('cardinality') == 'many-to-many']
        if m2m_rels:
            self.issues.append(AIReadinessIssue(
                category=AIReadinessCategory.RELATIONSHIPS,
                severity=AIReadinessSeverity.HIGH,
                title="Many-to-Many Relationships",
                description=f"Model has {len(m2m_rels)} many-to-many relationships. "
                           f"These can produce unexpected results and confuse Copilot.",
                recommendation="Document the expected behavior clearly. "
                             "Consider using bridge tables for clearer semantics.",
                impact_score=4,
                object_type="Relationships",
                object_name=f"{len(m2m_rels)} M:M relationships"
            ))
        
        # Check for bidirectional (ambiguous for Copilot)
        bidi_rels = [r for r in relationships if r.get('cross_filter_direction') == 'both']
        if len(bidi_rels) > 2:
            self.issues.append(AIReadinessIssue(
                category=AIReadinessCategory.RELATIONSHIPS,
                severity=AIReadinessSeverity.MEDIUM,
                title="Multiple Bidirectional Relationships",
                description=f"Model has {len(bidi_rels)} bidirectional relationships. "
                           f"Bidirectional filtering can produce unexpected results.",
                recommendation="Review if bidirectional filtering is truly needed. "
                             "Document the expected filter behavior for Copilot.",
                impact_score=3,
                object_type="Relationships",
                object_name=f"{len(bidi_rels)} bidirectional"
            ))
    
    def _check_display_folders(self, model_data: Dict) -> None:
        """Check if measures are organized in display folders."""
        total_measures = 0
        measures_without_folders = 0
        
        for table in model_data.get('tables', []):
            for measure in table.get('measures', []):
                total_measures += 1
                if not measure.get('display_folder'):
                    measures_without_folders += 1
        
        if total_measures > 20 and measures_without_folders > total_measures * 0.7:
            self.issues.append(AIReadinessIssue(
                category=AIReadinessCategory.NAVIGATION,
                severity=AIReadinessSeverity.MEDIUM,
                title="Measures Not Organized in Folders",
                description=f"{measures_without_folders} of {total_measures} measures lack display folders. "
                           f"Organization helps Copilot understand measure groupings.",
                recommendation="Organize measures into display folders by business area "
                             "(e.g., 'Sales Metrics', 'Financial KPIs', 'Customer Analytics').",
                impact_score=3,
                object_type="Measures",
                object_name=f"{measures_without_folders} unorganized"
            ))
    
    def _check_data_categories(self, model_data: Dict) -> None:
        """Check if appropriate data categories are set."""
        missing_categories = []
        
        category_hints = {
            'city': 'City',
            'country': 'Country',
            'state': 'StateOrProvince',
            'postal': 'PostalCode',
            'zip': 'PostalCode',
            'latitude': 'Latitude',
            'longitude': 'Longitude',
            'address': 'Address',
            'url': 'WebUrl',
            'website': 'WebUrl',
            'image': 'ImageUrl',
            'photo': 'ImageUrl',
        }
        
        for table in model_data.get('tables', []):
            table_name = table.get('name', '')
            for column in table.get('columns', []):
                col_name = column.get('name', '').lower()
                for hint, category in category_hints.items():
                    if hint in col_name and not column.get('data_category'):
                        missing_categories.append(f"{table_name}[{column.get('name', '')}] → {category}")
        
        if missing_categories:
            self.issues.append(AIReadinessIssue(
                category=AIReadinessCategory.DATA_QUALITY,
                severity=AIReadinessSeverity.LOW,
                title="Missing Data Categories",
                description=f"{len(missing_categories)} columns could benefit from data categories. "
                           f"Data categories help Copilot understand column semantics.",
                recommendation="Set data categories for geographic, URL, and image columns.",
                impact_score=2,
                object_type="Columns",
                object_name=", ".join(missing_categories[:3])
            ))
    
    def _check_hidden_objects(self, model_data: Dict) -> None:
        """Check for appropriate hiding of technical objects."""
        visible_technical = []
        
        technical_patterns = ['key', 'fk', 'pk', '_id', 'guid', 'hash']
        
        for table in model_data.get('tables', []):
            table_name = table.get('name', '')
            for column in table.get('columns', []):
                if not column.get('is_hidden', False):
                    col_name = column.get('name', '').lower()
                    for pattern in technical_patterns:
                        if pattern in col_name:
                            visible_technical.append(f"{table_name}[{column.get('name', '')}]")
                            break
        
        if visible_technical:
            self.issues.append(AIReadinessIssue(
                category=AIReadinessCategory.NAVIGATION,
                severity=AIReadinessSeverity.LOW,
                title="Technical Columns Visible",
                description=f"{len(visible_technical)} technical columns (keys, IDs) are visible. "
                           f"These can clutter Copilot's suggestions.",
                recommendation="Hide technical columns that end users don't need. "
                             "This helps Copilot focus on business-relevant fields.",
                impact_score=2,
                object_type="Columns",
                object_name=", ".join(visible_technical[:5])
            ))
    
    def _check_synonyms(self, model_data: Dict) -> None:
        """Check if synonyms are defined for better natural language understanding."""
        # This would require checking for Q&A synonyms in the model
        # For now, we'll add a general recommendation if model is large
        
        tables = model_data.get('tables', [])
        total_measures = sum(len(t.get('measures', [])) for t in tables)
        
        if total_measures > 30:
            self.issues.append(AIReadinessIssue(
                category=AIReadinessCategory.NAMING,
                severity=AIReadinessSeverity.INFO,
                title="Consider Adding Synonyms",
                description=f"With {total_measures} measures, consider adding Q&A synonyms "
                           f"to help Copilot understand different ways users might ask for data.",
                recommendation="Add synonyms in the Q&A setup for common terms. "
                             "E.g., 'Revenue' = 'Sales', 'Income'; 'YoY' = 'Year over Year'.",
                impact_score=0,
                object_type="Model",
                object_name="Model"
            ))
    
    def _check_aggregation_guidance(self, model_data: Dict) -> None:
        """Check if measures provide clear aggregation guidance."""
        measures_with_ambiguous_agg = []
        
        ambiguous_names = ['total', 'sum', 'count', 'average', 'avg']
        
        for table in model_data.get('tables', []):
            for measure in table.get('measures', []):
                measure_name = measure.get('name', '').lower()
                # Measures with generic aggregation names but no clear context
                if any(agg in measure_name for agg in ambiguous_names):
                    if not measure.get('description'):
                        measures_with_ambiguous_agg.append(measure.get('name', ''))
        
        if len(measures_with_ambiguous_agg) > 3:
            self.issues.append(AIReadinessIssue(
                category=AIReadinessCategory.DOCUMENTATION,
                severity=AIReadinessSeverity.MEDIUM,
                title="Ambiguous Aggregation Measures",
                description=f"{len(measures_with_ambiguous_agg)} measures have generic aggregation names "
                           f"without descriptions (e.g., 'Total Sales' - total of what? over what period?).",
                recommendation="Add descriptions clarifying: What is being aggregated? "
                             "Over what scope? With what filters?",
                impact_score=3,
                object_type="Measures",
                object_name=", ".join(measures_with_ambiguous_agg[:5])
            ))
    
    def _calculate_score(self) -> int:
        """Calculate AI readiness score."""
        score = self.max_score
        
        severity_multipliers = {
            AIReadinessSeverity.CRITICAL: 1.5,
            AIReadinessSeverity.HIGH: 1.0,
            AIReadinessSeverity.MEDIUM: 0.7,
            AIReadinessSeverity.LOW: 0.3,
            AIReadinessSeverity.INFO: 0
        }
        
        for issue in self.issues:
            penalty = issue.impact_score * severity_multipliers[issue.severity]
            score -= penalty
        
        return max(0, min(100, int(score)))
    
    def _generate_report(self, score: int, model_data: Dict) -> Dict[str, Any]:
        """Generate AI readiness report."""
        # Determine readiness level
        if score >= 90:
            readiness = "Excellent"
            emoji = "🟢"
            summary = "Model is well-optimized for Copilot"
        elif score >= 70:
            readiness = "Good"
            emoji = "🟡"
            summary = "Model will work with Copilot but has room for improvement"
        elif score >= 50:
            readiness = "Fair"
            emoji = "🟠"
            summary = "Copilot may struggle with this model"
        else:
            readiness = "Poor"
            emoji = "🔴"
            summary = "Model needs significant work for Copilot readiness"
        
        # Group issues by category
        issues_by_category = {}
        for issue in self.issues:
            cat = issue.category.value
            if cat not in issues_by_category:
                issues_by_category[cat] = []
            issues_by_category[cat].append({
                'severity': issue.severity.value,
                'title': issue.title,
                'description': issue.description,
                'recommendation': issue.recommendation,
                'object': f"{issue.object_type}: {issue.object_name}"
            })
        
        # Top recommendations
        sorted_issues = sorted(self.issues, key=lambda x: x.impact_score, reverse=True)
        top_recommendations = [issue.recommendation for issue in sorted_issues[:5]]
        
        return {
            'score': score,
            'max_score': self.max_score,
            'readiness_level': readiness,
            'readiness_emoji': emoji,
            'summary': summary,
            'total_issues': len(self.issues),
            'issues_by_severity': {
                'Critical': sum(1 for i in self.issues if i.severity == AIReadinessSeverity.CRITICAL),
                'High': sum(1 for i in self.issues if i.severity == AIReadinessSeverity.HIGH),
                'Medium': sum(1 for i in self.issues if i.severity == AIReadinessSeverity.MEDIUM),
                'Low': sum(1 for i in self.issues if i.severity == AIReadinessSeverity.LOW),
                'Info': sum(1 for i in self.issues if i.severity == AIReadinessSeverity.INFO),
            },
            'issues_by_category': issues_by_category,
            'top_recommendations': top_recommendations,
            'checklist': self._generate_checklist(model_data)
        }
    
    def _generate_checklist(self, model_data: Dict) -> Dict[str, bool]:
        """Generate a simple checklist for Copilot readiness."""
        tables = model_data.get('tables', [])
        relationships = model_data.get('relationships', [])
        
        total_tables = len(tables)
        tables_with_desc = sum(1 for t in tables if t.get('description'))
        
        total_measures = sum(len(t.get('measures', [])) for t in tables)
        measures_with_desc = sum(
            1 for t in tables 
            for m in t.get('measures', []) 
            if m.get('description')
        )
        
        return {
            'All tables have descriptions': tables_with_desc == total_tables,
            'All measures have descriptions': measures_with_desc == total_measures,
            'No inactive relationships': not any(not r.get('is_active', True) for r in relationships),
            'No many-to-many relationships': not any(r.get('cardinality') == 'many-to-many' for r in relationships),
            'Measures organized in folders': total_measures < 20 or any(
                m.get('display_folder') 
                for t in tables 
                for m in t.get('measures', [])
            ),
            'Model has date table': any(
                t.get('data_category') == 'Time' or 'date' in t.get('name', '').lower()
                for t in tables
            ),
            'Less than 50 tables': total_tables < 50,
            'Less than 200 measures': total_measures < 200,
        }
