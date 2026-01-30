import os
import re
import json
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from enum import Enum

class IssueCategory(Enum):
    PERFORMANCE = "Performance"
    DESIGN = "Model Design"
    RELATIONSHIPS = "Relationships"
    MEASURES = "Measures"
    NAMING = "Naming Convention"
    DOCUMENTATION = "Documentation"
    BEST_PRACTICES = "Best Practices"

class IssueSeverity(Enum):
    CRITICAL = "Critical"
    HIGH = "High"
    MEDIUM = "Medium"
    LOW = "Low"
    INFO = "Info"

@dataclass
class ModelIssue:
    category: IssueCategory
    severity: IssueSeverity
    title: str
    description: str
    recommendation: str
    impact_score: int  # 0-10 impact on overall score
    location: Optional[str] = None
    details: Optional[Dict] = None

class EnhancedSemanticModelAnalyzer:
    """
    Enhanced analyzer that checks for Power BI best practices similar to pqlint.com
    Uses a 100-point scoring system for more granular feedback.
    """
    
    def __init__(self):
        self.max_score = 100
        self.issues = []
        self.scoring_weights = {
            IssueCategory.PERFORMANCE: 0.25,
            IssueCategory.DESIGN: 0.20,
            IssueCategory.RELATIONSHIPS: 0.20,
            IssueCategory.MEASURES: 0.15,
            IssueCategory.BEST_PRACTICES: 0.10,
            IssueCategory.NAMING: 0.05,
            IssueCategory.DOCUMENTATION: 0.05
        }
        
    def analyze_tmdl_export(self, filepath: str) -> Dict:
        """
        Analyzes a TMDL export file and returns comprehensive rating out of 100.
        """
        if not os.path.exists(filepath):
            return {"error": f"File not found: {filepath}"}
        
        try:
            # Reset issues for new analysis
            self.issues = []

            # Detect JSON vs TMDL and parse accordingly
            if filepath.lower().endswith('.json'):
                with open(filepath, 'r', encoding='utf-8') as f:
                    model = json.load(f)
                sections = self._parse_tmsl_json_sections(model)
            else:
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = f.read()
                sections = self._parse_tmdl_sections(content)

            # Ensure aggregate collections exist (for checks and stats)
            if 'tables' in sections:
                agg_measures: List[Dict] = []
                agg_calc_cols: List[Dict] = []
                agg_hiers: List[Dict] = []
                for t in sections.get('tables', []):
                    agg_measures.extend(t.get('measures', []))
                    agg_calc_cols.extend(t.get('calculated_columns', []))
                    agg_hiers.extend(t.get('hierarchies', []))
                sections['measures'] = agg_measures
                sections['calculated_columns'] = agg_calc_cols
                sections['hierarchies'] = agg_hiers
            
            # Run all checks
            self._check_bidirectional_filters(sections)
            self._check_table_proliferation(sections)
            self._check_relationship_issues(sections)
            self._check_measure_complexity(sections)
            self._check_calculated_columns(sections)
            self._check_naming_conventions(sections)
            self._check_data_types(sections)
            self._check_hierarchies(sections)
            self._check_role_playing_dimensions(sections)
            self._check_measure_tables(sections)
            self._check_documentation(sections)
            self._check_rls_implementation(sections)
            self._check_aggregations(sections)
            self._check_incremental_refresh(sections)

            # Baseline penalties for underspecified models
            self._apply_baseline_penalties(sections)
            
            # Calculate score
            score = self._calculate_score()
            
            # Generate report
            report = self._generate_report(score, sections)
            
            return report
            
        except Exception as e:
            return {"error": f"Failed to analyze file: {str(e)}"}

    def _parse_tmsl_json_sections(self, model: Dict[str, Any]) -> Dict:
        """Parse TMSL/JSON model definition into the common sections structure."""
        sections: Dict[str, Any] = {
            'tables': [],
            'relationships': [],
            'measures': [],
            'calculated_columns': [],
            'hierarchies': [],
            'roles': [],
            'model_info': {},
            'cultures': [],
            'data_sources': [],
            'expressions': [],
            'metadata': {}
        }

        m = model.get('model', model)

        # Tables
        for t in m.get('tables', []) or []:
            table_name = t.get('name', '')

            columns: List[Dict[str, Any]] = []
            calc_cols: List[Dict[str, Any]] = []
            for c in t.get('columns', []) or []:
                col = {
                    'name': c.get('name', ''),
                    'data_type': c.get('dataType') or c.get('type'),
                    'is_hidden': bool(c.get('isHidden', False)),
                    'is_calculated': 'expression' in c,
                    'expression': c.get('expression')
                }
                columns.append(col)
                if col['is_calculated']:
                    calc_cols.append(col)

            measures: List[Dict[str, Any]] = []
            for mdef in t.get('measures', []) or []:
                expr = mdef.get('expression', '') or ''
                measures.append({
                    'name': mdef.get('name', ''),
                    'expression': expr,
                    'format_string': mdef.get('formatString')
                })

            partitions: List[Dict[str, Any]] = []
            for p in t.get('partitions', []) or []:
                source = p.get('source', {}) or {}
                src_type = (source.get('type') or '').lower()
                mode = 'directquery' if 'directquery' in src_type else 'import'
                partitions.append({
                    'name': p.get('name', ''),
                    'mode': mode,
                    'source': source
                })

            hierarchies: List[Dict[str, Any]] = []
            for h in t.get('hierarchies', []) or []:
                hierarchies.append({
                    'name': h.get('name', ''),
                    'levels': [lvl.get('name', '') for lvl in (h.get('levels') or [])]
                })

            sections['tables'].append({
                'name': table_name,
                'content': '',
                'columns': columns,
                'measures': measures,
                'calculated_columns': calc_cols,
                'partitions': partitions,
                'hierarchies': hierarchies,
                'description': t.get('description', ''),
                'm_code': ''
            })

        # Relationships
        for r in m.get('relationships', []) or []:
            card_raw = (r.get('cardinality') or '').lower()
            if card_raw == 'onetoone':
                cardinality = 'one-to-one'
            elif card_raw == 'onetomany':
                cardinality = 'one-to-many'
            else:
                cardinality = 'many-to-one'

            xfd = (r.get('crossFilterDirection') or '').lower()
            cross_dir = 'both' if xfd == 'both' else 'single'

            sections['relationships'].append({
                'name': r.get('name') or f"{r.get('fromTable','')}_{r.get('toTable','')}",
                'from_table': r.get('fromTable', ''),
                'from_column': r.get('fromColumn', ''),
                'to_table': r.get('toTable', ''),
                'to_column': r.get('toColumn', ''),
                'cardinality': cardinality,
                'cross_filter_direction': cross_dir,
                'is_active': bool(r.get('isActive', True))
            })

        # Roles
        for role in m.get('roles', []) or []:
            sections['roles'].append({'name': role.get('name', '')})

        # Metadata
        sections['model_info'] = {
            'default_mode': m.get('defaultMode') or ''
        }

        return sections

    def _apply_baseline_penalties(self, sections: Dict) -> None:
        """Add baseline issues for underspecified models to avoid perfect scores."""
        tables_count = len(sections.get('tables', []))
        relationships_count = len(sections.get('relationships', []))
        measures_total = sum(len(t.get('measures', [])) for t in sections.get('tables', []))

        if relationships_count == 0:
            self.issues.append(ModelIssue(
                category=IssueCategory.DESIGN,
                severity=IssueSeverity.HIGH,
                title="No Relationships Defined",
                description="The model has no relationships. This usually indicates insufficient dimensional modeling and may lead to poor report behavior.",
                recommendation="Add relationships between fact and dimension tables to model business logic and enable correct filtering.",
                impact_score=6,
                location="Model"
            ))

        if measures_total == 0:
            self.issues.append(ModelIssue(
                category=IssueCategory.BEST_PRACTICES,
                severity=IssueSeverity.MEDIUM,
                title="No Measures Present",
                description="No DAX measures found in the model.",
                recommendation="Create measures for core KPIs rather than relying on implicit measures.",
                impact_score=4,
                location="Model"
            ))

        if tables_count <= 1:
            self.issues.append(ModelIssue(
                category=IssueCategory.DESIGN,
                severity=IssueSeverity.MEDIUM,
                title="Single-Table Model",
                description="Model contains only one table. Star schemas with separate dimension and fact tables are generally preferred for performance and clarity.",
                recommendation="Consider splitting the model into a fact table and related dimension tables where appropriate.",
                impact_score=3,
                location="Model"
            ))
    
    def _parse_tmdl_sections(self, content: str) -> Dict:
        """Enhanced TMDL parsing to extract all relevant information."""
        sections = {
            'tables': [],
            'relationships': [],
            'measures': [],
            'calculated_columns': [],
            'hierarchies': [],
            'roles': [],
            'model_info': {},
            'cultures': [],
            'data_sources': [],
            'expressions': []
        }
        
        # Parse tables with their full content
        table_pattern = r'table\s+([^\n]+)\n(.*?)(?=\ntable\s+|\n─{40}|\Z)'
        table_matches = re.finditer(table_pattern, content, re.DOTALL)
        
        for match in table_matches:
            table_name = match.group(1).strip().strip("'")
            table_content = match.group(2)
            
            table_info = {
                'name': table_name,
                'content': table_content,
                'columns': self._extract_columns(table_content),
                'measures': self._extract_measures(table_content),
                'calculated_columns': self._extract_calculated_columns(table_content),
                'partitions': self._extract_partitions(table_content),
                'hierarchies': self._extract_hierarchies(table_content)
            }
            
            sections['tables'].append(table_info)
        
        # Parse relationships
        rel_pattern = r'relationship\s+([^\n]+)\n(.*?)(?=\nrelationship\s+|\n─{40}|\Z)'
        rel_matches = re.finditer(rel_pattern, content, re.DOTALL)
        
        for match in rel_matches:
            rel_name = match.group(1).strip()
            rel_content = match.group(2)
            
            rel_info = self._parse_relationship(rel_name, rel_content)
            if rel_info:
                sections['relationships'].append(rel_info)
        
        # Extract all measures across tables
        for table in sections['tables']:
            sections['measures'].extend(table['measures'])
            sections['calculated_columns'].extend(table['calculated_columns'])
            sections['hierarchies'].extend(table['hierarchies'])
        
        # Parse model metadata
        model_pattern = r'model\s+([^\n]+)\n(.*?)(?=\n─{40}|\Z)'
        model_match = re.search(model_pattern, content, re.DOTALL)
        if model_match:
            sections['model_info'] = self._parse_model_info(model_match.group(2))
        
        # Parse roles (RLS)
        role_pattern = r'role\s+([^\n]+)\n(.*?)(?=\nrole\s+|\n─{40}|\Z)'
        role_matches = re.finditer(role_pattern, content, re.DOTALL)
        
        for match in role_matches:
            role_name = match.group(1).strip().strip("'")
            role_content = match.group(2)
            sections['roles'].append({
                'name': role_name,
                'content': role_content
            })
        
        return sections
    
    def _extract_columns(self, table_content: str) -> List[Dict]:
        """Extract column information from table content."""
        columns = []
        
        column_pattern = r'column\s+([^\n]+)\n(.*?)(?=\ncolumn\s+|\nmeasure\s+|\npartition\s+|\nhierarchy\s+|\Z)'
        matches = re.finditer(column_pattern, table_content, re.DOTALL)
        
        for match in matches:
            column_name = match.group(1).strip().strip("'")
            column_content = match.group(2)
            
            column_info = {
                'name': column_name,
                'data_type': self._extract_data_type(column_content),
                'is_hidden': 'isHidden' in column_content,
                'is_calculated': 'expression' in column_content,
                'expression': self._extract_expression(column_content) if 'expression' in column_content else None
            }
            
            columns.append(column_info)
        
        return columns
    
    def _extract_measures(self, table_content: str) -> List[Dict]:
        """Extract measure information from table content."""
        measures = []
        
        measure_pattern = r'measure\s+([^=]+)\s*=\s*([^•]+?)(?=\s*(?:formatString:|lineageTag:|measure\s+|\Z))'
        matches = re.finditer(measure_pattern, table_content, re.DOTALL)
        
        for match in matches:
            measure_name = match.group(1).strip().strip("'")
            expression = match.group(2).strip()
            
            # Extract format string
            format_match = re.search(r'formatString:\s*([^\s]+)', table_content[match.end():match.end()+200])
            format_string = format_match.group(1) if format_match else None
            
            measure_info = {
                'name': measure_name,
                'expression': expression,
                'format_string': format_string,
                'complexity': self._calculate_dax_complexity(expression),
                'uses_calculate': 'CALCULATE' in expression.upper(),
                'uses_iterator': any(func in expression.upper() for func in ['SUMX', 'AVERAGEX', 'COUNTX', 'MAXX', 'MINX', 'PRODUCTX']),
                'line_count': len(expression.split('\n'))
            }
            
            measures.append(measure_info)
        
        return measures
    
    def _extract_calculated_columns(self, table_content: str) -> List[Dict]:
        """Extract calculated columns from table content."""
        calculated_columns = []
        
        for column in self._extract_columns(table_content):
            if column['is_calculated']:
                calculated_columns.append(column)
        
        return calculated_columns
    
    def _extract_partitions(self, table_content: str) -> List[Dict]:
        """Extract partition information from table content."""
        partitions = []
        
        partition_pattern = r'partition\s+([^\n]+)\n(.*?)(?=\npartition\s+|\Z)'
        matches = re.finditer(partition_pattern, table_content, re.DOTALL)
        
        for match in matches:
            partition_name = match.group(1).strip().strip("'")
            partition_content = match.group(2)
            
            partition_info = {
                'name': partition_name,
                'mode': 'import' if 'mode: import' in partition_content else 'directquery',
                'source': self._extract_source(partition_content)
            }
            
            partitions.append(partition_info)
        
        return partitions
    
    def _extract_hierarchies(self, table_content: str) -> List[Dict]:
        """Extract hierarchy information from table content."""
        hierarchies = []
        
        hierarchy_pattern = r'hierarchy\s+([^\n]+)\n(.*?)(?=\nhierarchy\s+|\Z)'
        matches = re.finditer(hierarchy_pattern, table_content, re.DOTALL)
        
        for match in matches:
            hierarchy_name = match.group(1).strip().strip("'")
            hierarchy_content = match.group(2)
            
            levels = re.findall(r'level\s+([^\n]+)', hierarchy_content)
            
            hierarchy_info = {
                'name': hierarchy_name,
                'levels': [level.strip().strip("'") for level in levels]
            }
            
            hierarchies.append(hierarchy_info)
        
        return hierarchies
    
    def _parse_relationship(self, rel_name: str, rel_content: str) -> Optional[Dict]:
        """Parse relationship details."""
        try:
            # Extract from and to tables/columns
            from_match = re.search(r'from\s+([^\[]+)\[([^\]]+)\]', rel_content)
            to_match = re.search(r'to\s+([^\[]+)\[([^\]]+)\]', rel_content)
            
            if not from_match or not to_match:
                return None
            
            rel_info = {
                'name': rel_name,
                'from_table': from_match.group(1).strip().strip("'"),
                'from_column': from_match.group(2).strip().strip("'"),
                'to_table': to_match.group(1).strip().strip("'"),
                'to_column': to_match.group(2).strip().strip("'"),
                'cardinality': 'many-to-one',  # default
                'cross_filter_direction': 'single',  # default
                'is_active': True  # default
            }
            
            # Check for cardinality
            if 'many-to-many' in rel_content.lower():
                rel_info['cardinality'] = 'many-to-many'
            elif 'one-to-many' in rel_content.lower():
                rel_info['cardinality'] = 'one-to-many'
            elif 'one-to-one' in rel_content.lower():
                rel_info['cardinality'] = 'one-to-one'
            
            # Check for cross filter direction
            if 'crossfilteringbehavior: bothDirections' in rel_content:
                rel_info['cross_filter_direction'] = 'both'
            
            # Check if active
            if 'isActive: false' in rel_content:
                rel_info['is_active'] = False
            
            return rel_info
            
        except Exception:
            return None
    
    def _calculate_dax_complexity(self, expression: str) -> str:
        """Calculate DAX expression complexity."""
        complexity_score = 0
        
        # Count nested functions
        open_parens = expression.count('(')
        complexity_score += open_parens * 0.5
        
        # Count complex functions
        complex_functions = ['CALCULATE', 'CALCULATETABLE', 'FILTER', 'ALL', 'ALLEXCEPT', 
                           'SUMX', 'AVERAGEX', 'COUNTX', 'MAXX', 'MINX', 'EARLIER', 'EARLIEST']
        for func in complex_functions:
            complexity_score += expression.upper().count(func) * 2
        
        # Count variables
        complexity_score += expression.upper().count('VAR ') * 1
        
        # Line count
        line_count = len(expression.split('\n'))
        complexity_score += line_count * 0.3
        
        if complexity_score < 5:
            return "Low"
        elif complexity_score < 15:
            return "Medium"
        elif complexity_score < 30:
            return "High"
        else:
            return "Very High"
    
    def _check_bidirectional_filters(self, sections: Dict):
        """Check for unnecessary bidirectional filters."""
        for rel in sections['relationships']:
            if rel['cross_filter_direction'] == 'both':
                # Check if it's necessary
                from_table = rel['from_table']
                to_table = rel['to_table']
                
                # Look for patterns that don't need bidirectional
                unnecessary_patterns = [
                    ('fact', 'dimension'),  # Fact to dimension rarely needs bidirectional
                    ('transaction', 'lookup'),
                    ('detail', 'master')
                ]
                
                is_unnecessary = False
                for pattern in unnecessary_patterns:
                    if (pattern[0] in from_table.lower() and pattern[1] in to_table.lower()) or \
                       (pattern[1] in from_table.lower() and pattern[0] in to_table.lower()):
                        is_unnecessary = True
                        break
                
                # Check for many-to-many with bidirectional (performance killer)
                if rel['cardinality'] == 'many-to-many':
                    self.issues.append(ModelIssue(
                        category=IssueCategory.PERFORMANCE,
                        severity=IssueSeverity.CRITICAL,
                        title="Many-to-Many with Bidirectional Filter",
                        description=f"Relationship '{rel['name']}' uses many-to-many cardinality with bidirectional filtering between {from_table} and {to_table}",
                        recommendation="Avoid many-to-many relationships with bidirectional filtering. Consider restructuring the model or using a bridge table.",
                        impact_score=10,
                        location=f"Relationship: {rel['name']}"
                    ))
                elif is_unnecessary:
                    self.issues.append(ModelIssue(
                        category=IssueCategory.PERFORMANCE,
                        severity=IssueSeverity.HIGH,
                        title="Unnecessary Bidirectional Filter",
                        description=f"Relationship '{rel['name']}' uses bidirectional filtering between {from_table} and {to_table}",
                        recommendation="Consider using single direction filtering. Bidirectional filters can impact performance and create ambiguity.",
                        impact_score=5,
                        location=f"Relationship: {rel['name']}"
                    ))
    
    def _check_table_proliferation(self, sections: Dict):
        """Check for table proliferation (e.g., separate budget tables for each year)."""
        tables = sections['tables']
        
        # Group tables by similar names
        table_groups = {}
        for table in tables:
            # Extract base name (remove year/month patterns)
            base_name = re.sub(r'(_|\s)?(19|20)\d{2}', '', table['name'])
            base_name = re.sub(r'(_|\s)?(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)', '', base_name)
            base_name = re.sub(r'(_|\s)?Q[1-4]', '', base_name)
            base_name = base_name.strip('_- ')
            
            if base_name not in table_groups:
                table_groups[base_name] = []
            table_groups[base_name].append(table['name'])
        
        # Check for proliferation
        for base_name, table_list in table_groups.items():
            if len(table_list) > 2:  # More than 2 similar tables
                # Check if they have similar structure
                if self._tables_have_similar_structure(tables, table_list):
                    self.issues.append(ModelIssue(
                        category=IssueCategory.DESIGN,
                        severity=IssueSeverity.HIGH,
                        title="Table Proliferation Detected",
                        description=f"Found {len(table_list)} similar tables: {', '.join(table_list[:5])}{'...' if len(table_list) > 5 else ''}",
                        recommendation="Consider combining these tables into a single table with an additional column for the varying dimension (e.g., Year, Period).",
                        impact_score=8,
                        location=f"Tables: {base_name}*",
                        details={'tables': table_list}
                    ))
    
    def _tables_have_similar_structure(self, all_tables: List[Dict], table_names: List[str]) -> bool:
        """Check if tables have similar column structure."""
        table_columns = []
        
        for table_name in table_names[:3]:  # Check first 3 tables
            for table in all_tables:
                if table['name'] == table_name:
                    columns = set(col['name'] for col in table['columns'])
                    table_columns.append(columns)
                    break
        
        if len(table_columns) < 2:
            return False
        
        # Check similarity
        common_columns = table_columns[0]
        for columns in table_columns[1:]:
            common_columns = common_columns.intersection(columns)
        
        # If 70% of columns are common, consider them similar
        avg_columns = sum(len(cols) for cols in table_columns) / len(table_columns)
        return len(common_columns) / avg_columns > 0.7
    
    def _check_relationship_issues(self, sections: Dict):
        """Check for relationship design issues."""
        # Check for circular relationships
        # Check for orphaned tables
        # Check for missing relationships to dimension tables
        pass
    
    def _check_calculated_columns(self, sections: Dict):
        """Check for excessive calculated columns that should be measures."""
        for table in sections['tables']:
            calc_columns = table['calculated_columns']
            
            if len(calc_columns) > 5:
                self.issues.append(ModelIssue(
                    category=IssueCategory.PERFORMANCE,
                    severity=IssueSeverity.MEDIUM,
                    title="Excessive Calculated Columns",
                    description=f"Table '{table['name']}' has {len(calc_columns)} calculated columns",
                    recommendation="Consider converting calculated columns to measures where possible. Calculated columns increase model size and refresh time.",
                    impact_score=4,
                    location=f"Table: {table['name']}"
                ))
            
            # Check for aggregations in calculated columns
            for col in calc_columns:
                if col['expression']:
                    expr_upper = col['expression'].upper()
                    if any(agg in expr_upper for agg in ['SUM(', 'AVERAGE(', 'COUNT(', 'MAX(', 'MIN(']):
                        self.issues.append(ModelIssue(
                            category=IssueCategory.BEST_PRACTICES,
                            severity=IssueSeverity.HIGH,
                            title="Aggregation in Calculated Column",
                            description=f"Calculated column '{col['name']}' in table '{table['name']}' uses aggregation functions",
                            recommendation="Move aggregations to measures. Calculated columns with aggregations can cause performance issues.",
                            impact_score=6,
                            location=f"Table: {table['name']}, Column: {col['name']}"
                        ))
    
    def _check_measure_complexity(self, sections: Dict):
        """Check for overly complex measures."""
        for measure in sections['measures']:
            if measure['complexity'] == "Very High":
                self.issues.append(ModelIssue(
                    category=IssueCategory.MEASURES,
                    severity=IssueSeverity.MEDIUM,
                    title="Very Complex Measure",
                    description=f"Measure '{measure['name']}' has very high complexity",
                    recommendation="Consider breaking down complex measures into smaller, reusable components using variables or helper measures.",
                    impact_score=3,
                    location=f"Measure: {measure['name']}"
                ))
            
            # Check for missing CALCULATE in measures that need context transition
            if measure['uses_iterator'] and not measure['uses_calculate']:
                self.issues.append(ModelIssue(
                    category=IssueCategory.MEASURES,
                    severity=IssueSeverity.LOW,
                    title="Iterator Without CALCULATE",
                    description=f"Measure '{measure['name']}' uses an iterator function without CALCULATE",
                    recommendation="Consider if context transition is needed. Iterator functions often require CALCULATE for proper context.",
                    impact_score=2,
                    location=f"Measure: {measure['name']}"
                ))
    
    def _check_naming_conventions(self, sections: Dict):
        """Check for consistent naming conventions."""
        # Check table names
        table_names = [t['name'] for t in sections['tables']]
        naming_styles = {
            'PascalCase': 0,
            'camelCase': 0,
            'snake_case': 0,
            'Mixed': 0
        }
        
        for name in table_names:
            if re.match(r'^[A-Z][a-zA-Z0-9]+$', name):
                naming_styles['PascalCase'] += 1
            elif re.match(r'^[a-z][a-zA-Z0-9]+$', name):
                naming_styles['camelCase'] += 1
            elif '_' in name:
                naming_styles['snake_case'] += 1
            else:
                naming_styles['Mixed'] += 1
        
        # Find dominant style
        dominant_style = max(naming_styles, key=naming_styles.get)
        inconsistent_count = sum(count for style, count in naming_styles.items() if style != dominant_style)
        
        if inconsistent_count > len(table_names) * 0.2:  # More than 20% inconsistent
            self.issues.append(ModelIssue(
                category=IssueCategory.NAMING,
                severity=IssueSeverity.LOW,
                title="Inconsistent Table Naming Convention",
                description=f"Tables use mixed naming conventions. Found {naming_styles}",
                recommendation=f"Standardize on {dominant_style} for all table names.",
                impact_score=2,
                location="All tables"
            ))
    
    def _check_data_types(self, sections: Dict):
        """Check for appropriate data types."""
        for table in sections['tables']:
            for column in table['columns']:
                col_name_lower = column['name'].lower()
                
                # Check for text columns that should be numbers
                if any(indicator in col_name_lower for indicator in ['amount', 'quantity', 'price', 'cost', 'revenue']):
                    if column['data_type'] and 'text' in column['data_type'].lower():
                        self.issues.append(ModelIssue(
                            category=IssueCategory.DESIGN,
                            severity=IssueSeverity.HIGH,
                            title="Incorrect Data Type",
                            description=f"Column '{column['name']}' in table '{table['name']}' uses text data type for numeric data",
                            recommendation="Change to appropriate numeric data type (Decimal, Integer) for better performance and functionality.",
                            impact_score=5,
                            location=f"Table: {table['name']}, Column: {column['name']}"
                        ))
    
    def _check_hierarchies(self, sections: Dict):
        """Check for proper hierarchy implementation."""
        all_hierarchies = sections['hierarchies']
        
        if len(all_hierarchies) == 0:
            # Check if there are obvious hierarchy candidates
            date_tables = [t for t in sections['tables'] if 'date' in t['name'].lower() or 'calendar' in t['name'].lower()]
            if date_tables:
                self.issues.append(ModelIssue(
                    category=IssueCategory.BEST_PRACTICES,
                    severity=IssueSeverity.LOW,
                    title="Missing Date Hierarchy",
                    description="No hierarchies defined in the model despite having date tables",
                    recommendation="Consider creating date hierarchies (Year > Quarter > Month > Day) for better user experience.",
                    impact_score=2,
                    location="Model"
                ))
    
    def _check_role_playing_dimensions(self, sections: Dict):
        """Check for role-playing dimensions (same dimension used multiple times)."""
        # Look for multiple relationships from the same dimension table
        dimension_relationships = {}
        
        for rel in sections['relationships']:
            if rel['to_table'] not in dimension_relationships:
                dimension_relationships[rel['to_table']] = []
            dimension_relationships[rel['to_table']].append(rel['from_table'])
        
        for dim_table, fact_tables in dimension_relationships.items():
            if len(set(fact_tables)) == 1 and len(fact_tables) > 1:
                # Same dimension connected to same fact table multiple times
                self.issues.append(ModelIssue(
                    category=IssueCategory.DESIGN,
                    severity=IssueSeverity.INFO,
                    title="Role-Playing Dimension Detected",
                    description=f"Dimension table '{dim_table}' is connected to '{fact_tables[0]}' multiple times",
                    recommendation="This is often valid (e.g., Order Date vs Ship Date). Ensure inactive relationships are properly managed in DAX.",
                    impact_score=0,
                    location=f"Table: {dim_table}"
                ))
    
    def _check_measure_tables(self, sections: Dict):
        """Check for dedicated measure tables."""
        measure_tables = [t for t in sections['tables'] if 'measure' in t['name'].lower()]
        
        # Count measures not in measure tables
        measures_in_fact_tables = 0
        for table in sections['tables']:
            if 'measure' not in table['name'].lower() and len(table['measures']) > 0:
                measures_in_fact_tables += len(table['measures'])
        
        if measures_in_fact_tables > 10 and not measure_tables:
            self.issues.append(ModelIssue(
                category=IssueCategory.BEST_PRACTICES,
                severity=IssueSeverity.MEDIUM,
                title="No Dedicated Measure Table",
                description=f"Found {measures_in_fact_tables} measures scattered across fact tables",
                recommendation="Create a dedicated measure table to organize measures centrally for better maintainability.",
                impact_score=3,
                location="Model"
            ))
    
    def _check_documentation(self, sections: Dict):
        """Check for model documentation."""
        total_measures = len(sections['measures'])
        documented_measures = sum(1 for m in sections['measures'] if '//' in m['expression'] or '/*' in m['expression'])
        
        if documented_measures < total_measures * 0.3:  # Less than 30% documented
            self.issues.append(ModelIssue(
                category=IssueCategory.DOCUMENTATION,
                severity=IssueSeverity.LOW,
                title="Poor Measure Documentation",
                description=f"Only {documented_measures} of {total_measures} measures have documentation",
                recommendation="Add comments to complex measures explaining business logic and calculations.",
                impact_score=2,
                location="Measures"
            ))
    
    def _check_rls_implementation(self, sections: Dict):
        """Check for Row-Level Security implementation."""
        if not sections['roles']:
            # Check if there are user/security related tables
            security_tables = [t for t in sections['tables'] if any(sec in t['name'].lower() for sec in ['user', 'security', 'access', 'permission'])]
            if security_tables:
                self.issues.append(ModelIssue(
                    category=IssueCategory.BEST_PRACTICES,
                    severity=IssueSeverity.INFO,
                    title="No RLS Implementation",
                    description="Security-related tables found but no RLS roles defined",
                    recommendation="Consider implementing Row-Level Security if data access control is required.",
                    impact_score=0,
                    location="Model"
                ))
    
    def _check_aggregations(self, sections: Dict):
        """Check for aggregation opportunities."""
        large_fact_tables = []
        for table in sections['tables']:
            # Check partition count as proxy for table size
            if len(table['partitions']) > 1 or any(keyword in table['name'].lower() for keyword in ['fact', 'transaction', 'detail']):
                large_fact_tables.append(table['name'])
        
        if large_fact_tables and not any('aggregation' in t['name'].lower() for t in sections['tables']):
            self.issues.append(ModelIssue(
                category=IssueCategory.PERFORMANCE,
                severity=IssueSeverity.INFO,
                title="Consider Aggregations",
                description=f"Large fact tables found: {', '.join(large_fact_tables[:3])}",
                recommendation="Consider creating aggregation tables for commonly used summaries to improve query performance.",
                impact_score=0,
                location="Model"
            ))
    
    def _check_incremental_refresh(self, sections: Dict):
        """Check for incremental refresh setup."""
        for table in sections['tables']:
            if len(table['partitions']) > 5:  # Multiple partitions might indicate incremental refresh
                self.issues.append(ModelIssue(
                    category=IssueCategory.PERFORMANCE,
                    severity=IssueSeverity.INFO,
                    title="Incremental Refresh Detected",
                    description=f"Table '{table['name']}' has {len(table['partitions'])} partitions",
                    recommendation="Good use of incremental refresh. Ensure refresh windows are optimized.",
                    impact_score=0,
                    location=f"Table: {table['name']}"
                ))
    
    def _extract_data_type(self, content: str) -> Optional[str]:
        """Extract data type from column definition."""
        type_match = re.search(r'dataType:\s*([^\s]+)', content)
        return type_match.group(1) if type_match else None
    
    def _extract_expression(self, content: str) -> Optional[str]:
        """Extract expression from calculated column."""
        expr_match = re.search(r'expression:\s*"([^"]+)"', content)
        return expr_match.group(1) if expr_match else None
    
    def _extract_source(self, content: str) -> Optional[str]:
        """Extract source from partition."""
        source_match = re.search(r'source\s*=\s*([^}]+)', content)
        return source_match.group(1) if source_match else None
    
    def _parse_model_info(self, content: str) -> Dict:
        """Parse model metadata."""
        info = {}
        
        # Extract various model properties
        culture_match = re.search(r'culture:\s*([^\s]+)', content)
        if culture_match:
            info['culture'] = culture_match.group(1)
        
        mode_match = re.search(r'defaultMode:\s*([^\s]+)', content)
        if mode_match:
            info['default_mode'] = mode_match.group(1)
        
        return info
    
    def _calculate_score(self) -> int:
        """Calculate overall score out of 100 based on issues found."""
        score = self.max_score
        
        # Group issues by category
        category_penalties = {category: 0 for category in IssueCategory}
        
        for issue in self.issues:
            # Apply penalty based on severity and impact
            penalty = issue.impact_score
            
            # Adjust penalty based on severity
            severity_multipliers = {
                IssueSeverity.CRITICAL: 1.5,
                IssueSeverity.HIGH: 1.0,
                IssueSeverity.MEDIUM: 0.7,
                IssueSeverity.LOW: 0.3,
                IssueSeverity.INFO: 0
            }
            
            penalty *= severity_multipliers[issue.severity]
            category_penalties[issue.category] += penalty
        
        # Apply weighted penalties
        for category, penalty in category_penalties.items():
            weight = self.scoring_weights[category]
            weighted_penalty = penalty * weight
            
            # Cap penalty per category at 30% of total score
            max_category_penalty = self.max_score * 0.3
            weighted_penalty = min(weighted_penalty, max_category_penalty)
            
            score -= weighted_penalty
        
        # Ensure score doesn't go below 0
        return max(0, int(score))
    
    def _generate_report(self, score: int, sections: Dict) -> Dict:
        """Generate comprehensive analysis report."""
        # Group issues by severity
        issues_by_severity = {
            IssueSeverity.CRITICAL: [],
            IssueSeverity.HIGH: [],
            IssueSeverity.MEDIUM: [],
            IssueSeverity.LOW: [],
            IssueSeverity.INFO: []
        }
        
        for issue in self.issues:
            issues_by_severity[issue.severity].append(issue)
        
        # Generate grade
        if score >= 90:
            grade = "A"
            grade_description = "Excellent"
        elif score >= 80:
            grade = "B"
            grade_description = "Good"
        elif score >= 70:
            grade = "C"
            grade_description = "Average"
        elif score >= 60:
            grade = "D"
            grade_description = "Below Average"
        else:
            grade = "F"
            grade_description = "Poor"
        
        # Create summary statistics
        stats = {
            'tables': len(sections['tables']),
            'relationships': len(sections['relationships']),
            'measures': len(sections['measures']),
            'calculated_columns': len(sections['calculated_columns']),
            'hierarchies': len(sections['hierarchies']),
            'roles': len(sections['roles'])
        }
        
        # Top recommendations
        top_recommendations = []
        critical_issues = issues_by_severity[IssueSeverity.CRITICAL][:3]
        high_issues = issues_by_severity[IssueSeverity.HIGH][:5]
        
        for issue in critical_issues + high_issues:
            if issue.recommendation not in top_recommendations:
                top_recommendations.append(issue.recommendation)
        
        return {
            'score': score,
            'max_score': self.max_score,
            'grade': grade,
            'grade_description': grade_description,
            'total_issues': len(self.issues),
            'issues_by_severity': {
                severity.value: len(issues) for severity, issues in issues_by_severity.items()
            },
            'issues': [self._issue_to_dict(issue) for issue in self.issues],
            'top_recommendations': top_recommendations[:5],
            'statistics': stats,
            'category_scores': self._calculate_category_scores()
        }
    
    def _issue_to_dict(self, issue: ModelIssue) -> Dict:
        """Convert ModelIssue to dictionary for JSON serialization."""
        return {
            'category': issue.category.value,
            'severity': issue.severity.value,
            'title': issue.title,
            'description': issue.description,
            'recommendation': issue.recommendation,
            'impact_score': issue.impact_score,
            'location': issue.location,
            'details': issue.details
        }
    
    def _calculate_category_scores(self) -> Dict[str, Dict]:
        """Calculate score breakdown by category."""
        category_scores = {}
        
        for category in IssueCategory:
            # Calculate penalty for this category
            penalty = sum(
                issue.impact_score * {
                    IssueSeverity.CRITICAL: 1.5,
                    IssueSeverity.HIGH: 1.0,
                    IssueSeverity.MEDIUM: 0.7,
                    IssueSeverity.LOW: 0.3,
                    IssueSeverity.INFO: 0
                }[issue.severity]
                for issue in self.issues if issue.category == category
            )
            
            # Calculate category score
            max_category_score = self.max_score * self.scoring_weights[category]
            category_score = max(0, max_category_score - penalty)
            
            category_scores[category.value] = {
                'score': int(category_score),
                'max_score': int(max_category_score),
                'percentage': int((category_score / max_category_score) * 100) if max_category_score > 0 else 0,
                'issues_count': sum(1 for issue in self.issues if issue.category == category)
            }
        
        return category_scores


def analyze_and_print_report(filepath: str):
    """Analyze a TMDL file and print a formatted report."""
    analyzer = EnhancedSemanticModelAnalyzer()
    report = analyzer.analyze_tmdl_export(filepath)
    
    if 'error' in report:
        print(f"❌ Error: {report['error']}")
        return
    
    # Print header
    print("\n" + "="*80)
    print(f"POWER BI MODEL ANALYSIS REPORT")
    print("="*80)
    
    # Print score and grade
    print(f"\n📊 OVERALL SCORE: {report['score']}/{report['max_score']} ({report['grade']})")
    print(f"   Grade: {report['grade_description']}")
    
    # Print statistics
    print(f"\n📈 MODEL STATISTICS:")
    for stat, value in report['statistics'].items():
        print(f"   • {stat.replace('_', ' ').title()}: {value}")
    
    # Print issues summary
    print(f"\n⚠️  ISSUES FOUND: {report['total_issues']}")
    for severity, count in report['issues_by_severity'].items():
        if count > 0:
            print(f"   • {severity}: {count}")
    
    # Print category scores
    print(f"\n📋 CATEGORY SCORES:")
    for category, scores in report['category_scores'].items():
        bar = "█" * (scores['percentage'] // 10) + "░" * (10 - scores['percentage'] // 10)
        print(f"   {category:20} [{bar}] {scores['percentage']}% ({scores['issues_count']} issues)")
    
    # Print top issues
    print(f"\n🔴 TOP ISSUES:")
    critical_and_high = [issue for issue in report['issues'] 
                        if issue['severity'] in ['Critical', 'High']][:5]
    
    for i, issue in enumerate(critical_and_high, 1):
        print(f"\n   {i}. [{issue['severity']}] {issue['title']}")
        print(f"      {issue['description']}")
        print(f"      📍 Location: {issue['location']}")
        print(f"      💡 Recommendation: {issue['recommendation']}")
    
    # Print top recommendations
    print(f"\n💡 TOP RECOMMENDATIONS:")
    for i, rec in enumerate(report['top_recommendations'], 1):
        print(f"   {i}. {rec}")
    
    print("\n" + "="*80)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        analyze_and_print_report(sys.argv[1])
    else:
        print("Usage: python enhanced_semantic_model_analyzer.py <tmdl_file_path>") 