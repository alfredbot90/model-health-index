#!/usr/bin/env python3
"""
Enhanced M-Code Analyzer for Power Query Best Practices
Provides detailed analysis similar to pqlint.com
"""

import re
import json
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from enum import Enum

class MCodeIssueCategory(Enum):
    PERFORMANCE = "Performance"
    QUERY_FOLDING = "Query Folding"
    DATA_QUALITY = "Data Quality"
    ERROR_HANDLING = "Error Handling"
    BEST_PRACTICES = "Best Practices"
    NAMING = "Naming Convention"
    DOCUMENTATION = "Documentation"
    SECURITY = "Security"

class MCodeSeverity(Enum):
    CRITICAL = "Critical"
    HIGH = "High"
    MEDIUM = "Medium"
    LOW = "Low"
    INFO = "Info"

@dataclass
class MCodeIssue:
    category: MCodeIssueCategory
    severity: MCodeSeverity
    title: str
    description: str
    recommendation: str
    impact_score: int  # 0-10 impact on overall score
    line_number: Optional[int] = None
    code_snippet: Optional[str] = None
    query_name: Optional[str] = None

class EnhancedMCodeAnalyzer:
    """
    Enhanced M-Code analyzer that checks for Power Query best practices.
    Uses a 100-point scoring system.
    """
    
    def __init__(self):
        self.max_score = 100
        self.issues = []
        self.scoring_weights = {
            MCodeIssueCategory.PERFORMANCE: 0.25,
            MCodeIssueCategory.QUERY_FOLDING: 0.20,
            MCodeIssueCategory.DATA_QUALITY: 0.15,
            MCodeIssueCategory.ERROR_HANDLING: 0.15,
            MCodeIssueCategory.BEST_PRACTICES: 0.10,
            MCodeIssueCategory.SECURITY: 0.10,
            MCodeIssueCategory.NAMING: 0.03,
            MCodeIssueCategory.DOCUMENTATION: 0.02
        }
        
        # Query folding breakers
        self.folding_breakers = [
            'Table.AddColumn',
            'Table.AddIndexColumn',
            'Table.AlternateRows',
            'Table.Combine',
            'Table.FillDown',
            'Table.FillUp',
            'Table.Range',
            'Table.Repeat',
            'Table.ReplaceMatchingRows',
            'Table.ReverseRows',
            'Table.Split',
            'Table.Transpose',
            'List.', # Most List functions break folding
            'Text.', # Most Text functions break folding
            'Date.', # Most Date functions break folding
        ]
        
        # Functions that preserve folding
        self.folding_preservers = [
            'Table.SelectRows',
            'Table.SelectColumns',
            'Table.RemoveColumns',
            'Table.RenameColumns',
            'Table.ReorderColumns',
            'Table.Sort',
            'Table.FirstN',
            'Table.LastN',
            'Table.Distinct',
            'Table.Join',
            'Table.NestedJoin',
            'Table.Group'
        ]
    
    def analyze_tmdl_file(self, filepath: str) -> Dict:
        """Analyze M-code from a TMDL file."""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Reset issues for new analysis
            self.issues = []
            
            # Extract M-code queries
            queries = self._extract_mcode_queries(content)
            
            if not queries:
                return {
                    'error': 'No M-code queries found in file',
                    'score': 100,
                    'issues': []
                }
            
            # Analyze each query
            query_analyses = {}
            for query_name, mcode in queries.items():
                query_analyses[query_name] = self._analyze_query(query_name, mcode)
            
            # Calculate overall score
            score = self._calculate_score()
            
            # Generate report
            report = self._generate_report(score, query_analyses)
            
            return report
            
        except Exception as e:
            return {'error': f'Failed to analyze file: {str(e)}'}
    
    def _extract_mcode_queries(self, content: str) -> Dict[str, str]:
        """Extract M-code queries from TMDL content."""
        queries = {}
        
        # Pattern to match partition definitions with M-code
        partition_pattern = r'partition\s+([^\s]+)\s*=\s*m\s*\n\s*mode:\s*import\s*\n\s*source\s*=\s*(.*?)(?=\n\s*partition\s+|\n\s*measure\s+|\n\s*column\s+|\n─{40}|\Z)'
        
        matches = re.finditer(partition_pattern, content, re.DOTALL)
        
        for match in matches:
            partition_name = match.group(1).strip().strip("'")
            mcode_content = match.group(2).strip()
            
            # Clean up the M-code
            if mcode_content.startswith('let'):
                queries[partition_name] = mcode_content
        
        return queries
    
    def _analyze_query(self, query_name: str, mcode: str) -> Dict:
        """Analyze a single M-code query."""
        analysis = {
            'query_name': query_name,
            'steps': [],
            'issues': [],
            'statistics': {
                'total_steps': 0,
                'folding_preserved': True,
                'has_error_handling': False,
                'has_documentation': False,
                'data_source_type': None
            }
        }
        
        # Parse query steps
        steps = self._parse_query_steps(mcode)
        analysis['steps'] = steps
        analysis['statistics']['total_steps'] = len(steps)
        
        # Run all checks
        self._check_query_folding(query_name, steps)
        self._check_performance_issues(query_name, steps)
        self._check_error_handling(query_name, mcode)
        self._check_data_quality(query_name, steps)
        self._check_naming_conventions(query_name, steps)
        self._check_documentation(query_name, mcode)
        self._check_security_issues(query_name, mcode)
        self._check_best_practices(query_name, steps)
        
        # Collect query-specific issues
        analysis['issues'] = [issue for issue in self.issues if issue.query_name == query_name]
        
        return analysis
    
    def _parse_query_steps(self, mcode: str) -> List[Dict]:
        """Parse M-code query into steps."""
        steps = []
        
        # Pattern to match step definitions
        step_pattern = r'(\w+)\s*=\s*([^,\n]+(?:\n\s+[^,\n]+)*)'
        
        # Find let...in block
        let_match = re.search(r'let\s+(.*?)\s+in\s+(\w+)', mcode, re.DOTALL)
        if not let_match:
            return steps
        
        steps_content = let_match.group(1)
        final_step = let_match.group(2)
        
        # Parse individual steps
        step_matches = re.finditer(step_pattern, steps_content)
        
        for i, match in enumerate(step_matches):
            step_name = match.group(1)
            step_expression = match.group(2).strip()
            
            # Identify step type
            step_type = self._identify_step_type(step_expression)
            
            steps.append({
                'name': step_name,
                'expression': step_expression,
                'type': step_type,
                'line_number': i + 1,
                'breaks_folding': self._check_breaks_folding(step_expression)
            })
        
        return steps
    
    def _identify_step_type(self, expression: str) -> str:
        """Identify the type of M-code step."""
        if expression.startswith('Sql.Database'):
            return 'SQL Source'
        elif expression.startswith('Excel.Workbook'):
            return 'Excel Source'
        elif expression.startswith('Csv.Document'):
            return 'CSV Source'
        elif expression.startswith('Web.Contents'):
            return 'Web Source'
        elif expression.startswith('OData.Feed'):
            return 'OData Source'
        elif expression.startswith('SharePoint.'):
            return 'SharePoint Source'
        elif 'Table.SelectRows' in expression:
            return 'Filter'
        elif 'Table.SelectColumns' in expression:
            return 'Select Columns'
        elif 'Table.TransformColumnTypes' in expression:
            return 'Change Types'
        elif 'Table.AddColumn' in expression:
            return 'Add Column'
        elif 'Table.Join' in expression or 'Table.NestedJoin' in expression:
            return 'Join'
        elif 'Table.Group' in expression:
            return 'Group By'
        elif 'Table.Sort' in expression:
            return 'Sort'
        elif 'Table.RenameColumns' in expression:
            return 'Rename Columns'
        else:
            return 'Transformation'
    
    def _check_breaks_folding(self, expression: str) -> bool:
        """Check if an expression breaks query folding."""
        for breaker in self.folding_breakers:
            if breaker in expression:
                return True
        return False
    
    def _check_query_folding(self, query_name: str, steps: List[Dict]):
        """Check for query folding issues."""
        folding_broken_at = None
        source_type = None
        
        # Identify source type
        if steps:
            first_step = steps[0]
            if 'Sql.Database' in first_step['expression']:
                source_type = 'SQL'
            elif 'OData.Feed' in first_step['expression']:
                source_type = 'OData'
            elif 'SharePoint.' in first_step['expression']:
                source_type = 'SharePoint'
        
        # Only check folding for foldable sources
        if source_type not in ['SQL', 'OData', 'SharePoint']:
            return
        
        # Check each step for folding
        for i, step in enumerate(steps):
            if step['breaks_folding'] and not folding_broken_at:
                folding_broken_at = i
                
                # Check if there are filterable operations after folding break
                remaining_steps = steps[i+1:]
                has_filters_after = any('SelectRows' in s['expression'] for s in remaining_steps)
                
                if has_filters_after:
                    self.issues.append(MCodeIssue(
                        category=MCodeIssueCategory.QUERY_FOLDING,
                        severity=MCodeSeverity.HIGH,
                        title="Query Folding Broken Before Filters",
                        description=f"Query folding breaks at step '{step['name']}' but filters are applied later",
                        recommendation="Move filtering operations before operations that break query folding to improve performance.",
                        impact_score=8,
                        line_number=step['line_number'],
                        code_snippet=step['expression'][:100] + '...' if len(step['expression']) > 100 else step['expression'],
                        query_name=query_name
                    ))
                else:
                    self.issues.append(MCodeIssue(
                        category=MCodeIssueCategory.QUERY_FOLDING,
                        severity=MCodeSeverity.MEDIUM,
                        title="Query Folding Broken",
                        description=f"Query folding breaks at step '{step['name']}' due to {step['type']} operation",
                        recommendation="Consider restructuring the query to preserve folding longer, or ensure this is necessary.",
                        impact_score=4,
                        line_number=step['line_number'],
                        code_snippet=step['expression'][:100] + '...' if len(step['expression']) > 100 else step['expression'],
                        query_name=query_name
                    ))
    
    def _check_performance_issues(self, query_name: str, steps: List[Dict]):
        """Check for performance-related issues."""
        # Check for nested queries
        nested_count = sum(1 for step in steps if 'Table.AddColumn' in step['expression'] and 'each' in step['expression'])
        if nested_count > 3:
            self.issues.append(MCodeIssue(
                category=MCodeIssueCategory.PERFORMANCE,
                severity=MCodeSeverity.HIGH,
                title="Excessive Nested Operations",
                description=f"Query has {nested_count} nested operations using 'each'",
                recommendation="Consider consolidating nested operations or using Table.TransformColumns for better performance.",
                impact_score=6,
                query_name=query_name
            ))
        
        # Check for multiple sorts
        sort_count = sum(1 for step in steps if 'Table.Sort' in step['expression'])
        if sort_count > 1:
            self.issues.append(MCodeIssue(
                category=MCodeIssueCategory.PERFORMANCE,
                severity=MCodeSeverity.MEDIUM,
                title="Multiple Sort Operations",
                description=f"Query contains {sort_count} sort operations",
                recommendation="Consolidate sort operations into a single step where possible.",
                impact_score=3,
                query_name=query_name
            ))
        
        # Check for Table.Contains usage (inefficient)
        for step in steps:
            if 'Table.Contains' in step['expression']:
                self.issues.append(MCodeIssue(
                    category=MCodeIssueCategory.PERFORMANCE,
                    severity=MCodeSeverity.HIGH,
                    title="Inefficient Table.Contains Usage",
                    description=f"Step '{step['name']}' uses Table.Contains which is inefficient",
                    recommendation="Use Table.Join or List.Contains with Table.SelectRows for better performance.",
                    impact_score=6,
                    line_number=step['line_number'],
                    query_name=query_name
                ))
        
        # Check for missing data type definitions
        has_type_definition = any('Table.TransformColumnTypes' in step['expression'] for step in steps)
        if not has_type_definition and len(steps) > 3:
            self.issues.append(MCodeIssue(
                category=MCodeIssueCategory.PERFORMANCE,
                severity=MCodeSeverity.MEDIUM,
                title="Missing Data Type Definitions",
                description="Query doesn't explicitly set data types",
                recommendation="Add Table.TransformColumnTypes early in the query to avoid automatic type detection overhead.",
                impact_score=4,
                query_name=query_name
            ))
    
    def _check_error_handling(self, query_name: str, mcode: str):
        """Check for proper error handling."""
        has_try = 'try' in mcode.lower()
        has_otherwise = 'otherwise' in mcode.lower()
        
        if not has_try and len(mcode) > 200:  # Only for non-trivial queries
            self.issues.append(MCodeIssue(
                category=MCodeIssueCategory.ERROR_HANDLING,
                severity=MCodeSeverity.MEDIUM,
                title="No Error Handling",
                description="Query lacks try...otherwise error handling",
                recommendation="Add try...otherwise blocks to handle potential errors gracefully.",
                impact_score=3,
                query_name=query_name
            ))
        
        # Check for error handling in critical operations
        critical_patterns = [
            (r'Web\.Contents', 'web requests'),
            (r'Sql\.Database', 'database connections'),
            (r'Value\.FromText', 'type conversions')
        ]
        
        for pattern, operation in critical_patterns:
            if re.search(pattern, mcode) and not has_try:
                self.issues.append(MCodeIssue(
                    category=MCodeIssueCategory.ERROR_HANDLING,
                    severity=MCodeSeverity.HIGH,
                    title=f"No Error Handling for {operation.title()}",
                    description=f"Query performs {operation} without error handling",
                    recommendation=f"Wrap {operation} in try...otherwise blocks to handle failures.",
                    impact_score=5,
                    query_name=query_name
                ))
    
    def _check_data_quality(self, query_name: str, steps: List[Dict]):
        """Check for data quality issues."""
        # Check for null handling
        has_null_handling = any(
            any(func in step['expression'] for func in ['Table.ReplaceValue', 'Table.FillDown', 'Table.FillUp', 'Table.TransformColumns'])
            for step in steps
        )
        
        if not has_null_handling and len(steps) > 5:
            self.issues.append(MCodeIssue(
                category=MCodeIssueCategory.DATA_QUALITY,
                severity=MCodeSeverity.LOW,
                title="No Explicit Null Handling",
                description="Query doesn't explicitly handle null values",
                recommendation="Consider adding null value handling using Table.ReplaceValue or conditional columns.",
                impact_score=2,
                query_name=query_name
            ))
        
        # Check for duplicate handling
        has_distinct = any('Table.Distinct' in step['expression'] for step in steps)
        has_group = any('Table.Group' in step['expression'] for step in steps)
        
        if not has_distinct and not has_group and len(steps) > 5:
            self.issues.append(MCodeIssue(
                category=MCodeIssueCategory.DATA_QUALITY,
                severity=MCodeSeverity.INFO,
                title="No Duplicate Handling",
                description="Query doesn't check for or remove duplicates",
                recommendation="Consider using Table.Distinct if duplicate rows should be removed.",
                impact_score=0,
                query_name=query_name
            ))
        
        # Check for data validation
        has_validation = any(
            'Table.SelectRows' in step['expression'] and any(op in step['expression'] for op in ['>', '<', '=', '<>'])
            for step in steps
        )
        
        if not has_validation and len(steps) > 5:
            self.issues.append(MCodeIssue(
                category=MCodeIssueCategory.DATA_QUALITY,
                severity=MCodeSeverity.LOW,
                title="No Data Validation",
                description="Query doesn't include data validation checks",
                recommendation="Consider adding data validation using Table.SelectRows to filter invalid data.",
                impact_score=2,
                query_name=query_name
            ))
    
    def _check_naming_conventions(self, query_name: str, steps: List[Dict]):
        """Check for consistent naming conventions."""
        step_names = [step['name'] for step in steps]
        
        # Check for inconsistent naming
        has_spaces = any(' ' in name for name in step_names)
        has_underscores = any('_' in name for name in step_names)
        has_camelCase = any(name[0].islower() and any(c.isupper() for c in name[1:]) for name in step_names)
        
        naming_styles = sum([has_spaces, has_underscores, has_camelCase])
        
        if naming_styles > 1:
            self.issues.append(MCodeIssue(
                category=MCodeIssueCategory.NAMING,
                severity=MCodeSeverity.LOW,
                title="Inconsistent Step Naming",
                description="Query uses mixed naming conventions for steps",
                recommendation="Use consistent naming convention (e.g., PascalCase without spaces).",
                impact_score=1,
                query_name=query_name
            ))
        
        # Check for non-descriptive names
        generic_names = ['Custom1', 'Custom2', 'Step1', 'Step2', 'Table1', 'Table2']
        generic_count = sum(1 for name in step_names if any(generic in name for generic in generic_names))
        
        if generic_count > 2:
            self.issues.append(MCodeIssue(
                category=MCodeIssueCategory.NAMING,
                severity=MCodeSeverity.MEDIUM,
                title="Non-Descriptive Step Names",
                description=f"Query contains {generic_count} generic step names",
                recommendation="Use descriptive names that indicate the purpose of each transformation step.",
                impact_score=2,
                query_name=query_name
            ))
    
    def _check_documentation(self, query_name: str, mcode: str):
        """Check for query documentation."""
        has_comments = '//' in mcode or '/*' in mcode
        
        if not has_comments and len(mcode) > 500:
            self.issues.append(MCodeIssue(
                category=MCodeIssueCategory.DOCUMENTATION,
                severity=MCodeSeverity.LOW,
                title="No Documentation",
                description="Complex query lacks comments or documentation",
                recommendation="Add comments to explain complex transformations and business logic.",
                impact_score=1,
                query_name=query_name
            ))
    
    def _check_security_issues(self, query_name: str, mcode: str):
        """Check for security-related issues."""
        # Check for hardcoded credentials
        credential_patterns = [
            (r'password\s*=\s*"[^"]+"', 'password'),
            (r'pwd\s*=\s*"[^"]+"', 'password'),
            (r'apikey\s*=\s*"[^"]+"', 'API key'),
            (r'token\s*=\s*"[^"]+"', 'token')
        ]
        
        for pattern, cred_type in credential_patterns:
            if re.search(pattern, mcode, re.IGNORECASE):
                self.issues.append(MCodeIssue(
                    category=MCodeIssueCategory.SECURITY,
                    severity=MCodeSeverity.CRITICAL,
                    title=f"Hardcoded {cred_type.title()}",
                    description=f"Query contains hardcoded {cred_type}",
                    recommendation=f"Use Power BI parameters or data source settings to store {cred_type}s securely.",
                    impact_score=10,
                    query_name=query_name
                ))
        
        # Check for unsecured web requests
        if 'Web.Contents' in mcode and 'https://' not in mcode:
            self.issues.append(MCodeIssue(
                category=MCodeIssueCategory.SECURITY,
                severity=MCodeSeverity.HIGH,
                title="Unsecured Web Request",
                description="Query makes HTTP requests without SSL/TLS",
                recommendation="Use HTTPS for all web requests to ensure data security.",
                impact_score=6,
                query_name=query_name
            ))
    
    def _check_best_practices(self, query_name: str, steps: List[Dict]):
        """Check for general best practices."""
        # Check for buffer usage
        has_buffer = any('Table.Buffer' in step['expression'] or 'List.Buffer' in step['expression'] 
                        for step in steps)
        
        # Check if buffer might be needed
        has_multiple_references = False
        step_names = [step['name'] for step in steps]
        for i, step in enumerate(steps):
            for prev_step_name in step_names[:i]:
                if step['expression'].count(prev_step_name) > 1:
                    has_multiple_references = True
                    break
        
        if has_multiple_references and not has_buffer:
            self.issues.append(MCodeIssue(
                category=MCodeIssueCategory.PERFORMANCE,
                severity=MCodeSeverity.MEDIUM,
                title="Missing Buffer for Multiple References",
                description="Query references previous steps multiple times without buffering",
                recommendation="Consider using Table.Buffer or List.Buffer for steps that are referenced multiple times.",
                impact_score=3,
                query_name=query_name
            ))
        
        # Check for column selection efficiency
        remove_columns_count = sum(1 for step in steps if 'Table.RemoveColumns' in step['expression'])
        select_columns_count = sum(1 for step in steps if 'Table.SelectColumns' in step['expression'])
        
        if remove_columns_count > 2 and select_columns_count == 0:
            self.issues.append(MCodeIssue(
                category=MCodeIssueCategory.BEST_PRACTICES,
                severity=MCodeSeverity.MEDIUM,
                title="Inefficient Column Removal",
                description=f"Query uses Table.RemoveColumns {remove_columns_count} times",
                recommendation="Consider using Table.SelectColumns once to keep only needed columns instead of multiple removals.",
                impact_score=3,
                query_name=query_name
            ))
        
        # Check for appropriate data source usage
        if len(steps) > 0:
            first_step = steps[0]
            if 'Excel.Workbook' in first_step['expression']:
                self.issues.append(MCodeIssue(
                    category=MCodeIssueCategory.BEST_PRACTICES,
                    severity=MCodeSeverity.INFO,
                    title="Excel as Data Source",
                    description="Query uses Excel as data source",
                    recommendation="For production models, consider using a database or data warehouse for better performance and reliability.",
                    impact_score=0,
                    query_name=query_name
                ))
        
        # Check for merge operations
        merge_count = sum(1 for step in steps if 'Table.Join' in step['expression'] or 'Table.NestedJoin' in step['expression'])
        if merge_count > 2:
            self.issues.append(MCodeIssue(
                category=MCodeIssueCategory.PERFORMANCE,
                severity=MCodeSeverity.MEDIUM,
                title="Multiple Merge Operations",
                description=f"Query contains {merge_count} merge/join operations",
                recommendation="Consider consolidating data at the source or using a star schema to reduce joins.",
                impact_score=4,
                query_name=query_name
            ))
        
        # Check for list operations in row context
        for step in steps:
            if 'Table.AddColumn' in step['expression'] and 'List.' in step['expression']:
                self.issues.append(MCodeIssue(
                    category=MCodeIssueCategory.PERFORMANCE,
                    severity=MCodeSeverity.HIGH,
                    title="List Operation in Row Context",
                    description=f"Step '{step['name']}' uses List operations in Table.AddColumn",
                    recommendation="List operations in row context can be slow. Consider alternative approaches or pre-calculating values.",
                    impact_score=5,
                    line_number=step['line_number'],
                    query_name=query_name
                ))
    
    def _calculate_score(self) -> int:
        """Calculate overall score out of 100 based on issues found."""
        score = self.max_score
        
        # Group issues by category
        category_penalties = {category: 0 for category in MCodeIssueCategory}
        
        for issue in self.issues:
            # Apply penalty based on severity and impact
            penalty = issue.impact_score
            
            # Adjust penalty based on severity
            severity_multipliers = {
                MCodeSeverity.CRITICAL: 1.5,
                MCodeSeverity.HIGH: 1.0,
                MCodeSeverity.MEDIUM: 0.7,
                MCodeSeverity.LOW: 0.3,
                MCodeSeverity.INFO: 0
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
    
    def _generate_report(self, score: int, query_analyses: Dict) -> Dict:
        """Generate comprehensive analysis report."""
        # Group issues by severity
        issues_by_severity = {
            MCodeSeverity.CRITICAL: [],
            MCodeSeverity.HIGH: [],
            MCodeSeverity.MEDIUM: [],
            MCodeSeverity.LOW: [],
            MCodeSeverity.INFO: []
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
        
        # Calculate statistics
        total_queries = len(query_analyses)
        total_steps = sum(len(analysis['steps']) for analysis in query_analyses.values())
        queries_with_folding_issues = sum(
            1 for issue in self.issues 
            if issue.category == MCodeIssueCategory.QUERY_FOLDING
        )
        
        # Top recommendations
        top_recommendations = []
        critical_issues = issues_by_severity[MCodeSeverity.CRITICAL][:3]
        high_issues = issues_by_severity[MCodeSeverity.HIGH][:5]
        
        for issue in critical_issues + high_issues:
            if issue.recommendation not in top_recommendations:
                top_recommendations.append(issue.recommendation)
        
        # Category breakdown
        category_scores = self._calculate_category_scores()
        
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
            'statistics': {
                'total_queries': total_queries,
                'total_steps': total_steps,
                'queries_with_folding_issues': queries_with_folding_issues
            },
            'query_analyses': query_analyses,
            'category_scores': category_scores
        }
    
    def _issue_to_dict(self, issue: MCodeIssue) -> Dict:
        """Convert MCodeIssue to dictionary for JSON serialization."""
        return {
            'category': issue.category.value,
            'severity': issue.severity.value,
            'title': issue.title,
            'description': issue.description,
            'recommendation': issue.recommendation,
            'impact_score': issue.impact_score,
            'line_number': issue.line_number,
            'code_snippet': issue.code_snippet,
            'query_name': issue.query_name
        }
    
    def _calculate_category_scores(self) -> Dict[str, Dict]:
        """Calculate score breakdown by category."""
        category_scores = {}
        
        for category in MCodeIssueCategory:
            # Calculate penalty for this category
            penalty = sum(
                issue.impact_score * {
                    MCodeSeverity.CRITICAL: 1.5,
                    MCodeSeverity.HIGH: 1.0,
                    MCodeSeverity.MEDIUM: 0.7,
                    MCodeSeverity.LOW: 0.3,
                    MCodeSeverity.INFO: 0
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
    """Analyze M-code from a TMDL file and print a formatted report."""
    analyzer = EnhancedMCodeAnalyzer()
    report = analyzer.analyze_tmdl_file(filepath)
    
    if 'error' in report:
        print(f"❌ Error: {report['error']}")
        return
    
    # Print header
    print("\n" + "="*80)
    print(f"M-CODE ANALYSIS REPORT (Power Query Best Practices)")
    print("="*80)
    
    # Print score and grade
    print(f"\n📊 OVERALL SCORE: {report['score']}/{report['max_score']} ({report['grade']})")
    print(f"   Grade: {report['grade_description']}")
    
    # Print statistics
    print(f"\n📈 QUERY STATISTICS:")
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
        print(f"      Query: {issue['query_name']}")
        print(f"      {issue['description']}")
        if issue['code_snippet']:
            print(f"      Code: {issue['code_snippet']}")
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
        print("Usage: python enhanced_mcode_analyzer.py <tmdl_file_path>") 