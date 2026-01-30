"""
DAX Analyzer - Checks DAX expressions against Microsoft Best Practice rules.

Based on Microsoft's official BPA rules:
https://github.com/microsoft/Analysis-Services/tree/master/BestPracticeRules
"""

import re
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum


class DaxSeverity(Enum):
    CRITICAL = 3
    WARNING = 2
    INFO = 1


@dataclass
class DaxIssue:
    rule_id: str
    name: str
    category: str
    severity: DaxSeverity
    description: str
    recommendation: str
    location: str
    expression_snippet: Optional[str] = None


class DaxAnalyzer:
    """
    Analyzes DAX expressions against Microsoft Best Practice rules.
    """
    
    def __init__(self):
        self.issues: List[DaxIssue] = []
    
    def analyze_measure(self, name: str, expression: str, table_name: str = "") -> List[DaxIssue]:
        """Analyze a single DAX measure expression."""
        issues = []
        location = f"Measure: {name}" if not table_name else f"Table: {table_name}, Measure: {name}"
        
        # Rule: USE_THE_DIVIDE_FUNCTION_FOR_DIVISION
        if self._check_division_operator(expression):
            issues.append(DaxIssue(
                rule_id="USE_THE_DIVIDE_FUNCTION_FOR_DIVISION",
                name="Use the DIVIDE function for division",
                category="DAX Expressions",
                severity=DaxSeverity.WARNING,
                description="Use the DIVIDE function instead of using '/'. DIVIDE handles divide-by-zero cases safely.",
                recommendation="Replace 'X / Y' with 'DIVIDE(X, Y)' or 'DIVIDE(X, Y, 0)' for safe division.",
                location=location,
                expression_snippet=self._get_snippet(expression, r'\]\s*/(?!/)')
            ))
        
        # Rule: AVOID_USING_THE_IFERROR_FUNCTION
        if self._check_iferror(expression):
            issues.append(DaxIssue(
                rule_id="AVOID_USING_THE_IFERROR_FUNCTION",
                name="Avoid using the IFERROR function",
                category="DAX Expressions",
                severity=DaxSeverity.WARNING,
                description="IFERROR can cause performance degradation. Use DIVIDE for divide-by-zero handling.",
                recommendation="Replace IFERROR with DIVIDE or proper error handling using IF/ISBLANK.",
                location=location
            ))
        
        # Rule: DAX_COLUMNS_FULLY_QUALIFIED
        if self._check_unqualified_columns(expression):
            issues.append(DaxIssue(
                rule_id="DAX_COLUMNS_FULLY_QUALIFIED",
                name="Column references should be fully qualified",
                category="DAX Expressions",
                severity=DaxSeverity.CRITICAL,
                description="Column references should include the table name for clarity and to avoid errors.",
                recommendation="Use 'TableName'[ColumnName] instead of just [ColumnName] for columns.",
                location=location
            ))
        
        # Rule: AVOID_USING_'1-(X/Y)'_SYNTAX
        if self._check_one_minus_syntax(expression):
            issues.append(DaxIssue(
                rule_id="AVOID_1_MINUS_SYNTAX",
                name="Avoid using '1-(x/y)' syntax",
                category="DAX Expressions",
                severity=DaxSeverity.WARNING,
                description="'1-(x/y)' syntax can cause performance issues and may return values when it should be blank.",
                recommendation="Use DIVIDE and basic arithmetic: DIVIDE(X - Y, X) instead of 1 - (Y/X).",
                location=location
            ))
        
        # Rule: USE_THE_TREATAS_FUNCTION_INSTEAD_OF_INTERSECT
        if self._check_intersect_usage(expression):
            issues.append(DaxIssue(
                rule_id="USE_TREATAS_INSTEAD_OF_INTERSECT",
                name="Use TREATAS instead of INTERSECT for virtual relationships",
                category="DAX Expressions",
                severity=DaxSeverity.WARNING,
                description="TREATAS is more efficient than INTERSECT for virtual relationships.",
                recommendation="Replace INTERSECT with TREATAS for better performance.",
                location=location
            ))
        
        # Rule: FILTER_COLUMN_VALUES (using FILTER with simple column comparison)
        if self._check_filter_table_syntax(expression):
            issues.append(DaxIssue(
                rule_id="FILTER_COLUMN_VALUES",
                name="Filter column values with proper syntax",
                category="DAX Expressions",
                severity=DaxSeverity.WARNING,
                description="Using FILTER('Table', 'Table'[Column] = value) is inefficient.",
                recommendation="Use 'Table'[Column] = value directly in CALCULATE, or use KEEPFILTERS.",
                location=location
            ))
        
        # Rule: MEASURES_SHOULD_NOT_BE_DIRECT_REFERENCES
        if self._check_direct_measure_reference(expression, name):
            issues.append(DaxIssue(
                rule_id="MEASURES_DIRECT_REFERENCE",
                name="Measure is just a reference to another measure",
                category="DAX Expressions",
                severity=DaxSeverity.WARNING,
                description="This measure is simply a reference to another measure, which is redundant.",
                recommendation="Remove duplicate measure or use the original measure directly.",
                location=location
            ))
        
        # Rule: AVOID_USING_NOW/TODAY_IN_MEASURES
        if self._check_time_functions(expression):
            issues.append(DaxIssue(
                rule_id="AVOID_NOW_TODAY",
                name="Avoid NOW() and TODAY() functions",
                category="DAX Expressions",
                severity=DaxSeverity.INFO,
                description="NOW() and TODAY() prevent query caching and can hurt performance.",
                recommendation="Use a date table with a 'IsToday' flag or pass the date as a parameter.",
                location=location
            ))
        
        # Rule: Use SELECTEDVALUE instead of VALUES with IF/HASONEVALUE
        if self._check_values_with_hasonevalue(expression):
            issues.append(DaxIssue(
                rule_id="USE_SELECTEDVALUE",
                name="Use SELECTEDVALUE instead of IF/HASONEVALUE pattern",
                category="DAX Expressions",
                severity=DaxSeverity.INFO,
                description="The HASONEVALUE + VALUES pattern can be simplified with SELECTEDVALUE.",
                recommendation="Replace IF(HASONEVALUE(...), VALUES(...)) with SELECTEDVALUE(...).",
                location=location
            ))
        
        # Rule: COUNTROWS instead of COUNT
        if self._check_count_vs_countrows(expression):
            issues.append(DaxIssue(
                rule_id="USE_COUNTROWS",
                name="Consider COUNTROWS instead of COUNT",
                category="DAX Expressions",
                severity=DaxSeverity.INFO,
                description="COUNTROWS is generally more efficient and clearer than COUNT.",
                recommendation="Use COUNTROWS(Table) instead of COUNT(Table[Column]).",
                location=location
            ))
        
        # Check for variables usage (positive pattern)
        if self._check_repeated_expression_no_vars(expression):
            issues.append(DaxIssue(
                rule_id="USE_VARIABLES",
                name="Consider using variables for repeated expressions",
                category="DAX Expressions",
                severity=DaxSeverity.INFO,
                description="Repeated subexpressions could be optimized with VAR statements.",
                recommendation="Use VAR to store repeated calculations and RETURN the result.",
                location=location
            ))
        
        return issues
    
    def analyze_calculated_column(self, name: str, expression: str, table_name: str) -> List[DaxIssue]:
        """Analyze a calculated column expression."""
        issues = []
        location = f"Table: {table_name}, Calculated Column: {name}"
        
        # Rule: REDUCE_USAGE_OF_CALCULATED_COLUMNS_THAT_USE_THE_RELATED_FUNCTION
        if self._check_related_function(expression):
            issues.append(DaxIssue(
                rule_id="CALC_COL_USES_RELATED",
                name="Calculated column uses RELATED function",
                category="Performance",
                severity=DaxSeverity.WARNING,
                description="Calculated columns with RELATED don't compress well and slow processing.",
                recommendation="Consider moving this logic to the data source or using a measure.",
                location=location
            ))
        
        # Check for aggregations in calculated columns
        if self._check_aggregation_functions(expression):
            issues.append(DaxIssue(
                rule_id="CALC_COL_AGGREGATION",
                name="Calculated column uses aggregation",
                category="Performance",
                severity=DaxSeverity.CRITICAL,
                description="Aggregations in calculated columns should be measures instead.",
                recommendation="Convert this calculated column to a measure.",
                location=location
            ))
        
        return issues
    
    def _check_division_operator(self, expression: str) -> bool:
        """Check if expression uses / operator instead of DIVIDE."""
        # Match ] followed by / but not // (comment) or /* (block comment)
        pattern = r'\]\s*/(?!/|\*)'
        if re.search(pattern, expression):
            return True
        # Also check for ) / pattern
        pattern2 = r'\)\s*/(?!/|\*)'
        return bool(re.search(pattern2, expression))
    
    def _check_iferror(self, expression: str) -> bool:
        """Check if expression uses IFERROR."""
        return bool(re.search(r'(?i)\bIFERROR\s*\(', expression))
    
    def _check_unqualified_columns(self, expression: str) -> bool:
        """Check for unqualified column references (columns without table prefix)."""
        # This is tricky - we need to find [Column] not preceded by table name
        # Look for [something] not preceded by ' or alphanumeric
        # Exclude measure references which legitimately don't have table names
        
        # Find all bracket references
        brackets = re.findall(r'(?<![\'"\w])\[([^\]]+)\]', expression)
        
        # If there are brackets not preceded by a table reference, flag it
        # But we need context - simple heuristic: if there are MANY such brackets, likely issue
        if len(brackets) > 3:
            # Check if most are qualified (preceded by ')
            qualified_count = len(re.findall(r"'[^']+'\s*\[[^\]]+\]", expression))
            unqualified_count = len(brackets) - qualified_count
            return unqualified_count > qualified_count
        
        return False
    
    def _check_one_minus_syntax(self, expression: str) -> bool:
        """Check for 1 - (x/y) or 1 + (x/y) patterns."""
        pattern = r'1\s*[-+]\s*\('
        return bool(re.search(pattern, expression))
    
    def _check_intersect_usage(self, expression: str) -> bool:
        """Check if expression uses INTERSECT."""
        return bool(re.search(r'(?i)\bINTERSECT\s*\(', expression))
    
    def _check_filter_table_syntax(self, expression: str) -> bool:
        """Check for FILTER('Table', 'Table'[Column] = value) pattern."""
        pattern = r"(?i)CALCULATE\s*\([^,]+,\s*FILTER\s*\(\s*'[^']+'\s*,"
        return bool(re.search(pattern, expression))
    
    def _check_direct_measure_reference(self, expression: str, measure_name: str) -> bool:
        """Check if measure is just a reference to another measure."""
        # Clean expression
        cleaned = expression.strip()
        # If it's just [MeasureName], it's a direct reference
        if re.match(r'^\[[^\]]+\]$', cleaned):
            return True
        return False
    
    def _check_time_functions(self, expression: str) -> bool:
        """Check for NOW() or TODAY() usage."""
        return bool(re.search(r'(?i)\b(NOW|TODAY)\s*\(\s*\)', expression))
    
    def _check_values_with_hasonevalue(self, expression: str) -> bool:
        """Check for IF(HASONEVALUE(...), VALUES(...)) pattern."""
        pattern = r'(?i)IF\s*\(\s*HASONEVALUE\s*\('
        return bool(re.search(pattern, expression))
    
    def _check_count_vs_countrows(self, expression: str) -> bool:
        """Check if COUNT is used where COUNTROWS would be better."""
        # COUNT on a column where COUNTROWS(table) would work
        return bool(re.search(r'(?i)\bCOUNT\s*\(\s*\'[^\']+\'\s*\[[^\]]+\]\s*\)', expression))
    
    def _check_repeated_expression_no_vars(self, expression: str) -> bool:
        """Check for repeated expressions that could use VAR."""
        if 'VAR ' in expression.upper():
            return False  # Already using variables
        
        # Find all function calls
        functions = re.findall(r'([A-Z]+\s*\([^)]+\))', expression.upper())
        
        # Check for duplicates
        seen = set()
        for func in functions:
            if func in seen and len(func) > 10:  # Ignore short expressions
                return True
            seen.add(func)
        
        return False
    
    def _check_related_function(self, expression: str) -> bool:
        """Check if expression uses RELATED function."""
        return bool(re.search(r'(?i)\bRELATED\s*\(', expression))
    
    def _check_aggregation_functions(self, expression: str) -> bool:
        """Check if expression uses aggregation functions."""
        agg_functions = ['SUM', 'AVERAGE', 'COUNT', 'COUNTROWS', 'MIN', 'MAX', 
                        'SUMX', 'AVERAGEX', 'COUNTX', 'MINX', 'MAXX']
        pattern = r'(?i)\b(' + '|'.join(agg_functions) + r')\s*\('
        return bool(re.search(pattern, expression))
    
    def _get_snippet(self, expression: str, pattern: str, context: int = 30) -> Optional[str]:
        """Get a snippet of expression around the matched pattern."""
        match = re.search(pattern, expression)
        if match:
            start = max(0, match.start() - context)
            end = min(len(expression), match.end() + context)
            snippet = expression[start:end]
            if start > 0:
                snippet = "..." + snippet
            if end < len(expression):
                snippet = snippet + "..."
            return snippet
        return None


def get_dax_complexity_score(expression: str) -> Tuple[int, str]:
    """
    Calculate DAX complexity score and level.
    
    Returns:
        Tuple of (score, level) where level is Low/Medium/High/Very High
    """
    score = 0
    
    # Count nested parentheses
    max_depth = 0
    current_depth = 0
    for char in expression:
        if char == '(':
            current_depth += 1
            max_depth = max(max_depth, current_depth)
        elif char == ')':
            current_depth -= 1
    score += max_depth * 2
    
    # Count complex functions
    complex_functions = {
        'CALCULATE': 3,
        'CALCULATETABLE': 3,
        'FILTER': 2,
        'ALL': 2,
        'ALLEXCEPT': 2,
        'ALLSELECTED': 2,
        'SUMX': 2,
        'AVERAGEX': 2,
        'COUNTX': 2,
        'MAXX': 2,
        'MINX': 2,
        'EARLIER': 3,
        'EARLIEST': 3,
        'SWITCH': 1,
        'USERELATIONSHIP': 2,
        'CROSSFILTER': 2,
        'TREATAS': 2,
        'SUMMARIZE': 2,
        'SUMMARIZECOLUMNS': 2,
        'ADDCOLUMNS': 2,
        'SELECTCOLUMNS': 1,
        'GENERATE': 3,
        'GENERATEALL': 3,
        'GROUPBY': 2,
    }
    
    expr_upper = expression.upper()
    for func, weight in complex_functions.items():
        count = len(re.findall(rf'\b{func}\s*\(', expr_upper))
        score += count * weight
    
    # Count variables (good practice, slight complexity)
    var_count = len(re.findall(r'\bVAR\s+', expr_upper))
    score += var_count * 0.5
    
    # Line count
    line_count = len(expression.strip().split('\n'))
    score += line_count * 0.5
    
    # Determine level
    if score < 5:
        level = "Low"
    elif score < 15:
        level = "Medium"
    elif score < 30:
        level = "High"
    else:
        level = "Very High"
    
    return int(score), level
