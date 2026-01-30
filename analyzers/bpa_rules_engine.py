"""
BPA Rules Engine - Loads and applies Microsoft Best Practice Analyzer rules.

This module translates Microsoft's C#-based BPA rules into Python checks.
"""

import json
import re
from pathlib import Path
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass
from enum import Enum


class BPACategory(Enum):
    PERFORMANCE = "Performance"
    DAX_EXPRESSIONS = "DAX Expressions"
    ERROR_PREVENTION = "Error Prevention"
    MAINTENANCE = "Maintenance"
    FORMATTING = "Formatting"


@dataclass
class BPARule:
    """Represents a Best Practice Analyzer rule."""
    id: str
    name: str
    category: str
    description: str
    severity: int  # 1=Info, 2=Warning, 3=Error
    scope: str  # What objects this applies to
    check_function: Optional[Callable] = None


@dataclass
class BPAViolation:
    """Represents a BPA rule violation."""
    rule_id: str
    rule_name: str
    category: str
    severity: int
    description: str
    recommendation: str
    object_type: str
    object_name: str
    table_name: Optional[str] = None
    details: Optional[Dict] = None


class BPARulesEngine:
    """
    Loads Microsoft BPA rules and applies them to semantic models.
    """
    
    def __init__(self, rules_path: Optional[str] = None):
        """
        Initialize the BPA rules engine.
        
        Args:
            rules_path: Path to the Microsoft BPA rules JSON file.
        """
        self.rules: List[BPARule] = []
        self.violations: List[BPAViolation] = []
        
        # Load rules from JSON if provided
        if rules_path:
            self._load_rules_from_json(rules_path)
        
        # Register built-in Python implementations
        self._register_rule_implementations()
    
    def _load_rules_from_json(self, rules_path: str) -> None:
        """Load rules from Microsoft's BPA rules JSON."""
        try:
            with open(rules_path, 'r', encoding='utf-8') as f:
                rules_data = json.load(f)
            
            for rule in rules_data:
                self.rules.append(BPARule(
                    id=rule.get('ID', ''),
                    name=rule.get('Name', ''),
                    category=rule.get('Category', 'Performance'),
                    description=rule.get('Description', ''),
                    severity=rule.get('Severity', 2),
                    scope=rule.get('Scope', '')
                ))
        except Exception as e:
            print(f"Warning: Could not load BPA rules from {rules_path}: {e}")
    
    def _register_rule_implementations(self) -> None:
        """Register Python implementations for key BPA rules."""
        # Map rule IDs to check functions
        self.rule_implementations = {
            'AVOID_FLOATING_POINT_DATA_TYPES': self._check_floating_point_types,
            'REMOVE_AUTO-DATE_TABLE': self._check_auto_date_tables,
            'MODEL_SHOULD_HAVE_A_DATE_TABLE': self._check_date_table_exists,
            'DATE/CALENDAR_TABLES_SHOULD_BE_MARKED_AS_A_DATE_TABLE': self._check_date_table_marked,
            'AVOID_EXCESSIVE_BI-DIRECTIONAL_OR_MANY-TO-MANY_RELATIONSHIPS': self._check_excessive_bidi,
            'REDUCE_NUMBER_OF_CALCULATED_COLUMNS': self._check_calculated_column_count,
            'SNOWFLAKE_SCHEMA_ARCHITECTURE': self._check_snowflake_schema,
            'HIDE_FOREIGN_KEYS': self._check_foreign_keys_hidden,
            'AVOID_DUPLICATE_MEASURES': self._check_duplicate_measures,
            'PROVIDE_FORMAT_STRING_FOR_MEASURES': self._check_measure_format_strings,
            'ADD_DATA_CATEGORY_FOR_COLUMNS': self._check_column_data_categories,
            'OBJECTS_SHOULD_NOT_START_OR_END_WITH_A_SPACE': self._check_object_names_spaces,
            'PERCENTAGE_FORMATTING': self._check_percentage_formatting,
            'DO_NOT_SUMMARIZE_NUMERIC_COLUMNS': self._check_summarize_by_none,
            'RELATIONSHIP_COLUMNS_SHOULD_BE_OF_INTEGER_DATA_TYPE': self._check_relationship_column_types,
            'PROVIDE_DESCRIPTION_FOR_MEASURES': self._check_measure_descriptions,
            'PROVIDE_DESCRIPTION_FOR_COLUMNS': self._check_column_descriptions,
            'PROVIDE_DESCRIPTION_FOR_TABLES': self._check_table_descriptions,
            'MARK_PRIMARY_KEYS': self._check_primary_keys_marked,
            'HIDE_FACT_TABLE_COLUMNS': self._check_fact_table_columns_hidden,
            'FIRST_LETTER_OF_OBJECTS_SHOULD_BE_CAPITALIZED': self._check_object_capitalization,
        }
    
    def analyze_model(self, model_data: Dict[str, Any]) -> List[BPAViolation]:
        """
        Analyze a semantic model against all BPA rules.
        
        Args:
            model_data: Parsed model data with tables, relationships, etc.
        
        Returns:
            List of BPA violations found.
        """
        self.violations = []
        
        # Run all implemented rules
        for rule_id, check_func in self.rule_implementations.items():
            try:
                violations = check_func(model_data)
                self.violations.extend(violations)
            except Exception as e:
                print(f"Warning: Error running rule {rule_id}: {e}")
        
        return self.violations
    
    # ==================== Rule Implementations ====================
    
    def _check_floating_point_types(self, model_data: Dict) -> List[BPAViolation]:
        """Check for Double/Float data types that should be Decimal."""
        violations = []
        
        for table in model_data.get('tables', []):
            for column in table.get('columns', []):
                data_type = (column.get('data_type') or '').lower()
                if data_type in ('double', 'float', 'real'):
                    violations.append(BPAViolation(
                        rule_id='AVOID_FLOATING_POINT_DATA_TYPES',
                        rule_name='[Performance] Do not use floating point data types',
                        category='Performance',
                        severity=2,
                        description='Double/Float data types can cause roundoff errors and decreased performance.',
                        recommendation='Use Int64 or Decimal data types instead.',
                        object_type='Column',
                        object_name=column.get('name', ''),
                        table_name=table.get('name', '')
                    ))
        
        return violations
    
    def _check_auto_date_tables(self, model_data: Dict) -> List[BPAViolation]:
        """Check for auto-generated date tables."""
        violations = []
        
        for table in model_data.get('tables', []):
            table_name = table.get('name', '')
            if table_name.startswith('DateTableTemplate_') or table_name.startswith('LocalDateTable_'):
                violations.append(BPAViolation(
                    rule_id='REMOVE_AUTO-DATE_TABLE',
                    rule_name='[Performance] Remove auto-date table',
                    category='Performance',
                    severity=2,
                    description='Auto-date tables waste memory. Turn off in Power BI Desktop settings.',
                    recommendation='Disable auto date/time in Options > Data Load, then delete these tables.',
                    object_type='Table',
                    object_name=table_name
                ))
        
        return violations
    
    def _check_date_table_exists(self, model_data: Dict) -> List[BPAViolation]:
        """Check if model has a proper date table."""
        violations = []
        
        # Look for a table marked as date table or with Date DataCategory
        has_date_table = False
        for table in model_data.get('tables', []):
            if table.get('data_category') == 'Time':
                has_date_table = True
                break
            # Also check for common date table names
            name_lower = table.get('name', '').lower()
            if name_lower in ('date', 'calendar', 'dim_date', 'dimdate', 'dates'):
                has_date_table = True
                break
        
        if not has_date_table and len(model_data.get('tables', [])) > 2:
            violations.append(BPAViolation(
                rule_id='MODEL_SHOULD_HAVE_A_DATE_TABLE',
                rule_name='[Performance] Model should have a date table',
                category='Performance',
                severity=2,
                description='Models should have a dedicated date table for time intelligence.',
                recommendation='Create a date table and mark it as a Date Table in Power BI.',
                object_type='Model',
                object_name='Model'
            ))
        
        return violations
    
    def _check_date_table_marked(self, model_data: Dict) -> List[BPAViolation]:
        """Check if date/calendar tables are marked as date tables."""
        violations = []
        
        for table in model_data.get('tables', []):
            table_name = table.get('name', '')
            name_lower = table_name.lower()
            
            if ('date' in name_lower or 'calendar' in name_lower) and \
               not name_lower.startswith('datetabletemplate_') and \
               not name_lower.startswith('localdatetable_'):
                
                if table.get('data_category') != 'Time':
                    violations.append(BPAViolation(
                        rule_id='DATE/CALENDAR_TABLES_SHOULD_BE_MARKED_AS_A_DATE_TABLE',
                        rule_name='[Performance] Date/calendar tables should be marked as a date table',
                        category='Performance',
                        severity=2,
                        description=f"Table '{table_name}' appears to be a date table but isn't marked as one.",
                        recommendation='Mark this table as a Date Table in Power BI Desktop.',
                        object_type='Table',
                        object_name=table_name
                    ))
        
        return violations
    
    def _check_excessive_bidi(self, model_data: Dict) -> List[BPAViolation]:
        """Check if more than 30% of relationships are bi-directional or many-to-many."""
        violations = []
        relationships = model_data.get('relationships', [])
        
        if not relationships:
            return violations
        
        bidi_count = sum(1 for r in relationships if r.get('cross_filter_direction') == 'both')
        m2m_count = sum(1 for r in relationships if r.get('cardinality') == 'many-to-many')
        
        total = len(relationships)
        problematic = bidi_count + m2m_count
        
        if total > 0 and (problematic / total) > 0.3:
            violations.append(BPAViolation(
                rule_id='AVOID_EXCESSIVE_BI-DIRECTIONAL_OR_MANY-TO-MANY_RELATIONSHIPS',
                rule_name='[Performance] Avoid excessive bi-directional or many-to-many relationships',
                category='Performance',
                severity=2,
                description=f'{problematic} of {total} relationships ({int(problematic/total*100)}%) are bi-directional or many-to-many.',
                recommendation='Limit bi-directional and many-to-many relationships to under 30% of total.',
                object_type='Model',
                object_name='Model',
                details={'bidi_count': bidi_count, 'm2m_count': m2m_count, 'total': total}
            ))
        
        return violations
    
    def _check_calculated_column_count(self, model_data: Dict) -> List[BPAViolation]:
        """Check if model has too many calculated columns."""
        violations = []
        
        calc_col_count = 0
        for table in model_data.get('tables', []):
            calc_col_count += len(table.get('calculated_columns', []))
        
        if calc_col_count > 5:
            violations.append(BPAViolation(
                rule_id='REDUCE_NUMBER_OF_CALCULATED_COLUMNS',
                rule_name='[Performance] Reduce number of calculated columns',
                category='Performance',
                severity=2,
                description=f'Model has {calc_col_count} calculated columns.',
                recommendation='Calculated columns slow processing and increase model size. Move logic to data warehouse.',
                object_type='Model',
                object_name='Model',
                details={'count': calc_col_count}
            ))
        
        return violations
    
    def _check_snowflake_schema(self, model_data: Dict) -> List[BPAViolation]:
        """Check for snowflake schema (dimension-to-dimension relationships)."""
        violations = []
        
        # Build set of tables that are "in the middle" of relationships
        tables_as_from = set()
        tables_as_to = set()
        
        for rel in model_data.get('relationships', []):
            tables_as_from.add(rel.get('from_table', ''))
            tables_as_to.add(rel.get('to_table', ''))
        
        # Tables that are both source and target are snowflake candidates
        snowflake_tables = tables_as_from & tables_as_to
        
        for table_name in snowflake_tables:
            # Skip fact tables (they legitimately have relationships on both sides)
            if 'fact' in table_name.lower():
                continue
            
            violations.append(BPAViolation(
                rule_id='SNOWFLAKE_SCHEMA_ARCHITECTURE',
                rule_name='[Performance] Consider a star-schema instead of a snowflake architecture',
                category='Performance',
                severity=2,
                description=f"Table '{table_name}' has relationships on both sides, suggesting snowflake schema.",
                recommendation='Consider denormalizing to a star schema for better performance.',
                object_type='Table',
                object_name=table_name
            ))
        
        return violations
    
    def _check_foreign_keys_hidden(self, model_data: Dict) -> List[BPAViolation]:
        """Check if foreign key columns are hidden."""
        violations = []
        
        # Get all relationship columns
        fk_columns = set()
        for rel in model_data.get('relationships', []):
            fk_columns.add((rel.get('from_table', ''), rel.get('from_column', '')))
        
        # Check if they're hidden
        for table in model_data.get('tables', []):
            table_name = table.get('name', '')
            for column in table.get('columns', []):
                col_name = column.get('name', '')
                if (table_name, col_name) in fk_columns:
                    if not column.get('is_hidden', False):
                        violations.append(BPAViolation(
                            rule_id='HIDE_FOREIGN_KEYS',
                            rule_name='[Maintenance] Hide foreign keys',
                            category='Maintenance',
                            severity=2,
                            description=f"Foreign key column '{col_name}' in table '{table_name}' is not hidden.",
                            recommendation='Hide foreign key columns to reduce clutter for end users.',
                            object_type='Column',
                            object_name=col_name,
                            table_name=table_name
                        ))
        
        return violations
    
    def _check_duplicate_measures(self, model_data: Dict) -> List[BPAViolation]:
        """Check for measures with identical expressions."""
        violations = []
        
        # Collect all measures
        measures = []
        for table in model_data.get('tables', []):
            for measure in table.get('measures', []):
                expr = measure.get('expression', '')
                # Normalize expression for comparison
                normalized = re.sub(r'\s+', '', expr)
                measures.append({
                    'name': measure.get('name', ''),
                    'table': table.get('name', ''),
                    'expression': expr,
                    'normalized': normalized
                })
        
        # Find duplicates
        seen = {}
        for m in measures:
            if m['normalized'] in seen:
                violations.append(BPAViolation(
                    rule_id='AVOID_DUPLICATE_MEASURES',
                    rule_name='[DAX Expressions] No two measures should have the same definition',
                    category='DAX Expressions',
                    severity=2,
                    description=f"Measure '{m['name']}' has the same definition as '{seen[m['normalized']]}'.",
                    recommendation='Remove duplicate measures and use the original.',
                    object_type='Measure',
                    object_name=m['name'],
                    table_name=m['table']
                ))
            else:
                seen[m['normalized']] = m['name']
        
        return violations
    
    def _check_measure_format_strings(self, model_data: Dict) -> List[BPAViolation]:
        """Check if measures have format strings."""
        violations = []
        
        for table in model_data.get('tables', []):
            for measure in table.get('measures', []):
                if not measure.get('format_string'):
                    violations.append(BPAViolation(
                        rule_id='PROVIDE_FORMAT_STRING_FOR_MEASURES',
                        rule_name='[Formatting] Provide format string for measures',
                        category='Formatting',
                        severity=1,
                        description=f"Measure '{measure.get('name', '')}' has no format string.",
                        recommendation='Add a format string for consistent display.',
                        object_type='Measure',
                        object_name=measure.get('name', ''),
                        table_name=table.get('name', '')
                    ))
        
        return violations
    
    def _check_column_data_categories(self, model_data: Dict) -> List[BPAViolation]:
        """Check if columns that could benefit from data categories have them."""
        violations = []
        
        category_hints = {
            'url': 'WebUrl',
            'link': 'WebUrl',
            'website': 'WebUrl',
            'image': 'ImageUrl',
            'photo': 'ImageUrl',
            'city': 'City',
            'country': 'Country',
            'state': 'StateOrProvince',
            'postal': 'PostalCode',
            'zip': 'PostalCode',
            'latitude': 'Latitude',
            'longitude': 'Longitude',
            'address': 'Address',
        }
        
        for table in model_data.get('tables', []):
            for column in table.get('columns', []):
                col_name = column.get('name', '').lower()
                for hint, category in category_hints.items():
                    if hint in col_name and not column.get('data_category'):
                        violations.append(BPAViolation(
                            rule_id='ADD_DATA_CATEGORY_FOR_COLUMNS',
                            rule_name='[Formatting] Add data category for columns',
                            category='Formatting',
                            severity=1,
                            description=f"Column '{column.get('name', '')}' might benefit from data category '{category}'.",
                            recommendation=f'Set the Data Category to "{category}" for enhanced functionality.',
                            object_type='Column',
                            object_name=column.get('name', ''),
                            table_name=table.get('name', '')
                        ))
                        break
        
        return violations
    
    def _check_object_names_spaces(self, model_data: Dict) -> List[BPAViolation]:
        """Check for objects starting or ending with spaces."""
        violations = []
        
        # Check tables
        for table in model_data.get('tables', []):
            name = table.get('name', '')
            if name != name.strip():
                violations.append(BPAViolation(
                    rule_id='OBJECTS_SHOULD_NOT_START_OR_END_WITH_A_SPACE',
                    rule_name='[Maintenance] Objects should not start or end with a space',
                    category='Maintenance',
                    severity=2,
                    description=f"Table '{name}' has leading or trailing spaces.",
                    recommendation='Remove spaces from the beginning and end of object names.',
                    object_type='Table',
                    object_name=name
                ))
            
            # Check columns
            for column in table.get('columns', []):
                col_name = column.get('name', '')
                if col_name != col_name.strip():
                    violations.append(BPAViolation(
                        rule_id='OBJECTS_SHOULD_NOT_START_OR_END_WITH_A_SPACE',
                        rule_name='[Maintenance] Objects should not start or end with a space',
                        category='Maintenance',
                        severity=2,
                        description=f"Column '{col_name}' has leading or trailing spaces.",
                        recommendation='Remove spaces from the beginning and end of object names.',
                        object_type='Column',
                        object_name=col_name,
                        table_name=name
                    ))
            
            # Check measures
            for measure in table.get('measures', []):
                m_name = measure.get('name', '')
                if m_name != m_name.strip():
                    violations.append(BPAViolation(
                        rule_id='OBJECTS_SHOULD_NOT_START_OR_END_WITH_A_SPACE',
                        rule_name='[Maintenance] Objects should not start or end with a space',
                        category='Maintenance',
                        severity=2,
                        description=f"Measure '{m_name}' has leading or trailing spaces.",
                        recommendation='Remove spaces from the beginning and end of object names.',
                        object_type='Measure',
                        object_name=m_name,
                        table_name=name
                    ))
        
        return violations
    
    def _check_percentage_formatting(self, model_data: Dict) -> List[BPAViolation]:
        """Check if percentage measures have percentage formatting."""
        violations = []
        
        percentage_indicators = ['percent', 'pct', '%', 'rate', 'ratio', 'margin']
        
        for table in model_data.get('tables', []):
            for measure in table.get('measures', []):
                name = measure.get('name', '').lower()
                format_string = measure.get('format_string', '') or ''
                
                is_percentage = any(ind in name for ind in percentage_indicators)
                has_pct_format = '%' in format_string or 'percent' in format_string.lower()
                
                if is_percentage and not has_pct_format:
                    violations.append(BPAViolation(
                        rule_id='PERCENTAGE_FORMATTING',
                        rule_name='[Formatting] Percentage formatting',
                        category='Formatting',
                        severity=1,
                        description=f"Measure '{measure.get('name', '')}' appears to be a percentage but isn't formatted as one.",
                        recommendation='Set format string to "0.00%" or similar.',
                        object_type='Measure',
                        object_name=measure.get('name', ''),
                        table_name=table.get('name', '')
                    ))
        
        return violations
    
    def _check_summarize_by_none(self, model_data: Dict) -> List[BPAViolation]:
        """Check if numeric columns have SummarizeBy set to None where appropriate."""
        violations = []
        
        # Columns that shouldn't be summarized
        no_summarize_indicators = ['id', 'key', 'code', 'number', 'year', 'month', 'day', 'quarter']
        
        for table in model_data.get('tables', []):
            for column in table.get('columns', []):
                name = column.get('name', '').lower()
                data_type = (column.get('data_type') or '').lower()
                
                is_numeric = data_type in ('int64', 'integer', 'decimal', 'double', 'currency')
                is_id_like = any(ind in name for ind in no_summarize_indicators)
                
                if is_numeric and is_id_like and column.get('summarize_by') not in (None, 'None', 'none'):
                    violations.append(BPAViolation(
                        rule_id='DO_NOT_SUMMARIZE_NUMERIC_COLUMNS',
                        rule_name='[Formatting] Do not summarize numeric columns',
                        category='Formatting',
                        severity=1,
                        description=f"Column '{column.get('name', '')}' is numeric but appears to be an ID/key.",
                        recommendation='Set Summarize By to "None" to prevent accidental aggregation.',
                        object_type='Column',
                        object_name=column.get('name', ''),
                        table_name=table.get('name', '')
                    ))
        
        return violations
    
    def _check_relationship_column_types(self, model_data: Dict) -> List[BPAViolation]:
        """Check if relationship columns are integer type for performance."""
        violations = []
        
        # Get all relationship columns
        rel_columns = []
        for rel in model_data.get('relationships', []):
            rel_columns.append((rel.get('from_table'), rel.get('from_column')))
            rel_columns.append((rel.get('to_table'), rel.get('to_column')))
        
        for table in model_data.get('tables', []):
            table_name = table.get('name', '')
            for column in table.get('columns', []):
                col_name = column.get('name', '')
                if (table_name, col_name) in rel_columns:
                    data_type = (column.get('data_type') or '').lower()
                    if data_type not in ('int64', 'integer', 'int', 'whole number'):
                        violations.append(BPAViolation(
                            rule_id='RELATIONSHIP_COLUMNS_SHOULD_BE_OF_INTEGER_DATA_TYPE',
                            rule_name='[Performance] Relationship columns should be of integer data type',
                            category='Performance',
                            severity=2,
                            description=f"Relationship column '{col_name}' is {data_type}, not integer.",
                            recommendation='Integer columns are most efficient for relationships.',
                            object_type='Column',
                            object_name=col_name,
                            table_name=table_name
                        ))
        
        return violations
    
    def _check_measure_descriptions(self, model_data: Dict) -> List[BPAViolation]:
        """Check if measures have descriptions."""
        violations = []
        
        for table in model_data.get('tables', []):
            for measure in table.get('measures', []):
                if not measure.get('description'):
                    violations.append(BPAViolation(
                        rule_id='PROVIDE_DESCRIPTION_FOR_MEASURES',
                        rule_name='[Maintenance] Provide description for measures',
                        category='Maintenance',
                        severity=1,
                        description=f"Measure '{measure.get('name', '')}' has no description.",
                        recommendation='Add a description explaining the business logic.',
                        object_type='Measure',
                        object_name=measure.get('name', ''),
                        table_name=table.get('name', '')
                    ))
        
        return violations
    
    def _check_column_descriptions(self, model_data: Dict) -> List[BPAViolation]:
        """Check if visible columns have descriptions."""
        violations = []
        
        for table in model_data.get('tables', []):
            for column in table.get('columns', []):
                if not column.get('is_hidden') and not column.get('description'):
                    violations.append(BPAViolation(
                        rule_id='PROVIDE_DESCRIPTION_FOR_COLUMNS',
                        rule_name='[Maintenance] Provide description for columns',
                        category='Maintenance',
                        severity=1,
                        description=f"Column '{column.get('name', '')}' has no description.",
                        recommendation='Add a description for end-user clarity.',
                        object_type='Column',
                        object_name=column.get('name', ''),
                        table_name=table.get('name', '')
                    ))
        
        return violations
    
    def _check_table_descriptions(self, model_data: Dict) -> List[BPAViolation]:
        """Check if tables have descriptions."""
        violations = []
        
        for table in model_data.get('tables', []):
            if not table.get('is_hidden') and not table.get('description'):
                violations.append(BPAViolation(
                    rule_id='PROVIDE_DESCRIPTION_FOR_TABLES',
                    rule_name='[Maintenance] Provide description for tables',
                    category='Maintenance',
                    severity=1,
                    description=f"Table '{table.get('name', '')}' has no description.",
                    recommendation='Add a description explaining the table purpose.',
                    object_type='Table',
                    object_name=table.get('name', '')
                ))
        
        return violations
    
    def _check_primary_keys_marked(self, model_data: Dict) -> List[BPAViolation]:
        """Check if dimension tables have primary keys marked."""
        violations = []
        
        # Get tables that are on the "to" side of relationships (dimension tables)
        dimension_tables = set()
        for rel in model_data.get('relationships', []):
            dimension_tables.add(rel.get('to_table'))
        
        for table in model_data.get('tables', []):
            table_name = table.get('name', '')
            if table_name in dimension_tables:
                has_key = any(col.get('is_key') for col in table.get('columns', []))
                if not has_key:
                    violations.append(BPAViolation(
                        rule_id='MARK_PRIMARY_KEYS',
                        rule_name='[Maintenance] Mark primary keys',
                        category='Maintenance',
                        severity=1,
                        description=f"Dimension table '{table_name}' has no marked primary key.",
                        recommendation='Mark the unique key column as a primary key.',
                        object_type='Table',
                        object_name=table_name
                    ))
        
        return violations
    
    def _check_fact_table_columns_hidden(self, model_data: Dict) -> List[BPAViolation]:
        """Check if non-measure columns in fact tables are hidden."""
        violations = []
        
        # Identify fact tables (tables on the "from" side of relationships)
        fact_tables = set()
        for rel in model_data.get('relationships', []):
            from_table = rel.get('from_table', '')
            # Skip if this table is also a dimension (to) table
            fact_tables.add(from_table)
        
        for table in model_data.get('tables', []):
            table_name = table.get('name', '')
            name_lower = table_name.lower()
            
            # Check if it's a fact table
            is_fact = table_name in fact_tables or 'fact' in name_lower
            
            if is_fact:
                visible_columns = [
                    col for col in table.get('columns', [])
                    if not col.get('is_hidden', False)
                ]
                
                if len(visible_columns) > 5:  # Allow a few visible columns
                    violations.append(BPAViolation(
                        rule_id='HIDE_FACT_TABLE_COLUMNS',
                        rule_name='[Maintenance] Hide fact table columns',
                        category='Maintenance',
                        severity=1,
                        description=f"Fact table '{table_name}' has {len(visible_columns)} visible columns.",
                        recommendation='Hide columns in fact tables to reduce clutter. Users should use measures.',
                        object_type='Table',
                        object_name=table_name,
                        details={'visible_columns': len(visible_columns)}
                    ))
        
        return violations
    
    def _check_object_capitalization(self, model_data: Dict) -> List[BPAViolation]:
        """Check if object names start with a capital letter."""
        violations = []
        
        for table in model_data.get('tables', []):
            name = table.get('name', '')
            if name and name[0].islower():
                violations.append(BPAViolation(
                    rule_id='FIRST_LETTER_OF_OBJECTS_SHOULD_BE_CAPITALIZED',
                    rule_name='[Formatting] First letter of objects should be capitalized',
                    category='Formatting',
                    severity=1,
                    description=f"Table '{name}' does not start with a capital letter.",
                    recommendation='Capitalize the first letter of object names for consistency.',
                    object_type='Table',
                    object_name=name
                ))
            
            for measure in table.get('measures', []):
                m_name = measure.get('name', '')
                if m_name and m_name[0].islower():
                    violations.append(BPAViolation(
                        rule_id='FIRST_LETTER_OF_OBJECTS_SHOULD_BE_CAPITALIZED',
                        rule_name='[Formatting] First letter of objects should be capitalized',
                        category='Formatting',
                        severity=1,
                        description=f"Measure '{m_name}' does not start with a capital letter.",
                        recommendation='Capitalize the first letter of object names.',
                        object_type='Measure',
                        object_name=m_name,
                        table_name=name
                    ))
        
        return violations
    
    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of all violations."""
        summary = {
            'total_violations': len(self.violations),
            'by_severity': {1: 0, 2: 0, 3: 0},
            'by_category': {},
            'critical_issues': [],
            'warnings': [],
            'info': []
        }
        
        for v in self.violations:
            summary['by_severity'][v.severity] = summary['by_severity'].get(v.severity, 0) + 1
            summary['by_category'][v.category] = summary['by_category'].get(v.category, 0) + 1
            
            if v.severity == 3:
                summary['critical_issues'].append(v)
            elif v.severity == 2:
                summary['warnings'].append(v)
            else:
                summary['info'].append(v)
        
        return summary
