"""
DAX Query Client - Execute DAX queries against live Power BI semantic models.

Uses the TmdlTools.exe with ADOMD.NET for XMLA connectivity.
Supports INFO.VIEW.* functions for metadata extraction.
"""

import json
import subprocess
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass


@dataclass
class DaxQueryResult:
    """Result of a DAX query execution."""
    success: bool
    columns: List[str]
    rows: List[Dict[str, Any]]
    row_count: int
    error: Optional[str] = None


class DaxQueryClient:
    """
    Client for executing DAX queries against Power BI semantic models.
    
    Uses INFO.VIEW.* functions to extract model metadata for analysis.
    """
    
    def __init__(self, exe_path: Optional[str] = None):
        """
        Initialize the DAX query client.
        
        Args:
            exe_path: Path to TmdlTools.exe. Defaults to relative path from this file.
        """
        if exe_path:
            self.exe_path = Path(exe_path)
        else:
            # Default to published path relative to this file
            self.exe_path = Path(__file__).parent / "tom_interop" / "bin" / "Release" / "net8.0" / "publish" / "TmdlTools.exe"
        
        if not self.exe_path.exists():
            # Try non-published path
            self.exe_path = Path(__file__).parent / "tom_interop" / "bin" / "Release" / "net8.0" / "TmdlTools.exe"
    
    def execute_query(
        self, 
        workspace: str, 
        dataset: str, 
        dax_query: str,
        token: Optional[str] = None
    ) -> DaxQueryResult:
        """
        Execute a DAX query against a semantic model.
        
        Args:
            workspace: Power BI workspace name
            dataset: Dataset (semantic model) name
            dax_query: DAX query to execute
            token: Optional access token (uses interactive auth if not provided)
        
        Returns:
            DaxQueryResult with columns, rows, and metadata
        """
        cmd = [
            str(self.exe_path),
            "query",
            "--workspace", workspace,
            "--dataset", dataset,
            "--dax", dax_query
        ]
        
        if token:
            cmd.extend(["--token", token])
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            if result.returncode != 0:
                return DaxQueryResult(
                    success=False,
                    columns=[],
                    rows=[],
                    row_count=0,
                    error=result.stderr or result.stdout
                )
            
            data = json.loads(result.stdout)
            return DaxQueryResult(
                success=True,
                columns=data.get("columns", []),
                rows=data.get("rows", []),
                row_count=data.get("rowCount", 0)
            )
            
        except subprocess.TimeoutExpired:
            return DaxQueryResult(
                success=False,
                columns=[],
                rows=[],
                row_count=0,
                error="Query timed out after 5 minutes"
            )
        except json.JSONDecodeError as e:
            return DaxQueryResult(
                success=False,
                columns=[],
                rows=[],
                row_count=0,
                error=f"Failed to parse result: {e}"
            )
        except Exception as e:
            return DaxQueryResult(
                success=False,
                columns=[],
                rows=[],
                row_count=0,
                error=str(e)
            )
    
    def get_dax_documentation(
        self, 
        workspace: str, 
        dataset: str,
        token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get all DAX documentation: measures, calculated columns, calculated tables.
        
        Uses Ryan's INFO.VIEW pattern:
        - INFO.VIEW.MEASURES() for measures
        - INFO.VIEW.COLUMNS() filtered for calculated columns
        - INFO.VIEW.TABLES() filtered for calculated tables
        
        Args:
            workspace: Power BI workspace name
            dataset: Dataset name
            token: Optional access token
        
        Returns:
            Dictionary with all DAX expressions and their metadata
        """
        cmd = [
            str(self.exe_path),
            "get-dax-docs",
            "--workspace", workspace,
            "--dataset", dataset
        ]
        
        if token:
            cmd.extend(["--token", token])
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300
            )
            
            if result.returncode != 0:
                return {"error": result.stderr or result.stdout}
            
            return json.loads(result.stdout)
            
        except Exception as e:
            return {"error": str(e)}
    
    def get_full_model_metadata(
        self, 
        workspace: str, 
        dataset: str,
        token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get comprehensive model metadata using INFO.VIEW functions.
        
        Retrieves:
        - Tables (INFO.VIEW.TABLES)
        - Columns (INFO.VIEW.COLUMNS)
        - Measures (INFO.VIEW.MEASURES)
        - Relationships (INFO.VIEW.RELATIONSHIPS)
        - Hierarchies (INFO.VIEW.HIERARCHIES)
        - Calculation Dependencies (INFO.CALCDEPENDENCY)
        
        Args:
            workspace: Power BI workspace name
            dataset: Dataset name
            token: Optional access token
        
        Returns:
            Dictionary with all model metadata
        """
        cmd = [
            str(self.exe_path),
            "get-model-metadata",
            "--workspace", workspace,
            "--dataset", dataset
        ]
        
        if token:
            cmd.extend(["--token", token])
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300
            )
            
            if result.returncode != 0:
                return {"error": result.stderr or result.stdout}
            
            return json.loads(result.stdout)
            
        except Exception as e:
            return {"error": str(e)}
    
    def analyze_live_model(
        self, 
        workspace: str, 
        dataset: str,
        token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Analyze a live model using INFO.VIEW functions.
        
        This is more reliable than parsing TMDL files because it queries
        the actual model state.
        
        Args:
            workspace: Power BI workspace name
            dataset: Dataset name
            token: Optional access token
        
        Returns:
            Analysis-ready model data structure
        """
        # Get full metadata
        metadata = self.get_full_model_metadata(workspace, dataset, token)
        
        if "error" in metadata:
            return metadata
        
        # Transform into analysis-friendly format
        model_data = {
            "tables": [],
            "relationships": [],
            "source": "live_xmla"
        }
        
        # Process tables
        tables_data = metadata.get("tables", {})
        if isinstance(tables_data, dict) and "rows" in tables_data:
            for row in tables_data["rows"]:
                model_data["tables"].append({
                    "name": row.get("Name"),
                    "description": row.get("Description"),
                    "is_hidden": row.get("IsHidden", False),
                    "storage_mode": row.get("StorageMode"),
                    "expression": row.get("Expression"),
                    "data_category": row.get("DataCategory"),
                    "columns": [],
                    "measures": []
                })
        
        # Process columns and add to tables
        columns_data = metadata.get("columns", {})
        if isinstance(columns_data, dict) and "rows" in columns_data:
            for row in columns_data["rows"]:
                table_name = row.get("Table")
                column = {
                    "name": row.get("Name"),
                    "description": row.get("Description"),
                    "data_type": row.get("DataType"),
                    "is_hidden": row.get("IsHidden", False),
                    "expression": row.get("Expression"),
                    "data_category": row.get("DataCategory"),
                    "is_key": row.get("IsKey", False),
                    "summarize_by": row.get("SummarizeBy"),
                    "is_calculated": bool(row.get("Expression"))
                }
                
                # Find the table and add the column
                for table in model_data["tables"]:
                    if table["name"] == table_name:
                        table["columns"].append(column)
                        if column["is_calculated"]:
                            if "calculated_columns" not in table:
                                table["calculated_columns"] = []
                            table["calculated_columns"].append(column)
                        break
        
        # Process measures and add to tables
        measures_data = metadata.get("measures", {})
        if isinstance(measures_data, dict) and "rows" in measures_data:
            for row in measures_data["rows"]:
                table_name = row.get("Table")
                measure = {
                    "name": row.get("Name"),
                    "description": row.get("Description"),
                    "expression": row.get("Expression"),
                    "format_string": row.get("FormatString"),
                    "is_hidden": row.get("IsHidden", False),
                    "display_folder": row.get("DisplayFolder")
                }
                
                # Find the table and add the measure
                for table in model_data["tables"]:
                    if table["name"] == table_name:
                        table["measures"].append(measure)
                        break
        
        # Process relationships
        rels_data = metadata.get("relationships", {})
        if isinstance(rels_data, dict) and "rows" in rels_data:
            for row in rels_data["rows"]:
                model_data["relationships"].append({
                    "from_table": row.get("FromTable"),
                    "from_column": row.get("FromColumn"),
                    "to_table": row.get("ToTable"),
                    "to_column": row.get("ToColumn"),
                    "is_active": row.get("IsActive", True),
                    "cross_filter_direction": row.get("CrossFilterDirection", "single").lower(),
                    "cardinality": row.get("Cardinality", "").lower().replace(" ", "-")
                })
        
        return model_data


# Convenience function for quick analysis
def analyze_model(workspace: str, dataset: str, token: Optional[str] = None) -> Dict[str, Any]:
    """
    Quick function to analyze a live Power BI model.
    
    Args:
        workspace: Power BI workspace name
        dataset: Dataset name
        token: Optional access token
    
    Returns:
        Model data ready for analysis
    """
    client = DaxQueryClient()
    return client.analyze_live_model(workspace, dataset, token)


# Common INFO.VIEW queries for reference
INFO_VIEW_QUERIES = {
    "all_dax": """
        VAR __measures =
            SELECTCOLUMNS (
                INFO.VIEW.MEASURES (),
                "Item", [Name],
                "Table", [Table],
                "DAX", [Expression],
                "Description", [Description],
                "Type", "Measure"
            )
        VAR __columns =
            SELECTCOLUMNS (
                FILTER ( INFO.VIEW.COLUMNS (), [Type] = "Calculated" ),
                "Item", [Name],
                "Table", [Table],
                "DAX", [Expression],
                "Description", [Description],
                "Type", "Column"
            )
        VAR __tables =
            SELECTCOLUMNS (
                FILTER ( INFO.VIEW.TABLES (), NOT ISBLANK ( [Expression] ) ),
                "Item", [Name],
                "Table", [Name],
                "DAX", [Expression],
                "Description", [Description],
                "Type", "Table"
            )
        RETURN
            UNION ( __measures, __columns, __tables )
    """,
    
    "tables": "EVALUATE INFO.VIEW.TABLES()",
    "columns": "EVALUATE INFO.VIEW.COLUMNS()",
    "measures": "EVALUATE INFO.VIEW.MEASURES()",
    "relationships": "EVALUATE INFO.VIEW.RELATIONSHIPS()",
    "hierarchies": "EVALUATE INFO.VIEW.HIERARCHIES()",
    "dependencies": "EVALUATE INFO.CALCDEPENDENCY()",
    "storage": "EVALUATE INFO.STORAGETABLES()",
}
