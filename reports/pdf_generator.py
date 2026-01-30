#!/usr/bin/env python3
"""
PDF Report Generator for Semantic Models
========================================

Automatically processes all TMDL files in the Outputs folder and generates
comprehensive PDF reports with both analysis review and documentation.

Usage:
    python pdf_report_generator.py
    python pdf_report_generator.py --workspace-id <id>
    python pdf_report_generator.py --model-name "PalmTreeReporting"
"""

import os
import sys
import json
import argparse
import re
import asyncio
from datetime import datetime
import asyncio
from typing import Dict, List, Tuple, Any, Optional
from pathlib import Path

# Ensure V7 project root is on sys.path when running this file directly
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

# V7 relative imports
from analyzers.semantic_analyzer import EnhancedSemanticModelAnalyzer
from analyzers.mcode_analyzer import EnhancedMCodeAnalyzer
from config.settings import REPORTS_DIR, CACHE_DIR
from core.orchestrator import ModelHealthOrchestrator
from core.powerbi_client import PowerBIClient
from tools.pbix_extractor import read_layout_from_pbix, extract_bindings_from_layout
from core.orchestrator import ModelHealthOrchestrator

# Optional legacy utilities (guarded). These are not required for API PDF generation.
try:  # fabric-reader compatibility (may not exist in V7)
    import importlib.util
    spec = importlib.util.spec_from_file_location('fabric_reader', 'fabric-reader.py')
    fabric_reader = importlib.util.module_from_spec(spec) if spec and spec.loader else None
    if spec and spec.loader and fabric_reader:
        spec.loader.exec_module(fabric_reader)
        SemanticModelAnalyzer = getattr(fabric_reader, 'SemanticModelAnalyzer', None)
        _parse_tmdl_sections = getattr(fabric_reader, '_parse_tmdl_sections', None)
        _generate_documentation_content = getattr(fabric_reader, '_generate_documentation_content', None)
    else:
        SemanticModelAnalyzer = None
        _parse_tmdl_sections = None
        _generate_documentation_content = None
except Exception:
    SemanticModelAnalyzer = None
    _parse_tmdl_sections = None
    _generate_documentation_content = None

# Optional helpers (not required for API PDF path)
try:
    from dax_formatter import DAXFormatter  # type: ignore
except Exception:
    DAXFormatter = None  # type: ignore

try:
    from tmdl_measure_parser import TMDLMeasureParser  # type: ignore
except Exception:
    TMDLMeasureParser = None  # type: ignore

# Try to import reportlab for PDF generation
try:
    from reportlab.lib.pagesizes import letter, A4
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
    REPORTLAB_AVAILABLE = True
except ImportError:
    print("⚠️  reportlab not available. Installing...")
    try:
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "reportlab"])
        from reportlab.lib.pagesizes import letter, A4
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import inch
        from reportlab.lib import colors
        from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
        REPORTLAB_AVAILABLE = True
    except Exception as e:
        print(f"❌ Failed to install reportlab: {e}")
        REPORTLAB_AVAILABLE = False


class PDFReportGenerator:
    """Generates comprehensive PDF reports for semantic models."""
    
    def __init__(self, outputs_dir: str = None):
        if outputs_dir is None:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            outputs_dir = os.path.join(script_dir, "Outputs")
        
        self.outputs_dir = outputs_dir
        self.analyzer = EnhancedSemanticModelAnalyzer()
        self.mcode_analyzer = EnhancedMCodeAnalyzer()
        # Optional utilities (available in some legacy contexts only)
        self.dax_formatter = DAXFormatter() if DAXFormatter else None  # type: ignore
        self.tmdl_parser = TMDLMeasureParser() if TMDLMeasureParser else None  # type: ignore
        
        # Optional fabric_reader (legacy)
        self.fabric_reader = fabric_reader
        
        if not REPORTLAB_AVAILABLE:
            raise ImportError("reportlab is required for PDF generation")

    # New: Minimal PDF generator for API use that accepts AnalysisResult or dict
    def generate(self, analysis_result: Any, output_path: Optional[str] = None) -> str:
        """Generate a concise PDF from an AnalysisResult (API path).

        Args:
            analysis_result: AnalysisResult dataclass or dict
            output_path: Optional explicit path; defaults to REPORTS_DIR/<model>_analysis.pdf

        Returns:
            Full path to the generated PDF file
        """
        # Normalize to dict
        if hasattr(analysis_result, 'to_dict') and callable(getattr(analysis_result, 'to_dict')):
            data: Dict[str, Any] = analysis_result.to_dict()
        elif isinstance(analysis_result, dict):
            data = analysis_result
        else:
            # Fallback: try dataclass asdict-like behavior
            try:
                data = dict(analysis_result)  # type: ignore
            except Exception:
                raise TypeError("Unsupported analysis_result type for PDF generation")

        model_name = data.get('model_name', 'Model') or 'Model'
        model_id = data.get('model_id', '')
        workspace_name = data.get('workspace_name', '')
        stats = data.get('statistics', {}) or {}
        cat_scores = data.get('category_scores', {}) or {}
        issues = data.get('detailed_issues', []) or []
        top_recs = data.get('top_recommendations', []) or []
        score = data.get('overall_score', data.get('score', 0))
        grade = data.get('grade', '')
        grade_desc = data.get('grade_description', '')

        # Determine output path
        safe_model = "".join(c for c in model_name if c.isalnum() or c in (' ', '-', '_')).strip() or 'model'
        filename = f"{safe_model}_analysis.pdf"
        out_dir = REPORTS_DIR
        try:
            out_dir.mkdir(exist_ok=True)
        except Exception:
            pass
        pdf_path = str(Path(out_dir) / filename) if output_path is None else output_path

        # Build PDF
        styles = getSampleStyleSheet()
        story: List[Any] = []

        # Title
        title_style = ParagraphStyle(
            'Title', parent=styles['Heading1'], fontSize=22, alignment=TA_CENTER, textColor=colors.darkblue
        )
        story.append(Paragraph("Semantic Model Health Report", title_style))
        story.append(Spacer(1, 12))
        story.append(Paragraph(f"Model: <b>{model_name}</b>", styles['Normal']))
        if workspace_name:
            story.append(Paragraph(f"Workspace: {workspace_name}", styles['Normal']))
        if model_id:
            story.append(Paragraph(f"Model ID: {model_id}", styles['Normal']))
        story.append(Paragraph(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", styles['Normal']))
        story.append(Spacer(1, 18))

        # Score summary
        score_color_name = 'green' if score >= 80 else ('orange' if score >= 60 else 'red')
        story.append(Paragraph(f"Overall Score: <font color='{score_color_name}'>{score}/100</font>", styles['Normal']))
        if grade:
            story.append(Paragraph(f"Grade: {grade} - {grade_desc}", styles['Normal']))
        story.append(Spacer(1, 12))

        # Stats table
        if stats:
            stats_data = [["Metric", "Value"]]
            for k, v in stats.items():
                stats_data.append([k.replace('_', ' ').title(), str(v)])
            table = Table(stats_data, colWidths=[2.2*inch, 1.2*inch])
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.black)
            ]))
            story.append(table)
            story.append(Spacer(1, 12))

        # Category scores
        if cat_scores:
            cat_data = [["Category", "Score", "%"]]
            for cat, val in cat_scores.items():
                if isinstance(val, dict):
                    cat_data.append([cat, f"{val.get('score', 0)}/{val.get('max_score', 0)}", f"{val.get('percentage', 0)}%"])
                else:
                    # Unified style: value is a percentage
                    try:
                        perc_val = int(val)
                    except Exception:
                        perc_val = 0
                    cat_data.append([cat, '—', f"{perc_val}%"])
            ctable = Table(cat_data, colWidths=[2.2*inch, 1.0*inch, 0.8*inch])
            ctable.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.black)
            ]))
            story.append(ctable)
            story.append(Spacer(1, 12))

        # Top recommendations
        if top_recs:
            story.append(Paragraph("Top Recommendations", styles['Heading2']))
            for rec in top_recs[:10]:
                story.append(Paragraph(f"• {rec}", styles['Normal']))
            story.append(Spacer(1, 12))

        # Issues summary (limit for brevity)
        if issues:
            story.append(Paragraph("Issues (sample)", styles['Heading2']))
            for issue in issues[:20]:
                title = issue.get('title', '')
                sev = issue.get('severity', '')
                desc = issue.get('description', '')
                loc = issue.get('location', '')
                story.append(Paragraph(f"[{sev}] {title}", styles['Normal']))
                if desc:
                    story.append(Paragraph(desc, styles['Normal']))
                if loc:
                    story.append(Paragraph(f"Location: {loc}", styles['Normal']))
                story.append(Spacer(1, 6))

        try:
            doc = SimpleDocTemplate(pdf_path, pagesize=letter)
            doc.build(story)
            return pdf_path
        except Exception as e:
            # Handle locked file (e.g., open in viewer) by retrying with a unique name
            if isinstance(e, PermissionError) or "Permission denied" in str(e):
                ts = datetime.now().strftime('%Y%m%d_%H%M%S')
                alt_path = str(Path(out_dir) / f"{safe_model}_analysis_{ts}.pdf")
                doc = SimpleDocTemplate(alt_path, pagesize=letter)
                doc.build(story)
                return alt_path
            raise

    def download_workspace_reports(self, workspace_id: str) -> Dict:
        """Discover all reports in a workspace and save them locally.

        For each report:
        - Export PBIX via Power BI REST API (if permitted)
        - Extract layout bindings into a compact JSON for analysis

        Returns a summary dictionary with counts and output directory.
        """
        output_root = CACHE_DIR / "report_definitions" / workspace_id
        try:
            output_root.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass

        client = PowerBIClient()
        try:
            reports = client.get_reports(workspace_id)
        except Exception as e:
            return {"error": f"Failed to list reports: {e}"}

        results: Dict[str, Any] = {
            "workspace_id": workspace_id,
            "total_reports": len(reports),
            "saved": 0,
            "failed": 0,
            "items": []
        }

        for report in reports:
            report_dir = output_root / report.id
            try:
                report_dir.mkdir(parents=True, exist_ok=True)
            except Exception:
                pass

            pbix_path = report_dir / f"{report.name or report.id}.pbix"
            structure_path = report_dir / "structure.json"

            status: Dict[str, Any] = {"report_id": report.id, "name": report.name}

            try:
                # Export PBIX (may fail if tenant disables export)
                if not pbix_path.exists():
                    client.export_report_pbix(report.workspace_id, report.id, str(pbix_path))

                # Extract bindings to JSON for downstream analysis
                try:
                    layout = read_layout_from_pbix(str(pbix_path))
                    structure = extract_bindings_from_layout(layout)
                    with open(structure_path, "w", encoding="utf-8") as f:
                        json.dump(structure, f, indent=2)
                    status.update({"saved": True, "pbix": str(pbix_path), "structure": str(structure_path)})
                except Exception as inner:
                    # PBIX saved, but structure extraction failed
                    status.update({"saved": True, "pbix": str(pbix_path), "structure_error": str(inner)})

                results["saved"] += 1
            except Exception as e:
                status.update({"saved": False, "error": str(e)})
                results["failed"] += 1

            results["items"].append(status)

        results["output_directory"] = str(output_root)
        return results
    
    def _handle_measure_section(self, story, table, styles):
        """Handle measure section formatting for PDF"""
        if table['measures']:
            story.append(Paragraph("Measures:", styles['Normal']))
            
            for measure in table['measures']:
                # Add measure name in bold
                story.append(Spacer(1, 6))
                story.append(Paragraph(f"<b>{measure['name']}</b>", styles['Normal']))
                
                # Use the TMDL parser to properly format the measure
                if measure.get('definition'):
                    # Parse the raw TMDL measure text
                    parsed_measures = self.tmdl_parser.parse_measures_from_tmdl(
                        f"measure {measure['name']} = {measure['definition']}"
                    )
                    
                    if parsed_measures:
                        parsed_measure = parsed_measures[0]
                        
                        # Format the expression with proper line breaks
                        expression = parsed_measure['expression']
                        
                        # Remove any remaining commented blocks
                        import re
                        expression = re.sub(r'/\*.*?\*/', '', expression, flags=re.DOTALL)
                        
                        # Create a code style for DAX
                        code_style = ParagraphStyle(
                            'DAXCode',
                            parent=styles['Normal'],
                            fontName='Courier',
                            fontSize=9,
                            leftIndent=20,
                            spaceAfter=6,
                            textColor=colors.darkblue
                        )
                        
                        # Display the expression line by line
                        story.append(Paragraph("Definition:", styles['Normal']))
                        
                        # Split expression by lines and display each
                        expression_lines = expression.split('\n')
                        for line in expression_lines:
                            if line.strip():
                                # Escape special characters for reportlab
                                safe_line = line.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
                                story.append(Paragraph(safe_line, code_style))
                        
                        # Add format string if available
                        if parsed_measure.get('format_string'):
                            story.append(Paragraph(
                                f"<i>Format: {parsed_measure['format_string']}</i>", 
                                styles['Normal']
                            ))
                    else:
                        # Fallback to original display
                        story.append(Paragraph("Definition:", styles['Normal']))
                        safe_def = measure['definition'].replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
                        story.append(Paragraph(f"<font face='Courier'>{safe_def}</font>", styles['Normal']))
                else:
                    story.append(Paragraph("Definition: No definition available", styles['Normal']))
                
                # Add annotations if any
                if measure.get('annotations'):
                    story.append(Paragraph("Annotations:", styles['Normal']))
                    for ann in measure['annotations']:
                        story.append(Paragraph(f"  • {ann['name']}: {ann['value']}", styles['Normal']))
                
                story.append(Spacer(1, 6))
    
    def get_workspace_name(self, workspace_id: str) -> str:
        """Get workspace name from the API."""
        try:
            url = f"{self.fabric_reader.FABRIC_API}/workspaces/{workspace_id}"
            result = self.fabric_reader.make_request(url)
            
            if isinstance(result, dict) and 'displayName' in result:
                return result['displayName']
            else:
                return workspace_id
        except Exception as e:
            print(f"⚠️  Warning: Could not fetch workspace name for {workspace_id}: {str(e)}")
            return workspace_id
    
    def find_all_tmdl_files(self) -> List[str]:
        """Find all TMDL export files in the Outputs directory."""
        if not os.path.exists(self.outputs_dir):
            print(f"❌ Outputs directory not found: {self.outputs_dir}")
            return []
        
        tmdl_files = [f for f in os.listdir(self.outputs_dir) 
                      if f.endswith('_TMDL.txt')]
        
        return tmdl_files
    
    def generate_all_reports(self, workspace_id: str = None, model_name: str = None) -> Dict:
        """Generate PDF reports for all TMDL files."""
        tmdl_files = self.find_all_tmdl_files()
        
        if not tmdl_files:
            # Fallback to orchestrator-driven analysis when no legacy exports exist
            if workspace_id:
                return self._generate_via_orchestrator(workspace_id, model_name)
            return {"error": "No TMDL files found in Outputs directory"}
        
        # Filter files if specified
        if workspace_id:
            # Extract workspace_id from TMDL file content
            filtered_files = []
            for filename in tmdl_files:
                filepath = os.path.join(self.outputs_dir, filename)
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        content = f.read()
                    
                    lines = content.split('\n')
                    file_workspace_id = "Unknown"
                    
                    for line in lines[:10]:  # Check first 10 lines
                        if line.startswith('Workspace ID:'):
                            file_workspace_id = line.replace('Workspace ID:', '').strip()
                            break
                    
                    if workspace_id == file_workspace_id:
                        filtered_files.append(filename)
                except Exception as e:
                    print(f"⚠️  Warning: Could not read workspace ID from {filename}: {str(e)}")
                    continue
            
            tmdl_files = filtered_files
        
        if model_name:
            tmdl_files = [f for f in tmdl_files if model_name.lower() in f.lower()]
        
        if not tmdl_files:
            return {"error": f"No matching TMDL files found for workspace_id={workspace_id}, model_name={model_name}"}
        
        generated_reports = []
        failed_reports = []
        
        for filename in tmdl_files:
            try:
                result = self.generate_single_report(filename)
                if result.get('success'):
                    generated_reports.append(result)
                else:
                    failed_reports.append(f"{filename}: {result.get('error', 'Unknown error')}")
            except Exception as e:
                failed_reports.append(f"{filename}: {str(e)}")
        
        return {
            'success': True,
            'generated_reports': generated_reports,
            'failed_reports': failed_reports,
            'total_files': len(tmdl_files),
            'successful': len(generated_reports),
            'failed': len(failed_reports)
        }

    def _generate_via_orchestrator(self, workspace_id: str, model_name: Optional[str]) -> Dict:
        """Analyze models via orchestrator and generate PDFs from results.

        This mirrors V5 behavior by driving analysis programmatically rather than
        relying on pre-exported TMDL text files.
        """
        async def _run() -> Dict:
            orchestrator = ModelHealthOrchestrator(workspace_id)
            # Analyze all models in workspace
            results = await orchestrator.analyze_workspace()

            generated: List[Dict[str, Any]] = []
            failed: List[str] = []

            for mid, result in results.items():
                try:
                    if model_name and model_name.lower() not in (result.model_name or '').lower():
                        continue
                    pdf_path = self.generate(result)
                    generated.append({
                        'model_name': result.model_name,
                        'model_id': result.model_id,
                        'pdf_file': os.path.basename(pdf_path),
                        'analysis_score': result.overall_score
                    })
                except Exception as e:
                    failed.append(f"{result.model_name or mid}: {e}")

            return {
                'success': True,
                'generated_reports': generated,
                'failed_reports': failed,
                'total_models': len(results),
                'successful': len(generated),
                'failed': len(failed)
            }

        try:
            return asyncio.run(_run())
        except RuntimeError:
            # If already within an event loop (e.g., IDE), create a new loop
            loop = asyncio.new_event_loop()
            try:
                return loop.run_until_complete(_run())
            finally:
                loop.close()
    
    def generate_single_report(self, filename: str) -> Dict:
        """Generate a PDF report for a single TMDL file."""
        filepath = os.path.join(self.outputs_dir, filename)
        
        if not os.path.exists(filepath):
            return {"error": f"File not found: {filepath}"}
        
        try:
            # Read and analyze the TMDL file
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Analyze the model
            analysis_result = self.analyzer.analyze_tmdl_export(filepath)
            
            if "error" in analysis_result:
                return {"error": analysis_result["error"]}
            
            # Parse TMDL sections for documentation
            sections = _parse_tmdl_sections(content)
            
            # Analyze M-code using Power Query best practices
            mcode_analysis = self.mcode_analyzer.analyze_tmdl_file(filepath)
            
            # Extract model metadata from TMDL file content
            lines = content.split('\n')
            model_name = "Unknown"
            model_id = "Unknown"
            
            for line in lines[:10]:  # Check first 10 lines
                if line.startswith('Model Name:'):
                    model_name = line.replace('Model Name:', '').strip()
                elif line.startswith('Model ID:'):
                    model_id = line.replace('Model ID:', '').strip()
            
            # Generate PDF
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_name = "".join(c for c in model_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
            
            # Extract workspace name from TMDL file content
            workspace_name = "Unknown"
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = f.read()
                lines = content.split('\n')
                for line in lines[:10]:  # Check first 10 lines
                    if line.startswith('Workspace Name:'):
                        workspace_name = line.replace('Workspace Name:', '').strip()
                        break
            except:
                pass
            
            safe_workspace_name = "".join(c for c in workspace_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
            pdf_filename = f"{safe_workspace_name}_{safe_name}_{timestamp}_report.pdf"
            pdf_filepath = str(Path(REPORTS_DIR) / pdf_filename)
            
            self._create_pdf_report(
                pdf_filepath, 
                model_name, 
                model_id, 
                analysis_result, 
                sections,
                mcode_analysis
            )
            
            return {
                'success': True,
                'model_name': model_name,
                'model_id': model_id,
                'pdf_file': pdf_filename,
                'analysis_score': analysis_result['score']
            }
            
        except Exception as e:
            return {"error": f"Failed to generate report: {str(e)}"}
    
    def _create_pdf_report(self, pdf_filepath: str, model_name: str, model_id: str, 
                          analysis_result: Dict, sections: Dict, mcode_analysis: Dict = None):
        """Create a comprehensive PDF report."""
        doc = SimpleDocTemplate(pdf_filepath, pagesize=letter)
        styles = getSampleStyleSheet()
        story = []
        
        # Create custom styles
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=24,
            spaceAfter=30,
            alignment=TA_CENTER,
            textColor=colors.darkblue
        )
        
        heading_style = ParagraphStyle(
            'CustomHeading',
            parent=styles['Heading2'],
            fontSize=16,
            spaceAfter=12,
            spaceBefore=20,
            textColor=colors.darkblue
        )
        
        subheading_style = ParagraphStyle(
            'CustomSubHeading',
            parent=styles['Heading3'],
            fontSize=14,
            spaceAfter=8,
            spaceBefore=12,
            textColor=colors.darkgreen
        )
        
        # Title page
        story.append(Paragraph(f"Semantic Model Report", title_style))
        story.append(Spacer(1, 20))
        story.append(Paragraph(f"<b>Model Name:</b> {model_name}", styles['Normal']))
        story.append(Paragraph(f"<b>Model ID:</b> {model_id}", styles['Normal']))
        story.append(Paragraph(f"<b>Generated:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", styles['Normal']))
        story.append(PageBreak())
        
        # Executive Summary
        story.append(Paragraph("Executive Summary", heading_style))
        story.append(Spacer(1, 12))
        
        overall_score = analysis_result['score']
        score_color = "green" if overall_score >= 80 else "orange" if overall_score >= 60 else "red"
        story.append(Paragraph(f"<b>Overall Quality Score:</b> <font color='{score_color}'>{overall_score}/100</font>", styles['Normal']))
        
        # Key statistics
        stats = analysis_result['statistics']
        
        stats_data = [
            ['Metric', 'Value'],
            ['Total Tables', str(stats['tables'])],
            ['Total Measures', str(stats['measures'])],
            ['Total Relationships', str(stats['relationships'])],
            ['Calculated Columns', str(stats['calculated_columns'])],
            ['Hierarchies', str(stats['hierarchies'])],
            ['Roles (RLS)', str(stats['roles'])]
        ]
        
        stats_table = Table(stats_data, colWidths=[2*inch, 1*inch])
        stats_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        story.append(stats_table)
        story.append(Spacer(1, 20))
        
        # Quality Analysis
        story.append(Paragraph("Quality Analysis", heading_style))
        story.append(Spacer(1, 12))
        
        # Category scores table
        category_data = [['Category', 'Score', 'Status']]
        for category, score_data in analysis_result['category_scores'].items():
            score = score_data['score']
            percentage = score_data['percentage']
            status = "✅ Excellent" if percentage >= 80 else "🟡 Good" if percentage >= 60 else "❌ Needs Improvement"
            category_data.append([category, f"{score}/{score_data['max_score']}", status])
        
        category_table = Table(category_data, colWidths=[1.5*inch, 1*inch, 2*inch])
        category_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        story.append(category_table)
        story.append(Spacer(1, 20))
        
        # M-Code Analysis (Power Query Best Practices)
        if mcode_analysis and 'error' not in mcode_analysis:
            story.append(Paragraph("M-Code Analysis (Power Query Best Practices)", heading_style))
            story.append(Spacer(1, 12))
            
            mcode_score = mcode_analysis.get('score', 0)
            mcode_color = "green" if mcode_score >= 80 else "orange" if mcode_score >= 60 else "red"
            story.append(Paragraph(f"<b>M-Code Quality Score:</b> <font color='{mcode_color}'>{mcode_score}/100</font>", styles['Normal']))
            
            # Show M-code statistics
            if mcode_analysis.get('statistics'):
                stats = mcode_analysis['statistics']
                story.append(Paragraph(f"<b>Queries Analyzed:</b> {stats.get('total_queries', 0)}", styles['Normal']))
                story.append(Paragraph(f"<b>Total Steps:</b> {stats.get('total_steps', 0)}", styles['Normal']))
                story.append(Paragraph(f"<b>Queries with Folding Issues:</b> {stats.get('queries_with_folding_issues', 0)}", styles['Normal']))
            
            # Show category scores
            if mcode_analysis.get('category_scores'):
                story.append(Paragraph("M-Code Category Scores:", subheading_style))
                for category, scores in mcode_analysis['category_scores'].items():
                    story.append(Paragraph(f"<b>{category}:</b> {scores['score']}/{scores['max_score']} ({scores['percentage']}%)", styles['Normal']))
            
            # Show top M-code issues
            if mcode_analysis.get('issues'):
                story.append(Paragraph("Top M-Code Issues:", subheading_style))
                critical_and_high = [issue for issue in mcode_analysis['issues'] 
                                   if issue['severity'] in ['Critical', 'High']][:3]
                
                for issue in critical_and_high:
                    story.append(Paragraph(f"<b>• [{issue['severity']}] {issue['title']}</b>", styles['Normal']))
                    story.append(Paragraph(f"  {issue['description']}", styles['Normal']))
                    if issue.get('query_name'):
                        story.append(Paragraph(f"  Query: {issue['query_name']}", styles['Normal']))
                    story.append(Paragraph(f"  💡 {issue['recommendation']}", styles['Normal']))
                    story.append(Spacer(1, 6))
            
            story.append(Spacer(1, 12))
        
        # Detailed Analysis
        story.append(Paragraph("Detailed Analysis", heading_style))
        story.append(Spacer(1, 12))
        
        for category, score_data in analysis_result['category_scores'].items():
            story.append(Paragraph(f"{category} Analysis", subheading_style))
            story.append(Paragraph(f"Score: {score_data['score']}/{score_data['max_score']} ({score_data['percentage']}%)", styles['Normal']))
            story.append(Paragraph(f"Issues Found: {score_data['issues_count']}", styles['Normal']))
            
            story.append(Spacer(1, 12))
        
        # Issues Found
        if analysis_result.get('issues'):
            story.append(Paragraph("Issues Found", heading_style))
            story.append(Spacer(1, 12))
            
            # Group issues by severity
            issues_by_severity = {}
            for issue in analysis_result['issues']:
                severity = issue['severity']
                if severity not in issues_by_severity:
                    issues_by_severity[severity] = []
                issues_by_severity[severity].append(issue)
            
            # Display issues by severity (Critical, High, Medium, Low, Info)
            severity_order = ['Critical', 'High', 'Medium', 'Low', 'Info']
            for severity in severity_order:
                if severity in issues_by_severity:
                    story.append(Paragraph(f"{severity} Issues ({len(issues_by_severity[severity])})", subheading_style))
                    story.append(Spacer(1, 6))
                    
                    for issue in issues_by_severity[severity]:
                        story.append(Paragraph(f"<b>• {issue['title']}</b>", styles['Normal']))
                        story.append(Paragraph(f"  Description: {issue['description']}", styles['Normal']))
                        story.append(Paragraph(f"  Location: {issue['location']}", styles['Normal']))
                        story.append(Paragraph(f"  Recommendation: {issue['recommendation']}", styles['Normal']))
                        story.append(Spacer(1, 6))
            
            story.append(Spacer(1, 20))
        
        # Recommendations
        if analysis_result.get('top_recommendations'):
            story.append(Paragraph("Top Recommendations", heading_style))
            story.append(Spacer(1, 12))
            
            for rec in analysis_result['top_recommendations']:
                story.append(Paragraph(f"• {rec}", styles['Normal']))
            
            story.append(Spacer(1, 20))
        
        # Model Documentation
        story.append(PageBreak())
        story.append(Paragraph("Model Documentation", heading_style))
        story.append(Spacer(1, 12))
        
        # Tables section
        story.append(Paragraph("Tables", subheading_style))
        for table in sections['tables']:
            story.append(Paragraph(f"<b>{table['name']}</b>", styles['Normal']))
            
            if table['description']:
                story.append(Paragraph(f"Description: {table['description']}", styles['Normal']))
            
            if table['lineage_tag']:
                story.append(Paragraph(f"Lineage Tag: {table['lineage_tag']}", styles['Normal']))
            
            # Measures in this table
            self._handle_measure_section(story, table, styles)
            
            # M-Code
            if table['m_code']:
                story.append(Paragraph("M-Code:", styles['Normal']))
                story.append(Paragraph(f"<font face='Courier'>{table['m_code']}</font>", styles['Normal']))
            
            story.append(Spacer(1, 12))
        
        # Relationships section
        if sections['relationships']:
            story.append(Paragraph("Relationships", subheading_style))
            for rel in sections['relationships']:
                story.append(Paragraph(f"<b>{rel['name']}</b>", styles['Normal']))
                story.append(Paragraph(f"From: {rel['from_table']}.{rel['from_column']}", styles['Normal']))
                story.append(Paragraph(f"To: {rel['to_table']}.{rel['to_column']}", styles['Normal']))
                
                if rel['cardinality']:
                    story.append(Paragraph(f"Cardinality: {rel['cardinality']}", styles['Normal']))
                
                if rel['cross_filter_direction']:
                    story.append(Paragraph(f"Cross Filter Direction: {rel['cross_filter_direction']}", styles['Normal']))
                
                story.append(Spacer(1, 6))
        
        # Technical Details
        story.append(PageBreak())
        story.append(Paragraph("Technical Details", heading_style))
        story.append(Spacer(1, 12))
        
        story.append(Paragraph("Model Metadata", subheading_style))
        for key, value in sections['metadata'].items():
            story.append(Paragraph(f"<b>{key.replace('_', ' ').title()}:</b> {value}", styles['Normal']))
        
        # Build the PDF
        doc.build(story)
    
    def generate_workspace_summary_report(self, workspace_id: str = None) -> Dict:
        """Generate a summary PDF report for all models in a workspace."""
        tmdl_files = self.find_all_tmdl_files()
        
        if workspace_id:
            # Extract workspace_id from TMDL file content
            filtered_files = []
            for filename in tmdl_files:
                filepath = os.path.join(self.outputs_dir, filename)
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        content = f.read()
                    
                    lines = content.split('\n')
                    file_workspace_id = "Unknown"
                    
                    for line in lines[:10]:  # Check first 10 lines
                        if line.startswith('Workspace ID:'):
                            file_workspace_id = line.replace('Workspace ID:', '').strip()
                            break
                    
                    if workspace_id == file_workspace_id:
                        filtered_files.append(filename)
                except Exception as e:
                    print(f"⚠️  Warning: Could not read workspace ID from {filename}: {str(e)}")
                    continue
            
            tmdl_files = filtered_files
        
        if not tmdl_files:
            return {"error": "No TMDL files found"}
        
        # Analyze all models
        all_results = []
        for filename in tmdl_files:
            filepath = os.path.join(self.outputs_dir, filename)
            result = self.analyzer.analyze_tmdl_export(filepath)
            
            if "error" not in result:
                # Extract model metadata from TMDL file content
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                lines = content.split('\n')
                model_name = "Unknown"
                model_id = "Unknown"
                
                for line in lines[:10]:  # Check first 10 lines
                    if line.startswith('Model Name:'):
                        model_name = line.replace('Model Name:', '').strip()
                    elif line.startswith('Model ID:'):
                        model_id = line.replace('Model ID:', '').strip()
                
                result['model_name'] = model_name
                result['model_id'] = model_id
                all_results.append(result)
        
        if not all_results:
            return {"error": "No valid results to analyze"}
        
        # Generate summary PDF
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Get workspace name from API or TMDL files
        workspace_name = "Unknown"
        if workspace_id:
            # Try to get workspace name from API
            try:
                workspace_name = self.get_workspace_name(workspace_id)
            except:
                # Fallback: try to get from TMDL files
                tmdl_files = self.find_all_tmdl_files()
                for filename in tmdl_files:
                    filepath = os.path.join(self.outputs_dir, filename)
                    try:
                        with open(filepath, 'r', encoding='utf-8') as f:
                            content = f.read()
                        lines = content.split('\n')
                        for line in lines[:10]:
                            if line.startswith('Workspace ID:') and workspace_id in line:
                                # Found matching workspace, get its name
                                for name_line in lines[:10]:
                                    if name_line.startswith('Workspace Name:'):
                                        workspace_name = name_line.replace('Workspace Name:', '').strip()
                                        break
                                break
                    except:
                        continue
        else:
            workspace_name = "all_workspaces"
        
        # Clean workspace name for filename
        safe_workspace_name = "".join(c for c in workspace_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
        pdf_filename = f"{safe_workspace_name}_{timestamp}_summary.pdf"
        pdf_filepath = str(Path(REPORTS_DIR) / pdf_filename)
        
        self._create_workspace_summary_pdf(pdf_filepath, all_results, workspace_id)
        
        return {
            'success': True,
            'pdf_file': pdf_filename,
            'total_models': len(all_results),
            'average_score': sum(r['score'] for r in all_results) / len(all_results)
        }
    
    def _create_workspace_summary_pdf(self, pdf_filepath: str, all_results: List[Dict], workspace_id: str = None):
        """Create a workspace summary PDF report."""
        doc = SimpleDocTemplate(pdf_filepath, pagesize=letter)
        styles = getSampleStyleSheet()
        story = []
        
        # Custom styles
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=20,
            spaceAfter=20,
            alignment=TA_CENTER,
            textColor=colors.darkblue
        )
        
        heading_style = ParagraphStyle(
            'CustomHeading',
            parent=styles['Heading2'],
            fontSize=14,
            spaceAfter=8,
            spaceBefore=12,
            textColor=colors.darkblue
        )
        
        # Header with generation time and workspace info
        story.append(Paragraph(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", styles['Normal']))
        
        # Get workspace name if workspace_id is provided
        if workspace_id:
            workspace_name = self.get_workspace_name(workspace_id)
            story.append(Paragraph(f"Workspace: {workspace_name}", styles['Normal']))
            story.append(Paragraph(f"Workspace ID: {workspace_id}", styles['Normal']))
        else:
            story.append(Paragraph(f"Workspace ID: Unknown", styles['Normal']))
        
        story.append(Spacer(1, 20))
        
        # Title
        story.append(Paragraph(f"Workspace Summary Report", title_style))
        story.append(Spacer(1, 15))
        
        # Summary statistics
        total_models = len(all_results)
        avg_score = sum(r['score'] for r in all_results) / total_models
        best_model = max(all_results, key=lambda x: x['score'])
        worst_model = min(all_results, key=lambda x: x['score'])
        
        story.append(Paragraph("Workspace Overview", heading_style))
        story.append(Spacer(1, 8))
        
        summary_data = [
            ['Metric', 'Result'],
            ['Total Models', str(total_models)],
            ['Average Score', f"{avg_score:.2f}/100"],
            ['Best Model', f"{best_model['model_name']} ({best_model['score']:.2f}/100)"],
            ['Worst Model', f"{worst_model['model_name']} ({worst_model['score']:.2f}/100)"]
        ]
        
        summary_table = Table(summary_data, colWidths=[2*inch, 3*inch])
        summary_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('FONTSIZE', (0, 1), (-1, -1), 9)
        ]))
        story.append(summary_table)
        story.append(Spacer(1, 12))
        
        # Individual model scores
        story.append(Paragraph("Individual Model Scores", heading_style))
        story.append(Spacer(1, 8))
        
        # Sort by score
        sorted_results = sorted(all_results, key=lambda x: x['score'], reverse=True)
        
        model_data = [['Rank', 'Model Name', 'Overall Score', 'Tables', 'Measures', 'Relationships']]
        for i, result in enumerate(sorted_results, 1):
            model_data.append([
                str(i),
                result['model_name'],
                f"{result['score']:.2f}",
                str(result['statistics']['tables']),
                str(result['statistics']['measures']),
                str(result['statistics']['relationships'])
            ])
        
        model_table = Table(model_data, colWidths=[0.5*inch, 2*inch, 1*inch, 0.8*inch, 0.8*inch, 1*inch])
        model_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 9),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('FONTSIZE', (0, 1), (-1, -1), 7)
        ]))
        story.append(model_table)
        
        # Build the PDF
        doc.build(story)


def main():
    """Main function to handle command line arguments."""
    parser = argparse.ArgumentParser(description='PDF Report Generator for Semantic Models')
    parser.add_argument('--workspace-id', help='Workspace ID to filter models')
    parser.add_argument('--model-id', help='Semantic model ID to analyze via API (bypasses TMDL)')
    parser.add_argument('--model-name', help='Specific model name to report on')
    parser.add_argument('--summary-only', action='store_true', 
                       help='Generate only workspace summary report')
    
    args = parser.parse_args()
    
    try:
        generator = PDFReportGenerator()

        # Step 0 (like V5): Discover and save reports for the workspace (if provided)
        if args.workspace_id:
            print("🗂️  Discovering and saving workspace report definitions...")
            save_summary = generator.download_workspace_reports(args.workspace_id)
            if save_summary.get("error"):
                print(f"❌ {save_summary['error']}")
            else:
                print(f"✅ Reports saved: {save_summary.get('saved', 0)}/{save_summary.get('total_reports', 0)}")
                print(f"📁 Output: {save_summary.get('output_directory', '')}")

        # New: Direct API mode using orchestrator if model-id provided
        if args.model_id:
            if not args.workspace_id:
                print("❌ --workspace-id is required when using --model-id")
                return

            print("📄 Generating report via API analysis (no TMDL required)...")

            async def _run():
                orchestrator = ModelHealthOrchestrator(workspace_id=args.workspace_id)
                result = await orchestrator.analyze_model(
                    model_id=args.model_id,
                    include_reports=True
                )
                pdf_path = generator.generate(result)
                print(f"✅ Generated PDF: {pdf_path}")

            asyncio.run(_run())
            return
        
        if args.summary_only:
            print("📊 Generating workspace summary report...")
            result = generator.generate_workspace_summary_report(args.workspace_id)
            
            if result.get('success'):
                print(f"✅ Generated workspace summary: {result['pdf_file']}")
                print(f"📈 Analyzed {result['total_models']} models")
                print(f"📊 Average score: {result['average_score']:.2f}/100")
            else:
                print(f"❌ {result.get('error', 'Unknown error')}")
        else:
            print("📄 Generating individual model reports...")
            result = generator.generate_all_reports(args.workspace_id, args.model_name)
            
            if result.get('success'):
                print(f"✅ Generated {result['successful']} reports")
                print(f"❌ Failed: {result['failed']} reports")
                
                for report in result['generated_reports']:
                    print(f"  • {report['model_name']} -> {report['pdf_file']} (Score: {report['analysis_score']:.2f}/100)")
                
                if result['failed_reports']:
                    print("\nFailed reports:")
                    for failure in result['failed_reports']:
                        print(f"  • {failure}")
            else:
                print(f"❌ {result.get('error', 'Unknown error')}")
                
    except ImportError as e:
        print(f"❌ {e}")
        print("Please install reportlab: pip install reportlab")
    except Exception as e:
        print(f"❌ Error: {str(e)}")


if __name__ == "__main__":
    main() 