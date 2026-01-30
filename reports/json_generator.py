#!/usr/bin/env python3
"""
Polished JSON Report Generator with Grouped Issues
=================================================

This module generates a polished PDF report from the output of
``JSONSemanticModelReviewer``. It reproduces a layout similar to a sample
report provided by the user: a title page, an executive summary with
statistics and category scores, a detailed analysis section, a grouped
issues section, and a comprehensive model documentation section.

The issues section groups repeated issues under a single heading with the
recommendation shown once. For example, orphaned tables are listed under
the “Orphaned Table” group, and missing column/measure descriptions are
aggregated by table with counts. This improves readability when many
similar issues occur.

PDF generation uses Matplotlib and Pillow. Each page is rendered to a
PNG and then combined into a multi‑page PDF. This avoids problems with
``PdfPages`` in certain environments.

Usage::

    python polished_json_report_generator.py --input model.json --output report.pdf

If ``--output`` is omitted, the output filename defaults to
``<input_basename>_polished_report.pdf``.
"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from datetime import datetime
from typing import Dict, List, Tuple, Any

from json_semantic_model_reviewer import JSONSemanticModelReviewer

try:
    import matplotlib.pyplot as plt  # type: ignore
    MATPLOTLIB_AVAILABLE = True
except Exception:
    MATPLOTLIB_AVAILABLE = False

try:
    from PIL import Image  # type: ignore
    PIL_AVAILABLE = True
except Exception:
    PIL_AVAILABLE = False


def wrap_text(text: str, max_chars: int) -> List[str]:
    """Wrap a string into lines with a maximum character length.

    Words are preserved when possible. Returns a list of lines. An empty
    string produces a list with one empty line.
    """
    if not text:
        return ['']
    words = text.split()
    lines: List[str] = []
    current: List[str] = []
    cur_len = 0
    for w in words:
        extra = 1 if current else 0
        if cur_len + len(w) + extra <= max_chars:
            current.append(w)
            cur_len += len(w) + extra
        else:
            lines.append(' '.join(current))
            current = [w]
            cur_len = len(w)
    if current:
        lines.append(' '.join(current))
    return lines


def status_from_percent(pct: int) -> str:
    """Map a percentage score to a human‑readable status."""
    if pct >= 90:
        return 'Excellent'
    if pct >= 75:
        return 'Good'
    if pct >= 50:
        return 'Fair'
    return 'Poor'


def parse_location(location: str) -> Tuple[str, str, str]:
    """Parse a location string into (table, column, measure).

    The location string from issues is formatted like ``"Table: Foo"``
    or ``"Table: Foo, Column: Bar"`` or ``"Table: Foo, Measure: Baz"``.
    Returns a tuple of table name, column name and measure name (any can
    be an empty string).
    """
    table = ''
    column = ''
    measure = ''
    if location:
        parts = [p.strip() for p in location.split(',')]
        for part in parts:
            if part.lower().startswith('table'):
                table = part.split(':', 1)[1].strip()
            elif part.lower().startswith('column'):
                column = part.split(':', 1)[1].strip()
            elif part.lower().startswith('measure'):
                measure = part.split(':', 1)[1].strip()
    return table, column, measure


def generate_polished_pdf(report: Dict[str, Any], model: Dict[str, Any], output_path: str) -> None:
    """Generate a multi‑page PDF report with grouped issues.

    Parameters
    ----------
    report : dict
        The report dictionary returned from ``JSONSemanticModelReviewer.analyze_json``.
    model : dict
        The original semantic model definition loaded from JSON.
    output_path : str
        Path to save the generated PDF file.
    """
    if not MATPLOTLIB_AVAILABLE or not PIL_AVAILABLE:
        raise RuntimeError('Matplotlib and Pillow are required to generate the PDF report.')

    # Extract basic metadata
    model_name = model.get('name') or model.get('model', {}).get('name') or 'Semantic Model'
    model_id = model.get('id') or model.get('model', {}).get('id') or ''
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    overall_score = report.get('score', 0)
    max_score = report.get('max_score', 100)
    grade = report.get('grade', '')
    grade_desc = report.get('grade_description', '')
    stats = report.get('statistics', {})
    cat_scores = report.get('category_scores', {})
    issues = report.get('issues', [])
    top_recs = report.get('top_recommendations', [])

    # Sort issues by severity and impact
    severity_order = {'Critical': 1, 'High': 2, 'Medium': 3, 'Low': 4, 'Info': 5}
    sorted_issues = sorted(issues, key=lambda i: (severity_order.get(i['severity'], 99), -i['impact_score']))

    # Group issues by severity and then by title
    grouped: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    for iss in sorted_issues:
        sev = iss['severity']
        title = iss['title']
        grouped.setdefault(sev, {}).setdefault(title, []).append(iss)

    # Map categories to status. If the category value is a dict (semantic model), use its percentage;
    # if it is a number (unified report), treat that as percentage directly.
    cat_status: Dict[str, str] = {}
    for cat, data in cat_scores.items():
        if isinstance(data, dict):
            pct = data.get('percentage', 0)
        else:
            try:
                pct = float(data)
            except Exception:
                pct = 0
        cat_status[cat] = status_from_percent(int(pct))

    page_paths: List[str] = []
    page_num = 1

    def save_fig(fig: Any) -> None:
        nonlocal page_num
        tmp_path = os.path.join(tempfile.gettempdir(), f'jsonreport_page_{page_num}.png')
        fig.savefig(tmp_path, dpi=300, bbox_inches='tight')
        page_paths.append(tmp_path)
        page_num += 1
        plt.close(fig)

    # Title Page
    fig = plt.figure(figsize=(8.5, 11))
    ax = fig.add_subplot(111)
    ax.axis('off')
    ax.text(0.5, 0.8, 'Semantic Model Report', fontsize=28, weight='bold', ha='center', color='#0044cc')
    ax.text(0.5, 0.72, f'Model Name: {model_name}', fontsize=10, ha='center')
    if model_id:
        ax.text(0.5, 0.69, f'Model ID: {model_id}', fontsize=10, ha='center')
    ax.text(0.5, 0.66, f'Generated: {now_str}', fontsize=9, ha='center', color='#666666')
    ax.text(0.5, 0.55, f'Overall Quality Score: {overall_score}/{max_score}', fontsize=16, weight='bold', ha='center')
    ax.text(0.5, 0.50, f'Grade: {grade} - {grade_desc}', fontsize=14, ha='center', color='#333333')
    save_fig(fig)

    # Executive Summary
    fig, ax = plt.subplots(figsize=(8.5, 11))
    ax.axis('off')
    ax.text(0.02, 0.95, 'Executive Summary', fontsize=20, weight='bold', color='#0044cc')
    # Statistics table
    stat_data = [[k.title(), stats[k]] for k in stats]
    stats_ax = fig.add_axes([0.05, 0.68, 0.4, 0.22])
    stats_ax.axis('off')
    stats_table = stats_ax.table(cellText=stat_data, colLabels=['Metric','Value'], colWidths=[0.5,0.4], loc='center')
    stats_table.auto_set_font_size(False)
    stats_table.set_fontsize(9)
    for (row, col), cell in stats_table.get_celld().items():
        if row == 0:
            cell.set_facecolor('#d1d1d1')
            cell.set_text_props(weight='bold')
        cell.set_edgecolor('#999999')
    stats_table.scale(1, 1.2)
    # Category scores table
    # Display names for categories. Provide fallbacks for unified categories.
    disp_names = {
        'Design': 'Design',
        'Relationships': 'Relationships',
        'Measures': 'Measures',
        'Naming': 'Naming',
        'Documentation': 'Docs',
        'Performance': 'Performance',
        'Model Design': 'Design',
        'DAX Measures': 'Measures',
        'M‑Code Performance': 'M‑Code Perf',
        'Query Folding': 'Query Folding',
        'Security': 'Security'
    }
    cat_rows: List[List[str]] = []
    for cat, data in cat_scores.items():
        # Determine percentage and other fields based on data type
        if isinstance(data, dict):
            perc = data.get('percentage', 0)
            score_str = f"{data.get('score')}/{data.get('max_score')}"
            issues_count = data.get('issues_count', 0)
        else:
            # Unified: data is a percentage
            try:
                perc = float(data)
            except Exception:
                perc = 0
            score_str = '—'
            # Count issues for this category by scanning issues list
            issues_count = sum(1 for iss in issues if iss.get('category') == cat)
        cat_rows.append([
            disp_names.get(cat, cat),
            score_str,
            f"{int(perc)}%",
            cat_status.get(cat, ''),
            issues_count
        ])
    cat_ax = fig.add_axes([0.50, 0.68, 0.45, 0.22])
    cat_ax.axis('off')
    cat_table = cat_ax.table(cellText=cat_rows, colLabels=['Category','Score','%','Status','Issues'], colWidths=[0.25,0.15,0.12,0.2,0.13], loc='center')
    cat_table.auto_set_font_size(False)
    cat_table.set_fontsize(9)
    for (row, col), cell in cat_table.get_celld().items():
        if row == 0:
            cell.set_facecolor('#d1d1d1')
            cell.set_text_props(weight='bold')
        cell.set_edgecolor('#999999')
    cat_table.scale(1, 1.2)
    # Top Issues
    y = 0.65
    ax.text(0.02, y, 'Top Issues', fontsize=16, weight='bold', color='#0044cc')
    y -= 0.04
    # Top 5 issues with severity >= Medium
    top_issues = [iss for iss in sorted_issues if severity_order.get(iss['severity'], 99) <= 3][:5]
    if top_issues:
        for i, iss in enumerate(top_issues, 1):
            if y < 0.15:
                save_fig(fig)
                fig, ax = plt.subplots(figsize=(8.5, 11))
                ax.axis('off')
                y = 0.95
                ax.text(0.02, y, 'Executive Summary (cont.)', fontsize=20, weight='bold', color='#0044cc')
                y -= 0.05
            ax.text(0.04, y, f"{i}. [{iss['severity']}] {iss['title']}", fontsize=10, weight='bold')
            y -= 0.02
            for line in wrap_text(iss['description'], 80):
                ax.text(0.06, y, line, fontsize=9)
                y -= 0.018
            if iss.get('location'):
                ax.text(0.06, y, f"Location: {iss['location']}", fontsize=8, style='italic')
                y -= 0.017
            rec_lines = wrap_text(iss['recommendation'], 80)
            for j, line in enumerate(rec_lines):
                prefix = 'Recommendation: ' if j == 0 else '              '
                ax.text(0.06, y, prefix + line, fontsize=9)
                y -= 0.018
            y -= 0.012
    else:
        ax.text(0.04, y, 'No significant issues detected.', fontsize=9)
        y -= 0.03
    # Top Recommendations
    y -= 0.02
    ax.text(0.02, y, 'Top Recommendations', fontsize=16, weight='bold', color='#0044cc')
    y -= 0.04
    if top_recs:
        for i, rec in enumerate(top_recs[:5], 1):
            lines = wrap_text(rec, 85)
            ax.text(0.04, y, f"{i}. {lines[0]}", fontsize=9)
            y -= 0.02
            for line in lines[1:]:
                ax.text(0.07, y, line, fontsize=9)
                y -= 0.018
            y -= 0.008
    else:
        ax.text(0.04, y, 'No recommendations.', fontsize=9)
    save_fig(fig)

    # Detailed Analysis
    fig, ax = plt.subplots(figsize=(8.5, 11))
    ax.axis('off')
    ypos = 0.95
    ax.text(0.02, ypos, 'Detailed Analysis', fontsize=20, weight='bold', color='#0044cc')
    ypos -= 0.05
    for cat, data in cat_scores.items():
        # Start a new page if space runs low
        if ypos < 0.15:
            save_fig(fig)
            fig, ax = plt.subplots(figsize=(8.5, 11))
            ax.axis('off')
            ypos = 0.95
            ax.text(0.02, ypos, 'Detailed Analysis', fontsize=20, weight='bold', color='#0044cc')
            ypos -= 0.05
        # Determine fields based on data type
        if isinstance(data, dict):
            perc = data.get('percentage', 0)
            score_str = f"{data.get('score')}/{data.get('max_score')} ({perc}%)"
            issues_count = data.get('issues_count', 0)
        else:
            try:
                perc = float(data)
            except Exception:
                perc = 0
            score_str = f"{int(perc)}%"
            issues_count = sum(1 for iss in issues if iss.get('category') == cat)
        status = cat_status.get(cat, '')
        ax.text(0.02, ypos, cat, fontsize=14, weight='bold', color='#008800')
        ypos -= 0.02
        ax.text(0.04, ypos, f"Score: {score_str}", fontsize=9)
        ypos -= 0.018
        ax.text(0.04, ypos, f"Status: {status}", fontsize=9)
        ypos -= 0.018
        ax.text(0.04, ypos, f"Issues Found: {issues_count}", fontsize=9)
        ypos -= 0.03
    save_fig(fig)

    # Issues Found with grouping
    for sev in ['Critical','High','Medium','Low','Info']:
        if sev not in grouped:
            continue
        sev_groups = grouped[sev]
        fig, ax = plt.subplots(figsize=(8.5, 11))
        ax.axis('off')
        ypos = 0.95
        ax.text(0.02, ypos, 'Issues Found', fontsize=20, weight='bold', color='#0044cc')
        ypos -= 0.05
        ax.text(0.02, ypos, f'{sev} Issues ({sum(len(v) for v in sev_groups.values())})', fontsize=16, weight='bold', color='#008800')
        ypos -= 0.04
        for title, iss_list in sev_groups.items():
            if ypos < 0.20:
                save_fig(fig)
                fig, ax = plt.subplots(figsize=(8.5, 11))
                ax.axis('off')
                ypos = 0.95
                ax.text(0.02, ypos, 'Issues Found', fontsize=20, weight='bold', color='#0044cc')
                ypos -= 0.05
                ax.text(0.02, ypos, f'{sev} Issues ({sum(len(v) for v in sev_groups.values())})', fontsize=16, weight='bold', color='#008800')
                ypos -= 0.04
            count = len(iss_list)
            ax.text(0.03, ypos, f'{title} ({count})', fontsize=12, weight='bold', color='#0055aa')
            ypos -= 0.02
            # Use first issue to summarise description and recommendation
            ref_issue = iss_list[0]
            # If all recommendations identical, show once
            recom = ref_issue.get('recommendation', '')
            desc = ref_issue.get('description', '')
            # Group by specific types
            if title == 'Orphaned Table':
                # Show general recommendation
                for line in wrap_text(recom, 90):
                    ax.text(0.05, ypos, line, fontsize=9)
                    ypos -= 0.018
                # List orphaned tables
                names = [parse_location(iss['location'])[0] for iss in iss_list]
                names = sorted(set(names))
                for name in names:
                    if ypos < 0.10:
                        save_fig(fig)
                        fig, ax = plt.subplots(figsize=(8.5, 11))
                        ax.axis('off')
                        ypos = 0.95
                        ax.text(0.02, ypos, 'Issues Found', fontsize=20, weight='bold', color='#0044cc')
                        ypos -= 0.05
                        ax.text(0.02, ypos, f'{sev} Issues ({sum(len(v) for v in sev_groups.values())})', fontsize=16, weight='bold', color='#008800')
                        ypos -= 0.04
                        ax.text(0.03, ypos, f'{title} ({count})', fontsize=12, weight='bold', color='#0055aa')
                        ypos -= 0.02
                        for line in wrap_text(recom, 90):
                            ax.text(0.05, ypos, line, fontsize=9)
                            ypos -= 0.018
                    ax.text(0.07, ypos, f'• {name}', fontsize=9)
                    ypos -= 0.016
            elif title == 'Column Lacks Description':
                # group counts per table
                table_counts: Dict[str, int] = {}
                for iss in iss_list:
                    table, col, _ = parse_location(iss['location'])
                    table_counts[table] = table_counts.get(table, 0) + 1
                # recommendation once
                for line in wrap_text(recom, 90):
                    ax.text(0.05, ypos, line, fontsize=9)
                    ypos -= 0.018
                # sort by table name for nice alignment
                for table_name in sorted(table_counts.keys()):
                    cnt = table_counts[table_name]
                    if ypos < 0.10:
                        save_fig(fig)
                        fig, ax = plt.subplots(figsize=(8.5, 11))
                        ax.axis('off')
                        ypos = 0.95
                        ax.text(0.02, ypos, 'Issues Found', fontsize=20, weight='bold', color='#0044cc')
                        ypos -= 0.05
                        ax.text(0.02, ypos, f'{sev} Issues ({sum(len(v) for v in sev_groups.values())})', fontsize=16, weight='bold', color='#008800')
                        ypos -= 0.04
                        ax.text(0.03, ypos, f'{title} ({count})', fontsize=12, weight='bold', color='#0055aa')
                        ypos -= 0.02
                        for line in wrap_text(recom, 90):
                            ax.text(0.05, ypos, line, fontsize=9)
                            ypos -= 0.018
                    ax.text(0.07, ypos, f'{cnt} – {table_name}', fontsize=9)
                    ypos -= 0.016
            elif title == 'Measure Lacks Description':
                table_counts: Dict[str, int] = {}
                for iss in iss_list:
                    table, _, meas = parse_location(iss['location'])
                    table_counts[table] = table_counts.get(table, 0) + 1
                for line in wrap_text(recom, 90):
                    ax.text(0.05, ypos, line, fontsize=9)
                    ypos -= 0.018
                for table_name in sorted(table_counts.keys()):
                    cnt = table_counts[table_name]
                    if ypos < 0.10:
                        save_fig(fig)
                        fig, ax = plt.subplots(figsize=(8.5, 11))
                        ax.axis('off')
                        ypos = 0.95
                        ax.text(0.02, ypos, 'Issues Found', fontsize=20, weight='bold', color='#0044cc')
                        ypos -= 0.05
                        ax.text(0.02, ypos, f'{sev} Issues ({sum(len(v) for v in sev_groups.values())})', fontsize=16, weight='bold', color='#008800')
                        ypos -= 0.04
                        ax.text(0.03, ypos, f'{title} ({count})', fontsize=12, weight='bold', color='#0055aa')
                        ypos -= 0.02
                        for line in wrap_text(recom, 90):
                            ax.text(0.05, ypos, line, fontsize=9)
                            ypos -= 0.018
                    ax.text(0.07, ypos, f'{cnt} – {table_name}', fontsize=9)
                    ypos -= 0.016
            elif title == 'Duplicate Column Name':
                # Sort duplicate column issues by column name then table name
                for line in wrap_text(desc, 90):
                    ax.text(0.05, ypos, line, fontsize=9)
                    ypos -= 0.018
                for line in wrap_text(recom, 90):
                    ax.text(0.05, ypos, line, fontsize=9, style='italic')
                    ypos -= 0.018
                # Sort by column then table
                sorted_issues = sorted(iss_list, key=lambda i: (
                    parse_location(i['location'])[1],  # column
                    parse_location(i['location'])[0]   # table
                ))
                for iss in sorted_issues:
                    loc_str = iss.get('location', '')
                    if not loc_str:
                        continue
                    if ypos < 0.10:
                        save_fig(fig)
                        fig, ax = plt.subplots(figsize=(8.5, 11))
                        ax.axis('off')
                        ypos = 0.95
                        ax.text(0.02, ypos, 'Issues Found', fontsize=20, weight='bold', color='#0044cc')
                        ypos -= 0.05
                        ax.text(0.02, ypos, f'{sev} Issues ({sum(len(v) for v in sev_groups.values())})', fontsize=16, weight='bold', color='#008800')
                        ypos -= 0.04
                        ax.text(0.03, ypos, f'{title} ({count})', fontsize=12, weight='bold', color='#0055aa')
                        ypos -= 0.02
                        for line in wrap_text(desc, 90):
                            ax.text(0.05, ypos, line, fontsize=9)
                            ypos -= 0.018
                        for line in wrap_text(recom, 90):
                            ax.text(0.05, ypos, line, fontsize=9, style='italic')
                            ypos -= 0.018
                    ax.text(0.07, ypos, f'• {loc_str}', fontsize=8)
                    ypos -= 0.015
            else:
                # Generic grouping: show description and recommendation once, list locations
                for line in wrap_text(desc, 90):
                    ax.text(0.05, ypos, line, fontsize=9)
                    ypos -= 0.018
                for line in wrap_text(recom, 90):
                    ax.text(0.05, ypos, line, fontsize=9, style='italic')
                    ypos -= 0.018
                for iss in iss_list:
                    tbl, col, meas = parse_location(iss.get('location', '') or '')
                    loc_str = iss.get('location', '')
                    if not loc_str:
                        continue
                    if ypos < 0.10:
                        save_fig(fig)
                        fig, ax = plt.subplots(figsize=(8.5, 11))
                        ax.axis('off')
                        ypos = 0.95
                        ax.text(0.02, ypos, 'Issues Found', fontsize=20, weight='bold', color='#0044cc')
                        ypos -= 0.05
                        ax.text(0.02, ypos, f'{sev} Issues ({sum(len(v) for v in sev_groups.values())})', fontsize=16, weight='bold', color='#008800')
                        ypos -= 0.04
                        ax.text(0.03, ypos, f'{title} ({count})', fontsize=12, weight='bold', color='#0055aa')
                        ypos -= 0.02
                        for line in wrap_text(desc, 90):
                            ax.text(0.05, ypos, line, fontsize=9)
                            ypos -= 0.018
                        for line in wrap_text(recom, 90):
                            ax.text(0.05, ypos, line, fontsize=9, style='italic')
                            ypos -= 0.018
                    ax.text(0.07, ypos, f'• {loc_str}', fontsize=8)
                    ypos -= 0.015
            ypos -= 0.02
        save_fig(fig)

    # Model Documentation
    tables = model.get('tables') or model.get('model', {}).get('tables') or []
    if tables:
        fig, ax = plt.subplots(figsize=(8.5, 11))
        ax.axis('off')
        ypos = 0.95
        ax.text(0.02, ypos, 'Model Documentation', fontsize=20, weight='bold', color='#0044cc')
        ypos -= 0.05
        for tbl in tables:
            name = tbl.get('name', '')
            columns = tbl.get('columns', [])
            measures = tbl.get('measures', [])
            if ypos < 0.20:
                save_fig(fig)
                fig, ax = plt.subplots(figsize=(8.5, 11))
                ax.axis('off')
                ypos = 0.95
                ax.text(0.02, ypos, 'Model Documentation', fontsize=20, weight='bold', color='#0044cc')
                ypos -= 0.05
            ax.text(0.02, ypos, f'Table: {name}', fontsize=16, weight='bold', color='#008800')
            ypos -= 0.025
            if columns:
                col_names = [c.get('name', '') for c in columns]
                col_text = ', '.join(col_names)
                for line in wrap_text(f'Columns ({len(columns)}): {col_text}', 100):
                    ax.text(0.04, ypos, line, fontsize=8)
                    ypos -= 0.017
            else:
                ax.text(0.04, ypos, 'Columns: None', fontsize=8)
                ypos -= 0.017
            if measures:
                ax.text(0.04, ypos, f'Measures ({len(measures)})', fontsize=9, weight='bold')
                ypos -= 0.018
                for m in measures:
                    mname = m.get('name', '')
                    expr = m.get('expression', '') or ''
                    if ypos < 0.10:
                        save_fig(fig)
                        fig, ax = plt.subplots(figsize=(8.5, 11))
                        ax.axis('off')
                        ypos = 0.95
                        ax.text(0.02, ypos, 'Model Documentation', fontsize=20, weight='bold', color='#0044cc')
                        ypos -= 0.05
                        ax.text(0.02, ypos, f'Table: {name}', fontsize=16, weight='bold', color='#008800')
                        ypos -= 0.025
                        col_names = [c.get('name', '') for c in columns]
                        col_text = ', '.join(col_names)
                        for line in wrap_text(f'Columns ({len(columns)}): {col_text}', 100):
                            ax.text(0.04, ypos, line, fontsize=8)
                            ypos -= 0.017
                        ax.text(0.04, ypos, f'Measures ({len(measures)})', fontsize=9, weight='bold')
                        ypos -= 0.018
                    ax.text(0.06, ypos, mname, fontsize=8, style='italic')
                    ypos -= 0.017
                    for line in wrap_text(expr, 100):
                        ax.text(0.08, ypos, line, fontsize=7, family='monospace')
                        ypos -= 0.015
                    ypos -= 0.010
            else:
                ax.text(0.04, ypos, 'Measures: None', fontsize=8)
                ypos -= 0.017
            ypos -= 0.02
        save_fig(fig)

    # Assemble PDF
    if not page_paths:
        raise RuntimeError('No pages generated.')
    images = [Image.open(p).convert('RGB') for p in page_paths]
    first, rest = images[0], images[1:]
    first.save(output_path, save_all=True, append_images=rest)
    # cleanup temporary files
    for p in page_paths:
        try:
            os.remove(p)
        except Exception:
            pass


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Generate a polished PDF report for a semantic model JSON file. '
                    'Optionally include M‑code analysis to produce a unified report.'
    )
    parser.add_argument('--input', '-i', required=True, help='Path to the semantic model JSON file.')
    parser.add_argument('--output', '-o', help='Path to the output PDF file.')
    parser.add_argument('--include-mcode', action='store_true',
                        help='Include M‑code analysis (unified report) by extracting and analysing M‑code partitions.')
    args = parser.parse_args()

    input_path = args.input
    if not os.path.exists(input_path):
        print(f'❌ Input file not found: {input_path}')
        return

    # Choose the appropriate analyzer
    if args.include_mcode:
        try:
            from unified_powerbi_analyzer import UnifiedPowerBIAnalyzer
        except Exception as exc:
            print(f'❌ Failed to import unified analyzer: {exc}')
            return
        analyzer = UnifiedPowerBIAnalyzer()
        report = analyzer.analyze_model(input_path)
        if 'error' in report:
            print(f"❌ {report['error']}")
            return
        # Determine output filename if not provided
        default_name = f"{os.path.splitext(os.path.basename(input_path))[0]}_unified_report.pdf"
    else:
        reviewer = JSONSemanticModelReviewer()
        report = reviewer.analyze_json(input_path)
        if 'error' in report:
            print(f"❌ {report['error']}")
            return
        default_name = f"{os.path.splitext(os.path.basename(input_path))[0]}_polished_report.pdf"

    # Load full model for documentation
    with open(input_path, 'r', encoding='utf-8') as f:
        model = json.load(f)

    output_path = args.output or default_name
    try:
        generate_polished_pdf(report, model, output_path)
        print(f'✅ Report generated: {output_path}')
    except Exception as exc:
        print(f'❌ Failed to generate report: {exc}')


if __name__ == '__main__':
    main()