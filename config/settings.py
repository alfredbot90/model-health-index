"""
Configuration settings for Power BI Model Health Analyzer
"""

from typing import Dict
from pathlib import Path

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
CACHE_DIR = PROJECT_ROOT / "cache"
TMDL_DIR = PROJECT_ROOT / "tmdl_exports"
REPORTS_DIR = PROJECT_ROOT / "reports_output"

# Ensure directories exist
CACHE_DIR.mkdir(exist_ok=True)
TMDL_DIR.mkdir(exist_ok=True)
REPORTS_DIR.mkdir(exist_ok=True)

# API Configuration
API_HOST = "0.0.0.0"
API_PORT = 8000
API_VERSION = "v1"

# Authentication
POWERBI_RESOURCE = "https://analysis.windows.net/powerbi/api"
FABRIC_RESOURCE = "https://api.fabric.microsoft.com"

# Analysis Configuration
DEFAULT_INCLUDE_REPORTS = True
DEFAULT_FORCE_REFRESH = False
CACHE_EXPIRY_HOURS = 6

# Scoring weights (must sum to 1.0)
SCORING_WEIGHTS: Dict[str, float] = {
    'Performance': 0.25,
    'Design': 0.20,
    'Relationships': 0.20,
    'Measures': 0.15,
    'M-Code': 0.10,
    'Best Practices': 0.05,
    'Documentation': 0.05
}

# Severity multipliers for issue scoring
SEVERITY_MULTIPLIERS: Dict[str, float] = {
    'Critical': 1.5,
    'High': 1.0,
    'Medium': 0.7,
    'Low': 0.3,
    'Info': 0.0
}

# Grading thresholds
GRADE_THRESHOLDS = {
    'A': 90,
    'B': 80,
    'C': 70,
    'D': 60,
    'F': 0
}

# Report configuration
MAX_ISSUES_IN_SUMMARY = 10
MAX_RECOMMENDATIONS = 5

# TMDL Tools
TMDL_TOOLS_PATH = PROJECT_ROOT / "tools" / "tom_interop" / "bin" / "Release" / "net8.0" / "win-x64" / "publish" / "TmdlTools.exe"

# Logging
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
