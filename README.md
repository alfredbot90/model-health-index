# Power BI Model Health Analyzer V7

**Comprehensive Power BI semantic model analysis platform** combining the best capabilities from V3, V4, V5, and Fabric Model Reader.

## 🎯 Overview

V7 is an enterprise-grade Power BI model analysis platform that provides:
- **Comprehensive Model Health Scoring** (100-point system with A-F grading)
- **Multi-dimensional Analysis** (Performance, Design, Relationships, DAX, M-Code)
- **TMDL Native Support** (Download, parse, analyze via TOM)
- **Report Structure Analysis** (PBIX/PBIR extraction and binding analysis)
- **MCP Server Integration** (Query models via Claude Code/LLM tools)
- **Multiple Output Formats** (JSON, PDF, Interactive Web UI)
- **RESTful API** (FastAPI endpoints for programmatic access)

## 🏗️ Architecture

```
V7/
├── core/                    # Core business logic
│   ├── auth.py             # Unified authentication (Azure CLI, token, keyring)
│   ├── fabric_client.py    # Fabric API client
│   ├── powerbi_client.py   # Power BI REST API client
│   └── orchestrator.py     # Main analysis orchestration
│
├── analyzers/              # Analysis engines
│   ├── semantic_analyzer.py      # Semantic model analysis (from V3/V5)
│   ├── mcode_analyzer.py         # M-Code performance analysis (from V3/V5)
│   ├── dax_analyzer.py           # DAX complexity & best practices
│   ├── relationship_analyzer.py  # Relationship pattern detection
│   ├── report_analyzer.py        # PBIX/PBIR structure analysis (from V4)
│   └── unified_analyzer.py       # Combined multi-dimensional analysis
│
├── tools/                  # TMDL & utility tools
│   ├── tmdl_client.py     # TMDL download orchestration (from V5)
│   ├── tmdl_parser.py     # TMDL parsing utilities
│   ├── tom_interop/       # .NET TOM interop layer (from V4)
│   │   ├── TmdlTools.exe
│   │   └── wrapper.py
│   ├── dataflow_client.py # Dataflow analysis (from V5)
│   └── pbix_extractor.py  # PBIX structure extraction (from V4)
│
├── api/                    # API layer
│   ├── main.py            # FastAPI application
│   ├── endpoints/
│   │   ├── health.py      # Health check endpoints
│   │   ├── analysis.py    # Analysis endpoints
│   │   ├── models.py      # Model management
│   │   └── reports.py     # Report generation
│   └── models/            # Pydantic request/response models
│
├── mcp/                    # MCP Server (from Fabric Model Reader)
│   ├── server.py          # FastMCP server implementation
│   ├── tools.py           # MCP tool definitions
│   └── prompts.py         # MCP prompt templates
│
├── reports/               # Report generators
│   ├── json_generator.py  # Structured JSON reports (from V3)
│   ├── pdf_generator.py   # Professional PDF reports (from V3)
│   ├── html_generator.py  # Interactive HTML reports
│   └── templates/         # Report templates
│
├── config/                # Configuration
│   ├── settings.py        # Application settings
│   ├── scoring_weights.py # Scoring configuration
│   └── defaults.json      # Default configurations
│
├── tests/                 # Test suite
│   ├── test_analyzers.py
│   ├── test_api.py
│   └── fixtures/
│
└── docs/                  # Documentation
    ├── api.md             # API documentation
    ├── scoring.md         # Scoring methodology
    ├── setup.md           # Setup guide
    └── examples/          # Usage examples
```

## 🚀 Quick Start

### Prerequisites
- Python 3.10+
- .NET 8.0 SDK (for TMDL tools)
- Azure CLI (for authentication)
- Power BI Premium/Fabric capacity (for XMLA/TMDL access)

### Installation

```powershell
# Create virtual environment
python -m venv .venv
.venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Build TMDL tools (one-time)
cd tools/tom_interop
dotnet publish -c Release
```

### Authentication

```powershell
# Option 1: Azure CLI (recommended)
az login
az account get-access-token --resource https://analysis.windows.net/powerbi/api

# Option 2: Environment variable
$env:POWERBI_TOKEN = "your-token-here"

# Option 3: Keyring (secure storage)
python -c "import keyring; keyring.set_password('powerbi', 'token', 'your-token-here')"
```

### Basic Usage

```python
from core.orchestrator import ModelHealthOrchestrator
from analyzers.unified_analyzer import UnifiedAnalyzer

# Initialize orchestrator
orchestrator = ModelHealthOrchestrator(
    workspace_id="your-workspace-id"
)

# Analyze a semantic model
result = await orchestrator.analyze_model(
    model_id="your-model-id",
    include_tmdl=True,
    include_reports=True,
    include_dataflows=True
)

# Generate reports
from reports import JSONGenerator, PDFGenerator

json_report = JSONGenerator().generate(result)
pdf_report = PDFGenerator().generate(result)
```

### API Server

```powershell
# From the repository root
cd .\V7

# Start FastAPI server (Option A)
uvicorn api.main:app --reload --port 8000

# Access interactive docs
# http://localhost:8000/docs
```

### PDF Reports

```powershell
# From the repository root
cd .\V7

# Generate PDFs (Option A)
python -m reports.pdf_generator
```

### MCP Server (for Claude Code)

```powershell
# Start MCP server
python -m mcp.server

# Configure in Claude Code settings:
{
  "mcpServers": {
    "powerbi": {
      "command": "python",
      "args": ["-m", "mcp.server"],
      "cwd": "C:\\path\\to\\V7"
    }
  }
}
```

## 📊 Analysis Capabilities

### 1. Semantic Model Analysis (from V3)
- ✅ 100-point health scoring with weighted categories
- ✅ Bidirectional filter detection (performance killers)
- ✅ Table proliferation analysis (Budget2020, Budget2021...)
- ✅ Many-to-many relationship warnings
- ✅ Calculated column vs measure recommendations
- ✅ Naming convention validation
- ✅ Data type optimization
- ✅ Role-playing dimension detection
- ✅ RLS implementation checks

### 2. M-Code Analysis (from V3/V5)
- ✅ Query folding detection
- ✅ Expensive operations identification
- ✅ Dataflow integration analysis
- ✅ Parameter usage validation
- ✅ Performance optimization suggestions

### 3. DAX Analysis
- ✅ Complexity scoring (Low/Medium/High/Very High)
- ✅ Iterator without CALCULATE detection
- ✅ Measure dependency mapping
- ✅ Best practice validation
- ✅ Documentation coverage analysis

### 4. TMDL Support (from V4/V5)
- ✅ Native TMDL download via TOM
- ✅ Full model metadata extraction
- ✅ Relationship mapping
- ✅ Expression analysis
- ✅ Validation utilities

#### Exporting TMDL definitions (mirrors V4)

```powershell
# From repo root
cd .\V7

# Export all semantic models' TMDL into tmdl_exports/<ModelName_Id>/definition
python -c "from tools.tmdl_client import TmdlClient; print(TmdlClient().export_workspace_models('<workspace-guid>', 'tmdl_exports'))"
```

### 5. Report Analysis (from V4)
- ✅ PBIX structure extraction
- ✅ Visual binding analysis
- ✅ Measure usage tracking
- ✅ Field reference mapping
- ✅ Report-to-model dependency graph

### 6. MCP Integration (from Fabric Model Reader)
- ✅ Natural language model queries
- ✅ DAX execution via XMLA
- ✅ Measure discovery
- ✅ Real-time data access

## 🎯 Scoring System

### Category Weights (Customizable)
- **Performance**: 25% (Relationships, calculated columns, aggregations)
- **Design**: 20% (Table structure, data types, normalization)
- **Relationships**: 20% (Cardinality, filter direction, role-playing dims)
- **Measures & DAX**: 15% (Complexity, best practices, documentation)
- **M-Code Performance**: 10% (Query folding, dataflows, optimization)
- **Best Practices**: 5% (Hierarchies, measure tables, RLS)
- **Naming & Documentation**: 5% (Consistency, comments, descriptions)

### Severity Levels
- **Critical** (1.5x multiplier): Immediate performance/data quality issues
- **High** (1.0x): Significant issues requiring attention
- **Medium** (0.7x): Moderate issues, recommended fixes
- **Low** (0.3x): Minor improvements
- **Info** (0x): Informational, no penalty

### Grading Scale
- **A (90-100)**: Excellent - Production ready
- **B (80-89)**: Good - Minor improvements needed
- **C (70-79)**: Average - Notable issues to address
- **D (60-69)**: Below Average - Significant refactoring required
- **F (0-59)**: Poor - Major redesign needed

## 🔌 API Endpoints

### Analysis
```
POST /api/v1/analyze/model
POST /api/v1/analyze/workspace
GET  /api/v1/analysis/{analysis_id}
```

### Models
```
GET  /api/v1/models
GET  /api/v1/models/{model_id}
GET  /api/v1/models/{model_id}/tmdl
```

### Reports
```
GET  /api/v1/reports/{analysis_id}/json
GET  /api/v1/reports/{analysis_id}/pdf
GET  /api/v1/reports/{analysis_id}/html
```

### Health
```
GET  /api/v1/health
GET  /api/v1/health/tmdl-status
```

## 📈 What's New in V7

### From V3 (fabricReader)
✅ Enhanced semantic model analyzer with comprehensive best practice checks
✅ M-Code analyzer with query folding detection
✅ Unified analyzer combining all dimensions
✅ Professional PDF report generation
✅ JSON report generation with full detail

### From V4 (TMDL Tools)
✅ .NET TOM interop for native TMDL support
✅ TMDL download, export, validate, copy utilities
✅ PBIX structure extractor with visual binding analysis
✅ Knowledge base builder for RAG/search

### From V5 (API + UI)
✅ Orchestration API for end-to-end workflows
✅ TMDL client wrapper for Python
✅ Fabric report downloader
✅ Dataflow integration
✅ Caching layer for performance

### From Fabric Model Reader
✅ MCP server for LLM integration
✅ Multi-method authentication (Azure CLI, token, keyring)
✅ DAX query execution via XMLA
✅ Natural language model exploration

## 🔧 Configuration

```python
# config/scoring_weights.py
SCORING_WEIGHTS = {
    'Performance': 0.25,
    'Design': 0.20,
    'Relationships': 0.20,
    'Measures': 0.15,
    'M-Code': 0.10,
    'Best Practices': 0.05,
    'Documentation': 0.05
}

# Customize for your organization
SEVERITY_MULTIPLIERS = {
    'Critical': 1.5,
    'High': 1.0,
    'Medium': 0.7,
    'Low': 0.3,
    'Info': 0
}
```

## 🧪 Testing

```powershell
# Run all tests
pytest tests/ -v

# Run specific test suite
pytest tests/test_analyzers.py -v

# Generate coverage report
pytest --cov=. --cov-report=html
```

## 📝 Examples

See `docs/examples/` for:
- Basic model analysis
- Workspace-wide health dashboard
- Custom scoring configurations
- Report generation workflows
- MCP integration examples
- API usage patterns

## 🤝 Contributing

This is an internal tool combining proven patterns from V3-V5. Follow the established patterns:
- Analyzers inherit from base classes
- Use dataclasses for structured data
- Include type hints
- Write docstrings for public APIs
- Add tests for new features

## 📄 License

Internal use only.

## 🙏 Acknowledgments

Built by combining the best features from:
- **V3_Test/fabricReader**: Core analysis engine and scoring
- **V4**: TMDL tools and report structure extraction
- **V5**: API orchestration and caching
- **Fabric_Model_Reader**: MCP server integration

---

**V7: The ultimate Power BI model health platform** 🚀
