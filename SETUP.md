# V7 Setup Guide

## Prerequisites

1. **Python 3.10+** installed
2. **.NET 8.0 SDK** for TMDL tools
3. **Azure CLI** for authentication
4. **Power BI Premium or Fabric** capacity (for TMDL/XMLA access)

## Installation Steps

### 1. Clone and Navigate

```powershell
cd V7
```

### 2. Create Virtual Environment

```powershell
python -m venv .venv
.venv\Scripts\activate
```

### 3. Install Python Dependencies

```powershell
pip install -r requirements.txt
```

### 4. Build TMDL Tools (.NET)

```powershell
cd tools\tom_interop
dotnet publish -c Release -r win-x64 --self-contained
cd ..\..
```

This creates: `tools\tom_interop\bin\Release\net8.0\win-x64\publish\TmdlTools.exe`

### 5. Configure Authentication

Choose one method:

#### Option A: Azure CLI (Recommended)
```powershell
az login
az account set --subscription "your-subscription"
```

#### Option B: Environment Variable
```powershell
$env:POWERBI_TOKEN = "your-bearer-token"
```

#### Option C: Keyring (Secure Storage)
```powershell
pip install keyring
python -c "import keyring; keyring.set_password('powerbi', 'token', 'your-token')"
```

### 6. Verify Installation

```powershell
# Test authentication
python -c "from core.auth import get_token; print('Token:', get_token()[:20] + '...')"

# Test TMDL tools
.\tools\tom_interop\bin\Release\net8.0\win-x64\publish\TmdlTools.exe --help
```

## Usage Examples

### Python API

```python
import asyncio
from core.orchestrator import ModelHealthOrchestrator

async def analyze():
    orchestrator = ModelHealthOrchestrator(
        workspace_id="your-workspace-guid"
    )

    result = await orchestrator.analyze_model(
        model_id="your-model-guid",
        include_reports=True
    )

    print(f"Score: {result.overall_score}/100 ({result.grade})")
    print(f"Issues: {result.total_issues}")

    for issue in result.detailed_issues[:5]:
        print(f"  - [{issue['severity']}] {issue['title']}")

asyncio.run(analyze())
```

### FastAPI Server

```powershell
# Start API server
python -m uvicorn api.main:app --reload --port 8000

# Access docs
# http://localhost:8000/docs
```

API Examples:
```powershell
# List workspaces
curl http://localhost:8000/api/v1/workspaces

# Analyze model
curl -X POST http://localhost:8000/api/v1/analyze/model \
  -H "Content-Type: application/json" \
  -d '{
    "workspace_id": "guid",
    "model_id": "guid",
    "include_reports": true
  }'

# Get PDF report
curl http://localhost:8000/api/v1/reports/{workspace_id}/{model_id}/pdf --output report.pdf
```

### MCP Server (Claude Code)

1. Start MCP server:
```powershell
python -m mcp.server
```

2. Configure in Claude Code (`%APPDATA%\Claude\claude_desktop_config.json`):
```json
{
  "mcpServers": {
    "powerbi-health": {
      "command": "python",
      "args": ["-m", "mcp.server"],
      "cwd": "C:\\path\\to\\V7"
    }
  }
}
```

3. Use in Claude Code:
```
- "List all workspaces"
- "Analyze model X in workspace Y"
- "What are the critical issues in model Z?"
- "Compare models A and B"
```

## Configuration

Edit `config/settings.py` to customize:

- Scoring weights
- Severity multipliers
- Grading thresholds
- Cache settings
- Report configuration

Example customization:
```python
# config/settings.py
SCORING_WEIGHTS = {
    'Performance': 0.30,      # Increase performance weight
    'Design': 0.20,
    'Relationships': 0.15,
    'Measures': 0.15,
    'M-Code': 0.10,
    'Best Practices': 0.05,
    'Documentation': 0.05
}
```

## Troubleshooting

### Authentication Issues

**Problem**: "No authentication token available"
```powershell
# Solution: Check authentication
az login
az account get-access-token --resource https://analysis.windows.net/powerbi/api
```

### TMDL Download Fails

**Problem**: "TMDL download failed"
- Ensure workspace has Premium/Fabric capacity
- Verify XMLA endpoints are enabled
- Check user has read permissions

### Import Errors

**Problem**: ModuleNotFoundError
```powershell
# Solution: Ensure virtual environment is activated
.venv\Scripts\activate
pip install -r requirements.txt
```

### .NET Tools Not Found

**Problem**: TmdlTools.exe not found
```powershell
# Rebuild tools
cd tools\tom_interop
dotnet publish -c Release -r win-x64 --self-contained
```

## Testing

```powershell
# Run all tests
pytest tests/ -v

# Run specific test
pytest tests/test_orchestrator.py -v

# With coverage
pytest --cov=. --cov-report=html
```

## Advanced Configuration

### Custom TMDL Tools Path

```python
from core.orchestrator import ModelHealthOrchestrator

orchestrator = ModelHealthOrchestrator(
    workspace_id="guid",
    tmdl_tools_path="C:\\custom\\path\\TmdlTools.exe"
)
```

### Custom Cache Directory

```python
orchestrator = ModelHealthOrchestrator(
    workspace_id="guid",
    cache_dir="D:\\my_cache"
)
```

### Workspace-Level Analysis

```python
# Analyze all models in workspace
results = await orchestrator.analyze_workspace()

for model_id, result in results.items():
    print(f"{result.model_name}: {result.grade}")
```

## Next Steps

1. Review [README.md](README.md) for architecture overview
2. Explore [API documentation](http://localhost:8000/docs) (after starting server)
3. Check `docs/examples/` for usage patterns
4. Customize scoring in `config/settings.py`

## Support

For issues:
1. Check authentication: `python -c "from core.auth import get_token; print(get_token()[:20])"`
2. Verify TMDL tools: `.\tools\tom_interop\bin\Release\net8.0\win-x64\publish\TmdlTools.exe --help`
3. Review logs in `cache/` directory
4. Test API health: `curl http://localhost:8000/health`
