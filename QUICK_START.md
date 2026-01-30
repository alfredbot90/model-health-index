# V7 Quick Start Guide

## 🚀 5-Minute Setup

### 1. Install & Authenticate

```powershell
# Install dependencies
pip install -r requirements.txt

# Authenticate with Azure CLI
az login
```

### 2. Analyze a Model (Python)

```python
import asyncio
from core.orchestrator import ModelHealthOrchestrator

async def main():
    orchestrator = ModelHealthOrchestrator(
        workspace_id="your-workspace-guid"
    )

    result = await orchestrator.analyze_model(
        model_id="your-model-guid"
    )

    print(f"📊 Score: {result.overall_score}/100 ({result.grade})")
    print(f"⚠️  Issues: {result.total_issues}")
    print(f"💡 Top Recommendation: {result.top_recommendations[0]}")

asyncio.run(main())
```

### 3. Start API Server

```powershell
python -m uvicorn api.main:app --reload
# Visit: http://localhost:8000/docs
```

### 4. Use MCP with Claude Code

Add to Claude Code config:
```json
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

Then in Claude Code:
```
"Analyze the Sales model and show me the critical issues"
```

## 📊 What You Get

### Analysis Output
- **Overall Score**: 0-100 with A-F grade
- **Category Breakdown**: Performance, Design, Relationships, DAX, M-Code
- **Detailed Issues**: Categorized by severity (Critical/High/Medium/Low/Info)
- **Recommendations**: Actionable steps to improve model health
- **Statistics**: Tables, measures, columns, relationships count
- **Report Bindings**: Which visuals use which measures (optional)

### Best Practices Checked

✅ **Performance**
- Bidirectional filter detection
- Many-to-many relationships
- Calculated column usage
- Aggregation opportunities

✅ **Design**
- Table proliferation (Budget2020, Budget2021...)
- Data type optimization
- Naming conventions

✅ **Relationships**
- Cardinality issues
- Role-playing dimensions
- Orphaned tables

✅ **DAX**
- Measure complexity
- Iterator without CALCULATE
- Documentation coverage

✅ **M-Code**
- Query folding detection
- Performance optimization
- Dataflow integration

## 🎯 Common Use Cases

### 1. Single Model Analysis
```python
result = await orchestrator.analyze_model("model-guid")
```

### 2. Workspace Health Dashboard
```python
results = await orchestrator.analyze_workspace()
avg_score = sum(r.overall_score for r in results.values()) / len(results)
print(f"Workspace Average: {avg_score}/100")
```

### 3. Compare Two Models
```python
# Via API
curl -X POST http://localhost:8000/api/v1/compare/models?workspace_id={id}&model_id_1={id1}&model_id_2={id2}
```

### 4. Get Issues by Severity
```python
# Via API
curl http://localhost:8000/api/v1/analysis/{workspace_id}/{model_id}/issues?severity=Critical
```

### 5. Generate PDF Report
```python
# Via API
curl http://localhost:8000/api/v1/reports/{workspace_id}/{model_id}/pdf --output report.pdf
```

## 🔑 Key Features

| Feature | V7 Source | Description |
|---------|-----------|-------------|
| **TMDL Extraction** | V4/V5 | Uses .NET TOM for native TMDL download |
| **Semantic Analysis** | V3 | 100-point scoring with 14+ checks |
| **M-Code Analysis** | V3 | Query folding & performance detection |
| **DAX Analysis** | V3 | Complexity scoring & best practices |
| **Report Bindings** | V4 | Visual-to-measure dependency mapping |
| **MCP Server** | Fabric Reader | Claude Code integration |
| **REST API** | V5 | FastAPI with OpenAPI docs |
| **Orchestration** | V5 | Unified analysis workflow |

## 📈 Understanding Scores

### Grade Scale
- **A (90-100)**: Excellent - Production ready
- **B (80-89)**: Good - Minor improvements
- **C (70-79)**: Average - Notable issues
- **D (60-69)**: Below Average - Major refactoring needed
- **F (0-59)**: Poor - Complete redesign required

### Category Weights (Default)
- Performance: 25%
- Design: 20%
- Relationships: 20%
- Measures: 15%
- M-Code: 10%
- Best Practices: 5%
- Documentation: 5%

### Severity Impact
- **Critical**: 1.5x penalty multiplier
- **High**: 1.0x penalty
- **Medium**: 0.7x penalty
- **Low**: 0.3x penalty
- **Info**: No penalty (informational only)

## 🛠️ Customization

### Change Scoring Weights
```python
# config/settings.py
SCORING_WEIGHTS = {
    'Performance': 0.30,  # Increase to 30%
    'Design': 0.20,
    # ... adjust others to sum to 1.0
}
```

### Custom TMDL Tools Path
```python
orchestrator = ModelHealthOrchestrator(
    workspace_id="guid",
    tmdl_tools_path="C:\\custom\\TmdlTools.exe"
)
```

### Cache Configuration
```python
# config/settings.py
CACHE_EXPIRY_HOURS = 12  # Cache for 12 hours instead of 6
```

## 🧪 Testing

```powershell
# Quick health check
curl http://localhost:8000/health

# Test authentication
python -c "from core.auth import get_token; print(get_token()[:20])"

# List workspaces
python -c "from core.fabric_client import FabricClient; import asyncio; print(FabricClient().get_workspaces())"
```

## 📚 Next Steps

1. **Full Setup**: See [SETUP.md](SETUP.md) for detailed installation
2. **Architecture**: Read [README.md](README.md) for system overview
3. **API Docs**: Visit `/docs` after starting API server
4. **Examples**: Check `docs/examples/` for patterns

## 💡 Tips

- **Use caching**: Second analysis is instant (uses cache)
- **Parallel analysis**: API handles concurrent model analysis
- **Filter issues**: Focus on Critical/High severity first
- **Compare regularly**: Track model health over time
- **Automate**: Schedule weekly workspace analysis via API

## ⚡ Performance

- **TMDL Download**: ~30-60 seconds (first time, cached after)
- **Analysis**: ~5-15 seconds (depends on model size)
- **Report Generation**: ~2-5 seconds
- **API Response**: <1 second (cached results)

## 🆘 Common Issues

**"No authentication token"**
→ Run `az login`

**"TMDL download failed"**
→ Ensure Premium/Fabric capacity & XMLA enabled

**"Module not found"**
→ Activate venv: `.venv\Scripts\activate`

**"TmdlTools.exe not found"**
→ Build: `cd tools\tom_interop && dotnet publish -c Release`

---

**Need Help?** Check [SETUP.md](SETUP.md) for troubleshooting or review API docs at `/docs`
