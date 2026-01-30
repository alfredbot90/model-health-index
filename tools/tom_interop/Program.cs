using System.CommandLine;
using System.Text.Json;
using Microsoft.AnalysisServices.Tabular;

var root = new RootCommand("TMDL Tools (export/inspect/validate/copy)");

// ---------- helpers ----------
static void EnsureDir(string path)
{
    Directory.CreateDirectory(path);
    if (!Directory.Exists(path)) throw new IOException($"Cannot create directory: {path}");
}

static (string? c, string? t, string? s) GetSpFromEnv()
    => (Environment.GetEnvironmentVariable("PBI_SP_CLIENT_ID"),
        Environment.GetEnvironmentVariable("PBI_SP_TENANT_ID"),
        Environment.GetEnvironmentVariable("PBI_SP_CLIENT_SECRET"));

static object RelToDto(Relationship r)
{
    // Relationship is abstract; SingleColumnRelationship has From/To columns
    if (r is SingleColumnRelationship scr)
    {
        // derive many-to-one
        bool isM2O =
            scr.FromCardinality == RelationshipEndCardinality.Many &&
            scr.ToCardinality   == RelationshipEndCardinality.One;

        return new
        {
            kind = "SingleColumnRelationship",
            fromTable = scr.FromTable.Name,
            fromColumn = scr.FromColumn.Name,
            toTable = scr.ToTable.Name,
            toColumn = scr.ToColumn.Name,
            isActive = scr.IsActive,
            isManyToOne = isM2O
        };
    }

    // Fallback for other relationship types (e.g., limited support)
    return new
    {
        kind = r.GetType().Name,
        fromTable = r.FromTable.Name,
        toTable = r.ToTable.Name,
        isActive = r.IsActive
    };
}

string SerializeSummary(Model model, bool includeDax, bool includeM)
{
    var obj = new
    {
        name = model.Name,
        culture = model.Culture,
        tables = model.Tables.Select(t => new
        {
            name = t.Name,
            isHidden = t.IsHidden,
            columns = t.Columns.Select(c => new
            {
                name = c.Name,
                dataType = (c as DataColumn)?.DataType.ToString(),
                isHidden = c.IsHidden
            }),
            measures = t.Measures.Select(m => new
            {
                name = m.Name,
                isHidden = m.IsHidden,
                formatString = m.FormatString,
                expression = includeDax ? m.Expression : null
            }),
            partitions = t.Partitions.Select(p => p.Name),
            hierarchies = t.Hierarchies.Select(h => h.Name)
        }),
        relationships = model.Relationships.Select(RelToDto),
        expressions = model.Expressions.Select(e => new
        {
            name = e.Name,
            queryGroup = e.QueryGroup,
            lineageTag = e.LineageTag,
            expression = includeM ? e.Expression : null
        })
    };

    // Fully-qualify to avoid ambiguity with Tabular.Json.JsonSerializer
    return System.Text.Json.JsonSerializer.Serialize(obj, new JsonSerializerOptions { WriteIndented = true });
}

// ---------- export-xmla ----------
var exportCmd = new Command("export-xmla", "Export a dataset from a workspace (XMLA) to a TMDL folder");
var wsOpt = new Option<string>("--workspace", description: "Workspace name") { IsRequired = true };
var dsOpt = new Option<string>("--dataset", description: "Dataset (database) name") { IsRequired = true };
var outOpt = new Option<string>("--out", () => Environment.CurrentDirectory, "Output directory");
var spClientOpt = new Option<string?>("--sp-client", () => null, "Service principal client (app) ID (optional)");
var spTenantOpt = new Option<string?>("--sp-tenant", () => null, "Tenant ID (optional)");
var spSecretOpt = new Option<string?>("--sp-secret", () => null, "Service principal secret (optional)");
var fabricCmd = new Command("fabric-download", "Download a model’s TMDL via Fabric REST");
var wsIdOpt   = new Option<string>("--workspace-id", "Workspace GUID") { IsRequired = true };
var smIdOpt   = new Option<string>("--semantic-model-id", "Semantic Model (dataset) GUID") { IsRequired = true };

fabricCmd.AddOption(wsIdOpt);
fabricCmd.AddOption(smIdOpt);
fabricCmd.AddOption(outOpt);
exportCmd.AddOption(wsOpt);
exportCmd.AddOption(dsOpt);
exportCmd.AddOption(outOpt);
exportCmd.AddOption(spClientOpt);
exportCmd.AddOption(spTenantOpt);
exportCmd.AddOption(spSecretOpt);

fabricCmd.SetHandler(async (string wsId, string smId, string outDir) =>
{
    try
    {
        Directory.CreateDirectory(outDir);
        var dest = await FabricDownload.DownloadTmdlAsync(wsId, smId, outDir);
        Console.WriteLine(Path.GetFullPath(dest));
        Environment.Exit(0);
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine(ex.Message);
        Environment.Exit(1);
    }
}, wsIdOpt, smIdOpt, outOpt);

exportCmd.SetHandler((string ws, string ds, string outDir, string? spc, string? spt, string? sps) =>
{
    try
    {
        EnsureDir(outDir);
        var xmla = TmdlInterop.BuildXmlaDataSource(ws);

        var (envC, envT, envS) = GetSpFromEnv();
        spc ??= envC; spt ??= envT; sps ??= envS;

        var cs = TmdlInterop.BuildConnectionString(xmla, ds, spc, spt, sps);

        using var server = new Server();
        server.Connect(cs);

        var db = server.Databases.GetByName(ds);
        var dest = Path.Combine(outDir, $"{db.Name}-tmdl");
        Directory.CreateDirectory(dest);

        // Database-level export
        TmdlSerializer.SerializeDatabaseToFolder(db, dest);
        Console.WriteLine(dest);
        Environment.Exit(0);
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine(ex.Message);
        Environment.Exit(1);
    }
}, wsOpt, dsOpt, outOpt, spClientOpt, spTenantOpt, spSecretOpt);

// ---------- inspect ----------
var inspectCmd = new Command("inspect", "Load a TMDL folder and emit a JSON summary");
var inOpt = new Option<string>("--in", description: "TMDL folder path") { IsRequired = true };
var includeDaxOpt = new Option<bool>("--include-dax", () => false, "Include full DAX expressions");
var includeMOpt = new Option<bool>("--include-m", () => false, "Include full M queries");

inspectCmd.AddOption(inOpt);
inspectCmd.AddOption(includeDaxOpt);
inspectCmd.AddOption(includeMOpt);

inspectCmd.SetHandler((string input, bool includeDax, bool includeM) =>
{
    try
    {
        if (!Directory.Exists(input)) throw new DirectoryNotFoundException(input);

        Model model;
        if (File.Exists(Path.Combine(input, "database.tmdl")))
        {
            var db = TmdlInterop.LoadDatabaseFromFolder(input);
            model = db.Model;
        }
        else
        {
            model = TmdlInterop.LoadModelFromFolder(input);
        }

        Console.WriteLine(SerializeSummary(model, includeDax, includeM));
        Environment.Exit(0);
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine(ex.Message);
        Environment.Exit(1);
    }
}, inOpt, includeDaxOpt, includeMOpt);

// ---------- validate ----------
var validateCmd = new Command("validate", "Validate a TMDL folder (exit 0 ok)");
validateCmd.AddOption(inOpt);
validateCmd.SetHandler((string input) =>
{
    try
    {
        if (!Directory.Exists(input)) throw new DirectoryNotFoundException(input);

        if (File.Exists(Path.Combine(input, "database.tmdl")))
            _ = TmdlInterop.LoadDatabaseFromFolder(input);
        else
            _ = TmdlInterop.LoadModelFromFolder(input);

        Console.WriteLine("OK");
        Environment.Exit(0);
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine(ex.Message);
        Environment.Exit(1);
    }
}, inOpt);

// ---------- copy ----------
var copyCmd = new Command("copy", "Load from a TMDL folder and write to another TMDL folder");
var out2Opt = new Option<string>("--out", description: "Destination folder") { IsRequired = true };
copyCmd.AddOption(inOpt);
copyCmd.AddOption(out2Opt);

copyCmd.SetHandler((string input, string output) =>
{
    try
    {
        if (!Directory.Exists(input)) throw new DirectoryNotFoundException(input);
        EnsureDir(output);

        if (File.Exists(Path.Combine(input, "database.tmdl")))
        {
            var db = TmdlInterop.LoadDatabaseFromFolder(input);
            TmdlInterop.SaveDatabaseToFolder(db, output);
        }
        else
        {
            var model = TmdlInterop.LoadModelFromFolder(input);
            TmdlInterop.SaveModelToFolder(model, output);
        }

        Console.WriteLine(Path.GetFullPath(output));
        Environment.Exit(0);
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine(ex.Message);
        Environment.Exit(1);
    }
}, inOpt, out2Opt);

// ---------- query ----------
var queryCmd = new Command("query", "Execute a DAX query against a live semantic model");
var queryWsOpt = new Option<string>("--workspace", "Workspace name") { IsRequired = true };
var queryDsOpt = new Option<string>("--dataset", "Dataset name") { IsRequired = true };
var queryDaxOpt = new Option<string>("--dax", "DAX query to execute") { IsRequired = true };
var queryTokenOpt = new Option<string?>("--token", () => null, "Access token (optional, uses interactive auth if not provided)");

queryCmd.AddOption(queryWsOpt);
queryCmd.AddOption(queryDsOpt);
queryCmd.AddOption(queryDaxOpt);
queryCmd.AddOption(queryTokenOpt);

queryCmd.SetHandler((string ws, string ds, string dax, string? token) =>
{
    try
    {
        var cs = DaxQueryExecutor.BuildConnectionString(ws, ds, token);
        var result = DaxQueryExecutor.ExecuteQueryAsJson(cs, dax);
        Console.WriteLine(result);
        Environment.Exit(0);
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine(ex.Message);
        Environment.Exit(1);
    }
}, queryWsOpt, queryDsOpt, queryDaxOpt, queryTokenOpt);

// ---------- get-dax-docs ----------
var getDaxDocsCmd = new Command("get-dax-docs", "Get all DAX documentation (measures, calc columns, calc tables)");
getDaxDocsCmd.AddOption(queryWsOpt);
getDaxDocsCmd.AddOption(queryDsOpt);
getDaxDocsCmd.AddOption(queryTokenOpt);

getDaxDocsCmd.SetHandler((string ws, string ds, string? token) =>
{
    try
    {
        var cs = DaxQueryExecutor.BuildConnectionString(ws, ds, token);
        var result = DaxQueryExecutor.GetDaxDocumentation(cs);
        Console.WriteLine(result);
        Environment.Exit(0);
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine(ex.Message);
        Environment.Exit(1);
    }
}, queryWsOpt, queryDsOpt, queryTokenOpt);

// ---------- get-model-metadata ----------
var getMetadataCmd = new Command("get-model-metadata", "Get full model metadata (tables, columns, measures, relationships)");
getMetadataCmd.AddOption(queryWsOpt);
getMetadataCmd.AddOption(queryDsOpt);
getMetadataCmd.AddOption(queryTokenOpt);

getMetadataCmd.SetHandler((string ws, string ds, string? token) =>
{
    try
    {
        var cs = DaxQueryExecutor.BuildConnectionString(ws, ds, token);
        var result = DaxQueryExecutor.GetFullModelMetadata(cs);
        Console.WriteLine(result);
        Environment.Exit(0);
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine(ex.Message);
        Environment.Exit(1);
    }
}, queryWsOpt, queryDsOpt, queryTokenOpt);

// wire up
root.AddCommand(fabricCmd);
root.AddCommand(exportCmd);
root.AddCommand(inspectCmd);
root.AddCommand(validateCmd);
root.AddCommand(copyCmd);
root.AddCommand(queryCmd);
root.AddCommand(getDaxDocsCmd);
root.AddCommand(getMetadataCmd);

return await root.InvokeAsync(args);
