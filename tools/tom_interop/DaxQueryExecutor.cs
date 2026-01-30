using System.Data;
using System.Text.Json;
using Microsoft.AnalysisServices.AdomdClient;

/// <summary>
/// Executes DAX queries against semantic models via XMLA endpoint.
/// Supports INFO.VIEW.* functions for metadata extraction.
/// </summary>
public static class DaxQueryExecutor
{
    /// <summary>
    /// Execute a DAX query and return results as JSON.
    /// </summary>
    public static string ExecuteQueryAsJson(string connectionString, string daxQuery)
    {
        using var connection = new AdomdConnection(connectionString);
        connection.Open();

        using var command = new AdomdCommand(daxQuery, connection);
        using var reader = command.ExecuteReader();

        var results = new List<Dictionary<string, object?>>();
        var columns = new List<string>();

        // Get column names
        for (int i = 0; i < reader.FieldCount; i++)
        {
            columns.Add(reader.GetName(i));
        }

        // Read all rows
        while (reader.Read())
        {
            var row = new Dictionary<string, object?>();
            for (int i = 0; i < reader.FieldCount; i++)
            {
                var value = reader.GetValue(i);
                row[columns[i]] = value == DBNull.Value ? null : value;
            }
            results.Add(row);
        }

        return JsonSerializer.Serialize(new
        {
            columns,
            rows = results,
            rowCount = results.Count
        }, new JsonSerializerOptions { WriteIndented = true });
    }

    /// <summary>
    /// Get all DAX documentation (measures, calculated columns, calculated tables).
    /// Uses the same DAX query pattern Ryan provided.
    /// </summary>
    public static string GetDaxDocumentation(string connectionString)
    {
        var query = @"
            VAR __measures =
                SELECTCOLUMNS (
                    INFO.VIEW.MEASURES (),
                    ""Item"", [Name],
                    ""Table"", [Table],
                    ""DAX"", [Expression],
                    ""Description"", [Description],
                    ""FormatString"", [Format String],
                    ""IsHidden"", [Is Hidden],
                    ""Type"", ""Measure""
                )
            VAR __columns =
                SELECTCOLUMNS (
                    FILTER ( INFO.VIEW.COLUMNS (), [Type] = ""Calculated"" ),
                    ""Item"", [Name],
                    ""Table"", [Table],
                    ""DAX"", [Expression],
                    ""Description"", [Description],
                    ""FormatString"", """",
                    ""IsHidden"", [Is Hidden],
                    ""Type"", ""Calculated Column""
                )
            VAR __tables =
                SELECTCOLUMNS (
                    FILTER ( INFO.VIEW.TABLES (), NOT ISBLANK ( [Expression] ) ),
                    ""Item"", [Name],
                    ""Table"", [Name],
                    ""DAX"", [Expression],
                    ""Description"", [Description],
                    ""FormatString"", """",
                    ""IsHidden"", [Is Hidden],
                    ""Type"", ""Calculated Table""
                )
            RETURN
                UNION ( __measures, __columns, __tables )";

        return ExecuteQueryAsJson(connectionString, "EVALUATE " + query);
    }

    /// <summary>
    /// Get full model metadata including relationships, hierarchies, etc.
    /// </summary>
    public static string GetFullModelMetadata(string connectionString)
    {
        var metadata = new Dictionary<string, object>();

        // Tables
        var tablesQuery = @"
            EVALUATE
            SELECTCOLUMNS(
                INFO.VIEW.TABLES(),
                ""Name"", [Name],
                ""Description"", [Description],
                ""IsHidden"", [Is Hidden],
                ""StorageMode"", [Storage Mode],
                ""Expression"", [Expression],
                ""DataCategory"", [Data Category]
            )";
        metadata["tables"] = JsonSerializer.Deserialize<object>(ExecuteQueryAsJson(connectionString, tablesQuery));

        // Columns
        var columnsQuery = @"
            EVALUATE
            SELECTCOLUMNS(
                INFO.VIEW.COLUMNS(),
                ""Table"", [Table],
                ""Name"", [Name],
                ""Description"", [Description],
                ""DataType"", [Data Type],
                ""IsHidden"", [Is Hidden],
                ""Expression"", [Expression],
                ""DataCategory"", [Data Category],
                ""SortByColumn"", [Sort By Column],
                ""IsKey"", [Is Key],
                ""SummarizeBy"", [Summarize By]
            )";
        metadata["columns"] = JsonSerializer.Deserialize<object>(ExecuteQueryAsJson(connectionString, columnsQuery));

        // Measures
        var measuresQuery = @"
            EVALUATE
            SELECTCOLUMNS(
                INFO.VIEW.MEASURES(),
                ""Table"", [Table],
                ""Name"", [Name],
                ""Description"", [Description],
                ""Expression"", [Expression],
                ""FormatString"", [Format String],
                ""IsHidden"", [Is Hidden],
                ""DisplayFolder"", [Display Folder]
            )";
        metadata["measures"] = JsonSerializer.Deserialize<object>(ExecuteQueryAsJson(connectionString, measuresQuery));

        // Relationships
        var relationshipsQuery = @"
            EVALUATE
            SELECTCOLUMNS(
                INFO.VIEW.RELATIONSHIPS(),
                ""FromTable"", [From Table],
                ""FromColumn"", [From Column],
                ""ToTable"", [To Table],
                ""ToColumn"", [To Column],
                ""IsActive"", [Is Active],
                ""CrossFilterDirection"", [Cross Filter Direction],
                ""Cardinality"", [Cardinality],
                ""SecurityFilterDirection"", [Security Filter Direction]
            )";
        metadata["relationships"] = JsonSerializer.Deserialize<object>(ExecuteQueryAsJson(connectionString, relationshipsQuery));

        // Hierarchies
        var hierarchiesQuery = @"
            EVALUATE
            SELECTCOLUMNS(
                INFO.VIEW.HIERARCHIES(),
                ""Table"", [Table],
                ""Name"", [Name],
                ""Description"", [Description],
                ""IsHidden"", [Is Hidden]
            )";
        metadata["hierarchies"] = JsonSerializer.Deserialize<object>(ExecuteQueryAsJson(connectionString, hierarchiesQuery));

        // Calculation dependencies (for measure dependency analysis)
        var dependenciesQuery = @"
            EVALUATE
            INFO.CALCDEPENDENCY()";
        try
        {
            metadata["dependencies"] = JsonSerializer.Deserialize<object>(ExecuteQueryAsJson(connectionString, dependenciesQuery));
        }
        catch
        {
            // Some models may not support this
            metadata["dependencies"] = null;
        }

        return JsonSerializer.Serialize(metadata, new JsonSerializerOptions { WriteIndented = true });
    }

    /// <summary>
    /// Build connection string for XMLA endpoint.
    /// </summary>
    public static string BuildConnectionString(string workspaceName, string datasetName, string? accessToken = null)
    {
        var server = $"powerbi://api.powerbi.com/v1.0/myorg/{workspaceName}";
        
        if (!string.IsNullOrEmpty(accessToken))
        {
            return $"Data Source={server};Initial Catalog={datasetName};Password={accessToken}";
        }
        
        // Use interactive auth if no token
        return $"Data Source={server};Initial Catalog={datasetName}";
    }
}
