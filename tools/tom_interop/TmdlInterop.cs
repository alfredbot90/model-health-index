using Microsoft.AnalysisServices.Tabular;

public static class TmdlInterop
{
    public static Model LoadModelFromFolder(string folder)
        => TmdlSerializer.DeserializeModelFromFolder(folder);

    public static Database LoadDatabaseFromFolder(string folder)
        => TmdlSerializer.DeserializeDatabaseFromFolder(folder);

    public static void SaveModelToFolder(Model model, string folder)
        => TmdlSerializer.SerializeModelToFolder(model, folder);

    public static void SaveDatabaseToFolder(Database db, string folder)
        => TmdlSerializer.SerializeDatabaseToFolder(db, folder);

    public static string BuildXmlaDataSource(string workspaceName)
        => $"powerbi://api.powerbi.com/v1.0/myorg/{workspaceName}";

    public static string BuildConnectionString(string xmla, string databaseName,
        string? spClientId, string? spTenantId, string? spSecret)
    {
        if (!string.IsNullOrWhiteSpace(spClientId) &&
            !string.IsNullOrWhiteSpace(spTenantId) &&
            !string.IsNullOrWhiteSpace(spSecret))
        {
            return $"Data Source={xmla};Initial Catalog={databaseName};User ID=app:{spClientId}@{spTenantId};Password={spSecret};";
        }
        // user auth (interactive / device code, depending on host)
        return $"Data Source={xmla};Initial Catalog={databaseName};";
    }
}
