using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Linq;
using System.Collections.Generic;
using Azure.Core;
using Azure.Identity;

public static class FabricDownload
{
    static readonly Uri FabricScope = new("https://api.fabric.microsoft.com/.default");

    // Shapes of the Fabric response (minimal)
    record Part(string Path, string PayloadType, string? Payload);
    record Result(string? DisplayName, Part[] Parts);

    public static async Task<string> DownloadTmdlAsync(string workspaceId, string semanticModelId, string outDir)
    {
        Directory.CreateDirectory(outDir);
        try
        {
            // 1) Acquire token (works with az login, SP, or managed identity)
            var cred = new DefaultAzureCredential(new DefaultAzureCredentialOptions { ExcludeInteractiveBrowserCredential = false });
            AccessToken token = await cred.GetTokenAsync(new TokenRequestContext(new[] { FabricScope.ToString() }));

            using var http = new HttpClient();
            http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token.Token);
            http.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

            // 2) Kick off async export (explicitly request TMDL)
            var url = $"https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/semanticModels/{semanticModelId}/getDefinition?format=TMDL";
            using var startReq = new HttpRequestMessage(HttpMethod.Post, url)
            {
                // Some gateways want a content-type even for empty bodies
                Content = new StringContent(string.Empty, Encoding.UTF8, "application/json")
            };
            startReq.Headers.TryAddWithoutValidation("Prefer", "respond-async");
            startReq.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

            using var startResp = await http.SendAsync(startReq);
            if (startResp.StatusCode != System.Net.HttpStatusCode.Accepted && !startResp.IsSuccessStatusCode)
            {
                var body = await startResp.Content.ReadAsStringAsync();
                throw new InvalidOperationException($"getDefinition start failed: {(int)startResp.StatusCode} {body}");
            }

            // 3) Poll until done (prefer Operation-Location header)
            var opLocation = startResp.Headers.TryGetValues("Operation-Location", out var opVals)
                ? opVals.First()
                : startResp.Headers.Location?.ToString()
                  ?? throw new InvalidOperationException("Operation location header not found.");

            for (;;)
            {
                await Task.Delay(TimeSpan.FromSeconds(1.5));
                using var opResp = await http.GetAsync(opLocation);
                if (opResp.StatusCode == System.Net.HttpStatusCode.Accepted) continue;

                if (!opResp.IsSuccessStatusCode)
                {
                    var b = await opResp.Content.ReadAsStringAsync();
                    throw new InvalidOperationException($"Operation poll failed: {(int)opResp.StatusCode} {b}");
                }
                // Some operations return 200 with a body containing a status field. If present and not completed, keep polling.
                try
                {
                    var opJson = await opResp.Content.ReadAsStringAsync();
                    using var opDoc = JsonDocument.Parse(opJson);
                    if (TryGetStringIgnoreCase(opDoc.RootElement, "status") is string status)
                    {
                        var s = status.ToLowerInvariant();
                        if (s is "running" or "notstarted" or "inprogress" or "queued") continue;
                        // treat succeeded/completed/success as done
                    }
                }
                catch { /* if not JSON or no status, assume it's done */ }
                break;
            }

            // 4) Fetch the result JSON (some ops return it at .../result, some at op URL)
            var resultUrl = opLocation.TrimEnd('/') + "/result";
            using var resResp = await http.GetAsync(resultUrl);

            string json;
            if (resResp.IsSuccessStatusCode)
            {
                json = await resResp.Content.ReadAsStringAsync();
            }
            else
            {
                using var resResp2 = await http.GetAsync(opLocation);
                if (!resResp2.IsSuccessStatusCode)
                {
                    var b1 = await resResp.Content.ReadAsStringAsync();
                    var b2 = await resResp2.Content.ReadAsStringAsync();
                    throw new InvalidOperationException($"Result fetch failed: first {(int)resResp.StatusCode} {b1}; fallback {(int)resResp2.StatusCode} {b2}");
                }
                json = await resResp2.Content.ReadAsStringAsync();
            }

            // --- Save raw result for inspection
            var debugPath = Path.Combine(outDir, "definition.raw.json");
            await File.WriteAllTextAsync(debugPath, json, Encoding.UTF8);

            // Accept multiple response shapes
            var result = JsonSerializer.Deserialize<Result>(json, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
            if (result is null || result.Parts is null)
            {
                // Try wrapper shape: { "definition": { parts: [...] } }
                try
                {
                    var wrapper = JsonSerializer.Deserialize<DefinitionWrapper>(json, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
                    if (wrapper?.Definition?.Parts is not null)
                    {
                        result = wrapper.Definition;
                    }
                }
                catch { /* ignore */ }
            }

            if (result is null || result.Parts is null)
            {
                // Last resort: search JSON recursively for any 'parts' array
                if (TryExtractPartsFromJson(json, out var extractedParts))
                {
                    result = new Result(null, extractedParts.ToArray());
                }
            }

            if (result is null || result.Parts is null)
                throw new InvalidOperationException("Fabric returned no 'parts' in result. Check IDs/permissions/workspace type.");

            // 5) Handle InlineBase64, Inline, and Link/FileUrl
            int written = 0;
            foreach (var part in result.Parts ?? Array.Empty<Part>())
            {
                if (string.IsNullOrEmpty(part.Path)) continue;

                string? text = null;

                // 1) InlineBase64 → decode to UTF-8 text
                if (part.PayloadType.Equals("InlineBase64", StringComparison.OrdinalIgnoreCase) && !string.IsNullOrEmpty(part.Payload))
                {
                    var bytes = Convert.FromBase64String(part.Payload);
                    text = Encoding.UTF8.GetString(bytes);
                }
                // 2) Inline → payload already contains the text
                else if (part.PayloadType.Equals("Inline", StringComparison.OrdinalIgnoreCase) && !string.IsNullOrEmpty(part.Payload))
                {
                    text = part.Payload;
                }
                // 3) Link/FileUrl → download then treat as text (many TMDL parts are plain text files)
                else if ((part.PayloadType.Equals("Link", StringComparison.OrdinalIgnoreCase) ||
                          part.PayloadType.Equals("FileUrl", StringComparison.OrdinalIgnoreCase)) &&
                          !string.IsNullOrEmpty(part.Payload))
                {
                    // 'Payload' contains a URL
                    text = await http.GetStringAsync(part.Payload);
                }

                if (text is null) continue;

                var fullPath = Path.Combine(outDir, part.Path.TrimStart('/', '\\'));
                Directory.CreateDirectory(Path.GetDirectoryName(fullPath)!);
                await File.WriteAllTextAsync(fullPath, text, Encoding.UTF8);
                written++;
            }

            if (written == 0)
            {
                // show first few part descriptors to help debug
                var sample = string.Join(", ", (result.Parts ?? Array.Empty<Part>()).Take(5).Select(p => $"{p.Path}:{p.PayloadType}"));
                throw new InvalidOperationException($"No writable parts. Parts seen: [{sample}]. See {debugPath} for the full response.");
            }

            Console.Error.WriteLine($"Wrote {written} files to {Path.GetFullPath(outDir)}");
            return outDir;
        }
        catch (Exception ex)
        {
            // Leave an error file so an empty folder isn't misleading
            var errPath = Path.Combine(outDir, "error.txt");
            await File.WriteAllTextAsync(errPath, ex.ToString(), Encoding.UTF8);
            throw;
        }
    }

    // Wrapper shape used by some Fabric responses
    private record DefinitionWrapper(Result? Definition);

    private static bool TryExtractPartsFromJson(string json, out List<Part> parts)
    {
        parts = new List<Part>();
        try
        {
            using var doc = JsonDocument.Parse(json);
            if (TryFindPartsArray(doc.RootElement, out var partsArray))
            {
                foreach (var item in partsArray.EnumerateArray())
                {
                    string? path = TryGetStringIgnoreCase(item, "path");
                    string? payloadType = TryGetStringIgnoreCase(item, "payloadType");
                    string? payload = TryGetStringIgnoreCase(item, "payload");
                    if (!string.IsNullOrEmpty(path) && !string.IsNullOrEmpty(payloadType))
                    {
                        parts.Add(new Part(path!, payloadType!, payload));
                    }
                }
                return parts.Count > 0;
            }
        }
        catch
        {
            // ignore and return false
        }
        return false;
    }

    private static bool TryFindPartsArray(JsonElement element, out JsonElement partsArray)
    {
        partsArray = default;
        if (element.ValueKind == JsonValueKind.Array)
        {
            return false;
        }
        if (element.ValueKind == JsonValueKind.Object)
        {
            foreach (var prop in element.EnumerateObject())
            {
                if (prop.NameEquals("parts") && prop.Value.ValueKind == JsonValueKind.Array)
                {
                    partsArray = prop.Value;
                    return true;
                }
                if (TryFindPartsArray(prop.Value, out partsArray))
                {
                    return true;
                }
            }
        }
        return false;
    }

    private static string? TryGetStringIgnoreCase(JsonElement element, string name)
    {
        if (element.ValueKind != JsonValueKind.Object) return null;
        foreach (var prop in element.EnumerateObject())
        {
            if (prop.Name.Equals(name, StringComparison.OrdinalIgnoreCase))
            {
                return prop.Value.ValueKind == JsonValueKind.String ? prop.Value.GetString() : prop.Value.ToString();
            }
        }
        return null;
    }
}




