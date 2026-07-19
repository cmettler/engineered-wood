// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using DeltaSnapshot = EngineeredWood.DeltaLake.Snapshot.Snapshot;
using EngineeredWood.IO;

namespace EngineeredWood.DeltaLake.Table.Vacuum;

/// <summary>
/// Identifies and deletes unreferenced data files that are older than
/// the retention period.
/// </summary>
internal static class VacuumExecutor
{
    /// <summary>
    /// Finds unreferenced files and optionally deletes them.
    ///
    /// <para>A non-dry-run vacuum writes the Spark-parity <c>VACUUM START</c> / <c>VACUUM END</c>
    /// commitInfo-only commits around the physical deletes, so the operation is visible in the table
    /// history (auditability — and other engines can see WHY older versions stopped being physically
    /// readable). A dry run writes nothing.</para>
    /// </summary>
    public static async ValueTask<VacuumResult> ExecuteAsync(
        ITableFileSystem fs,
        TransactionLog log,
        DeltaSnapshot snapshot,
        TimeSpan retentionPeriod,
        bool dryRun,
        CancellationToken cancellationToken)
    {
        // Collect all referenced file paths from the current snapshot. add.path is URL-encoded, so decode to
        // the on-disk name before comparing against the physical directory listing (below).
        var referencedPaths = new HashSet<string>(StringComparer.Ordinal);
        foreach (var addFile in snapshot.ActiveFiles.Values)
            referencedPaths.Add(EngineeredWood.DeltaLake.DeltaPath.Decode(addFile.Path));

        // List all data files in the table directory
        // Data files are Parquet files not in _delta_log/
        var allFiles = new List<TableFileInfo>();
        await foreach (var file in fs.ListAsync("", cancellationToken).ConfigureAwait(false))
        {
            // Skip log files and non-parquet files
            if (file.Path.StartsWith("_delta_log/", StringComparison.Ordinal) ||
                file.Path.StartsWith("_delta_log\\", StringComparison.Ordinal))
                continue;

            if (!file.Path.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase))
                continue;

            allFiles.Add(file);
        }

        // Find unreferenced files older than the retention period
        var cutoff = DateTimeOffset.UtcNow - retentionPeriod;
        var filesToDelete = new List<string>();
        long bytesToDelete = 0;

        foreach (var file in allFiles)
        {
            if (!referencedPaths.Contains(file.Path) && file.LastModified < cutoff)
            {
                filesToDelete.Add(file.Path);
                bytesToDelete += file.Size;
            }
        }

        int deleted = 0;
        if (!dryRun)
        {
            long startVersion = await WriteCommitInfoAsync(
                log, snapshot, "VACUUM START",
                new Dictionary<string, JsonElement>
                {
                    ["operationParameters"] = ParseJson(
                        $"{{\"retentionDurationMillis\":{(long)retentionPeriod.TotalMilliseconds}}}"),
                    ["operationMetrics"] = ParseJson(
                        $"{{\"numFilesToDelete\":\"{filesToDelete.Count}\",\"sizeOfDataToDelete\":\"{bytesToDelete}\"}}"),
                },
                firstCandidateVersion: snapshot.Version + 1,
                cancellationToken).ConfigureAwait(false);

            foreach (string path in filesToDelete)
            {
                await fs.DeleteAsync(path, cancellationToken).ConfigureAwait(false);
                deleted++;
            }

            await WriteCommitInfoAsync(
                log, snapshot, "VACUUM END",
                new Dictionary<string, JsonElement>
                {
                    ["operationParameters"] = ParseJson("{\"status\":\"COMPLETED\"}"),
                    ["operationMetrics"] = ParseJson(
                        $"{{\"numDeletedFiles\":\"{deleted}\",\"numVacuumedDirectories\":\"0\"}}"),
                },
                firstCandidateVersion: startVersion + 1,
                cancellationToken).ConfigureAwait(false);
        }

        return new VacuumResult
        {
            FilesToDelete = filesToDelete,
            FilesDeleted = deleted,
        };
    }

    // Writes a commitInfo-only commit, retrying past versions a concurrent writer takes (the commit carries
    // no data actions, so re-attempting at the next version is always safe). Returns the committed version.
    private static async ValueTask<long> WriteCommitInfoAsync(
        TransactionLog log, DeltaSnapshot snapshot, string operation,
        IDictionary<string, JsonElement> additionalValues, long firstCandidateVersion,
        CancellationToken cancellationToken)
    {
        var commitInfo = InCommitTimestamp.CreateCommitInfo(
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), operation, additionalValues,
            includeInCommitTimestamp: InCommitTimestamp.IsEnabled(snapshot.Metadata.Configuration));
        IReadOnlyList<DeltaAction> actions = new DeltaAction[] { commitInfo };

        const int maxAttempts = 16;
        long version = firstCandidateVersion;
        for (int attempt = 0; ; attempt++, version++)
        {
            try
            {
                await log.WriteCommitAsync(version, actions, cancellationToken).ConfigureAwait(false);
                return version;
            }
            catch (DeltaConflictException) when (attempt + 1 < maxAttempts)
            {
                // A concurrent writer took this version — try the next one.
            }
        }
    }

    private static JsonElement ParseJson(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.Clone();
    }
}
