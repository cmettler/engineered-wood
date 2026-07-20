// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.DeletionVectors;
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
    ///
    /// <para>Sweeps BOTH data files (<c>.parquet</c>) and on-disk deletion-vector files (<c>.bin</c>): a
    /// <c>.bin</c> not referenced by any active add's deletion vector is an orphan (left by a superseded
    /// DELETE/UPDATE) and is collected past retention like any other unreferenced file. Live DVs are
    /// protected by deriving each active add's on-disk DV path (the SAME derivation the reader uses —
    /// <see cref="DeletionVectorReader.DecodeUuidFromPath"/>). A table with an absolute-path (<c>p</c>)
    /// deletion vector is refused: its file cannot be proven to lie inside the swept directory.</para>
    /// </summary>
    public static async ValueTask<VacuumResult> ExecuteAsync(
        ITableFileSystem fs,
        TransactionLog log,
        DeltaSnapshot snapshot,
        TimeSpan retentionPeriod,
        bool dryRun,
        CancellationToken cancellationToken)
    {
        // Collect all referenced DATA file paths + the live on-disk DELETION-VECTOR file paths from the
        // current snapshot. Anything on disk not in these sets (and older than the cutoff) is an orphan.
        var referencedPaths = new HashSet<string>(StringComparer.Ordinal);
        var liveDvPaths = new HashSet<string>(StringComparer.Ordinal);
        foreach (var addFile in snapshot.ActiveFiles.Values)
        {
            referencedPaths.Add(addFile.Path);
            referencedPaths.Add(DeltaPath.Decode(addFile.Path)); // on-disk name (add.path is URL-encoded)

            var dv = addFile.DeletionVector;
            if (dv is null)
                continue;
            switch (dv.StorageType)
            {
                case "i": // inline — no file to protect or collect
                    break;
                case "u": // on-disk, table-relative (optional random prefix dir)
                    string rel = OnDiskDvPath(dv.PathOrInlineDv);
                    liveDvPaths.Add(rel);
                    liveDvPaths.Add(DeltaPath.Decode(rel));
                    break;
                default: // "p" absolute path (or unknown) — can't prove it's inside the swept dir
                    throw new NotSupportedException(
                        $"VACUUM: an active deletion vector uses storage type '{dv.StorageType}', whose "
                        + "file cannot be proven to lie inside the table directory; refusing to vacuum "
                        + "rather than risk deleting a live deletion vector.");
            }
        }

        // List candidate files: data (.parquet) + on-disk deletion vectors (.bin), never the log dir.
        var candidates = new List<TableFileInfo>();
        await foreach (var file in fs.ListAsync("", cancellationToken).ConfigureAwait(false))
        {
            if (file.Path.StartsWith("_delta_log/", StringComparison.Ordinal) ||
                file.Path.StartsWith("_delta_log\\", StringComparison.Ordinal))
                continue;

            bool isParquet = file.Path.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase);
            bool isDv = file.Path.EndsWith(".bin", StringComparison.OrdinalIgnoreCase);
            if (isParquet || isDv)
                candidates.Add(file);
        }

        // Find unreferenced files older than the retention period.
        var cutoff = DateTimeOffset.UtcNow - retentionPeriod;
        var filesToDelete = new List<string>();
        long bytesToDelete = 0;

        foreach (var file in candidates)
        {
            bool live = referencedPaths.Contains(file.Path) || liveDvPaths.Contains(file.Path);
            if (!live && file.LastModified < cutoff)
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

    // The on-disk relative path of a storage-type-"u" deletion vector. MUST match the reader's derivation
    // (DeletionVectorReader.ReadUuidFileAsync) exactly — if the two disagreed, vacuum could delete a live
    // deletion vector. pathOrInlineDv = "<optional random-prefix dir><z85-encoded uuid (last 20 chars)>";
    // resolves to "<prefix>/deletion_vector_<uuid>.bin" (or without the dir when there is no prefix).
    private static string OnDiskDvPath(string pathOrUuid)
    {
        string uuid = DeletionVectorReader.DecodeUuidFromPath(pathOrUuid);
        string prefix = pathOrUuid.Length > 20 ? pathOrUuid.Substring(0, pathOrUuid.Length - 20) : "";
        return prefix.Length > 0
            ? $"{prefix}/deletion_vector_{uuid}.bin"
            : $"deletion_vector_{uuid}.bin";
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
