// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Schema;
using DeltaSnapshot = EngineeredWood.DeltaLake.Snapshot.Snapshot;
using EngineeredWood.IO;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table.Compaction;

/// <summary>
/// Executes file compaction: reads small files, rewrites them as larger files,
/// and commits the add/remove actions.
/// </summary>
internal static class CompactionExecutor
{
    /// <summary>
    /// Selects files eligible for compaction and rewrites them.
    /// Returns the new version number, or null if no files were compacted.
    /// </summary>
    public static async ValueTask<long?> ExecuteAsync(
        ITableFileSystem fs,
        TransactionLog log,
        DeltaSnapshot snapshot,
        CompactionOptions options,
        ParquetWriteOptions parquetOptions,
        ParquetReadOptions parquetReadOptions,
        CancellationToken cancellationToken,
        IDataFileWriter? dataFileWriter = null,
        IDataFileReader? dataFileReader = null)
    {
        // Select small files as compaction candidates and group them BY PARTITION: a data file belongs to
        // exactly ONE partition (its add.partitionValues), so each group compacts independently — mixing
        // partitions into one file stamped with one partition's values (the previous behavior) silently
        // corrupted the partition column of every other row. Unpartitioned tables form a single group.
        // Canonical keys tolerate mixed logical/physical partitionValues vintages under column mapping.
        var mappingMode = ColumnMapping.GetMode(snapshot.Metadata.Configuration);
        var logicalToPhysical = mappingMode != ColumnMappingMode.None
            ? ColumnMapping.BuildLogicalToPhysicalMap(snapshot.Schema, mappingMode)
            : null;
        var groups = snapshot.ActiveFiles.Values
            .Where(f => f.Size < options.MinFileSize)
            .OrderBy(f => f.Size)
            .Take(options.MaxFilesPerCommit)
            .GroupBy(f => DeltaTable.CanonicalPartitionKey(f.PartitionValues, logicalToPhysical))
            .Select(g => g.ToList())
            .Where(g => g.Count >= 2) // not worth compacting a single file
            .ToList();

        if (groups.Count == 0)
            return null;

        // Build target schema for type widening during compaction — EXCLUDING partition columns: per the
        // Delta layout, data files do not carry them (values live in add.partitionValues; readers re-add
        // them). Backfilling them as all-NULL columns (the previous behavior) wrote junk columns into the
        // compacted file and misaligned a pluggable IDataFileReader's raw batches.
        var targetSchema = SchemaConverter.ToArrowSchema(
            DeltaSchemaSerializer.Parse(snapshot.Metadata.SchemaString));
        if (snapshot.Metadata.PartitionColumns.Count > 0)
        {
            var partSet = new HashSet<string>(snapshot.Metadata.PartitionColumns, StringComparer.Ordinal);
            targetSchema = new Apache.Arrow.Schema(
                targetSchema.FieldsList.Where(f => !partSet.Contains(f.Name)).ToList(), null);
        }

        // Column mapping: the data files store PHYSICAL column names (both name and id mode), and the compacted
        // file must keep them + re-stamp each column's parquet field_id (readers resolve by physicalName/field_id;
        // a compacted file without them would read as all-NULL). Widening therefore matches on the
        // physical-renamed schema, and each batch is rebuilt CLEAN (reader field metadata dropped) before
        // SetParquetFieldIds — re-stamping over reader-carried metadata malforms the footer.
        if (mappingMode != ColumnMappingMode.None)
        {
            var physFields = new List<Field>(targetSchema.FieldsList.Count);
            foreach (var f in targetSchema.FieldsList)
            {
                physFields.Add(logicalToPhysical!.TryGetValue(f.Name, out var p) && p != f.Name
                    ? new Field(p, f.DataType, f.IsNullable)
                    : f);
            }
            targetSchema = new Apache.Arrow.Schema(physFields, null);
        }

        // When the table declares materialized row tracking, compaction must PRESERVE each row's ORIGINAL stable
        // id AND commit version — rows from several source files mix into one compacted file, so the compacted
        // add's single baseRowId / defaultRowCommitVersion cannot represent them. Materialize both columns from
        // the per-source-file baseRowId + physical position (or the source's own materialized __delta_row_id when
        // present) and the source's defaultRowCommitVersion. Default off (no declared column) → the original
        // behavior: strip __delta_row_id, write a fresh baseRowId (readers use baseRowId + position).
        string? matRowIdCol = snapshot.Metadata.Configuration is { } matCfg
            && matCfg.TryGetValue("delta.rowTracking.materializedRowIdColumnName", out var mrc) ? mrc : null;
        bool materialize = matRowIdCol is not null;

        var dvReader = new DeletionVectors.DeletionVectorReader(fs);
        var actions = new List<DeltaAction>();
        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        bool rowTrackingEnabled = EngineeredWood.DeltaLake.RowTracking.RowTrackingConfig.IsEnabled(
            snapshot.Metadata.Configuration);
        long nextRowId = rowTrackingEnabled ? snapshot.RowIdHighWaterMark : 0;
        bool anyAdds = false;

        foreach (var group in groups)
        {
            (bool compacted, nextRowId) = await CompactGroupAsync(
                fs, snapshot, options, parquetOptions, group, targetSchema, mappingMode, materialize,
                dvReader, actions, now, rowTrackingEnabled, nextRowId, parquetReadOptions,
                dataFileWriter, dataFileReader, cancellationToken).ConfigureAwait(false);
            anyAdds |= compacted;
        }

        if (!anyAdds)
            return null;

        if (rowTrackingEnabled && nextRowId > snapshot.RowIdHighWaterMark)
        {
            actions.Add(EngineeredWood.DeltaLake.RowTracking.RowTrackingConfig.BuildHighWaterMarkAction(nextRowId));
        }

        // Commit — with the always-on commitInfo (operation + timestamp) every other commit path writes:
        // history readers surface the operation, and timestamp time travel resolves through this commit.
        long newVersion = snapshot.Version + 1;
        IReadOnlyList<DeltaAction> commitActions =
            InCommitTimestamp.EnsureCommitInfo(actions, snapshot.Metadata.Configuration, "OPTIMIZE");
        await log.WriteCommitAsync(newVersion, commitActions, cancellationToken)
            .ConfigureAwait(false);

        return newVersion;
    }

    /// <summary>Compacts ONE partition group's candidate files (the whole table when unpartitioned):
    /// reads the live rows, appends the group's remove + add actions (adds carry the group's
    /// partitionValues and land in the group's Hive directory), and returns whether anything was
    /// compacted plus the advanced row-tracking id cursor.</summary>
    private static async ValueTask<(bool Compacted, long NextRowId)> CompactGroupAsync(
        ITableFileSystem fs,
        DeltaSnapshot snapshot,
        CompactionOptions options,
        ParquetWriteOptions parquetOptions,
        IReadOnlyList<AddFile> group,
        Apache.Arrow.Schema targetSchema,
        ColumnMappingMode mappingMode,
        bool materialize,
        DeletionVectors.DeletionVectorReader dvReader,
        List<DeltaAction> actions,
        long now,
        bool rowTrackingEnabled,
        long nextRowId,
        ParquetReadOptions parquetReadOptions,
        IDataFileWriter? dataFileWriter,
        IDataFileReader? dataFileReader,
        CancellationToken cancellationToken)
    {
        // Read all LIVE data from candidate files, widening types if needed. A candidate may carry a deletion
        // vector (deletion vectors are the default DML mode) — its deleted rows MUST be excluded, else compaction
        // would resurrect them. The internal materialized row-id column (__delta_row_id) is stripped from the data
        // (it is re-materialized below when `materialize`, else dropped).
        var allBatches = new List<RecordBatch>();
        var batchIds = materialize ? new List<Int64Array>() : null;   // aligned 1:1 with allBatches
        var batchVers = materialize ? new List<Int64Array>() : null;  // aligned 1:1 with allBatches
        foreach (var addFile in group)
        {
            var deletedRows = addFile.DeletionVector is not null
                ? await dvReader.ReadAsync(addFile.DeletionVector, cancellationToken).ConfigureAwait(false)
                : null;
            long baseId = addFile.BaseRowId ?? 0;
            long commitVer = addFile.DefaultRowCommitVersion ?? 0;

            // Pluggable read half: raw physical batches in file order (positions drive the DV exclusion and
            // the row-id materialization below) — the same contract as the built-in reader.
            IRandomAccessFile? file = null;
            ParquetFileReader? reader = null;
            IAsyncEnumerable<RecordBatch> rawBatches;
            if (dataFileReader is not null)
            {
                rawBatches = dataFileReader.ReadAsync(DeltaPath.Decode(addFile.Path), null, cancellationToken);
            }
            else
            {
                file = await fs.OpenReadAsync(DeltaPath.Decode(addFile.Path), cancellationToken)
                    .ConfigureAwait(false);
                reader = new ParquetFileReader(file, ownsFile: false, parquetReadOptions);
                rawBatches = reader.ReadAllAsync(cancellationToken: cancellationToken);
            }
            try
            {
            long batchStartRow = 0;
            await foreach (var batch in rawBatches.ConfigureAwait(false))
            {
                // Drop the internal row-id column (no-op when absent) — count physical rows for DV positions first.
                var (userBatch, srcRowIds) = RowTracking.RowTrackingWriter.StripRowIdColumn(batch);
                long physicalRows = batch.Length;

                // Build the survivor id/version arrays in the SAME order DeletionVectorFilter keeps (ascending
                // physical index, skipping DV-deleted rows) so they stay aligned with the filtered batch.
                Int64Array? survivorIds = null, survivorVers = null;
                if (materialize)
                {
                    var idb = new Int64Array.Builder();
                    var vrb = new Int64Array.Builder();
                    for (int i = 0; i < physicalRows; i++)
                    {
                        if (deletedRows is not null && deletedRows.Contains(batchStartRow + i))
                            continue;
                        // Prefer the source file's OWN materialized id (handles a source that was itself an
                        // UPDATE-append carrying preserved ids); else compute baseRowId + physical position.
                        long id = srcRowIds is not null && !srcRowIds.IsNull(i)
                            ? srcRowIds.GetValue(i)!.Value
                            : baseId + batchStartRow + i;
                        idb.Append(id);
                        vrb.Append(commitVer);
                    }
                    survivorIds = idb.Build();
                    survivorVers = vrb.Build();
                }

                if (deletedRows is not null)
                {
                    userBatch = DeletionVectors.DeletionVectorFilter.Filter(userBatch, deletedRows, batchStartRow);
                }
                batchStartRow += physicalRows;
                if (userBatch.Length == 0)
                    continue; // whole batch was deleted
                // Reconcile each source batch to the current (physical, under mapping) schema FIRST: candidate
                // files of different vintages differ after ADD/DROP COLUMN — a missing column backfills NULL, a
                // dropped one is removed — so every batch shares ONE column set (row count unchanged → the
                // materialized id/version arrays stay aligned; widening below indexes against the full schema).
                var outBatch = DeltaTable.BackfillMissingColumns(userBatch, targetSchema.FieldsList);
                // Widen values from old files to match current schema (row order/count preserved → ids stay aligned)
                outBatch = TypeWidening.ValueWidener.WidenBatch(outBatch, targetSchema);
                if (mappingMode != ColumnMappingMode.None)
                {
                    // Rebuild with a CLEAN schema (drop reader-carried field metadata), then stamp the field_ids
                    // so the compacted file keeps the column-mapping identity its readers resolve by.
                    var cleanFields = new List<Field>(outBatch.Schema.FieldsList.Count);
                    foreach (var f in outBatch.Schema.FieldsList)
                        cleanFields.Add(DeltaTable.CleanField(f));
                    var cleanArrays = new List<IArrowArray>(outBatch.ColumnCount);
                    for (int c = 0; c < outBatch.ColumnCount; c++)
                        cleanArrays.Add(outBatch.Column(c));
                    outBatch = new RecordBatch(
                        new Apache.Arrow.Schema(cleanFields, null), cleanArrays, outBatch.Length);
                    outBatch = ColumnMapping.SetParquetFieldIds(outBatch, snapshot.Schema, mappingMode);
                }
                allBatches.Add(outBatch);
                if (materialize)
                {
                    batchIds!.Add(survivorIds!);
                    batchVers!.Add(survivorVers!);
                }
            }
            }
            finally
            {
                reader?.Dispose();
                if (file is not null)
                {
                    await file.DisposeAsync().ConfigureAwait(false);
                }
            }
        }

        if (allBatches.Count == 0)
            return (false, nextRowId); // every live row DV-deleted — leave the group's files alone

        // Remove the group's old files (with dataChange: false since this is rearrangement)
        foreach (var oldFile in group)
        {
            actions.Add(new RemoveFile
            {
                Path = oldFile.Path,
                DeletionTimestamp = now,
                DataChange = false,
                ExtendedFileMetadata = true,
                PartitionValues = oldFile.PartitionValues,
                Size = oldFile.Size,
                DeletionVector = oldFile.DeletionVector, // match the active (path, DV) entry so the remove takes effect
            });
        }

        // Earliest defaultRowCommitVersion from source files (preserved through compaction)
        long? earliestCommitVersion = group
            .Where(c => c.DefaultRowCommitVersion.HasValue)
            .Select(c => c.DefaultRowCommitVersion!.Value)
            .DefaultIfEmpty(-1)
            .Min();
        if (earliestCommitVersion == -1) earliestCommitVersion = null;

        // The group's Hive directory: one partition = one directory, so the compacted file joins its
        // sources' directory (the add keeps the ENCODED prefix; the physical write path is the decoded
        // form). Empty for an unpartitioned table (files at the table root).
        string encodedDir = "";
        int dirSlash = group[0].Path.LastIndexOf('/');
        if (dirSlash >= 0)
            encodedDir = group[0].Path.Substring(0, dirSlash + 1);
        string physicalDir = DeltaPath.Decode(encodedDir);

        // Write new compacted file(s)
        // Group batches to target file size (approximate by row count)
        long totalRows = allBatches.Sum(b => (long)b.Length);
        long totalBytes = group.Sum(f => f.Size);
        double bytesPerRow = totalRows > 0 ? (double)totalBytes / totalRows : 0;
        long rowsPerFile = bytesPerRow > 0
            ? Math.Max(1, (long)(options.TargetFileSize / bytesPerRow))
            : totalRows;

        int batchIdx = 0;
        long currentRowCount = 0;
        var currentBatches = new List<RecordBatch>();          // USER columns (for stats)
        var currentWrite = new List<RecordBatch>();            // what gets written (== currentBatches unless materialize)

        while (batchIdx < allBatches.Count)
        {
            var b = allBatches[batchIdx];
            currentBatches.Add(b);
            // When materializing, append the ORIGINAL id + commit-version columns to the written batch (stats are
            // still collected over the user columns only — the internal columns must not appear in Delta stats).
            currentWrite.Add(materialize
                ? RowTracking.RowTrackingWriter.AddRowIdAndCommitVersionColumns(b, batchIds![batchIdx], batchVers![batchIdx])
                : b);
            currentRowCount += b.Length;
            batchIdx++;

            if (currentRowCount >= rowsPerFile || batchIdx == allBatches.Count)
            {
                string baseName = $"{Guid.NewGuid():N}.parquet";
                string fileName = physicalDir + baseName;
                long fileSize;
                long fileBaseRowId = nextRowId;

                if (dataFileWriter is not null)
                {
                    // native_write: DuckDB's parquet writer produces the compacted file (bloom/stats/footer),
                    // so an OPTIMIZE keeps the native-write quality instead of reverting to the built-in codec.
                    fileSize = await dataFileWriter.WriteAsync(currentWrite, fileName, cancellationToken)
                        .ConfigureAwait(false);
                }
                else
                {
                    await using var outFile = await fs.CreateAsync(
                        fileName, cancellationToken: cancellationToken).ConfigureAwait(false);
                    await using var writer = new ParquetFileWriter(
                        outFile, ownsFile: false, parquetOptions);
                    foreach (var batch in currentWrite)
                    {
                        await writer.WriteRowGroupAsync(batch, cancellationToken)
                            .ConfigureAwait(false);
                    }
                    await writer.DisposeAsync().ConfigureAwait(false);
                    fileSize = outFile.Position;
                }

                if (rowTrackingEnabled)
                    nextRowId += currentRowCount;

                string? stats = Stats.StatsCollector.Collect(currentBatches);

                actions.Add(new AddFile
                {
                    Path = encodedDir + baseName,
                    PartitionValues = group[0].PartitionValues,
                    Size = fileSize,
                    ModificationTime = now,
                    DataChange = false,
                    Stats = stats,
                    BaseRowId = rowTrackingEnabled ? fileBaseRowId : null,
                    DefaultRowCommitVersion = earliestCommitVersion,
                });

                currentBatches.Clear();
                currentWrite.Clear();
                currentRowCount = 0;
            }
        }

        return (true, nextRowId);
    }
}
