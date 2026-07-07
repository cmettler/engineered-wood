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
        // Select small files as compaction candidates
        var candidates = snapshot.ActiveFiles.Values
            .Where(f => f.Size < options.MinFileSize)
            .OrderBy(f => f.Size)
            .Take(options.MaxFilesPerCommit)
            .ToList();

        if (candidates.Count < 2)
            return null; // Not worth compacting a single file

        // Build target schema for type widening during compaction
        var targetSchema = SchemaConverter.ToArrowSchema(
            DeltaSchemaSerializer.Parse(snapshot.Metadata.SchemaString));

        // Column mapping: the data files store PHYSICAL column names (both name and id mode), and the compacted
        // file must keep them + re-stamp each column's parquet field_id (readers resolve by physicalName/field_id;
        // a compacted file without them would read as all-NULL). Widening therefore matches on the
        // physical-renamed schema, and each batch is rebuilt CLEAN (reader field metadata dropped) before
        // SetParquetFieldIds — re-stamping over reader-carried metadata malforms the footer.
        var mappingMode = ColumnMapping.GetMode(snapshot.Metadata.Configuration);
        if (mappingMode != ColumnMappingMode.None)
        {
            var logicalToPhysical = ColumnMapping.BuildLogicalToPhysicalMap(snapshot.Schema, mappingMode);
            var physFields = new List<Field>(targetSchema.FieldsList.Count);
            foreach (var f in targetSchema.FieldsList)
            {
                physFields.Add(logicalToPhysical.TryGetValue(f.Name, out var p) && p != f.Name
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

        // Read all LIVE data from candidate files, widening types if needed. A candidate may carry a deletion
        // vector (deletion vectors are the default DML mode) — its deleted rows MUST be excluded, else compaction
        // would resurrect them. The internal materialized row-id column (__delta_row_id) is stripped from the data
        // (it is re-materialized below when `materialize`, else dropped).
        var dvReader = new DeletionVectors.DeletionVectorReader(fs);
        var allBatches = new List<RecordBatch>();
        var batchIds = materialize ? new List<Int64Array>() : null;   // aligned 1:1 with allBatches
        var batchVers = materialize ? new List<Int64Array>() : null;  // aligned 1:1 with allBatches
        foreach (var addFile in candidates)
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
            return null;

        // Write compacted data into new files
        var actions = new List<DeltaAction>();
        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        // Remove old files (with dataChange: false since this is rearrangement)
        foreach (var oldFile in candidates)
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

        // Row tracking state
        bool rowTrackingEnabled = EngineeredWood.DeltaLake.RowTracking.RowTrackingConfig.IsEnabled(
            snapshot.Metadata.Configuration);
        long nextRowId = rowTrackingEnabled ? snapshot.RowIdHighWaterMark : 0;

        // Earliest defaultRowCommitVersion from source files (preserved through compaction)
        long? earliestCommitVersion = candidates
            .Where(c => c.DefaultRowCommitVersion.HasValue)
            .Select(c => c.DefaultRowCommitVersion!.Value)
            .DefaultIfEmpty(-1)
            .Min();
        if (earliestCommitVersion == -1) earliestCommitVersion = null;

        // Write new compacted file(s)
        // Group batches to target file size (approximate by row count)
        long totalRows = allBatches.Sum(b => (long)b.Length);
        long totalBytes = candidates.Sum(f => f.Size);
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
                string fileName = $"{Guid.NewGuid():N}.parquet";
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
                    Path = fileName,
                    PartitionValues = candidates[0].PartitionValues,
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
}
