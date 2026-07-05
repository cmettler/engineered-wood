// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.ChangeDataFeed;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table.ChangeDataFeed;

/// <summary>
/// Reads Change Data Feed (CDC) to return row-level changes between versions.
/// For each version, if CDC files exist they are used; otherwise changes are
/// inferred from add/remove actions.
/// </summary>
internal static class CdfReader
{
    /// <summary>
    /// Reads changes from <paramref name="startVersion"/> to <paramref name="endVersion"/> (inclusive).
    /// Each batch includes <c>_change_type</c>, <c>_commit_version</c>, and <c>_commit_timestamp</c> columns.
    /// <paramref name="snapshot"/> is the CURRENT snapshot: its schema names/types the feed's user columns
    /// (column-mapping physical→logical rename), its partition columns are re-added to rows inferred from
    /// DATA files (which exclude them — the values come from the action's <c>partitionValues</c>), and a
    /// removed/added file's <b>deletion vector</b> is applied so already-deleted rows are not re-reported.
    /// </summary>
    public static async IAsyncEnumerable<RecordBatch> ReadChangesAsync(
        ITableFileSystem fs,
        TransactionLog log,
        long startVersion,
        long endVersion,
        ParquetReadOptions? readOptions,
        Snapshot.Snapshot snapshot,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var mappingMode = ColumnMapping.GetMode(snapshot.Metadata.Configuration);
        var physicalToLogical = ColumnMapping.BuildPhysicalToLogicalMap(snapshot.Schema, mappingMode);
        var logicalToPhysical = ColumnMapping.BuildLogicalToPhysicalMap(snapshot.Schema, mappingMode);
        var partitionColumns = snapshot.Metadata.PartitionColumns;
        var arrowSchema = snapshot.ArrowSchema;
        var dvReader = new DeletionVectors.DeletionVectorReader(fs);

        for (long version = startVersion; version <= endVersion; version++)
        {
            IReadOnlyList<DeltaAction> actions;
            try
            {
                actions = await log.ReadCommitAsync(version, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch
            {
                continue; // Skip missing versions
            }

            // Get commit timestamp
            long? commitTimestamp = InCommitTimestamp.GetTimestampFromActions(actions);

            // Check if this version has CDC files
            var cdcFiles = actions.OfType<CdcFile>().ToList();

            if (cdcFiles.Count > 0)
            {
                // Read from CDC files — they have _change_type already
                foreach (var cdcFile in cdcFiles)
                {
                    await foreach (var batch in ReadCdcFileAsync(
                        fs, cdcFile, version, commitTimestamp, readOptions,
                        physicalToLogical, cancellationToken).ConfigureAwait(false))
                    {
                        yield return batch;
                    }
                }
            }
            else
            {
                // Infer changes from add/remove actions
                var adds = actions.OfType<AddFile>()
                    .Where(a => a.DataChange).ToList();
                var removes = actions.OfType<RemoveFile>()
                    .Where(r => r.DataChange).ToList();

                // A DV-only "delete" commit re-adds the SAME file with a new deletion vector (remove old-DV +
                // add new-DV) — inferring "all rows deleted + survivors re-inserted" from it would be wrong,
                // but such commits are only produced with an explicit CDC file when CDF is enabled, so the
                // inference below (removes → deletes, adds → inserts) only ever sees genuine add/remove data
                // changes (append, overwrite, dynamic partition overwrite).

                // Removed files → "delete" rows (excluding rows the file's DV had ALREADY deleted — those were
                // reported as deletes when the DV was written).
                foreach (var remove in removes)
                {
                    var deleted = remove.DeletionVector is not null
                        ? await dvReader.ReadAsync(remove.DeletionVector, cancellationToken).ConfigureAwait(false)
                        : null;
                    await foreach (var batch in ReadDataFileAsChangesAsync(
                        fs, remove.Path, remove.PartitionValues ?? new Dictionary<string, string>(),
                        deleted, CdfConfig.Delete, version,
                        commitTimestamp, readOptions, physicalToLogical, logicalToPhysical,
                        partitionColumns, arrowSchema, cancellationToken)
                        .ConfigureAwait(false))
                    {
                        yield return batch;
                    }
                }

                // Added files → "insert" rows (a freshly-added file's DV, if any, likewise excludes rows).
                foreach (var add in adds)
                {
                    var deleted = add.DeletionVector is not null
                        ? await dvReader.ReadAsync(add.DeletionVector, cancellationToken).ConfigureAwait(false)
                        : null;
                    await foreach (var batch in ReadDataFileAsChangesAsync(
                        fs, add.Path, add.PartitionValues, deleted, CdfConfig.Insert, version,
                        commitTimestamp, readOptions, physicalToLogical, logicalToPhysical,
                        partitionColumns, arrowSchema, cancellationToken)
                        .ConfigureAwait(false))
                    {
                        yield return batch;
                    }
                }
            }
        }
    }

    private static async IAsyncEnumerable<RecordBatch> ReadCdcFileAsync(
        ITableFileSystem fs,
        CdcFile cdcFile,
        long commitVersion,
        long? commitTimestamp,
        ParquetReadOptions? readOptions,
        Dictionary<string, string>? physicalToLogical,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await using var file = await fs.OpenReadAsync(cdcFile.Path, cancellationToken)
            .ConfigureAwait(false);
        using var reader = new ParquetFileReader(file, ownsFile: false, readOptions);

        await foreach (var batch in reader.ReadAllAsync(
            cancellationToken: cancellationToken).ConfigureAwait(false))
        {
            // CDC files already have _change_type — add version and timestamp columns. Column mapping: rename any
            // physical column names back to logical (a Spark-written _change_data file uses physical names; our
            // own CDC files are written from logical batches, so this no-ops).
            var b = physicalToLogical is { Count: > 0 }
                ? Schema.ColumnMapping.RenameColumns(batch, physicalToLogical)
                : batch;
            yield return AddMetadataColumns(b, commitVersion, commitTimestamp);
        }
    }

    private static async IAsyncEnumerable<RecordBatch> ReadDataFileAsChangesAsync(
        ITableFileSystem fs,
        string path,
        IReadOnlyDictionary<string, string> partitionValues,
        HashSet<long>? deletedRows,
        string changeType,
        long commitVersion,
        long? commitTimestamp,
        ParquetReadOptions? readOptions,
        Dictionary<string, string>? physicalToLogical,
        Dictionary<string, string>? logicalToPhysical,
        IReadOnlyList<string> partitionColumns,
        Apache.Arrow.Schema arrowSchema,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // Try to read the file — it may have been deleted already
        if (!await fs.ExistsAsync(path, cancellationToken).ConfigureAwait(false))
            yield break;

        await using var file = await fs.OpenReadAsync(path, cancellationToken)
            .ConfigureAwait(false);
        using var reader = new ParquetFileReader(file, ownsFile: false, readOptions);

        long batchStartRow = 0;
        await foreach (var batch in reader.ReadAllAsync(
            cancellationToken: cancellationToken).ConfigureAwait(false))
        {
            // Strip _change_type column if present in the data file
            var cleanBatch = StripChangeTypeColumn(batch);

            // On a rowTracking table the data files carry the physical __delta_row_id column; strip it so the
            // inferred change rows share the user-column schema with cdc-file-derived rows (a 6-vs-5-col mismatch
            // across change batches otherwise breaks strict consumers / the Arrow C-stream boundary).
            (cleanBatch, _) = RowTracking.RowTrackingWriter.StripRowIdColumn(cleanBatch);

            // Exclude rows the file's deletion vector had already deleted — they are not part of THIS change
            // (they were reported as deletes when the DV was committed). Positions are file-absolute.
            long physicalRows = batch.Length;
            if (deletedRows is not null)
            {
                cleanBatch = DeletionVectors.DeletionVectorFilter.Filter(cleanBatch, deletedRows, batchStartRow);
            }
            batchStartRow += physicalRows;
            if (cleanBatch.Length == 0)
                continue;

            // Column mapping: a DATA file stores physical column names — rename back to the logical names so
            // the change rows match the table schema (no-op without mapping / for already-logical batches).
            if (physicalToLogical is { Count: > 0 })
                cleanBatch = Schema.ColumnMapping.RenameColumns(cleanBatch, physicalToLogical);

            // A partitioned table's DATA files exclude the partition columns — re-add them as constant arrays
            // from the action's partitionValues (dual logical|physical key lookup, like the normal read path),
            // so the feed's user columns match the table schema.
            if (partitionColumns.Count > 0)
            {
                cleanBatch = Partitioning.PartitionUtils.AddPartitionColumns(
                    cleanBatch, arrowSchema, partitionValues, partitionColumns, logicalToPhysical);
            }

            // Add _change_type, _commit_version, _commit_timestamp
            var withChangeType = CdfWriter.AddChangeTypeColumn(cleanBatch, changeType);
            yield return AddMetadataColumns(withChangeType, commitVersion, commitTimestamp);
        }
    }

    private static RecordBatch AddMetadataColumns(
        RecordBatch batch, long commitVersion, long? commitTimestamp)
    {
        var versionBuilder = new Int64Array.Builder();
        var timestampBuilder = new Int64Array.Builder();

        for (int i = 0; i < batch.Length; i++)
        {
            versionBuilder.Append(commitVersion);
            if (commitTimestamp.HasValue)
                timestampBuilder.Append(commitTimestamp.Value);
            else
                timestampBuilder.AppendNull();
        }

        var columns = new IArrowArray[batch.ColumnCount + 2];
        var fields = new List<Field>(batch.ColumnCount + 2);

        for (int i = 0; i < batch.ColumnCount; i++)
        {
            columns[i] = batch.Column(i);
            fields.Add(batch.Schema.FieldsList[i]);
        }

        columns[batch.ColumnCount] = versionBuilder.Build();
        fields.Add(new Field(CdfConfig.CommitVersionColumn, Int64Type.Default, false));

        columns[batch.ColumnCount + 1] = timestampBuilder.Build();
        fields.Add(new Field(CdfConfig.CommitTimestampColumn, Int64Type.Default, true));

        var schema = new Apache.Arrow.Schema.Builder();
        foreach (var f in fields)
            schema.Field(f);

        return new RecordBatch(schema.Build(), columns, batch.Length);
    }

    private static RecordBatch StripChangeTypeColumn(RecordBatch batch)
    {
        int ctIdx = batch.Schema.GetFieldIndex(CdfConfig.ChangeTypeColumn);
        if (ctIdx < 0)
            return batch;

        var columns = new IArrowArray[batch.ColumnCount - 1];
        var fields = new List<Field>(batch.ColumnCount - 1);
        int outIdx = 0;

        for (int i = 0; i < batch.ColumnCount; i++)
        {
            if (i == ctIdx) continue;
            columns[outIdx++] = batch.Column(i);
            fields.Add(batch.Schema.FieldsList[i]);
        }

        var schema = new Apache.Arrow.Schema.Builder();
        foreach (var f in fields)
            schema.Field(f);

        return new RecordBatch(schema.Build(), columns, batch.Length);
    }
}
