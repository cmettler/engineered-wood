// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.ChangeDataFeed;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table.ChangeDataFeed;

/// <summary>
/// Writes Change Data Feed (CDC) files during operations that modify data.
/// CDC files are stored in <c>_change_data/</c> and contain rows with an
/// additional <c>_change_type</c> column.
/// </summary>
internal static class CdfWriter
{
    /// <summary>
    /// Writes CDC files for a set of changed rows, splitting by PARTITION exactly like a data write —
    /// the rows arrive WITH the table's partition columns (the read paths re-add them), each partition's
    /// rows land in their own <c>_change_data</c> file with the partition columns EXCLUDED from the file
    /// bytes and carried on the <see cref="CdcFile"/> action's <c>partitionValues</c> (physical-keyed
    /// under column mapping — the data-file convention). Handles rows that span partitions (an
    /// update_postimage after a SET of the partition column). Unpartitioned tables degrade to one file.
    /// </summary>
    public static async ValueTask<List<CdcFile>> WriteSplitAsync(
        ITableFileSystem fs,
        Snapshot.Snapshot snapshot,
        RecordBatch rows,
        string changeType,
        ParquetWriteOptions? parquetOptions,
        CancellationToken cancellationToken)
    {
        var result = new List<CdcFile>();
        if (rows.Length == 0)
            return result;
        var partitionColumns = snapshot.Metadata.PartitionColumns;
        if (partitionColumns.Count == 0)
        {
            result.Add(await WriteAsync(fs, snapshot, rows, changeType,
                new Dictionary<string, string>(), parquetOptions, cancellationToken).ConfigureAwait(false));
            return result;
        }

        var mappingMode = ColumnMapping.GetMode(snapshot.Metadata.Configuration);
        var logicalToPhysical = ColumnMapping.BuildLogicalToPhysicalMap(snapshot.Schema, mappingMode);
        foreach (var (partValues, dataBatch) in Partitioning.PartitionUtils.SplitByPartition(rows, partitionColumns))
        {
            if (dataBatch.Length == 0)
                continue;
            var tracked = partValues;
            if (mappingMode != ColumnMappingMode.None && partValues.Count > 0)
            {
                var t = new Dictionary<string, string>(partValues.Count);
                foreach (var kv in partValues)
                    t[logicalToPhysical.TryGetValue(kv.Key, out var p) ? p : kv.Key] = kv.Value;
                tracked = t;
            }
            result.Add(await WriteAsync(fs, snapshot, dataBatch, changeType, tracked,
                parquetOptions, cancellationToken).ConfigureAwait(false));
        }
        return result;
    }

    /// <summary>
    /// Writes a CDC file for a set of changed rows and returns the <see cref="CdcFile"/> action.
    /// </summary>
    public static async ValueTask<CdcFile> WriteAsync(
        ITableFileSystem fs,
        Snapshot.Snapshot snapshot,
        RecordBatch rows,
        string changeType,
        IReadOnlyDictionary<string, string> partitionValues,
        ParquetWriteOptions? parquetOptions,
        CancellationToken cancellationToken)
    {
        // Under column mapping the _change_data files follow the table's file layout: PHYSICAL column
        // names + parquet field_ids, exactly like data files (Spark reads cdc parquet through the same
        // mapping). The synthetic _change_type column is added AFTER the rename so it stays unmapped.
        var mappingMode = ColumnMapping.GetMode(snapshot.Metadata.Configuration);
        if (mappingMode != ColumnMappingMode.None)
        {
            rows = ColumnMappingRecursive.ToPhysical(rows, snapshot.Schema, mappingMode);
        }

        // Add _change_type column
        var batchWithChangeType = AddChangeTypeColumn(rows, changeType);

        string fileName = $"{CdfConfig.ChangeDataDir}/{Guid.NewGuid():N}.parquet";
        long fileSize;

        await using (var file = await fs.CreateAsync(
            fileName, cancellationToken: cancellationToken).ConfigureAwait(false))
        {
            await using var writer = new ParquetFileWriter(
                file, ownsFile: false, parquetOptions);
            await writer.WriteRowGroupAsync(batchWithChangeType, cancellationToken)
                .ConfigureAwait(false);
            await writer.DisposeAsync().ConfigureAwait(false);
            fileSize = file.Position;
        }

        return new CdcFile
        {
            Path = fileName,
            PartitionValues = CopyDict(partitionValues),
            Size = fileSize,
            DataChange = false,
        };
    }

    /// <summary>
    /// Adds a <c>_change_type</c> column to a RecordBatch.
    /// </summary>
    public static RecordBatch AddChangeTypeColumn(RecordBatch batch, string changeType)
    {
        var changeTypeBuilder = new StringArray.Builder();
        for (int i = 0; i < batch.Length; i++)
            changeTypeBuilder.Append(changeType);

        var columns = new IArrowArray[batch.ColumnCount + 1];
        var fields = new List<Field>(batch.ColumnCount + 1);

        for (int i = 0; i < batch.ColumnCount; i++)
        {
            columns[i] = batch.Column(i);
            fields.Add(batch.Schema.FieldsList[i]);
        }

        columns[batch.ColumnCount] = changeTypeBuilder.Build();
        fields.Add(new Field(CdfConfig.ChangeTypeColumn, StringType.Default, false));

        var schema = new Apache.Arrow.Schema.Builder();
        foreach (var f in fields)
            schema.Field(f);

        return new RecordBatch(schema.Build(), columns, batch.Length);
    }

    private static Dictionary<string, string> CopyDict(IReadOnlyDictionary<string, string> source)
    {
        var result = new Dictionary<string, string>();
        foreach (var kvp in source)
            result[kvp.Key] = kvp.Value;
        return result;
    }
}
