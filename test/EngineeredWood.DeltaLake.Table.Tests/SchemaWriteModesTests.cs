// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Schema-changing and partition-changing WRITE modes:
///
/// <list type="bullet">
/// <item><see cref="DeltaTable.SetSchemaAsync"/> — schema OVERWRITE (adopt exactly the incoming schema:
/// drops/adds/retypes in one metadata-only commit; the CREATE-OR-REPLACE / SCHEMA_MODE 'overwrite'
/// building block), with a logical no-op compare;</item>
/// <item><c>WriteAsync(repartitionTo:)</c> — REPARTITION-ON-OVERWRITE, the only protocol-legal way to
/// change <c>metaData.partitionColumns</c> (readers interpret every <c>add.partitionValues</c> against
/// the current partition schema, so the swap is valid only when every active file is removed in the
/// same commit = a full overwrite; Spark exposes this as <c>overwriteSchema=true</c> + new
/// <c>partitionBy</c>);</item>
/// <item><see cref="DeltaTable.OverwritePartitionsAsync"/> — STATIC partition overwrite
/// (delta-rs <c>replaceWhere</c> on partition columns): file-exact removal, one atomic commit;</item>
/// <item><see cref="DeltaTable.DynamicOverwriteAsync"/> — DYNAMIC partition overwrite (Spark
/// <c>partitionOverwriteMode=dynamic</c>): the replaced set derives from the DATA;</item>
/// <item><c>delta.appendOnly</c> enforcement (<c>HonorWriterFeatures</c>): a declared append-only table
/// rejects non-append writes instead of silently violating the contract.</item>
/// </list>
/// </summary>
public class SchemaWriteModesTests : IDisposable
{
    private readonly string _tempDir;

    public SchemaWriteModesTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_swm_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static RecordBatch PartitionedBatch(Apache.Arrow.Schema schema, params (string Region, long Id)[] rows)
    {
        var regions = new StringArray.Builder();
        var ids = new Int64Array.Builder();
        foreach (var (region, id) in rows)
        {
            regions.Append(region);
            ids.Append(id);
        }
        return new RecordBatch(schema, [regions.Build(), ids.Build()], rows.Length);
    }

    private static async Task<List<(string Region, long Id)>> ReadRowsAsync(DeltaTable table)
    {
        var rows = new List<(string, long)>();
        await foreach (var batch in table.ReadAllAsync())
        {
            // partition column position can differ from the write layout — resolve by name
            int regionIdx = -1, idIdx = -1;
            for (int c = 0; c < batch.Schema.FieldsList.Count; c++)
            {
                if (batch.Schema.FieldsList[c].Name == "region") regionIdx = c;
                if (batch.Schema.FieldsList[c].Name == "id") idIdx = c;
            }
            var regions = (StringArray)batch.Column(regionIdx);
            var ids = (Int64Array)batch.Column(idIdx);
            for (int i = 0; i < batch.Length; i++)
                rows.Add((regions.GetString(i), ids.GetValue(i)!.Value));
        }
        rows.Sort();
        return rows;
    }

    // ---- SetSchemaAsync (schema overwrite) ----

    [Fact]
    public async Task SetSchema_AdoptsIncomingSchema_DropAndAdd()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("old", StringType.Default, true))
            .Build();
        await using var table = await DeltaTable.CreateAsync(fs, schema);
        await table.WriteAsync([new RecordBatch(schema,
        [
            new Int64Array.Builder().Append(1).Build(),
            new StringArray.Builder().Append("x").Build(),
        ], 1)]);

        // adopt EXACTLY the new shape: `old` dropped, `fresh` added — one metadata-only commit
        var newSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("fresh", Int32Type.Default, true))
            .Build();
        long v = await table.SetSchemaAsync(newSchema);
        Assert.Equal(2, v);

        // the CREATE-OR-REPLACE shape: the schema swap pairs with an Overwrite of the data
        await table.WriteAsync([new RecordBatch(newSchema,
        [
            new Int64Array.Builder().Append(10).Build(),
            new Int32Array.Builder().Append(99).Build(),
        ], 1)], DeltaWriteMode.Overwrite);

        var names = table.ArrowSchema.FieldsList.Select(f => f.Name).ToArray();
        Assert.Equal(new[] { "id", "fresh" }, names);
        int rows = 0;
        await foreach (var batch in table.ReadAllAsync())
        {
            rows += batch.Length;
            Assert.Equal(99, ((Int32Array)batch.Column(1)).GetValue(0));
        }
        Assert.Equal(1, rows);
    }

    [Fact]
    public async Task SetSchema_LogicallyIdentical_IsNoOp()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();
        await using var table = await DeltaTable.CreateAsync(fs, schema);
        long before = table.CurrentSnapshot.Version;

        await table.SetSchemaAsync(schema);
        Assert.Equal(before, table.CurrentSnapshot.Version); // no metadata commit for a no-op
    }

    // ---- repartition-on-overwrite ----

    [Fact]
    public async Task RepartitionOnOverwrite_ChangesPartitionColumnsAtomically()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("region", StringType.Default, false))
            .Field(new Field("id", Int64Type.Default, false))
            .Build();
        await using var table = await DeltaTable.CreateAsync(fs, schema, partitionColumns: ["region"]);
        await table.WriteAsync([PartitionedBatch(schema, ("US", 1), ("EU", 2))]);
        Assert.Equal(["region"], table.CurrentSnapshot.Metadata.PartitionColumns);

        // a FULL overwrite may swap metaData.partitionColumns in the SAME commit (every active file is
        // removed, so no add.partitionValues is left to misinterpret) — here: departition entirely
        await table.WriteAsync([PartitionedBatch(schema, ("US", 10), ("EU", 20))],
            DeltaWriteMode.Overwrite, repartitionTo: []);

        Assert.Empty(table.CurrentSnapshot.Metadata.PartitionColumns);
        Assert.Equal([("EU", 20L), ("US", 10L)], await ReadRowsAsync(table));
    }

    // ---- partition overwrites ----

    [Fact]
    public async Task OverwritePartitions_ReplacesOnlyTheTargetPartition()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("region", StringType.Default, false))
            .Field(new Field("id", Int64Type.Default, false))
            .Build();
        await using var table = await DeltaTable.CreateAsync(fs, schema, partitionColumns: ["region"]);
        await table.WriteAsync([PartitionedBatch(schema, ("US", 1), ("US", 2), ("EU", 3), ("EU", 4))]);
        long before = table.CurrentSnapshot.Version;

        // static replaceWhere: exactly the US partition swaps, EU untouched, ONE commit
        await table.OverwritePartitionsAsync(
            [PartitionedBatch(schema, ("US", 100))],
            new Dictionary<string, string> { ["region"] = "US" });

        Assert.Equal(before + 1, table.CurrentSnapshot.Version);
        Assert.Equal([("EU", 3L), ("EU", 4L), ("US", 100L)], await ReadRowsAsync(table));
    }

    [Fact]
    public async Task DynamicOverwrite_ReplacesExactlyThePartitionsPresentInTheInput()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("region", StringType.Default, false))
            .Field(new Field("id", Int64Type.Default, false))
            .Build();
        await using var table = await DeltaTable.CreateAsync(fs, schema, partitionColumns: ["region"]);
        await table.WriteAsync([PartitionedBatch(schema, ("US", 1), ("EU", 2), ("APAC", 3))]);

        // the input touches US only — US swaps, EU/APAC untouched (Spark partitionOverwriteMode=dynamic)
        await table.DynamicOverwriteAsync([PartitionedBatch(schema, ("US", 100), ("US", 101))]);

        Assert.Equal([("APAC", 3L), ("EU", 2L), ("US", 100L), ("US", 101L)], await ReadRowsAsync(table));
    }

    // ---- writer-feature enforcement ----

    [Fact]
    public async Task AppendOnlyTable_RejectsOverwrite_AllowsAppend()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();
        await using var table = await DeltaTable.CreateAsync(fs, schema,
            configuration: new Dictionary<string, string> { ["delta.appendOnly"] = "true" });

        RecordBatch Batch(long id) => new(schema, [new Int64Array.Builder().Append(id).Build()], 1);

        await table.WriteAsync([Batch(1)]); // appends are the contract — fine
        await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await table.WriteAsync([Batch(2)], DeltaWriteMode.Overwrite));
    }
}
