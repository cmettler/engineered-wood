// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Delta VARIANT column support: the <c>variant</c> schema type, the <c>variantType</c> reader+writer
/// table feature, and the round trip through the parquet codec (which annotates the group with the
/// VARIANT logical type on write and materialises a <see cref="VariantArray"/> on read).
/// </summary>
public class VariantTests : IDisposable
{
    private readonly string _tempDir;

    public VariantTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_variant_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    // Canonical empty variant metadata: version=1, dictionary_size=0, one zero offset.
    private static readonly byte[] EmptyMetadata = [0x01, 0x00, 0x00];

    private static Apache.Arrow.Schema VariantSchema() =>
        new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("v", VariantType.Default, true))
            .Build();

    /// <summary>Builds a batch of (id, variant) rows; a null <c>value</c> entry appends a null variant.</summary>
    private static RecordBatch VariantBatch(params byte[]?[] values)
    {
        var ids = new Int64Array.Builder();
        var variants = new VariantArray.Builder();
        for (int i = 0; i < values.Length; i++)
        {
            ids.Append(i + 1);
            if (values[i] is null) variants.AppendNull();
            else variants.Append(EmptyMetadata, values[i]!);
        }
        return new RecordBatch(
            VariantSchema(),
            [ids.Build(), variants.Build(allocator: null)],
            values.Length);
    }

    // Variant primitive encodings (basic_type=primitive): boolean true / false, int8(42).
    private static byte[] True => [0x0C];
    private static byte[] False => [0x08];
    private static byte[] Int8Val => [0x0C + 0x04, 42]; // primitive header for int8, then the value

    private static async Task<List<RecordBatch>> ReadAllAsync(DeltaTable table)
    {
        var batches = new List<RecordBatch>();
        await foreach (var b in table.ReadAllAsync())
            batches.Add(b);
        return batches;
    }

    [Fact]
    public async Task WriteAndRead_Variant_RoundTrips()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, VariantSchema());

        await table.WriteAsync([VariantBatch(True, False, null)]);

        var read = await ReadAllAsync(table);
        Assert.Single(read);
        Assert.Equal(3, read[0].Length);

        // The column must come back as a VariantArray, not the bare storage struct — that is what the
        // registry injection in DeltaTable guarantees.
        var v = Assert.IsType<VariantArray>(read[0].Column(1));
        Assert.False(v.IsNull(0));
        Assert.False(v.IsNull(1));
        Assert.True(v.IsNull(2));
        Assert.Equal(True, v.GetValueBytes(0).ToArray());
        Assert.Equal(False, v.GetValueBytes(1).ToArray());
        Assert.Equal(EmptyMetadata, v.GetMetadataBytes(0).ToArray());
    }

    [Fact]
    public async Task CreateAsync_DeclaresVariantTypeFeature()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, VariantSchema());

        var log = new TransactionLog(fs);
        var actions = await log.ReadCommitAsync(0);
        var protocol = actions.OfType<Actions.ProtocolAction>().Single();

        // variantType is a reader-3 / writer-7 named feature, so declaring it puts the table in
        // table-features mode on both sides.
        Assert.Equal(3, protocol.MinReaderVersion);
        Assert.Equal(7, protocol.MinWriterVersion);
        Assert.Contains("variantType", protocol.ReaderFeatures!);
        Assert.Contains("variantType", protocol.WriterFeatures!);
    }

    [Fact]
    public async Task Metadata_SerializesVariantSchemaType()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, VariantSchema());

        var log = new TransactionLog(fs);
        var actions = await log.ReadCommitAsync(0);
        var metadata = actions.OfType<Actions.MetadataAction>().Single();

        // The spec's schema type name is the bare string "variant" — not a struct of metadata/value.
        using var doc = JsonDocument.Parse(metadata.SchemaString);
        var field = doc.RootElement.GetProperty("fields").EnumerateArray()
            .Single(f => f.GetProperty("name").GetString() == "v");
        Assert.Equal("variant", field.GetProperty("type").GetString());
    }

    [Fact]
    public async Task Stats_OmitMinMaxForVariant_ButCountNulls()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, VariantSchema());

        await table.WriteAsync([VariantBatch(True, null, False)]);

        var log = new TransactionLog(fs);
        var add = (await log.ReadCommitAsync(1)).OfType<Actions.AddFile>().Single();
        using var doc = JsonDocument.Parse(add.Stats!);
        var root = doc.RootElement;

        Assert.Equal(3, root.GetProperty("numRecords").GetInt32());
        // Min/max are meaningless for variant — the spec omits them; nullCount is still required.
        Assert.False(root.GetProperty("minValues").TryGetProperty("v", out _));
        Assert.False(root.GetProperty("maxValues").TryGetProperty("v", out _));
        Assert.Equal(1, root.GetProperty("nullCount").GetProperty("v").GetInt32());
    }

    [Fact]
    public async Task Delete_OnVariantTable_PreservesTheExtensionType()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, VariantSchema());
        await table.WriteAsync([VariantBatch(True, False, Int8Val)]);

        // Copy-on-write DELETE filters every column through TakeRows; without an extension arm the
        // variant column threw and the whole DML path was unusable.
        await table.DeleteAsync(batch =>
        {
            var ids = (Int64Array)batch.Column(0);
            var b = new BooleanArray.Builder();
            for (int i = 0; i < batch.Length; i++) b.Append(ids.GetValue(i)!.Value == 2);
            return b.Build();
        });

        var read = await ReadAllAsync(table);
        var rows = read.Sum(b => b.Length);
        Assert.Equal(2, rows);

        var v = Assert.IsType<VariantArray>(read[0].Column(1));
        Assert.Equal(True, v.GetValueBytes(0).ToArray());
        Assert.Equal(Int8Val, v.GetValueBytes(1).ToArray());
    }

    [Fact]
    public async Task PartitionedWrite_WithVariantColumn_Works()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("region", StringType.Default, false))
            .Field(new Field("v", VariantType.Default, true))
            .Build();
        await using var table = await DeltaTable.CreateAsync(
            fs, schema, partitionColumns: ["region"]);

        var regions = new StringArray.Builder().Append("us").Append("eu").Append("us").Build();
        var variants = new VariantArray.Builder();
        variants.Append(EmptyMetadata, True);
        variants.Append(EmptyMetadata, False);
        variants.Append(EmptyMetadata, Int8Val);
        var batch = new RecordBatch(schema, [regions, variants.Build(allocator: null)], 3);

        // Partitioned writes take rows from EVERY non-partition column, so this exercised the same
        // TakeRows gap even though variant is not the partition column.
        await table.WriteAsync([batch]);

        var read = await ReadAllAsync(table);
        Assert.Equal(3, read.Sum(b => b.Length));
        foreach (var b in read)
            Assert.IsType<VariantArray>(b.Column(b.Schema.GetFieldIndex("v")));
    }

    [Fact]
    public async Task Compaction_OfVariantTable_PreservesValues()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, VariantSchema());

        await table.WriteAsync([VariantBatch(True)]);
        await table.WriteAsync([VariantBatch(False)]);
        await table.WriteAsync([VariantBatch(Int8Val)]);

        await table.CompactAsync();

        var read = await ReadAllAsync(table);
        var values = read.SelectMany(b =>
        {
            var v = Assert.IsType<VariantArray>(b.Column(1));
            return Enumerable.Range(0, v.Length).Select(i => v.GetValueBytes(i).ToArray());
        }).ToList();

        Assert.Equal(3, values.Count);
        Assert.Contains(values, x => x.SequenceEqual(True));
        Assert.Contains(values, x => x.SequenceEqual(False));
        Assert.Contains(values, x => x.SequenceEqual(Int8Val));
    }

    [Fact]
    public async Task AddColumn_Variant_UpgradesProtocolAndBackfillsNull()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var baseSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();
        await using var table = await DeltaTable.CreateAsync(fs, baseSchema);

        await table.WriteAsync([new RecordBatch(
            baseSchema, [new Int64Array.Builder().Append(1).Append(2).Build()], 2)]);

        // Introducing variant AFTER create must emit a protocol upgrade, and pre-existing files must
        // backfill the new column as a typed NULL variant (not a string column).
        await table.AddColumnAsync(new Field("v", VariantType.Default, true));

        var read = await ReadAllAsync(table);
        Assert.Equal(2, read.Sum(b => b.Length));
        var v = Assert.IsType<VariantArray>(read[0].Column(1));
        Assert.True(v.IsNull(0));
        Assert.True(v.IsNull(1));

        var protocol = table.CurrentSnapshot.Protocol;
        Assert.Contains("variantType", protocol.ReaderFeatures!);
        Assert.Contains("variantType", protocol.WriterFeatures!);
    }

    [Fact]
    public async Task VariantPartitionColumn_IsRejected()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("v", VariantType.Default, false))
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        // Delta forbids a variant partition column. Whatever the layer that catches it, the write
        // must fail rather than encode a .NET type name (or the storage struct) into the directory.
        await using var table = await DeltaTable.CreateAsync(
            fs, schema, partitionColumns: ["v"]);

        var variants = new VariantArray.Builder();
        variants.Append(EmptyMetadata, True);
        var batch = new RecordBatch(
            schema,
            [variants.Build(allocator: null), new Int64Array.Builder().Append(1).Build()],
            1);

        await Assert.ThrowsAnyAsync<NotSupportedException>(
            async () => await table.WriteAsync([batch]));
    }
}
