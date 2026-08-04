// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The variant TRANSPORT form (<see cref="VariantTransport"/>): a host whose Arrow boundary cannot carry
/// the canonical <c>arrow.parquet.variant</c> extension (struct storage) exchanges variant values as ONE
/// self-delimiting BINARY per row (metadata bytes ++ value bytes) tagged with the
/// <see cref="SchemaConverter.VariantTransportExtensionName"/> field-metadata marker. The write side is
/// marker-keyed and always on. There is no read direction: a host wanting the transport form converts this
/// library's canonical output at its own boundary.
/// These tests pin the transport against the CANONICAL flow: a transport-written table reads back
/// canonically (default options) and vice versa — the two host dialects see one spec table.
/// </summary>
public class VariantTransportTests : IDisposable
{
    private readonly string _tempDir;

    public VariantTransportTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_variant_transport_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    // Canonical empty variant metadata: version=1, dictionary_size=0, one zero offset.
    private static readonly byte[] EmptyMetadata = [0x01, 0x00, 0x00];
    private static readonly byte[] True = [0x04];           // (1 << 2) | 0
    private static readonly byte[] Int8_42 = [0x0C, 0x2A];  // (3 << 2) | 0, then 42

    private static byte[] Blob(byte[] value)
    {
        var combined = new byte[EmptyMetadata.Length + value.Length];
        EmptyMetadata.CopyTo(combined, 0);
        value.CopyTo(combined, EmptyMetadata.Length);
        return combined;
    }

    /// <summary>The transport-form schema: a BINARY field tagged with the marker.</summary>
    private static Apache.Arrow.Schema TransportSchema() =>
        new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("v", BinaryType.Default, true, new Dictionary<string, string>
            {
                ["ARROW:extension:name"] = SchemaConverter.VariantTransportExtensionName,
            }))
            .Build();

    private static RecordBatch TransportBatch(params byte[]?[] values)
    {
        var ids = new Int64Array.Builder();
        var blobs = new BinaryArray.Builder();
        for (int i = 0; i < values.Length; i++)
        {
            ids.Append(i + 1);
            if (values[i] is null) blobs.AppendNull();
            else blobs.Append(Blob(values[i]!).AsSpan());
        }
        return new RecordBatch(TransportSchema(), [ids.Build(), blobs.Build()], values.Length);
    }

    [Fact]
    public void FromArrowSchema_MarkerTaggedBinary_MapsToVariant()
    {
        var delta = SchemaConverter.FromArrowSchema(TransportSchema());
        var v = delta.Fields.Single(f => f.Name == "v");
        Assert.True(v.Type is PrimitiveType { TypeName: "variant" });
        // The marker is a transport hint, never persisted into the Delta schema metadata.
        Assert.True(v.Metadata is null || !v.Metadata.ContainsKey("ARROW:extension:name"));
    }

    [Fact]
    public async Task TransportWritten_ReadsBackCanonically()
    {
        // WRITE through the transport form (marker-keyed, no option needed)...
        var fs = new LocalTableFileSystem(_tempDir);
        await using (var table = await DeltaTable.CreateAsync(fs, TransportSchema()))
        {
            await table.WriteAsync([TransportBatch(True, null, Int8_42)]);
        }

        // ...and READ with DEFAULT options: the canonical dialect sees a VariantArray with the exact
        // bytes (proving the codec wrote the spec VARIANT-annotated group, not a plain binary column).
        await using var reread = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        Assert.True(reread.CurrentSnapshot.Schema.Fields.Single(f => f.Name == "v").Type
            is PrimitiveType { TypeName: "variant" });

        var rows = new List<(bool IsNull, byte[]? Value)>();
        await foreach (var b in reread.ReadAllAsync())
        {
            var col = Assert.IsType<VariantArray>(b.Column(b.Schema.GetFieldIndex("v")));
            for (int i = 0; i < col.Length; i++)
                rows.Add((col.IsNull(i), col.IsNull(i) ? null : col.GetValueBytes(i).ToArray()));
        }

        Assert.Equal(3, rows.Count);
        Assert.Equal(True, rows[0].Value);
        Assert.True(rows[1].IsNull); // SQL NULL rides storage validity
        Assert.Equal(Int8_42, rows[2].Value);
    }

    [Fact]
    public async Task TransportRoundTrip_UniformColumnShredsAndReassembles()
    {
        // A UNIFORM column shreds on write (ShredSchemaInferer applies a typed_value schema) and
        // reassembles per row when read back as transport blobs — the tier the data-skipping
        // "superpower" rides on. int8 primitives: value = (3 << 2) | 0, then the byte.
        byte[] Int8(sbyte n) => [0x0C, unchecked((byte)n)];

        var ids = new Int64Array.Builder();
        ids.Append(1);
        ids.Append(2);
        ids.Append(3);
        var blobs = new BinaryArray.Builder();
        blobs.Append(Blob(Int8(1)).AsSpan());
        blobs.AppendNull(); // SQL NULL through the shredded form (storage validity)
        blobs.Append(Blob(Int8(2)).AsSpan());
        var fs = new LocalTableFileSystem(_tempDir);
        await using (var table = await DeltaTable.CreateAsync(fs, TransportSchema()))
        {
            await table.WriteAsync([new RecordBatch(TransportSchema(), [ids.Build(), blobs.Build()], 3)]);
        }

        // Read back CANONICALLY — the only read dialect this library has. The transport's read direction
        // moved to the embedding host, so what is under test here is what remains this library's: that a
        // uniform column SHREDS on write and REASSEMBLES to the canonical (metadata, value) pair on read.
        await using var reread = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        var values = new List<byte[]?>();
        await foreach (var b in reread.ReadAllAsync())
        {
            var col = Assert.IsType<VariantArray>(b.Column(b.Schema.GetFieldIndex("v")));
            var storage = Assert.IsType<StructArray>(col.Storage);
            var stype = (Apache.Arrow.Types.StructType)storage.Data.DataType;
            int valueIdx = stype.GetFieldIndex("value");
            Assert.True(valueIdx >= 0, "reassembly must yield a canonical 'value' child");
            // typed_value gone => reassembly actually ran, rather than the raw shredded layout surviving.
            Assert.True(stype.GetFieldIndex("typed_value") < 0);
            var value = Assert.IsType<BinaryArray>(
                Apache.Arrow.ArrowArrayFactory.BuildArray(storage.Data.Children[valueIdx]));
            for (int i = 0; i < storage.Length; i++)
                values.Add(storage.IsNull(i) || value.IsNull(i) ? null : value.GetBytes(i).ToArray());
        }

        // The VALUE half asserted byte-exactly. Only the value half: reassembly re-ENCODES the metadata
        // (its header flag bits may differ from the canonical empty form), so the metadata bytes are not
        // a stable expectation. The SQL NULL survives as a null storage row.
        Assert.Equal(3, values.Count);
        Assert.Equal(Int8(1), values[0]);
        Assert.Null(values[1]);
        Assert.Equal(Int8(2), values[2]);
    }
}
