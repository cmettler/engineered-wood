// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System;
using System.Collections.Generic;
using Apache.Arrow;
using EngineeredWood.DeltaLake.Schema;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Converts between the Delta layer's variant TRANSPORT form — one self-delimiting binary per row
/// (the parquet-variant metadata bytes immediately followed by the value bytes), marked with
/// <see cref="SchemaConverter.VariantExtensionName"/> field metadata — and the parquet codec's
/// <see cref="VariantArray"/> (the VARIANT-annotated <c>group&lt;metadata, value&gt;</c>). The variant
/// metadata header carries its own size, so the transport splits without a length prefix. This is what
/// lets the BUILT-IN parquet codec read and write variant columns: the writer takes VariantArray columns
/// (emitting the spec annotation), the reader returns the bare storage struct, and the Delta read
/// pipeline converts back to the transport blob the embedding host expects.
/// </summary>
internal static class VariantTransport
{
    /// <summary>True when the Delta field is a <c>variant</c> primitive.</summary>
    internal static bool IsVariantField(StructField field) =>
        field.Type is PrimitiveType p && string.Equals(p.TypeName, "variant", StringComparison.Ordinal);

    /// <summary>
    /// The metadata prefix length of a concatenated variant blob (header byte ++ dictionary_size ++
    /// offsets ++ dictionary bytes — every piece sized by the header's offset_size, so the prefix is
    /// self-delimiting per the Variant binary spec v1).
    /// </summary>
    internal static int MetadataLength(ReadOnlySpan<byte> blob)
    {
        if (blob.Length < 3)
            throw new DeltaFormatException($"variant transport blob too short ({blob.Length} bytes).");
        byte header = blob[0];
        int version = header & 0x0F;
        if (version != 1)
            throw new DeltaFormatException($"unsupported variant metadata version {version}.");
        int offsetSize = ((header >> 6) & 0x3) + 1;
        long dictSize = ReadLittleEndian(blob, 1, offsetSize);
        int offsetsStart = 1 + offsetSize;
        long lastOffset = ReadLittleEndian(blob, offsetsStart + (int)dictSize * offsetSize, offsetSize);
        long total = offsetsStart + (dictSize + 1) * offsetSize + lastOffset;
        if (total <= 0 || total >= blob.Length)
            throw new DeltaFormatException("variant transport blob has a malformed metadata prefix.");
        return (int)total;
    }

    private static long ReadLittleEndian(ReadOnlySpan<byte> blob, int offset, int size)
    {
        if (offset + size > blob.Length)
            throw new DeltaFormatException("variant transport blob truncated inside the metadata prefix.");
        long v = 0;
        for (int i = 0; i < size; i++)
            v |= (long)blob[offset + i] << (8 * i);
        return v;
    }

    /// <summary>
    /// WRITE direction: replaces every transport-marked binary column in <paramref name="batch"/> with a
    /// <see cref="VariantArray"/> (splitting each blob into its metadata/value halves), so the built-in
    /// parquet writer emits the VARIANT-annotated group. Marker-keyed (works on logical- and
    /// physical-named batches alike); a no-op when the batch carries no variant column.
    /// </summary>
    internal static RecordBatch ToVariantArrays(RecordBatch batch)
    {
        List<Field>? fields = null;
        List<IArrowArray>? arrays = null;
        for (int c = 0; c < batch.ColumnCount; c++)
        {
            var f = batch.Schema.FieldsList[c];
            if (!SchemaConverter.IsVariantArrowField(f) || batch.Column(c) is not BinaryArray blob)
                continue;
            if (fields is null)
            {
                fields = new List<Field>(batch.Schema.FieldsList);
                arrays = new List<IArrowArray>(batch.ColumnCount);
                for (int i = 0; i < batch.ColumnCount; i++)
                    arrays.Add(batch.Column(i));
            }
            var builder = new VariantArray.Builder();
            for (int r = 0; r < blob.Length; r++)
            {
                if (blob.IsNull(r))
                {
                    builder.AppendNull();
                    continue;
                }
                var bytes = blob.GetBytes(r);
                int metaLen = MetadataLength(bytes);
                builder.Append(bytes.Slice(0, metaLen), bytes.Slice(metaLen));
            }
            var variant = builder.Build();
            // Keep the field metadata (column-mapping physicalName/PARQUET:field_id survive) — only the
            // type changes to the extension; the transport marker is harmless alongside it.
            fields[c] = new Field(f.Name, variant.Data.DataType, f.IsNullable, f.Metadata);
            arrays![c] = variant;
        }
        if (fields is null)
            return batch;
        var sb = new Apache.Arrow.Schema.Builder();
        foreach (var f in fields)
            sb.Field(f);
        return new RecordBatch(sb.Build(), arrays!, batch.Length);
    }

    /// <summary>
    /// READ direction: converts each column whose DELTA type is <c>variant</c> from the codec reader's
    /// bare storage struct (<c>metadata, value</c> — the reader is registry-less, so the annotation
    /// surfaces as a plain struct) back to the transport blob, tagging the field with the marker. A
    /// column that is already binary (a pluggable host reader delivered the transport directly) passes
    /// through. A SHREDDED file (a <c>typed_value</c> child) is rejected — reassembly needs a variant
    /// engine the codec does not have.
    /// </summary>
    internal static RecordBatch FromStorageStructs(RecordBatch batch, StructType deltaSchema)
    {
        List<Field>? fields = null;
        List<IArrowArray>? arrays = null;
        for (int c = 0; c < batch.ColumnCount; c++)
        {
            var f = batch.Schema.FieldsList[c];
            StructField? deltaField = null;
            foreach (var df in deltaSchema.Fields)
            {
                if (string.Equals(df.Name, f.Name, StringComparison.Ordinal))
                {
                    deltaField = df;
                    break;
                }
            }
            if (deltaField is null || !IsVariantField(deltaField) || batch.Column(c) is not StructArray st)
                continue;

            var structType = (Apache.Arrow.Types.StructType)st.Data.DataType;
            BinaryArray? meta = null, val = null;
            for (int i = 0; i < structType.Fields.Count; i++)
            {
                string name = structType.Fields[i].Name;
                if (string.Equals(name, "typed_value", StringComparison.Ordinal))
                {
                    throw new DeltaFormatException(
                        $"column '{f.Name}' is a SHREDDED variant — the built-in reader cannot reassemble "
                        + "shredded variants; read the table with a variant-capable host reader.");
                }
                var child = ArrowArrayFactory.BuildArray(st.Data.Children[i]) as BinaryArray;
                if (string.Equals(name, "metadata", StringComparison.Ordinal))
                    meta = child;
                else if (string.Equals(name, "value", StringComparison.Ordinal))
                    val = child;
            }
            if (meta is null || val is null)
                throw new DeltaFormatException(
                    $"column '{f.Name}' is annotated VARIANT but lacks binary metadata/value children.");

            int off = st.Data.Offset; // struct children do NOT incorporate the parent's offset
            var builder = new BinaryArray.Builder();
            for (int r = 0; r < st.Length; r++)
            {
                if (st.IsNull(r) || meta.IsNull(off + r) || val.IsNull(off + r))
                {
                    builder.AppendNull();
                    continue;
                }
                var m = meta.GetBytes(off + r);
                var v = val.GetBytes(off + r);
                var combined = new byte[m.Length + v.Length];
                m.CopyTo(combined);
                v.CopyTo(combined.AsSpan(m.Length));
                builder.Append(combined.AsSpan());
            }

            if (fields is null)
            {
                fields = new List<Field>(batch.Schema.FieldsList);
                arrays = new List<IArrowArray>(batch.ColumnCount);
                for (int i = 0; i < batch.ColumnCount; i++)
                    arrays.Add(batch.Column(i));
            }
            var tagged = new Dictionary<string, string>
            {
                ["ARROW:extension:name"] = SchemaConverter.VariantExtensionName,
            };
            if (f.Metadata is { } src)
            {
                foreach (var kv in src)
                    tagged[kv.Key] = kv.Value;
            }
            fields[c] = new Field(f.Name, Apache.Arrow.Types.BinaryType.Default, f.IsNullable, tagged);
            arrays![c] = builder.Build();
        }
        if (fields is null)
            return batch;
        var sb = new Apache.Arrow.Schema.Builder();
        foreach (var f in fields)
            sb.Field(f);
        return new RecordBatch(sb.Build(), arrays!, batch.Length);
    }

    /// <summary>True when any top-level Delta field is a variant (the cheap gate for the transforms).</summary>
    internal static bool SchemaHasVariant(StructType deltaSchema)
    {
        foreach (var f in deltaSchema.Fields)
        {
            if (IsVariantField(f))
                return true;
        }
        return false;
    }
}
