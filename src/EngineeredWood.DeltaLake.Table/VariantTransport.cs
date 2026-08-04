// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Scalars.Variant;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Converts between the variant TRANSPORT form — one self-delimiting binary per row (the parquet-variant
/// metadata bytes immediately followed by the value bytes), marked with
/// <see cref="SchemaConverter.VariantTransportExtensionName"/> field metadata — and the canonical
/// <see cref="VariantArray"/> (<c>arrow.parquet.variant</c> over <c>struct&lt;metadata, value&gt;</c>).
/// The variant metadata header carries its own size, so the transport splits without a length prefix.
/// Embedding hosts whose Arrow boundary cannot carry an extension type over struct storage exchange
/// variant values in this LEAF-binary form: the WRITE side converts marked blob columns before the
/// built-in parquet codec (marker-keyed — a no-op for canonical input).
///
/// <para><b>WRITE direction only.</b> The read-side counterpart is gone: a host that wants the transport
/// form converts the pipeline's canonical output at its OWN boundary, which is where the constraint lives
/// and which needs no knowledge of the four physical layouts <see cref="VariantColumnCoercion.Coerce"/>
/// already normalises (canonical, shredded, a bare struct from an unannotated file, a seam-delivered
/// blob). Only the write direction has to be here, because it feeds the built-in parquet codec.</para>
///
/// <para><b>Shredding</b> (the VariantShredding spec) is NOT this type's concern: it is a physical-layout
/// decision owned by <see cref="VariantShredding"/> in the parquet layer, which this type calls in both
/// directions — <see cref="VariantShredding.TryShred(IReadOnlyList{VariantValue}, ReadOnlySpan{bool}, out VariantArray)"/>
/// on the way in (passing the values it has ALREADY decoded out of the blobs, so the decode stays at one
/// per row) and <see cref="VariantShredding.Reassemble"/> on the way out (which normalises ANY spec layout
/// — unshredded, partially or fully shredded, ours or a foreign writer's — to the canonical form this
/// type then concatenates). SQL NULL rows ride the storage struct's validity, distinct from a variant
/// JSON null.</para>
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
    /// physical-named batches alike); a no-op for canonical input or when the batch carries no variant
    /// column.
    /// </summary>
    internal static RecordBatch ToVariantArrays(RecordBatch batch)
    {
        List<Field>? fields = null;
        List<IArrowArray>? arrays = null;
        for (int c = 0; c < batch.ColumnCount; c++)
        {
            var f = batch.Schema.FieldsList[c];
            if (!SchemaConverter.IsVariantTransportField(f) || batch.Column(c) is not BinaryArray blob)
                continue;
            if (fields is null)
            {
                fields = new List<Field>(batch.Schema.FieldsList);
                arrays = new List<IArrowArray>(batch.ColumnCount);
                for (int i = 0; i < batch.ColumnCount; i++)
                    arrays.Add(batch.Column(i));
            }
            var variant = BuildVariantColumn(blob);
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
    /// Builds the codec-facing variant column from a transport-blob column. Each blob is parsed ONCE and
    /// the decoded values are offered to <see cref="VariantShredding"/>, which owns the layout decision:
    /// when a shredding schema applies (uniform objects/primitives/arrays) the rows are shredded into
    /// typed columns + residuals — data skipping on shredded leaves is the variant "superpower" spec
    /// readers (Spark, DuckDB) exploit. When it declines, we build the unshredded array from the ORIGINAL
    /// bytes, so a mixed-shape column costs no re-encode. SQL NULL rows become null STORAGE rows
    /// (validity), which is distinct from a variant JSON null riding in the value bytes.
    /// </summary>
    private static VariantArray BuildVariantColumn(BinaryArray blob)
    {
        int n = blob.Length;
        var values = new VariantValue[n];
        var isNull = new bool[n];
        bool anyNull = false;
        for (int r = 0; r < n; r++)
        {
            if (blob.IsNull(r))
            {
                isNull[r] = true;
                anyNull = true;
                values[r] = VariantValue.Null; // placeholder; masked by validity in the shredder
                continue;
            }
            var bytes = blob.GetBytes(r);
            int metaLen = MetadataLength(bytes);
            var reader = new VariantReader(bytes.Slice(0, metaLen), bytes.Slice(metaLen));
            values[r] = reader.ToVariantValue();
        }

        if (VariantShredding.TryShred(values, anyNull ? isNull : default, out var shredded))
        {
            return shredded;
        }

        // Unshredded: pass the original bytes through untouched (no re-encode).
        var builder = new VariantArray.Builder();
        for (int r = 0; r < n; r++)
        {
            if (isNull[r])
            {
                builder.AppendNull();
                continue;
            }
            var bytes = blob.GetBytes(r);
            int metaLen = MetadataLength(bytes);
            builder.Append(bytes.Slice(0, metaLen), bytes.Slice(metaLen));
        }
        return builder.Build();
    }
}
