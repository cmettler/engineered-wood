// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.DeltaLake.DeletionVectors;

/// <summary>
/// Filters deleted rows from a RecordBatch using a deletion vector.
/// </summary>
public static class DeletionVectorFilter
{
    /// <summary>
    /// Returns a new RecordBatch with rows marked as deleted removed.
    /// Row indices in <paramref name="deletedRows"/> are relative to the
    /// start of the data file (absolute row positions).
    /// </summary>
    /// <param name="batch">The source batch to filter.</param>
    /// <param name="deletedRows">Set of absolute row indices that are deleted.</param>
    /// <param name="batchStartRow">
    /// The absolute row index of the first row in this batch within the data file.
    /// Used to translate absolute DV row indices to batch-relative indices.
    /// </param>
    public static RecordBatch Filter(
        RecordBatch batch, HashSet<long> deletedRows, long batchStartRow)
    {
        if (deletedRows.Count == 0)
            return batch;

        // Find which rows in this batch are NOT deleted
        var keepRows = new List<int>();
        for (int i = 0; i < batch.Length; i++)
        {
            long absoluteRow = batchStartRow + i;
            if (!deletedRows.Contains(absoluteRow))
                keepRows.Add(i);
        }

        if (keepRows.Count == batch.Length)
            return batch; // No rows deleted in this batch

        if (keepRows.Count == 0)
            return CreateEmptyBatch(batch.Schema);

        // Build filtered batch by taking only kept rows
        var columns = new IArrowArray[batch.ColumnCount];
        for (int col = 0; col < batch.ColumnCount; col++)
            columns[col] = TakeRows(batch.Column(col), keepRows);

        return new RecordBatch(batch.Schema, columns, keepRows.Count);
    }

    private static RecordBatch CreateEmptyBatch(Apache.Arrow.Schema schema)
    {
        var columns = new IArrowArray[schema.FieldsList.Count];
        for (int i = 0; i < columns.Length; i++)
        {
            columns[i] = CreateEmptyArray(schema.FieldsList[i].DataType);
        }
        return new RecordBatch(schema, columns, 0);
    }

    private static IArrowArray CreateEmptyArray(Apache.Arrow.Types.IArrowType type) =>
        type switch
        {
            Apache.Arrow.Types.Int64Type => new Int64Array.Builder().Build(),
            Apache.Arrow.Types.Int32Type => new Int32Array.Builder().Build(),
            Apache.Arrow.Types.Int16Type => new Int16Array.Builder().Build(),
            Apache.Arrow.Types.Int8Type => new Int8Array.Builder().Build(),
            Apache.Arrow.Types.DoubleType => new DoubleArray.Builder().Build(),
            Apache.Arrow.Types.FloatType => new FloatArray.Builder().Build(),
            Apache.Arrow.Types.StringType => new StringArray.Builder().Build(),
            Apache.Arrow.Types.BooleanType => new BooleanArray.Builder().Build(),
            Apache.Arrow.Types.BinaryType => new BinaryArray.Builder().Build(),
            _ => new StringArray.Builder().Build(), // Fallback
        };

    /// <summary>
    /// Takes specific rows from an array. Reuses the PartitionUtils pattern.
    /// </summary>
    /// <summary>
    /// Takes specific rows from an array. Public for use by update operations.
    /// </summary>
    public static IArrowArray TakeRowsPublic(IArrowArray source, List<int> rows) =>
        TakeRows(source, rows);

    private static IArrowArray TakeRows(IArrowArray source, List<int> rows)
    {
        switch (source)
        {
            case Int64Array a:
            {
                var b = new Int64Array.Builder();
                foreach (int r in rows) { if (a.IsNull(r)) b.AppendNull(); else b.Append(a.GetValue(r)!.Value); }
                return b.Build();
            }
            case Int32Array a:
            {
                var b = new Int32Array.Builder();
                foreach (int r in rows) { if (a.IsNull(r)) b.AppendNull(); else b.Append(a.GetValue(r)!.Value); }
                return b.Build();
            }
            case Int16Array a:
            {
                var b = new Int16Array.Builder();
                foreach (int r in rows) { if (a.IsNull(r)) b.AppendNull(); else b.Append(a.GetValue(r)!.Value); }
                return b.Build();
            }
            case Int8Array a:
            {
                var b = new Int8Array.Builder();
                foreach (int r in rows) { if (a.IsNull(r)) b.AppendNull(); else b.Append(a.GetValue(r)!.Value); }
                return b.Build();
            }
            case DoubleArray a:
            {
                var b = new DoubleArray.Builder();
                foreach (int r in rows) { if (a.IsNull(r)) b.AppendNull(); else b.Append(a.GetValue(r)!.Value); }
                return b.Build();
            }
            case FloatArray a:
            {
                var b = new FloatArray.Builder();
                foreach (int r in rows) { if (a.IsNull(r)) b.AppendNull(); else b.Append(a.GetValue(r)!.Value); }
                return b.Build();
            }
            case StringArray a:
            {
                var b = new StringArray.Builder();
                foreach (int r in rows) { if (a.IsNull(r)) b.AppendNull(); else b.Append(a.GetString(r)); }
                return b.Build();
            }
            case LargeStringArray a:
            {
                var b = new StringArray.Builder();
                foreach (int r in rows) { if (a.IsNull(r)) b.AppendNull(); else b.Append(a.GetString(r)); }
                return b.Build();
            }
            case BooleanArray a:
            {
                var b = new BooleanArray.Builder();
                foreach (int r in rows) { if (a.IsNull(r)) b.AppendNull(); else b.Append(a.GetValue(r)!.Value); }
                return b.Build();
            }
            case BinaryArray a:
            {
                var b = new BinaryArray.Builder();
                foreach (int r in rows) { if (a.IsNull(r)) b.AppendNull(); else b.Append(a.GetBytes(r)); }
                return b.Build();
            }
            default:
            {
                // Every other fixed-width type (unsigned ints, decimals, Date/Time/Timestamp, HalfFloat,
                // FixedSizeBinary, ...) is filtered by copying its raw value-slot bytes — this preserves the
                // exact type (timestamp unit/tz, decimal precision/scale) and avoids per-type builders /
                // System.Decimal's 28-digit cap. A type we cannot slice (nested list/struct/map/...) must
                // THROW, never fall through unfiltered: an unfiltered column has the wrong length and silently
                // corrupts the copy-on-write rewrite (mispaired rows + a read-buffer overrun downstream).
                int? width = FixedWidthBytes(source.Data.DataType);
                if (width is int byteWidth)
                    return TakeFixedWidth(source, rows, byteWidth);
                throw new NotSupportedException(
                    $"DeletionVectorFilter.TakeRows cannot filter column type {source.Data.DataType.TypeId} — "
                    + "copy-on-write DELETE/UPDATE does not support this type.");
            }
        }
    }

    /// <summary>
    /// Byte width of a fixed-width Arrow type (Boolean is bit-packed → excluded, handled by its own builder
    /// case), or null for variable-width / nested types. Used to filter any fixed-width column generically by
    /// slicing its value buffer.
    /// </summary>
    private static int? FixedWidthBytes(IArrowType type) => type switch
    {
        Int8Type or UInt8Type => 1,
        Int16Type or UInt16Type or HalfFloatType => 2,
        Int32Type or UInt32Type or FloatType or Date32Type or Time32Type => 4,
        Int64Type or UInt64Type or DoubleType or Date64Type or Time64Type or TimestampType or DurationType => 8,
        // Decimal128Type / Decimal256Type derive from FixedSizeBinaryType, so ByteWidth (16 / 32) is exact.
        FixedSizeBinaryType fsb => fsb.ByteWidth,
        _ => null,
    };

    /// <summary>
    /// Takes specific rows from a fixed-width array by copying the raw value-slot bytes (accounting for the
    /// source array's logical offset) and rebuilding the validity bitmap. Type-agnostic and lossless.
    /// </summary>
    private static IArrowArray TakeFixedWidth(IArrowArray source, List<int> rows, int byteWidth)
    {
        ReadOnlySpan<byte> srcValues = source.Data.Buffers[1].Span;
        int srcOffset = source.Data.Offset;
        var valueBytes = new byte[rows.Count * byteWidth];
        var validity = new ArrowBuffer.BitmapBuilder(rows.Count);
        int nullCount = 0;
        int i = 0;
        foreach (int r in rows)
        {
            bool isNull = source.IsNull(r);
            validity.Append(!isNull);
            if (isNull)
                nullCount++;
            else
                srcValues.Slice((srcOffset + r) * byteWidth, byteWidth)
                         .CopyTo(valueBytes.AsSpan(i * byteWidth, byteWidth));
            i++;
        }

        var buffers = new[] { validity.Build(), new ArrowBuffer(valueBytes) };
        var data = new ArrayData(source.Data.DataType, rows.Count, nullCount, 0, buffers);
        return ArrowArrayFactory.BuildArray(data);
    }
}
