// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Runtime.InteropServices;
using Apache.Arrow;
using EngineeredWood.IO;
using EngineeredWood.Lance.Format;
using EngineeredWood.Lance.Proto;
using EngineeredWood.Lance.Proto.Encodings.V20;
using EngineeredWood.Lance.Proto.Encodings.V21;
using EngineeredWood.Lance.Proto.V2;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

namespace EngineeredWood.Lance;

/// <summary>
/// Writer for v2.1 Lance files. Phase 13 MVP: produces a single-file
/// (no Lance dataset wrapper, no manifest) Lance v2.1 file containing
/// one or more columns of either fixed-width primitives
/// (<see cref="sbyte"/>, <see cref="short"/>, <see cref="int"/>,
/// <see cref="long"/> and unsigned counterparts, <see cref="float"/>,
/// <see cref="double"/>) or variable-length strings/binary, nullable
/// or not, with all rows packed into a single MiniBlockLayout page per
/// column. Fixed-width columns use <see cref="Flat"/> value
/// compression; strings/binary use <see cref="Variable"/> with
/// Flat(u32) offsets.
///
/// <para><b>What this writer covers</b>:</para>
/// <list type="bullet">
///   <item>v2.1 file envelope: page buffers → column metadata blobs →
///   CMO table → FileDescriptor (global buffer 0) → GBO table →
///   40-byte footer + <c>"LANC"</c> magic.</item>
///   <item><see cref="MiniBlockLayout"/> with <c>num_buffers=1</c>,
///   <c>layers=[ALL_VALID_ITEM]</c> for null-free columns or
///   <c>layers=[NULLABLE_ITEM]</c> with <c>def_compression=Flat(16)</c>
///   for nullable ones; single chunk, no rep.</item>
///   <item>Schema: top-level primitive leaves with sequential field
///   ids.</item>
/// </list>
///
/// <para><b>What's deferred</b>:</para>
/// <list type="bullet">
///   <item>Booleans (bit-packed encoding, not byte-aligned).</item>
///   <item>Lists, structs, FSL.</item>
///   <item>Other value encodings (FullZip, Bitpacking, FSST,
///   Dictionary, ByteStreamSplit, ...).</item>
///   <item>Multi-page columns (large data → page splits).</item>
///   <item>Lance dataset wrapper (manifest, fragments, _versions/).</item>
///   <item>Cloud / multipart upload — only <see cref="ISequentialFile"/>
///   to a local file or memory.</item>
/// </list>
/// </summary>
public sealed class LanceFileWriter : IAsyncDisposable
{
    private const int MiniBlockAlignment = 8;

    // pylance aligns page-buffer file positions to 64 bytes (the typical
    // local-disk minimum-read sector for lance-rs). lance-rs's reader
    // assumes this alignment when computing buffer ranges, and panics on
    // unaligned input — so we mirror it.
    private const int PageBufferAlignment = 64;

    private readonly ISequentialFile _file;
    private readonly bool _ownsFile;
    private readonly LanceVersion _version;
    private readonly List<Proto.Field> _fields = new();
    private readonly List<ColumnMetadata> _columns = new();
    private long _totalRows = -1;
    private bool _finalized;
    private bool _disposed;

    private LanceFileWriter(ISequentialFile file, bool ownsFile, LanceVersion version)
    {
        _file = file;
        _ownsFile = ownsFile;
        _version = version;
    }

    /// <summary>
    /// Creates a writer over a caller-provided <see cref="ISequentialFile"/>.
    /// The caller controls the file's lifetime when <paramref name="ownsFile"/>
    /// is <c>false</c>.
    /// </summary>
    public static LanceFileWriter Create(ISequentialFile file, bool ownsFile = false)
    {
        if (file is null) throw new ArgumentNullException(nameof(file));
        return new LanceFileWriter(file, ownsFile, LanceVersion.V2_1);
    }

    /// <summary>
    /// Creates a writer that emits to <paramref name="path"/> on the local
    /// filesystem, overwriting any existing file. The returned writer owns
    /// the underlying file handle.
    /// </summary>
    public static async ValueTask<LanceFileWriter> CreateAsync(
        string path, CancellationToken cancellationToken = default)
    {
        var fs = new EngineeredWood.IO.Local.LocalTableFileSystem(
            System.IO.Path.GetDirectoryName(System.IO.Path.GetFullPath(path))!);
        var seq = await fs.CreateAsync(
            System.IO.Path.GetFileName(path), overwrite: true, cancellationToken).ConfigureAwait(false);
        return new LanceFileWriter(seq, ownsFile: true, LanceVersion.V2_1);
    }

    private static CompressiveEncoding FlatEncoding(int bitsPerValue) =>
        new() { Flat = new Proto.Encodings.V21.Flat { BitsPerValue = (ulong)bitsPerValue } };

    /// <summary>Append a non-nullable <see cref="sbyte"/> column.</summary>
    public Task WriteInt8ColumnAsync(string name, ReadOnlySpan<sbyte> values, CancellationToken cancellationToken = default)
        => WriteSinglePageColumnAsync(name, "int8", FlatEncoding(8), values.Length, MemoryMarshal.AsBytes(values).ToArray(), validityBitmap: null, nullCount: 0, cancellationToken);

    /// <summary>Append a non-nullable <see cref="byte"/> column.</summary>
    public Task WriteUInt8ColumnAsync(string name, ReadOnlySpan<byte> values, CancellationToken cancellationToken = default)
        => WriteSinglePageColumnAsync(name, "uint8", FlatEncoding(8), values.Length, values.ToArray(), validityBitmap: null, nullCount: 0, cancellationToken);

    /// <summary>Append a non-nullable <see cref="short"/> column.</summary>
    public Task WriteInt16ColumnAsync(string name, ReadOnlySpan<short> values, CancellationToken cancellationToken = default)
        => WriteSinglePageColumnAsync(name, "int16", FlatEncoding(16), values.Length, MemoryMarshal.AsBytes(values).ToArray(), validityBitmap: null, nullCount: 0, cancellationToken);

    /// <summary>Append a non-nullable <see cref="ushort"/> column.</summary>
    public Task WriteUInt16ColumnAsync(string name, ReadOnlySpan<ushort> values, CancellationToken cancellationToken = default)
        => WriteSinglePageColumnAsync(name, "uint16", FlatEncoding(16), values.Length, MemoryMarshal.AsBytes(values).ToArray(), validityBitmap: null, nullCount: 0, cancellationToken);

    /// <summary>
    /// Append a non-nullable <see cref="int"/> column to the file.
    /// Every column added must have the same length as the first; the
    /// length becomes the file's row count.
    /// </summary>
    public Task WriteInt32ColumnAsync(string name, ReadOnlySpan<int> values, CancellationToken cancellationToken = default)
        => WriteSinglePageColumnAsync(name, "int32", FlatEncoding(32), values.Length, MemoryMarshal.AsBytes(values).ToArray(), validityBitmap: null, nullCount: 0, cancellationToken);

    /// <summary>Append a non-nullable <see cref="uint"/> column.</summary>
    public Task WriteUInt32ColumnAsync(string name, ReadOnlySpan<uint> values, CancellationToken cancellationToken = default)
        => WriteSinglePageColumnAsync(name, "uint32", FlatEncoding(32), values.Length, MemoryMarshal.AsBytes(values).ToArray(), validityBitmap: null, nullCount: 0, cancellationToken);

    /// <summary>Append a non-nullable <see cref="long"/> column.</summary>
    public Task WriteInt64ColumnAsync(string name, ReadOnlySpan<long> values, CancellationToken cancellationToken = default)
        => WriteSinglePageColumnAsync(name, "int64", FlatEncoding(64), values.Length, MemoryMarshal.AsBytes(values).ToArray(), validityBitmap: null, nullCount: 0, cancellationToken);

    /// <summary>Append a non-nullable <see cref="ulong"/> column.</summary>
    public Task WriteUInt64ColumnAsync(string name, ReadOnlySpan<ulong> values, CancellationToken cancellationToken = default)
        => WriteSinglePageColumnAsync(name, "uint64", FlatEncoding(64), values.Length, MemoryMarshal.AsBytes(values).ToArray(), validityBitmap: null, nullCount: 0, cancellationToken);

    /// <summary>Append a non-nullable <see cref="float"/> (Float32) column.</summary>
    public Task WriteFloatColumnAsync(string name, ReadOnlySpan<float> values, CancellationToken cancellationToken = default)
        => WriteSinglePageColumnAsync(name, "float", FlatEncoding(32), values.Length, MemoryMarshal.AsBytes(values).ToArray(), validityBitmap: null, nullCount: 0, cancellationToken);

    /// <summary>Append a non-nullable <see cref="double"/> (Float64) column.</summary>
    public Task WriteDoubleColumnAsync(string name, ReadOnlySpan<double> values, CancellationToken cancellationToken = default)
        => WriteSinglePageColumnAsync(name, "double", FlatEncoding(64), values.Length, MemoryMarshal.AsBytes(values).ToArray(), validityBitmap: null, nullCount: 0, cancellationToken);

    /// <summary>
    /// Append an Apache Arrow column. Supported types:
    /// fixed-width primitives (Int8/16/32/64, UInt8/16/32/64, Float,
    /// Double) plus variable-length <see cref="StringArray"/> and
    /// <see cref="BinaryArray"/>. The array's null bitmap (if any) is
    /// converted to a Lance <c>NULLABLE_ITEM</c> def buffer; if the array
    /// has no nulls the page uses <c>ALL_VALID_ITEM</c>.
    /// </summary>
    public Task WriteColumnAsync(string name, IArrowArray array, CancellationToken cancellationToken = default)
    {
        if (array is null) throw new ArgumentNullException(nameof(array));

        // StringArray inherits from BinaryArray, so test it first; the only
        // wire-level difference is the schema's logical_type string.
        if (array is StringArray sa)
            return WriteVariableColumnAsync(name, "string", sa, cancellationToken);
        if (array is BinaryArray ba)
            return WriteVariableColumnAsync(name, "binary", ba, cancellationToken);

        var (logicalType, bitsPerValue, valueBytes) = array switch
        {
            Int8Array a => ("int8", 8, MemoryMarshal.AsBytes(a.Values).ToArray()),
            UInt8Array a => ("uint8", 8, a.Values.ToArray()),
            Int16Array a => ("int16", 16, MemoryMarshal.AsBytes(a.Values).ToArray()),
            UInt16Array a => ("uint16", 16, MemoryMarshal.AsBytes(a.Values).ToArray()),
            Int32Array a => ("int32", 32, MemoryMarshal.AsBytes(a.Values).ToArray()),
            UInt32Array a => ("uint32", 32, MemoryMarshal.AsBytes(a.Values).ToArray()),
            Int64Array a => ("int64", 64, MemoryMarshal.AsBytes(a.Values).ToArray()),
            UInt64Array a => ("uint64", 64, MemoryMarshal.AsBytes(a.Values).ToArray()),
            FloatArray a => ("float", 32, MemoryMarshal.AsBytes(a.Values).ToArray()),
            DoubleArray a => ("double", 64, MemoryMarshal.AsBytes(a.Values).ToArray()),
            _ => throw new NotSupportedException(
                $"LanceFileWriter doesn't yet support Arrow array type '{array.GetType().Name}'."),
        };

        return WriteSinglePageColumnAsync(
            name, logicalType, FlatEncoding(bitsPerValue), array.Length, valueBytes,
            ExtractValidityBitmap(array), array.NullCount, cancellationToken);
    }

    private static byte[]? ExtractValidityBitmap(IArrowArray array)
    {
        if (array.NullCount <= 0) return null;
        // Validity bitmap lives on Apache.Arrow.ArrayData.Buffers[0] for
        // every primitive/variable-length Arrow array. Spec: LSB-first,
        // bit set = valid.
        var span = array.Data.Buffers[0].Span;
        return span.IsEmpty ? null : span.ToArray();
    }

    private Task WriteVariableColumnAsync(
        string name, string logicalType, BinaryArray array,
        CancellationToken cancellationToken)
    {
        // Lance v2.1 Variable wire format (single chunk):
        //   value buffer = (numItems + 1) × u32 absolute offsets, then
        //                  concatenated data bytes
        // offsets[0] = (numItems + 1) * 4 (start of data section);
        // offsets[i+1] = offsets[i] + len(item i). For null items we still
        // emit an offset (with len = 0); the def buffer carries the null bit.
        int numItems = array.Length;
        int totalDataLen = 0;
        for (int i = 0; i < numItems; i++)
            totalDataLen += array.GetBytes(i).Length;
        int offsetsBytes = checked((numItems + 1) * sizeof(uint));
        int valueBufLen = checked(offsetsBytes + totalDataLen);

        byte[] valueBuf = new byte[valueBufLen];
        uint dataCursor = (uint)offsetsBytes;
        BinaryPrimitives.WriteUInt32LittleEndian(valueBuf.AsSpan(0, sizeof(uint)), dataCursor);
        for (int i = 0; i < numItems; i++)
        {
            var rowBytes = array.GetBytes(i);
            rowBytes.CopyTo(valueBuf.AsSpan((int)dataCursor));
            dataCursor += (uint)rowBytes.Length;
            BinaryPrimitives.WriteUInt32LittleEndian(
                valueBuf.AsSpan((i + 1) * sizeof(uint), sizeof(uint)), dataCursor);
        }

        var valueEncoding = new CompressiveEncoding
        {
            Variable = new Proto.Encodings.V21.Variable
            {
                Offsets = new CompressiveEncoding
                {
                    Flat = new Proto.Encodings.V21.Flat { BitsPerValue = 32 },
                },
                // values: leave unset = no further compression on the data bytes
            },
        };

        return WriteSinglePageColumnAsync(
            name, logicalType, valueEncoding, numItems, valueBuf,
            ExtractValidityBitmap(array), array.NullCount, cancellationToken);
    }

    /// <summary>
    /// Single-page MiniBlockLayout for any value-encoding shape with
    /// <c>num_buffers = 1</c> and a single chunk. Caller pre-builds
    /// <paramref name="valueBytes"/> in whatever layout
    /// <paramref name="valueEncoding"/> describes (Flat: little-endian
    /// fixed-width values; Variable: <c>(numItems+1)</c> u32 absolute
    /// offsets followed by raw data bytes). An optional Arrow-style LSB
    /// <paramref name="validityBitmap"/> (bit set = valid) emits a
    /// Flat(16) def buffer and the <c>NULLABLE_ITEM</c> layer; otherwise
    /// the page uses <c>ALL_VALID_ITEM</c>.
    /// </summary>
    private async Task WriteSinglePageColumnAsync(
        string name,
        string logicalType,
        CompressiveEncoding valueEncoding,
        int numItems,
        byte[] valueBytes,
        byte[]? validityBitmap,
        int nullCount,
        CancellationToken cancellationToken)
    {
        ThrowIfFinalized();
        if (_totalRows < 0) _totalRows = numItems;
        else if (_totalRows != numItems)
            throw new ArgumentException(
                $"Column '{name}' has {numItems} rows but earlier columns have {_totalRows}.",
                nameof(numItems));

        bool hasDef = nullCount > 0 && validityBitmap is not null;

        // --- Build the page's two buffers (chunk metadata + chunk data) ---
        // Chunk inner layout (v2.1 MiniBlockLayout, num_buffers = 1, no rep):
        //   u16 num_levels      (= numItems if hasDef, else 0)
        //   u16 def_size        (only if hasDef)
        //   u16 valueBufSize
        //   pad to 8-byte alignment
        //   def buffer          (only if hasDef; numItems × u16, padded to 8)
        //   value buffer        (valueBytes, padded to 8)
        int valueByteLen = valueBytes.Length;
        int chunkHeaderLen = hasDef ? 6 : 4;  // num_levels [+ def_size] + valueBufSize
        int chunkHeaderPadded = AlignUp(chunkHeaderLen, MiniBlockAlignment);
        int defByteLen = hasDef ? numItems * sizeof(ushort) : 0;
        int defPadded = hasDef ? AlignUp(defByteLen, MiniBlockAlignment) : 0;
        int chunkBeforePad = chunkHeaderPadded + defPadded + valueByteLen;
        int chunkTotal = AlignUp(chunkBeforePad, MiniBlockAlignment);
        int dividedBytes = (chunkTotal / MiniBlockAlignment) - 1;
        if (dividedBytes < 0 || dividedBytes > 0xFFF)
            throw new InvalidOperationException(
                $"Chunk byte length {chunkTotal} cannot be encoded as a v2.1 mini-block word.");

        // Buffer 0: chunk metadata. v2.1 uses u16 words: low 4 bits =
        // log_num_values (ignored for the last chunk; here the only chunk
        // is also the last), high 12 bits = divided_bytes.
        byte[] chunkMeta = new byte[2];
        ushort word = (ushort)((dividedBytes << 4) | 0);
        BinaryPrimitives.WriteUInt16LittleEndian(chunkMeta.AsSpan(0, 2), word);

        // Buffer 1: chunk data.
        byte[] chunkData = new byte[chunkTotal];
        int headCursor = 0;
        BinaryPrimitives.WriteUInt16LittleEndian(
            chunkData.AsSpan(headCursor, 2), checked((ushort)(hasDef ? numItems : 0)));
        headCursor += 2;
        if (hasDef)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(
                chunkData.AsSpan(headCursor, 2), checked((ushort)defByteLen));
            headCursor += 2;
        }
        BinaryPrimitives.WriteUInt16LittleEndian(
            chunkData.AsSpan(headCursor, 2), checked((ushort)valueByteLen));
        // remaining header padding bytes already zero
        if (hasDef)
        {
            // Convert Arrow LSB-first validity bitmap → Lance Flat(16) def
            // levels: bit set ⇒ def = 0 (valid); bit clear ⇒ def = 1 (null).
            Span<byte> defOut = chunkData.AsSpan(chunkHeaderPadded, defByteLen);
            for (int i = 0; i < numItems; i++)
            {
                bool valid = (validityBitmap![i >> 3] & (1 << (i & 7))) != 0;
                BinaryPrimitives.WriteUInt16LittleEndian(
                    defOut.Slice(i * sizeof(ushort), sizeof(ushort)),
                    valid ? (ushort)0 : (ushort)1);
            }
        }
        valueBytes.CopyTo(chunkData.AsSpan(chunkHeaderPadded + defPadded));
        // trailing padding already zero

        // --- Write the buffers, recording their absolute file offsets ---
        await PadToAlignmentAsync(PageBufferAlignment, cancellationToken).ConfigureAwait(false);
        long buf0Offset = _file.Position;
        await _file.WriteAsync(chunkMeta, cancellationToken).ConfigureAwait(false);
        long buf0Size = chunkMeta.Length;

        await PadToAlignmentAsync(PageBufferAlignment, cancellationToken).ConfigureAwait(false);
        long buf1Offset = _file.Position;
        await _file.WriteAsync(chunkData, cancellationToken).ConfigureAwait(false);
        long buf1Size = chunkData.Length;

        // --- Build the page's encoding (PageLayout → Any → DirectEncoding → Encoding) ---
        var miniBlock = new MiniBlockLayout
        {
            ValueCompression = valueEncoding,
            NumBuffers = 1,
            NumItems = (ulong)numItems,
        };
        if (hasDef)
        {
            // Reader expects Flat(16) for the def buffer per the v2.1 chunk
            // layout (each def level is a u16; 0 = valid, !=0 = null).
            miniBlock.DefCompression = new CompressiveEncoding
            {
                Flat = new Proto.Encodings.V21.Flat { BitsPerValue = 16 },
            };
            miniBlock.Layers.Add(RepDefLayer.RepdefNullableItem);
        }
        else
        {
            miniBlock.Layers.Add(RepDefLayer.RepdefAllValidItem);
        }

        var pageLayout = new PageLayout { MiniBlockLayout = miniBlock };

        var pageAny = new Any
        {
            // The reader doesn't validate this URL — it parses Any.value
            // directly as PageLayout. Use the upstream-style URL anyway so
            // a future tool that does check it sees the right thing.
            TypeUrl = "/lance.encodings21.PageLayout",
            Value = pageLayout.ToByteString(),
        };
        var pageEncoding = new Proto.V2.Encoding
        {
            Direct = new DirectEncoding { Encoding = pageAny.ToByteString() },
        };

        // --- Build the Page proto ---
        var page = new ColumnMetadata.Types.Page
        {
            Length = (ulong)numItems,
            Encoding = pageEncoding,
            Priority = 0,
        };
        page.BufferOffsets.Add((ulong)buf0Offset);
        page.BufferOffsets.Add((ulong)buf1Offset);
        page.BufferSizes.Add((ulong)buf0Size);
        page.BufferSizes.Add((ulong)buf1Size);

        // --- Column-level encoding (lance-rs's reader unwraps this) ---
        // For non-blob columns the official writer emits ColumnEncoding {
        // column_encoding = Values(Empty) }, then wraps it in Any and
        // stores Any-bytes inside DirectEncoding.encoding. The wrapping
        // layers exactly mirror the per-page Encoding above.
        var columnEncodingProto = new ColumnEncoding { Values = new Empty() };
        var columnAny = new Any
        {
            TypeUrl = "/lance.encodings.ColumnEncoding",
            Value = columnEncodingProto.ToByteString(),
        };
        var columnEncoding = new Proto.V2.Encoding
        {
            Direct = new DirectEncoding { Encoding = columnAny.ToByteString() },
        };

        // --- Build the ColumnMetadata + accumulate the schema field ---
        var columnMeta = new ColumnMetadata { Encoding = columnEncoding };
        columnMeta.Pages.Add(page);
        _columns.Add(columnMeta);

        int fieldId = _fields.Count;
        _fields.Add(new Proto.Field
        {
            Type = Proto.Field.Types.Type.Leaf,
            Name = name,
            Id = fieldId,
            // pylance marks top-level fields with parent_id = -1 (the
            // "no parent" sentinel). Setting parent_id = 0 would make the
            // second top-level field look like a child of the first
            // because the IsRoot check in our reader treats any
            // existing-field-id as a real reference.
            ParentId = -1,
            LogicalType = logicalType,
            // pylance writes nullable=true in the schema even when the
            // Arrow column is non-nullable; the actual presence of nulls is
            // encoded by the page's RepDefLayer (ALL_VALID_ITEM vs
            // NULLABLE_ITEM). Setting nullable=false here makes lance-rs
            // interpret the schema in a way that doesn't match the layer
            // shape we emit, so it panics on read.
            Nullable = true,
        });
    }

    /// <summary>
    /// Finalises the file: writes column metadata blobs, the CMO table,
    /// the FileDescriptor (global buffer 0), the GBO table, and the
    /// 40-byte footer with <c>"LANC"</c> magic. Once called, the writer
    /// rejects further <see cref="WriteInt32ColumnAsync"/> calls.
    /// </summary>
    public async Task FinishAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfFinalized();
        if (_columns.Count == 0)
            throw new InvalidOperationException(
                "Cannot finalise a Lance file with no columns.");

        // The order below is mandatory: lance-rs's reader assumes
        // `column_meta_start >= schema_start` and computes
        // `column_meta_start - schema_start` as a u64, panicking on
        // underflow. So the FileDescriptor (global buffer 0) MUST come
        // before the column metadata blobs.

        // --- FileDescriptor (global buffer 0) ---
        // Global buffer positions are 64-byte alignment-checked by the
        // reader for v2.1 (see do_decode_gbo_table). Page writes already
        // pad to PageBufferAlignment after each buffer, so the position
        // here is normally already 64-aligned, but be defensive.
        var fileDescriptor = new FileDescriptor
        {
            Schema = new Proto.Schema(),
            Length = checked((ulong)Math.Max(0L, _totalRows)),
        };
        foreach (var f in _fields)
            fileDescriptor.Schema.Fields.Add(f);

        await PadToAlignmentAsync(PageBufferAlignment, cancellationToken).ConfigureAwait(false);
        long fdOffset = _file.Position;
        byte[] fdBytes = fileDescriptor.ToByteArray();
        await _file.WriteAsync(fdBytes, cancellationToken).ConfigureAwait(false);

        // --- Column metadata blobs (back-to-back, no padding) ---
        long columnMetaStart = _file.Position;
        var columnEntries = new OffsetSizeEntry[_columns.Count];
        for (int i = 0; i < _columns.Count; i++)
        {
            long pos = _file.Position;
            byte[] bytes = _columns[i].ToByteArray();
            await _file.WriteAsync(bytes, cancellationToken).ConfigureAwait(false);
            columnEntries[i] = new OffsetSizeEntry(pos, bytes.Length);
        }

        // --- CMO table ---
        long cmoTableOffset = _file.Position;
        {
            byte[] cmoTable = new byte[_columns.Count * OffsetSizeEntry.Bytes];
            for (int i = 0; i < _columns.Count; i++)
                columnEntries[i].WriteTo(cmoTable.AsSpan(i * OffsetSizeEntry.Bytes));
            await _file.WriteAsync(cmoTable, cancellationToken).ConfigureAwait(false);
        }

        // --- GBO table (single entry: FileDescriptor) ---
        long gboTableOffset = _file.Position;
        {
            byte[] gboTable = new byte[OffsetSizeEntry.Bytes];
            new OffsetSizeEntry(fdOffset, fdBytes.Length).WriteTo(gboTable);
            await _file.WriteAsync(gboTable, cancellationToken).ConfigureAwait(false);
        }

        // --- Footer (40 bytes) ---
        var footer = new LanceFooter(
            ColumnMetaStart: columnMetaStart,
            CmoTableOffset: cmoTableOffset,
            GboTableOffset: gboTableOffset,
            NumGlobalBuffers: 1,
            NumColumns: _columns.Count,
            Version: _version);
        byte[] footerBytes = new byte[LanceFooter.Size];
        footer.WriteTo(footerBytes);
        await _file.WriteAsync(footerBytes, cancellationToken).ConfigureAwait(false);

        await _file.FlushAsync(cancellationToken).ConfigureAwait(false);
        _finalized = true;
    }

    private async ValueTask PadToAlignmentAsync(int alignment, CancellationToken cancellationToken)
    {
        long pos = _file.Position;
        long aligned = (pos + (alignment - 1)) & ~(alignment - 1L);
        int padBytes = checked((int)(aligned - pos));
        if (padBytes == 0) return;
        byte[] zeros = new byte[padBytes];
        await _file.WriteAsync(zeros, cancellationToken).ConfigureAwait(false);
    }

    private static int AlignUp(int value, int alignment) =>
        (value + (alignment - 1)) & ~(alignment - 1);

    private void ThrowIfFinalized()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_finalized)
            throw new InvalidOperationException(
                "LanceFileWriter has already been finalised; no further writes are allowed.");
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        if (_ownsFile)
            await _file.DisposeAsync().ConfigureAwait(false);
    }
}
