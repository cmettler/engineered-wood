// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The read-side TRANSIENT row-id surface — <see cref="DeltaTable.ReadAllWithRowIdsAsync"/> /
/// <see cref="DeltaTable.ReadAtVersionWithRowIdsAsync"/> append a trailing <c>_metadata.row_id</c> =
/// <c>(fileOrdinal &lt;&lt; 40) | absolutePosition</c>, and <see cref="DeltaTable.OrderedActiveBaseRowIdsAsync"/>
/// gives the per-ordinal <c>baseRowId</c>. NOT a stable Delta id — it round-trips WITHIN a snapshot to the
/// row-id DML surface (a host reads rows, keeps the ids, then deletes/updates exactly those rows). This is the
/// maximally reader-compatible copy-on-write path: no deletion vectors or row tracking required on the table.
/// </summary>
public class ReadWithRowIdsTests : IDisposable
{
    private readonly string _tempDir;

    public ReadWithRowIdsTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_readrowid_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private const int RowIdPositionBits = 40;

    private static Apache.Arrow.Schema IdSchema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Build();

    private static RecordBatch Batch(long startId, int count)
    {
        var ids = new Int64Array.Builder();
        for (int i = 0; i < count; i++)
            ids.Append(startId + i);
        return new RecordBatch(IdSchema, [ids.Build(), ], count);
    }

    private Task<DeltaTable> OpenAsync() => DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir)).AsTask();

    /// <summary>Reads every (id, transientRowId) pair the with-rowids read surfaces.</summary>
    private static async Task<List<(long Id, long RowId)>> ReadWithIds(
        IAsyncEnumerable<RecordBatch> batches)
    {
        var rows = new List<(long, long)>();
        await foreach (var batch in batches)
        {
            var id = (Int64Array)batch.Column("id");
            var rid = (Int64Array)batch.Column("_metadata.row_id");
            for (int i = 0; i < batch.Length; i++)
                rows.Add((id.GetValue(i)!.Value, rid.GetValue(i)!.Value));
        }
        return rows;
    }

    [Fact]
    public async Task ReadAllWithRowIds_SingleFile_EncodesOrdinalZeroAndPosition()
    {
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdSchema);
        await table.WriteAsync([Batch(10, 4)]); // ids 10..13, one file → ordinal 0, positions 0..3

        var rows = await ReadWithIds(table.ReadAllWithRowIdsAsync(null, null));
        rows.Sort();
        Assert.Equal(new long[] { 10, 11, 12, 13 }, rows.Select(r => r.Id).ToArray());
        // ordinal 0 → transient ids are exactly the in-file positions 0..3
        Assert.Equal(new long[] { 0, 1, 2, 3 }, rows.Select(r => r.RowId).ToArray());
    }

    [Fact]
    public async Task ReadAllWithRowIds_MultiFile_EncodesPerFileOrdinal()
    {
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdSchema);
        await table.WriteAsync([Batch(1, 2)]); // file A
        await table.WriteAsync([Batch(100, 3)]); // file B — a second active file

        var rows = await ReadWithIds(table.ReadAllWithRowIdsAsync(null, null));

        // Each transient id decodes to (path-sorted ordinal, in-file position). Two files → ordinals 0 and 1;
        // every id maps back to exactly one file, and positions restart per file.
        var byOrdinal = rows
            .GroupBy(r => (int)(r.RowId >> RowIdPositionBits))
            .OrderBy(g => g.Key).ToList();
        Assert.Equal(2, byOrdinal.Count);
        foreach (var g in byOrdinal)
        {
            var positions = g.Select(r => r.RowId & ((1L << RowIdPositionBits) - 1)).OrderBy(p => p).ToArray();
            Assert.Equal(Enumerable.Range(0, g.Count()).Select(i => (long)i).ToArray(), positions);
        }
    }

    [Fact]
    public async Task ReadAllWithRowIds_RoundTripsToReadRowsByRowIds()
    {
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdSchema);
        await table.WriteAsync([Batch(1, 6)]); // ids 1..6

        var rows = await ReadWithIds(table.ReadAllWithRowIdsAsync(null, null));
        // pick the transient ids of ids 2 and 5 and read them straight back
        var picked = rows.Where(r => r.Id is 2 or 5).Select(r => r.RowId).ToList();
        Assert.Equal(2, picked.Count);

        var readBack = new List<long>();
        await foreach (var batch in table.ReadRowsByRowIdsAsync(picked))
        {
            var id = (Int64Array)batch.Column("id");
            for (int i = 0; i < batch.Length; i++)
                readBack.Add(id.GetValue(i)!.Value);
        }
        readBack.Sort();
        Assert.Equal(new long[] { 2, 5 }, readBack);
    }

    [Fact]
    public async Task ReadAllWithRowIds_RoundTripsToDeletionVectorDml()
    {
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdSchema, enableDeletionVectors: true);
        await table.WriteAsync([Batch(1, 6)]); // ids 1..6

        // a host reads the rows, keeps the transient ids, and deletes ids 3 and 4 by those ids
        var rows = await ReadWithIds(table.ReadAllWithRowIdsAsync(null, null));
        var toDelete = rows.Where(r => r.Id is 3 or 4).Select(r => r.RowId).ToList();

        // decode the transient ids into positionsByOrdinal and drive the DV DELETE
        long posMask = (1L << RowIdPositionBits) - 1;
        var positionsByOrdinal = toDelete
            .GroupBy(rid => (int)(rid >> RowIdPositionBits))
            .ToDictionary(g => g.Key, g => (IReadOnlyCollection<long>)g.Select(rid => rid & posMask).ToList());

        var pinned = table.CurrentSnapshot;
        var (dvActions, deleted) = await table.ComputeDeletionVectorActionsAsync(
            positionsByOrdinal, resolveAgainst: pinned);
        Assert.Equal(2, deleted);
        await table.CommitDataFilesAsync([], DeltaWriteMode.Append,
            extraActions: dvActions, expectedVersion: pinned.Version, operation: "DELETE");

        var remaining = new List<long>();
        await using (var check = await OpenAsync())
        {
            await foreach (var batch in check.ReadAllAsync())
            {
                var id = (Int64Array)batch.Column("id");
                for (int i = 0; i < batch.Length; i++)
                    remaining.Add(id.GetValue(i)!.Value);
            }
        }
        remaining.Sort();
        Assert.Equal(new long[] { 1, 2, 5, 6 }, remaining); // ids 3, 4 gone
    }

    [Fact]
    public async Task OrderedActiveBaseRowIds_RowTrackingTable_ReturnsBaseRowIdPerOrdinal()
    {
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdSchema, enableRowTracking: true);
        await table.WriteAsync([Batch(1, 3)]); // file A → baseRowId 0
        await table.WriteAsync([Batch(100, 2)]); // file B → baseRowId 3

        var baseIds = await table.OrderedActiveBaseRowIdsAsync();
        Assert.Equal(2, baseIds.Count);
        // path-sorted ordinal order; the HWM advanced 0 → 3 → 5
        Assert.Equal(new long?[] { 0, 3 }, baseIds.OrderBy(x => x!.Value).ToArray());
    }

    [Fact]
    public async Task ReadAtVersionWithRowIds_PastVersion_ReadsThatVersionsFiles()
    {
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdSchema);
        await table.WriteAsync([Batch(1, 3)]); // v1: ids 1..3
        long v1 = table.CurrentSnapshot.Version;
        await table.WriteAsync([Batch(100, 2)]); // v2 adds more

        var rows = await ReadWithIds(table.ReadAtVersionWithRowIdsAsync(v1, null, null));
        rows.Sort();
        Assert.Equal(new long[] { 1, 2, 3 }, rows.Select(r => r.Id).ToArray());
        Assert.Equal(new long[] { 0, 1, 2 }, rows.Select(r => r.RowId).ToArray()); // one file at v1
    }
}
