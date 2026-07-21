// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The one-shot DML-by-TRANSIENT-rowid surface: a host reads rows with
/// <see cref="DeltaTable.ReadAllWithRowIdsAsync"/>, keeps the ids, and deletes/updates exactly those rows —
/// <see cref="DeltaTable.DeleteByRowIdsViaVectorsAsync"/> (deletion-vector soft delete, row-level-concurrency
/// aware) and the copy-on-write <see cref="DeltaTable.DeleteByRowIdsAsync"/> /
/// <see cref="DeltaTable.UpdateByRowIdsAsync"/> (file rewrite, no DVs — maximally reader-compatible).
/// </summary>
public class RowIdDmlTests : IDisposable
{
    private readonly string _tempDir;

    public RowIdDmlTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_rowiddml_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static Apache.Arrow.Schema IdSchema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Build();

    private static RecordBatch Batch(long startId, int count)
    {
        var ids = new Int64Array.Builder();
        for (int i = 0; i < count; i++)
            ids.Append(startId + i);
        return new RecordBatch(IdSchema, [ids.Build()], count);
    }

    private Task<DeltaTable> OpenAsync() => DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir)).AsTask();

    /// <summary>Transient rowids of the rows whose id is in <paramref name="ids"/>, in the current snapshot.</summary>
    private static async Task<List<long>> RowIdsOf(DeltaTable table, params long[] ids)
    {
        var wanted = new HashSet<long>(ids);
        var result = new List<long>();
        await foreach (var batch in table.ReadAllWithRowIdsAsync(null, null))
        {
            var id = (Int64Array)batch.Column("id");
            var rid = (Int64Array)batch.Column("_metadata.row_id");
            for (int i = 0; i < batch.Length; i++)
                if (wanted.Contains(id.GetValue(i)!.Value))
                    result.Add(rid.GetValue(i)!.Value);
        }
        return result;
    }

    private async Task<List<long>> ReadIdsFresh()
    {
        await using var reader = await OpenAsync();
        var ids = new List<long>();
        await foreach (var batch in reader.ReadAllAsync())
        {
            var col = (Int64Array)batch.Column("id");
            for (int i = 0; i < batch.Length; i++)
                ids.Add(col.GetValue(i)!.Value);
        }
        ids.Sort();
        return ids;
    }

    [Fact]
    public async Task DeleteByRowIdsViaVectors_DeletesExactlyThoseRows()
    {
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdSchema, enableDeletionVectors: true);
        await table.WriteAsync([Batch(1, 6)]); // ids 1..6

        var ids = await RowIdsOf(table, 3, 4);
        var (deleted, _) = await table.DeleteByRowIdsViaVectorsAsync(ids);
        Assert.Equal(2, deleted);

        Assert.Equal(new long[] { 1, 2, 5, 6 }, await ReadIdsFresh());
    }

    [Fact]
    public async Task DeleteByRowIdsViaVectors_NonDvTable_Throws()
    {
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdSchema);
        await table.WriteAsync([Batch(1, 3)]);
        var ids = await RowIdsOf(table, 2);
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await table.DeleteByRowIdsViaVectorsAsync(ids));
    }

    [Fact]
    public async Task DeleteByRowIdsViaVectors_RowLevelRetry_ConcurrentDisjoint_BothLand()
    {
        await using (var setup = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdSchema, enableDeletionVectors: true))
        {
            await setup.WriteAsync([Batch(1, 6)]); // one file, ids 1..6
        }

        // two handles at the same base version, each deleting a DISJOINT row of the SAME file by rowid
        await using var tableA = await OpenAsync();
        await using var tableB = await OpenAsync();
        var idsA = await RowIdsOf(tableA, 2);
        var idsB = await RowIdsOf(tableB, 5);

        await tableA.DeleteByRowIdsViaVectorsAsync(idsA, rowLevelRetry: true);
        await tableB.DeleteByRowIdsViaVectorsAsync(idsB, rowLevelRetry: true); // stale → row-level rebase (DV union)

        Assert.Equal(new long[] { 1, 3, 4, 6 }, await ReadIdsFresh()); // both deletes composed
    }

    [Fact]
    public async Task DeleteByRowIdsViaVectors_EmptyIds_NoOp()
    {
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdSchema, enableDeletionVectors: true);
        await table.WriteAsync([Batch(1, 3)]);
        long before = table.CurrentSnapshot.Version;
        var (deleted, version) = await table.DeleteByRowIdsViaVectorsAsync([]);
        Assert.Equal(0, deleted);
        Assert.Equal(before, version);
    }
}
