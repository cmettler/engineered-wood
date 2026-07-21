// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Row tracking + optimistic-concurrency rebase (the other half of "limitation 2"). An append or a
/// copy-on-write UPDATE on a <c>delta.enableRowTracking=true</c> table used to ABORT on any rebase, because a
/// rebased fresh file's <c>baseRowId</c> would collide with row-id space a concurrent commit consumed. It now
/// rebases: <see cref="DeltaTable.CommitOccAsync"/> RE-DERIVES each post-image add's <c>baseRowId</c> from the
/// advanced high-water mark (and its <c>defaultRowCommitVersion</c> from the new version), rebuilding the
/// <c>delta.rowTracking</c> domain to match. These tests pin that the re-derived ids are contiguous and
/// non-overlapping — the corruption the old abort was protecting against.
///
/// <para>Row-level (same-file, disjoint-row) DELETE concurrency is a separate concern in
/// <see cref="RowLevelConcurrencyTests"/>; this covers fresh-file (append / rewrite post-image) rebases.</para>
/// </summary>
public class RowTrackingRebaseTests : IDisposable
{
    private readonly string _tempDir;

    public RowTrackingRebaseTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_rtrebase_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static Apache.Arrow.Schema Schema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("value", StringType.Default, true))
        .Build();

    private static RecordBatch Batch(long startId, int count)
    {
        var ids = new Int64Array.Builder();
        var values = new StringArray.Builder();
        for (int i = 0; i < count; i++)
        {
            ids.Append(startId + i);
            values.Append("v" + (startId + i));
        }
        return new RecordBatch(Schema, [ids.Build(), values.Build()], count);
    }

    private static Func<RecordBatch, BooleanArray> IdEquals(long target) => batch =>
    {
        var id = (Int64Array)batch.Column("id");
        var mask = new BooleanArray.Builder();
        for (int i = 0; i < id.Length; i++)
            mask.Append(id.GetValue(i) == target);
        return mask.Build();
    };

    private Task<DeltaTable> OpenAsync() =>
        DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir)).AsTask();

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

    /// <summary>
    /// Two concurrent appends to a row-tracking table both land; the loser rebases and its file's baseRowId is
    /// re-derived so it does NOT overlap the winner's. Before this fix the loser aborted with a conflict.
    /// </summary>
    [Fact]
    public async Task ConcurrentAppends_RowTracking_LoserRebases_BaseRowIdsContiguous()
    {
        await using (var setup = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), Schema, enableRowTracking: true))
        {
            // create only — v0, no data, high-water mark starts at 0
        }

        await using var tableA = await OpenAsync();
        await using var tableB = await OpenAsync();
        long baseVersion = tableA.CurrentSnapshot.Version;

        long vA = await tableA.WriteAsync([Batch(10, 2)]);        // file: baseRowId 0, rows 0,1
        long vB = await tableB.WriteAsync([Batch(30, 1)]);        // stale -> collides -> rebases

        Assert.Equal(baseVersion + 1, vA);
        Assert.Equal(baseVersion + 2, vB); // landed one past the winner, not thrown

        await using var check = await OpenAsync();
        var files = check.CurrentSnapshot.ActiveFiles.Values
            .OrderBy(f => f.BaseRowId ?? -1).ToList();
        Assert.Equal(2, files.Count);
        Assert.Equal(0L, files[0].BaseRowId);
        Assert.Equal(1L, files[0].DefaultRowCommitVersion);
        Assert.Equal(2L, files[1].BaseRowId);              // re-derived past the winner's 2 rows, no overlap
        Assert.Equal(baseVersion + 2, files[1].DefaultRowCommitVersion); // the version it actually committed at
        Assert.Equal(3L, check.CurrentSnapshot.RowIdHighWaterMark); // 3 rows -> next id 3

        Assert.Equal([10L, 11L, 30L], await ReadIdsFresh());
    }

    /// <summary>
    /// The transactional append path composes with the same rebase: a transaction stages an append, a
    /// concurrent append lands, and the transaction rebases (re-deriving its baseRowId) at commit.
    /// </summary>
    [Fact]
    public async Task Transaction_RowTrackingAppend_RebasesPastConcurrentAppend()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, Schema, enableRowTracking: true);
        await table.WriteAsync([Batch(1, 3)]); // v1: file baseRowId 0, rows 0,1,2
        long baseVersion = table.CurrentSnapshot.Version;

        var tx = table.StartTransaction();
        await tx.WriteAsync([Batch(100, 2)]); // staged against v1

        // A concurrent append lands through the same handle while tx is open.
        await table.WriteAsync([Batch(50, 1)]); // v2: baseRowId 3, row 3

        long committed = await tx.CommitAsync(); // rebases: its baseRowId re-derived past the concurrent append
        Assert.Equal(baseVersion + 2, committed);

        await using var check = await OpenAsync();
        var txFile = check.CurrentSnapshot.ActiveFiles.Values
            .OrderByDescending(f => f.BaseRowId ?? -1).First();
        Assert.Equal(4L, txFile.BaseRowId);            // 0..2 original, 3 concurrent -> tx starts at 4
        Assert.Equal(committed, txFile.DefaultRowCommitVersion);
        Assert.Equal(6L, check.CurrentSnapshot.RowIdHighWaterMark); // 3 + 1 + 2 rows

        Assert.Equal([1L, 2L, 3L, 50L, 100L, 101L], await ReadIdsFresh());
    }

    /// <summary>
    /// A copy-on-write UPDATE rebases past a concurrent append: the update reads and rewrites its own file
    /// (untouched by the append, so no conflict), and its post-image add's baseRowId is re-derived past the
    /// row-id space the concurrent append consumed — while the rewritten rows keep their ORIGINAL materialized
    /// ids. Before the fix the update aborted on the rebase.
    /// </summary>
    [Fact]
    public async Task ConcurrentAppendAndUpdate_RowTracking_UpdateRebases_ReDerivesPostImageId()
    {
        await using (var setup = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), Schema, enableRowTracking: true))
        {
            await setup.WriteAsync([Batch(1, 3)]); // v1: file F1 baseRowId 0, ids 0,1,2
        }

        await using var tableA = await OpenAsync();
        await using var tableB = await OpenAsync();

        long vA = await tableA.WriteAsync([Batch(4, 1)]); // v2: new file, baseRowId 3, id 3

        // B (stale) updates id 2 -> a copy-on-write rewrite of F1; collides with A's append at v2, rebases.
        var (updated, vB) = await tableB.UpdateAsync(
            IdEquals(2),
            b =>
            {
                var id = (Int64Array)b.Column("id");
                var vals = new StringArray.Builder();
                for (int i = 0; i < b.Length; i++)
                    vals.Append(id.GetValue(i) == 2 ? "updated" : ((StringArray)b.Column("value")).GetString(i));
                return new RecordBatch(Schema, [b.Column("id"), vals.Build()], b.Length);
            });
        Assert.Equal(1, updated);
        Assert.True(vB > vA); // rebased past the concurrent append rather than aborting

        await using var check = await OpenAsync();
        // Active files: A's append (baseRowId 3, 1 row) and B's rewrite of F1 (post-image, 3 rows). The
        // rewrite's baseRowId is re-derived past A's consumed id 3 -> 4.
        var rewrite = check.CurrentSnapshot.ActiveFiles.Values
            .OrderByDescending(f => f.BaseRowId ?? -1).First();
        Assert.Equal(4L, rewrite.BaseRowId);
        Assert.Equal(vB, rewrite.DefaultRowCommitVersion);
        Assert.Equal(7L, check.CurrentSnapshot.RowIdHighWaterMark); // 3(orig) + 1(append) + 3(rewrite) = 7

        Assert.Equal([1L, 2L, 3L, 4L], await ReadIdsFresh());
    }
}
