// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// ROW-LEVEL CONCURRENCY (the Databricks-style capability, and beyond): concurrent DML touching the
/// SAME data file composes when the touched ROWS are disjoint, instead of failing the file-level
/// delete/delete check. Two mechanisms, both exercised here through the public API:
///
/// <list type="bullet">
/// <item><b>v1 — deletion-vector re-union</b> (<see cref="DeltaTable.RebaseDvDmlActionsAsync"/>, driven
/// by the <c>rowLevelRetry</c> flag on <see cref="DeltaTable.DeleteByRowIdsViaVectorsAsync"/> /
/// <see cref="DeltaTable.UpdateByRowIdsAsync"/>): a concurrent writer swapped the file's DV — the
/// loser's positions are checked DISJOINT against the concurrent deletions (absolute in-file positions
/// are stable across DV swaps) and the DVs union. Same-row overlap ⇒ row-level conflict.</item>
/// <item><b>v2 — remap across rewrites</b> (<c>RemapRowsAcrossRewriteAsync</c>, automatic within the
/// rebase): a concurrent compaction/copy-on-write REPLACED the file — the rows are relocated by STABLE
/// ROW ID (materialized <c>__delta_row_id</c>, else baseRowId+position), with the row's COMMIT VERSION
/// as the concurrent-modification discriminator (relocated-untouched keeps its original version; a
/// concurrently updated/deleted row conflicts). Databricks' own row-level concurrency still conflicts
/// with compaction — the remap goes beyond it. Requires row tracking.</item>
/// </list>
///
/// The rowid encoding used throughout: <c>(fileOrdinal &lt;&lt; 40) | absolutePositionInFile</c>, from
/// <see cref="DeltaTable.ReadAllWithRowIdsAsync"/>.
/// </summary>
public class RowLevelConcurrencyTests : IDisposable
{
    private readonly string _tempDir;

    public RowLevelConcurrencyTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_rlc_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    // Deletion vectors + row tracking with materialized columns — the configuration the remap relies on
    // (compaction and copy-on-write preserve each row's ORIGINAL id + commit version in the rewritten file).
    private static readonly Dictionary<string, string> TableConfig = new()
    {
        ["delta.enableDeletionVectors"] = "true",
        ["delta.enableRowTracking"] = "true",
        ["delta.rowTracking.materializedRowIdColumnName"] = "__delta_row_id",
        ["delta.rowTracking.materializedRowCommitVersionColumnName"] = "__delta_row_commit_version",
    };

    private static Apache.Arrow.Schema BuildSchema() => new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("value", StringType.Default, true))
        .Build();

    private static RecordBatch BuildBatch(Apache.Arrow.Schema schema, long startId, int count)
    {
        var ids = new Int64Array.Builder();
        var values = new StringArray.Builder();
        for (int i = 0; i < count; i++)
        {
            ids.Append(startId + i);
            values.Append("v" + (startId + i));
        }
        return new RecordBatch(schema, [ids.Build(), values.Build()], count);
    }

    /// <summary>Creates the table with rows id 1..<paramref name="rows"/> in ONE data file.</summary>
    private async Task<DeltaTable> CreateTableAsync(int rows)
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = BuildSchema();
        var table = await DeltaTable.CreateAsync(fs, schema, configuration: TableConfig);
        await table.WriteAsync([BuildBatch(schema, 1, rows)]);
        return table;
    }

    private Task<DeltaTable> OpenAsync() => DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir)).AsTask();

    /// <summary>The transient rowids of the rows whose <c>id</c> column matches, on THIS handle's snapshot.</summary>
    private static async Task<List<long>> RowIdsOfAsync(DeltaTable table, params long[] dataIds)
    {
        var wanted = new HashSet<long>(dataIds);
        var result = new List<long>();
        await foreach (var batch in table.ReadAllWithRowIdsAsync(null, null))
        {
            var ids = (Int64Array)batch.Column(0);
            var rowIds = (Int64Array)batch.Column(batch.ColumnCount - 1);
            for (int i = 0; i < batch.Length; i++)
            {
                if (!ids.IsNull(i) && wanted.Contains(ids.GetValue(i)!.Value))
                    result.Add(rowIds.GetValue(i)!.Value);
            }
        }
        Assert.Equal(dataIds.Length, result.Count);
        return result;
    }

    private static async Task<List<long>> ReadIdsAsync(DeltaTable table)
    {
        var result = new List<long>();
        await foreach (var batch in table.ReadAllAsync())
        {
            var ids = (Int64Array)batch.Column(0);
            for (int i = 0; i < batch.Length; i++)
                result.Add(ids.GetValue(i)!.Value);
        }
        result.Sort();
        return result;
    }

    // ---- v1: deletion-vector re-union ----

    [Fact]
    public async Task ConcurrentDeletes_SameFile_DisjointRows_BothLand()
    {
        await using var stale = await CreateTableAsync(10);
        var staleRowIds = await RowIdsOfAsync(stale, 2);

        // A concurrent writer deletes a DIFFERENT row of the SAME file (swapping its deletion vector).
        await using (var racer = await OpenAsync())
        {
            await racer.DeleteByRowIdsViaVectorsAsync(await RowIdsOfAsync(racer, 4));
        }

        // The stale handle's delete conflicts on the version — rowLevelRetry rebases: the touched rows
        // are disjoint, so the deletion vectors UNION and the commit lands.
        var (deleted, _) = await stale.DeleteByRowIdsViaVectorsAsync(staleRowIds, rowLevelRetry: true);
        Assert.Equal(1, deleted);

        await using var check = await OpenAsync();
        Assert.Equal(new long[] { 1, 3, 5, 6, 7, 8, 9, 10 }, await ReadIdsAsync(check));
    }

    [Fact]
    public async Task ConcurrentDeletes_SameRow_RowLevelConflict()
    {
        await using var stale = await CreateTableAsync(10);
        var staleRowIds = await RowIdsOfAsync(stale, 6);

        await using (var racer = await OpenAsync())
        {
            await racer.DeleteByRowIdsViaVectorsAsync(await RowIdsOfAsync(racer, 6));
        }

        var ex = await Assert.ThrowsAsync<DeltaConflictException>(async () =>
            await stale.DeleteByRowIdsViaVectorsAsync(staleRowIds, rowLevelRetry: true));
        Assert.Contains("row-level conflict", ex.Message);

        // First committer won — the row is deleted exactly once, nothing else changed.
        await using var check = await OpenAsync();
        Assert.Equal(9, (await ReadIdsAsync(check)).Count);
    }

    [Fact]
    public async Task WithoutRowLevelRetry_VersionConflictSurfaces()
    {
        // Default (rowLevelRetry: false) keeps the strict pre-existing behavior: the first version
        // conflict surfaces — the serializable isolation level's contract.
        await using var stale = await CreateTableAsync(10);
        var staleRowIds = await RowIdsOfAsync(stale, 2);

        await using (var racer = await OpenAsync())
        {
            await racer.DeleteByRowIdsViaVectorsAsync(await RowIdsOfAsync(racer, 4));
        }

        await Assert.ThrowsAsync<DeltaConflictException>(async () =>
            await stale.DeleteByRowIdsViaVectorsAsync(staleRowIds));
    }

    [Fact]
    public async Task ConcurrentUpdateAndDelete_DisjointRows_BothLand()
    {
        await using var stale = await CreateTableAsync(10);
        var updateRowIds = await RowIdsOfAsync(stale, 8);

        await using (var racer = await OpenAsync())
        {
            await racer.DeleteByRowIdsViaVectorsAsync(await RowIdsOfAsync(racer, 9));
        }

        // Merge-on-read UPDATE (DV-delete the old row + append the post-image) on the stale handle:
        // the rebase re-unions the DV AND re-derives the post-image's baseRowId from the latest snapshot.
        var targets = new HashSet<long>(updateRowIds);
        await stale.UpdateByRowIdsAsync(updateRowIds, (ordinal, batches) =>
        {
            var result = new List<RecordBatch>(batches.Count);
            foreach (var batch in batches)
            {
                var ids = (Int64Array)batch.Column(0);
                var rowIds = (Int64Array)batch.Column(batch.ColumnCount - 1);
                var values = new StringArray.Builder();
                for (int i = 0; i < batch.Length; i++)
                    values.Append(targets.Contains(rowIds.GetValue(i)!.Value) ? "updated" : "v" + ids.GetValue(i));
                // user columns only — the trailing rowid column is dropped from the returned batches
                result.Add(new RecordBatch(BuildSchema(), [batch.Column(0), values.Build()], batch.Length));
            }
            return result;
        }, rowLevelRetry: true);

        await using var check = await OpenAsync();
        Assert.Equal(9, (await ReadIdsAsync(check)).Count); // id 9 deleted by the racer; id 8 updated
        await foreach (var batch in check.ReadAllAsync())
        {
            var ids = (Int64Array)batch.Column(0);
            var values = (StringArray)batch.Column(1);
            for (int i = 0; i < batch.Length; i++)
            {
                if (ids.GetValue(i) == 8)
                    Assert.Equal("updated", values.GetString(i));
            }
        }
    }

    // ---- v2: remap across a concurrent rewrite ----

    [Fact]
    public async Task DeleteThroughConcurrentCompaction_Remapped()
    {
        await using var stale = await CreateTableAsync(6);
        // second file so the compaction has something to merge
        await stale.WriteAsync([BuildBatch(BuildSchema(), 7, 4)]);
        var staleRowIds = await RowIdsOfAsync(stale, 3);

        // A concurrent OPTIMIZE replaces both files with one compacted file (materialized ids preserved).
        await using (var racer = await OpenAsync())
        {
            await racer.CompactAsync(new CompactionOptions { MinFileSize = long.MaxValue });
        }

        // The stale delete's file is GONE — the row is remapped by stable id onto the compacted file.
        var (deleted, _) = await stale.DeleteByRowIdsViaVectorsAsync(staleRowIds, rowLevelRetry: true);
        Assert.Equal(1, deleted);

        await using var check = await OpenAsync();
        Assert.Equal(new long[] { 1, 2, 4, 5, 6, 7, 8, 9, 10 }, await ReadIdsAsync(check));
    }

    [Fact]
    public async Task DeleteThroughCompaction_RowConcurrentlyDeleted_RowLevelConflict()
    {
        await using var stale = await CreateTableAsync(6);
        await stale.WriteAsync([BuildBatch(BuildSchema(), 7, 4)]);
        var staleRowIds = await RowIdsOfAsync(stale, 5);

        // The racer compacts AND deletes the same row — its stable id is gone from the post-rewrite state.
        await using (var racer = await OpenAsync())
        {
            await racer.CompactAsync(new CompactionOptions { MinFileSize = long.MaxValue });
            await racer.DeleteByRowIdsViaVectorsAsync(await RowIdsOfAsync(racer, 5));
        }

        var ex = await Assert.ThrowsAsync<DeltaConflictException>(async () =>
            await stale.DeleteByRowIdsViaVectorsAsync(staleRowIds, rowLevelRetry: true));
        Assert.Contains("row-level conflict", ex.Message);

        await using var check = await OpenAsync();
        Assert.Equal(9, (await ReadIdsAsync(check)).Count); // deleted exactly once
    }

    // ---- the buffered-transaction pattern (what a multi-statement consumer drives explicitly) ----

    [Fact]
    public async Task BufferedFlow_ComputeThenRebaseThenCommit_ComposesWithConcurrentDelete()
    {
        // The consumer shape: DML positions are captured against a PINNED snapshot; at COMMIT the
        // actions rebase onto the latest snapshot and land in one commit (with the fused appends via
        // CommitDataFilesAsync(extraActions:)).
        await using var table = await CreateTableAsync(10);
        var pinned = table.CurrentSnapshot;

        // positions of id=2 in the pinned snapshot (single file → ordinal 0, position = id - 1)
        var positions = new Dictionary<int, IReadOnlyCollection<long>> { [0] = new long[] { 1 } };
        var (actions, rowsDeleted) = await table.ComputeDeletionVectorActionsAsync(positions, resolveAgainst: pinned);
        Assert.Equal(1, rowsDeleted);

        // a concurrent writer lands while the "transaction" is open
        await using (var racer = await OpenAsync())
        {
            await racer.DeleteByRowIdsViaVectorsAsync(await RowIdsOfAsync(racer, 7));
        }

        // rebase the pinned-resolved actions onto the latest snapshot, validate, commit
        await using var committer = await OpenAsync();
        var rebased = await committer.RebaseDvDmlActionsAsync(actions, positions, pinned, committer.CurrentSnapshot);
        await committer.CheckLogicalRebaseAsync(pinned, rebased, rowLevelDml: true);
        await committer.CommitDataFilesAsync(
            System.Array.Empty<WrittenDataFile>(), DeltaWriteMode.Append,
            extraActions: rebased, expectedVersion: committer.CurrentSnapshot.Version, operation: "DELETE");

        await using var check = await OpenAsync();
        Assert.Equal(new long[] { 1, 3, 4, 5, 6, 8, 9, 10 }, await ReadIdsAsync(check));
    }
}
