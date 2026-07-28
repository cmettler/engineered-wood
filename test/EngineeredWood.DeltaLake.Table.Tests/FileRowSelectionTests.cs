// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The PATH-KEYED row-level DML entry points (<see cref="FileRowSelection"/>) and their equivalence with —
/// and improvement over — the ordinal-keyed overloads.
/// </summary>
/// <remarks>
/// <para>
/// Every table here has AT LEAST TWO FILES on purpose. With one file the path-sorted ordinal is always 0, so
/// an ordinal and a path carry the same information and any mis-resolution is invisible; a single-file fixture
/// cannot fail these tests.
/// </para>
/// <para>
/// The property that motivates the API is in <c>OrdinalKeyed_ResolvedAgainstAShrunkSnapshot_SilentlyDeletesNothing</c>:
/// an ordinal that does not resolve is indistinguishable from a file with nothing to delete, so it is skipped,
/// and a caller whose identifiers came from a different snapshot loses the delete WITHOUT AN ERROR. A path
/// cannot be misread that way, so the path-keyed overload reports it.
/// </para>
/// </remarks>
public class FileRowSelectionTests : IDisposable
{
    private readonly string _tempDir;

    public FileRowSelectionTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_frsel_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static Apache.Arrow.Schema BuildSchema() => new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Build();

    private static RecordBatch BuildBatch(long startId, int count)
    {
        var ids = new Int64Array.Builder();
        for (int i = 0; i < count; i++)
            ids.Append(startId + i);
        return new RecordBatch(BuildSchema(), [ids.Build(), ], count);
    }

    /// <summary>A THREE-FILE DV-enabled table: one commit per file, ids 1..3 / 11..13 / 21..23.</summary>
    private async Task<DeltaTable> CreateThreeFileTableAsync()
    {
        var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), BuildSchema(), enableDeletionVectors: true);
        await table.WriteAsync([BuildBatch(1, 3)]);
        await table.WriteAsync([BuildBatch(11, 3)]);
        await table.WriteAsync([BuildBatch(21, 3)]);
        return table;
    }

    private Task<DeltaTable> OpenAsync() => DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir)).AsTask();

    private async Task<List<long>> ReadIdsFreshAsync()
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
    /// The case a range check CANNOT catch, and the reason a file ordinal is the wrong KEY for a DML boundary
    /// even now that <see cref="TransientRowAddress"/> gives the encoding a name and a documented contract.
    ///
    /// <para>A concurrent commit that REMOVES an earlier file renumbers the path-sorted set, so an ordinal
    /// captured before it stays perfectly IN RANGE and silently names a DIFFERENT file. The ordinal form
    /// therefore does not fail, and does not merely delete nothing — it deletes THE WRONG ROW. Validating an
    /// ordinal against the active count cannot tell that case from a correct one; only a key that says WHICH
    /// FILE can. Given the same intent, the path-keyed form deletes exactly the row that was selected.</para>
    /// </summary>
    [Fact]
    public async Task OrdinalKeyed_AfterAConcurrentRemoveRenumbersTheSet_DeletesTheWRONGRow()
    {
        await using var created = await CreateThreeFileTableAsync();
        var pinned = created.CurrentSnapshot;
        var pathsAtPin = PathsByOrdinal(created, pinned);
        var idsAtPin = await FirstIdByOrdinalAsync();

        // The row we intend to delete: position 0 of the file at ordinal 1, identified BOTH ways at the pin.
        // Ordinal 1 is the interesting choice — after an earlier file is removed it still EXISTS, so nothing
        // can reject it, but it has come to mean the file that was at ordinal 2.
        string intendedPath = pathsAtPin[1];
        long intendedId = idsAtPin[1];

        // A concurrent writer removes the file at ordinal 0 outright (copy-on-write delete of all its rows).
        await using (var other = await OpenAsync())
        {
            await other.DeleteBySelectionAsync(new FileRowSelection(
                new Dictionary<string, IReadOnlyCollection<long>>
                {
                    [pathsAtPin[0]] = new long[] { 0, 1, 2 },
                }));
        }

        await using var table = await OpenAsync();
        var pathsNow = PathsByOrdinal(table, table.CurrentSnapshot);
        // Ordinal 1 still EXISTS — so a range check passes — but it no longer means the same file.
        Assert.True(pathsNow.ContainsKey(1));
        Assert.NotEqual(pathsAtPin[1], pathsNow[1]);

        long idNowAtOrdinal1 = (await FirstIdByOrdinalAsync())[1];
        Assert.NotEqual(intendedId, idNowAtOrdinal1);

        // Reuse the captured ordinal, exactly as a caller holding a stale row identifier would. It resolves,
        // and it removes a row nobody selected.
        await table.DeleteByRowIdsViaVectorsAsync(new[] { TransientRowAddress.Pack(1, 0) });
        var survivors = await ReadIdsFreshAsync();
        Assert.DoesNotContain(idNowAtOrdinal1, survivors);
        Assert.Contains(intendedId, survivors);

        // The path did not change meaning, so the path-keyed form hits the row that was actually selected.
        await table.DeleteBySelectionViaVectorsAsync(new FileRowSelection(
            new Dictionary<string, IReadOnlyCollection<long>> { [intendedPath] = new long[] { 0 } }));
        Assert.DoesNotContain(intendedId, await ReadIdsFreshAsync());
    }

    /// <summary>
    /// The lowest id held by each file, keyed by that file's CURRENT path-sorted ordinal — read through
    /// <see cref="DeltaTable.ReadAllWithRowIdsAsync"/>, whose trailing address column carries the very ordinal
    /// the DML overloads are keyed by, so the mapping comes from the library rather than from an assumption
    /// about write order (data files are GUID-named, so path order is uncorrelated with it).
    /// </summary>
    private async Task<Dictionary<int, long>> FirstIdByOrdinalAsync()
    {
        await using var table = await OpenAsync();
        var lowest = new Dictionary<int, long>();
        await foreach (var batch in table.ReadAllWithRowIdsAsync(null, null))
        {
            var ids = (Int64Array)batch.Column("id");
            var addr = (Int64Array)batch.Column(TransientRowAddress.ColumnName);
            for (int i = 0; i < batch.Length; i++)
            {
                int ordinal = TransientRowAddress.FileOrdinal(addr.GetValue(i)!.Value);
                long id = ids.GetValue(i)!.Value;
                if (!lowest.TryGetValue(ordinal, out long cur) || id < cur)
                    lowest[ordinal] = id;
            }
        }
        return lowest;
    }

    /// <summary>PlanFiles is the ordinal↔path dictionary a caller pairs with these overloads — the same
    /// planner that produces the ordinals a positional row identifier packs.</summary>
    private static Dictionary<int, string> PathsByOrdinal(DeltaTable table, Snapshot.Snapshot snapshot)
        => table.PlanFiles(snapshot: snapshot).ToDictionary(p => p.FileOrdinal, p => p.File.Path);

    /// <summary>The two keyings name the same rows: deleting via paths and via the ordinals those paths sit
    /// at produces action sets that differ only in deletion-vector identity (each run writes a fresh DV
    /// file), and the same row count. Two of the three files are touched, so an off-by-one in either
    /// direction would show.</summary>
    [Fact]
    public async Task PathKeyed_AndOrdinalKeyed_NameTheSameRows()
    {
        await using var table = await CreateThreeFileTableAsync();
        var snap = table.CurrentSnapshot;
        Assert.Equal(3, snap.ActiveFiles.Count);
        var paths = PathsByOrdinal(table, snap);

        // second row of the files at ordinals 0 and 2
        var byOrdinal = new Dictionary<int, IReadOnlyCollection<long>>
        {
            [0] = new long[] { 1 },
            [2] = new long[] { 1 },
        };
        var byPath = new Dictionary<string, IReadOnlyCollection<long>>
        {
            [paths[0]] = new long[] { 1 },
            [paths[2]] = new long[] { 1 },
        };

        var (ordinalActions, ordinalRows) = await table.ComputeDeletionVectorActionsAsync(
            byOrdinal, resolveAgainst: snap);
        var (pathActions, pathRows) = await table.ComputeDeletionVectorActionsAsync(
            new FileRowSelection(byPath), resolveAgainst: snap);

        Assert.Equal(2, ordinalRows);
        Assert.Equal(ordinalRows, pathRows);

        // Same files touched, same remove/add shape. (DV uniqueIds differ — each call writes its own DV.)
        static (List<string> Removes, List<string> Adds) Shape(IReadOnlyList<DeltaAction> actions)
        {
            var removes = actions.OfType<RemoveFile>().Select(r => r.Path).OrderBy(p => p, StringComparer.Ordinal).ToList();
            var adds = actions.OfType<AddFile>().Select(a => a.Path).OrderBy(p => p, StringComparer.Ordinal).ToList();
            return (removes, adds);
        }
        var (oRemoves, oAdds) = Shape(ordinalActions);
        var (pRemoves, pAdds) = Shape(pathActions);
        Assert.Equal(oRemoves, pRemoves);
        Assert.Equal(oAdds, pAdds);
        Assert.Equal(new[] { paths[0], paths[2] }.OrderBy(p => p, StringComparer.Ordinal).ToList(), pRemoves);
        Assert.All(pathActions.OfType<AddFile>(), a => Assert.NotNull(a.DeletionVector));
    }

    /// <summary>End to end: a path-keyed selection committed as a fused DV DELETE removes exactly the named
    /// rows across several files, and nothing else.</summary>
    [Fact]
    public async Task PathKeyed_Delete_RemovesExactlyTheSelectedRowsAcrossFiles()
    {
        await using var table = await CreateThreeFileTableAsync();
        var snap = table.CurrentSnapshot;
        var paths = PathsByOrdinal(table, snap);

        // In each file, drop its FIRST row. Which ids those are depends on which commit sorted where, so
        // assert on the count and on "one survivor pair per file" rather than on a fixed id list.
        var selection = new FileRowSelection(new Dictionary<string, IReadOnlyCollection<long>>
        {
            [paths[0]] = new long[] { 0 },
            [paths[1]] = new long[] { 0 },
            [paths[2]] = new long[] { 0 },
        });
        var (actions, rows) = await table.ComputeDeletionVectorActionsAsync(selection, resolveAgainst: snap);
        Assert.Equal(3, rows);

        await table.CommitDataFilesAsync([], DeltaWriteMode.Append,
            extraActions: actions, expectedVersion: snap.Version, operation: "DELETE");

        var remaining = await ReadIdsFreshAsync();
        Assert.Equal(6, remaining.Count);
        // one row gone from each of the three id groups
        Assert.Equal(2, remaining.Count(i => i is >= 1 and <= 3));
        Assert.Equal(2, remaining.Count(i => i is >= 11 and <= 13));
        Assert.Equal(2, remaining.Count(i => i is >= 21 and <= 23));
    }

    /// <summary>THE MOTIVATING CASE. Row identifiers captured against one snapshot, resolved against a
    /// SHRUNK one (an overwrite/compaction replaced three files with one): ordinals 1 and 2 no longer
    /// resolve, so the ordinal-keyed overload SKIPS them and reports zero rows deleted — a lost DELETE with
    /// no error. The path-keyed overload names files that are demonstrably gone, so it THROWS.</summary>
    [Fact]
    public async Task OrdinalKeyed_ResolvedAgainstAShrunkSnapshot_SilentlyDeletesNothing()
    {
        await using var table = await CreateThreeFileTableAsync();
        var stalePaths = PathsByOrdinal(table, table.CurrentSnapshot);

        // Replace all three files with one (the shape a compaction / CREATE OR REPLACE leaves behind).
        await table.WriteAsync([BuildBatch(100, 2)], DeltaWriteMode.Overwrite);
        var shrunk = table.CurrentSnapshot;
        Assert.Single(shrunk.ActiveFiles);

        var staleOrdinals = new Dictionary<int, IReadOnlyCollection<long>>
        {
            [1] = new long[] { 0 },
            [2] = new long[] { 0 },
        };
        var (actions, rows) = await table.ComputeDeletionVectorActionsAsync(
            staleOrdinals, resolveAgainst: shrunk);
        Assert.Empty(actions);
        Assert.Equal(0, rows);   // silently nothing — the defect the path key removes

        var staleSelection = new FileRowSelection(new Dictionary<string, IReadOnlyCollection<long>>
        {
            [stalePaths[1]] = new long[] { 0 },
        });
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await table.ComputeDeletionVectorActionsAsync(staleSelection, resolveAgainst: shrunk));
        Assert.Contains(stalePaths[1], ex.Message);
        Assert.Contains("not active", ex.Message);
    }

    /// <summary>A path that never existed is reported too — not silently ignored.</summary>
    [Fact]
    public async Task PathKeyed_UnknownPath_Throws()
    {
        await using var table = await CreateThreeFileTableAsync();
        var selection = new FileRowSelection(new Dictionary<string, IReadOnlyCollection<long>>
        {
            ["part-does-not-exist.parquet"] = new long[] { 0 },
        });
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await table.ComputeDeletionVectorActionsAsync(selection));
        Assert.Contains("part-does-not-exist.parquet", ex.Message);
    }

    /// <summary>The row-level rebase takes a selection too: positions captured on the pinned snapshot compose
    /// with a concurrent DV delete of a DIFFERENT row of the SAME file (disjoint ⇒ re-union, not conflict) —
    /// on a multi-file table, so the pinned ordinal of the touched file is not trivially 0.</summary>
    [Fact]
    public async Task PathKeyed_Rebase_ComposesWithAConcurrentDeleteOnTheSameFile()
    {
        await using var table = await CreateThreeFileTableAsync();
        var pinned = table.CurrentSnapshot;
        var paths = PathsByOrdinal(table, pinned);

        // this transaction deletes row 0 of the file at ordinal 1
        var selection = new FileRowSelection(new Dictionary<string, IReadOnlyCollection<long>>
        {
            [paths[1]] = new long[] { 0 },
        });
        var (actions, rows) = await table.ComputeDeletionVectorActionsAsync(selection, resolveAgainst: pinned);
        Assert.Equal(1, rows);

        // a concurrent writer deletes row 2 of the SAME file while the transaction is open
        await using (var racer = await OpenAsync())
        {
            var racerPaths = PathsByOrdinal(racer, racer.CurrentSnapshot);
            Assert.Equal(paths[1], racerPaths[1]);   // same file at the same ordinal — nothing moved yet
            var racerSel = new FileRowSelection(new Dictionary<string, IReadOnlyCollection<long>>
            {
                [racerPaths[1]] = new long[] { 2 },
            });
            var (racerActions, racerRows) = await racer.ComputeDeletionVectorActionsAsync(racerSel);
            Assert.Equal(1, racerRows);
            await racer.CommitDataFilesAsync([], DeltaWriteMode.Append,
                extraActions: racerActions, expectedVersion: racer.CurrentSnapshot.Version, operation: "DELETE");
        }

        await using var committer = await OpenAsync();
        var rebased = await committer.RebaseDvDmlActionsAsync(
            actions, selection, pinned, committer.CurrentSnapshot);
        await committer.CheckLogicalRebaseAsync(pinned, rebased, rowLevelDml: true);
        await committer.CommitDataFilesAsync([], DeltaWriteMode.Append,
            extraActions: rebased, expectedVersion: committer.CurrentSnapshot.Version, operation: "DELETE");

        // Both deletes composed rather than conflicting: 9 - 2 rows remain, and BOTH losses came out of the
        // same file — so exactly one of the three consecutive id groups is down to a single row.
        var remaining = await ReadIdsFreshAsync();
        Assert.Equal(7, remaining.Count);
        int[] groups =
        [
            remaining.Count(i => i is >= 1 and <= 3),
            remaining.Count(i => i is >= 11 and <= 13),
            remaining.Count(i => i is >= 21 and <= 23),
        ];
        Assert.Equal([1, 3, 3], groups.OrderBy(g => g).ToArray());
    }

    /// <summary>The COMMITTING deletion-vector DELETE takes a selection too, and agrees row-for-row with the
    /// rowid form: deleting the same two rows via paths leaves exactly what the rowid form leaves.</summary>
    [Fact]
    public async Task DeleteBySelectionViaVectors_MatchesTheRowIdForm()
    {
        // rowid form, on its own table
        long[] viaRowIds;
        await using (var table = await CreateThreeFileTableAsync())
        {
            var rowIds = await CollectRowIdsAsync(table, id => id is 2 or 22);
            var (deleted, _) = await table.DeleteByRowIdsViaVectorsAsync(rowIds);
            Assert.Equal(2, deleted);
        }
        viaRowIds = [.. await ReadIdsFreshAsync()];

        // path form, on a fresh table built identically
        Directory.Delete(_tempDir, recursive: true);
        Directory.CreateDirectory(_tempDir);
        await using (var table = await CreateThreeFileTableAsync())
        {
            var selection = await SelectionForIdsAsync(table, id => id is 2 or 22);
            var (deleted, _) = await table.DeleteBySelectionViaVectorsAsync(selection);
            Assert.Equal(2, deleted);
        }
        Assert.Equal(viaRowIds, await ReadIdsFreshAsync());
        Assert.DoesNotContain(2L, viaRowIds);
        Assert.DoesNotContain(22L, viaRowIds);
    }

    /// <summary>The COPY-ON-WRITE DELETE likewise: the file is rewritten (no deletion vector) and the surviving
    /// rows match the rowid form. Runs on a DV-DISABLED table, which is what forces the rewrite path.</summary>
    [Fact]
    public async Task DeleteBySelection_CopyOnWrite_MatchesTheRowIdForm()
    {
        async Task<DeltaTable> PlainTableAsync()
        {
            var t = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), BuildSchema());
            await t.WriteAsync([BuildBatch(1, 3)]);
            await t.WriteAsync([BuildBatch(11, 3)]);
            return t;
        }

        long[] viaRowIds;
        await using (var table = await PlainTableAsync())
        {
            var rowIds = await CollectRowIdsAsync(table, id => id is 1 or 12);
            var (deleted, _) = await table.DeleteByRowIdsAsync(rowIds);
            Assert.Equal(2, deleted);
        }
        viaRowIds = [.. await ReadIdsFreshAsync()];

        Directory.Delete(_tempDir, recursive: true);
        Directory.CreateDirectory(_tempDir);
        await using (var table = await PlainTableAsync())
        {
            var selection = await SelectionForIdsAsync(table, id => id is 1 or 12);
            var (deleted, _) = await table.DeleteBySelectionAsync(selection);
            Assert.Equal(2, deleted);
            // copy-on-write: rewritten adds, no deletion vector anywhere
            Assert.All(table.CurrentSnapshot.ActiveFiles.Values, f => Assert.Null(f.DeletionVector));
        }
        Assert.Equal(viaRowIds, await ReadIdsFreshAsync());
    }

    /// <summary>A stale path reaches the committing DELETE paths as an error too, not a silent no-op.</summary>
    [Fact]
    public async Task DeleteBySelection_UnknownPath_Throws()
    {
        await using var table = await CreateThreeFileTableAsync();
        var bogus = new FileRowSelection(new Dictionary<string, IReadOnlyCollection<long>>
        {
            ["part-gone.parquet"] = new long[] { 0 },
        });
        var dv = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await table.DeleteBySelectionViaVectorsAsync(bogus));
        Assert.Contains("part-gone.parquet", dv.Message);
        var cow = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await table.DeleteBySelectionAsync(bogus));
        Assert.Contains("part-gone.parquet", cow.Message);
    }

    /// <summary>Reads the transient rowids of the rows whose id matches, via the same path an engine uses.</summary>
    private static async Task<List<long>> CollectRowIdsAsync(DeltaTable table, Func<long, bool> match)
    {
        var rowIds = new List<long>();
        await foreach (var batch in table.ReadAllWithRowIdsAsync(null, null))
        {
            var ids = (Int64Array)batch.Column("id");
            var rids = (Int64Array)batch.Column(batch.ColumnCount - 1);
            for (int i = 0; i < batch.Length; i++)
                if (match(ids.GetValue(i)!.Value))
                    rowIds.Add(rids.GetValue(i)!.Value);
        }
        return rowIds;
    }

    /// <summary>The same rows as a path-keyed selection — decoded exactly as an engine that owns the rowid
    /// encoding would: ordinal -> PlanFiles -> add.path, position = the low bits.</summary>
    private static async Task<FileRowSelection> SelectionForIdsAsync(DeltaTable table, Func<long, bool> match)
    {
        var paths = PathsByOrdinal(table, table.CurrentSnapshot);
        var byFile = new Dictionary<string, IReadOnlyCollection<long>>(StringComparer.Ordinal);
        foreach (long rid in await CollectRowIdsAsync(table, match))
        {
            string path = paths[(int)(rid >> 40)];
            if (!byFile.TryGetValue(path, out var set))
                byFile[path] = set = new HashSet<long>();
            ((HashSet<long>)set).Add(rid & ((1L << 40) - 1));
        }
        return new FileRowSelection(byFile);
    }

    /// <summary>A selection naming a file that was not active in the snapshot it claims to come from is a
    /// caller error, and the rebase says so — where an out-of-range ordinal was dropped in silence.</summary>
    [Fact]
    public async Task PathKeyed_Rebase_PathNotActiveInFrom_Throws()
    {
        await using var table = await CreateThreeFileTableAsync();
        var pinned = table.CurrentSnapshot;
        var paths = PathsByOrdinal(table, pinned);
        var selection = new FileRowSelection(new Dictionary<string, IReadOnlyCollection<long>>
        {
            [paths[0]] = new long[] { 0 },
        });
        var (actions, _) = await table.ComputeDeletionVectorActionsAsync(selection, resolveAgainst: pinned);

        // move the table forward so the rebase actually runs, then rebase a selection naming a bogus file
        await using (var racer = await OpenAsync())
        {
            await racer.WriteAsync([BuildBatch(200, 1)]);
        }
        await using var committer = await OpenAsync();
        var bogus = new FileRowSelection(new Dictionary<string, IReadOnlyCollection<long>>
        {
            ["part-never-existed.parquet"] = new long[] { 0 },
        });
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await committer.RebaseDvDmlActionsAsync(actions, bogus, pinned, committer.CurrentSnapshot));
        Assert.Contains("part-never-existed.parquet", ex.Message);
    }
}
