// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The trailing <c>_metadata</c> struct from <see cref="DeltaTable.ReadAllWithMetadataAsync"/>: one per-row
/// identity surface carrying BOTH halves in Spark's vocabulary — the LOCATOR (<c>file_path</c> +
/// <c>row_index</c>, a physical address valid for this snapshot) and the durable IDENTITY (<c>row_id</c> +
/// <c>row_commit_version</c>).
/// </summary>
/// <remarks>
/// The load-bearing test here is <c>StableIdSurvivesARewrite_WhileTheLocatorDoesNot</c>: it is the one that
/// shows the two halves genuinely diverge, which is the whole reason both belong in one struct. Every fixture
/// uses SEVERAL files, because with one file a locator and a fresh-append stable id coincide numerically and
/// nothing can tell them apart.
/// </remarks>
public class MetadataColumnTests : IDisposable
{
    private readonly string _tempDir;

    public MetadataColumnTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_meta_{Guid.NewGuid():N}");
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
        return new RecordBatch(BuildSchema(), [ids.Build()], count);
    }

    private Task<DeltaTable> OpenAsync() => DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir)).AsTask();

    /// <summary>Row-tracking + DV table, three files (one commit each).</summary>
    private async Task<DeltaTable> CreateTrackedAsync()
    {
        var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), BuildSchema(),
            enableDeletionVectors: true, enableRowTracking: true);
        await table.WriteAsync([BuildBatch(1, 3)]);
        await table.WriteAsync([BuildBatch(11, 3)]);
        await table.WriteAsync([BuildBatch(21, 3)]);
        return table;
    }

    private readonly record struct MetaRow(long Id, string FilePath, long RowIndex, long? RowId, long? Version);

    /// <summary>
    /// Reads the LOCATOR pair from <see cref="DeltaTable.ReadAllWithMetadataAsync"/> and, when
    /// <paramref name="withIdentity"/>, the IDENTITY pair from
    /// <see cref="DeltaTable.ReadAllWithRowTrackingAsync"/> — two reads, because the two surfaces own
    /// different columns. They stream the same snapshot's files in the same path-sorted order with no filter,
    /// so row N of one is row N of the other; that alignment is ASSERTED below rather than assumed, since it
    /// is the only thing making a zip legitimate.
    /// </summary>
    private static async Task<List<MetaRow>> ReadMetaAsync(DeltaTable table, bool withIdentity = true)
    {
        var locators = new List<(long Id, string FilePath, long RowIndex)>();
        await foreach (var batch in table.ReadAllWithMetadataAsync())
        {
            var ids = (Int64Array)batch.Column("id");
            var path = (StringArray)batch.Column(MetadataPredicate.FilePathColumn);
            var idx = (Int64Array)batch.Column(MetadataPredicate.RowIndexColumn);
            for (int i = 0; i < batch.Length; i++)
                locators.Add((ids.GetValue(i)!.Value, path.GetString(i), idx.GetValue(i)!.Value));
        }

        // A table that does not track row identity has no identity surface to read — it REFUSES rather than
        // serving all-null columns — so report the locator alone and leave the id members null, which is what
        // a caller can actually learn about such a table.
        bool tracked = EngineeredWood.DeltaLake.RowTracking.RowTrackingConfig.IsEnabled(
            table.CurrentSnapshot.Metadata.Configuration);
        if (!withIdentity || !tracked)
            return locators.Select(l => new MetaRow(l.Id, l.FilePath, l.RowIndex, null, null)).ToList();

        var identity = new List<(long Id, long? RowId, long? Version)>();
        await foreach (var batch in table.ReadAllWithRowTrackingAsync(columns: null, filter: null))
        {
            var ids = (Int64Array)batch.Column("id");
            var rid = (Int64Array)batch.Column(
                EngineeredWood.DeltaLake.RowTracking.RowTrackingConfig.RowIdColumnName);
            var ver = (Int64Array)batch.Column(
                EngineeredWood.DeltaLake.RowTracking.RowTrackingConfig.RowCommitVersionColumnName);
            for (int i = 0; i < batch.Length; i++)
            {
                identity.Add((ids.GetValue(i)!.Value,
                    rid.IsNull(i) ? null : rid.GetValue(i),
                    ver.IsNull(i) ? null : ver.GetValue(i)));
            }
        }

        Assert.Equal(locators.Count, identity.Count);
        var rows = new List<MetaRow>(locators.Count);
        for (int i = 0; i < locators.Count; i++)
        {
            // The zip is only meaningful if both surfaces emitted the same row at the same offset.
            Assert.Equal(locators[i].Id, identity[i].Id);
            rows.Add(new MetaRow(
                locators[i].Id, locators[i].FilePath, locators[i].RowIndex,
                identity[i].RowId, identity[i].Version));
        }
        return rows;
    }

    /// <summary>The locator arrives as two FLAT dot-named columns — the same `_metadata.*` spelling
    /// ReadAllWithRowTrackingAsync uses for identity — and both are non-null, because a row always has a
    /// physical location even when its identity is underivable.</summary>
    [Fact]
    public async Task Metadata_EmitsTheTwoFlatLocatorColumns_BothNonNull()
    {
        await using var table = await CreateTrackedAsync();
        await foreach (var batch in table.ReadAllWithMetadataAsync())
        {
            var path = batch.Schema.GetFieldByName(MetadataPredicate.FilePathColumn);
            var idx = batch.Schema.GetFieldByName(MetadataPredicate.RowIndexColumn);
            Assert.NotNull(path);
            Assert.NotNull(idx);
            Assert.IsType<StringType>(path!.DataType);
            Assert.IsType<Int64Type>(idx!.DataType);
            Assert.False(path.IsNullable);
            Assert.False(idx.IsNullable);
            // The identity pair is NOT ours to emit — one concept, one owner.
            Assert.Null(batch.Schema.GetFieldByName(
                EngineeredWood.DeltaLake.RowTracking.RowTrackingConfig.RowIdColumnName));
            break;
        }
    }

    /// <summary>Every row names a real active file, positions are per-file dense from 0, and the stable ids are
    /// present and distinct across the whole table.</summary>
    [Fact]
    public async Task Metadata_LocatorNamesActiveFiles_AndIdsAreDistinct()
    {
        await using var table = await CreateTrackedAsync();
        var rows = await ReadMetaAsync(table);
        Assert.Equal(9, rows.Count);

        var active = table.CurrentSnapshot.ActiveFiles.Values.Select(a => a.Path).ToHashSet(StringComparer.Ordinal);
        Assert.All(rows, r => Assert.Contains(r.FilePath, active));
        Assert.Equal(3, rows.Select(r => r.FilePath).Distinct().Count());
        foreach (var g in rows.GroupBy(r => r.FilePath))
            Assert.Equal(new long[] { 0, 1, 2 }, g.Select(r => r.RowIndex).OrderBy(x => x).ToArray());

        Assert.All(rows, r => Assert.NotNull(r.RowId));
        Assert.Equal(9, rows.Select(r => r.RowId).Distinct().Count());
    }

    /// <summary>The locator round-trips into a <see cref="RowSelection"/> that deletes exactly the intended
    /// rows — i.e. the struct is directly usable as the DML key, with no packing step.</summary>
    [Fact]
    public async Task Metadata_LocatorFeedsAFileRowSelection_Directly()
    {
        await using (var table = await CreateTrackedAsync())
        {
            var rows = await ReadMetaAsync(table);
            var byFile = new Dictionary<string, IReadOnlyCollection<long>>(StringComparer.Ordinal);
            foreach (var r in rows.Where(r => r.Id is 2 or 22))
            {
                if (!byFile.TryGetValue(r.FilePath, out var set))
                    byFile[r.FilePath] = set = new HashSet<long>();
                ((HashSet<long>)set).Add(r.RowIndex);
            }
            var (deleted, _) = await table.DeleteRowsAsync(RowSelection.ByPath(byFile));
            Assert.Equal(2, deleted);
        }

        await using var check = await OpenAsync();
        var left = (await ReadMetaAsync(check)).Select(r => r.Id).OrderBy(x => x).ToArray();
        Assert.Equal(new long[] { 1, 3, 11, 12, 13, 21, 23 }, left);
    }

    /// <summary><c>row_index</c> is ABSOLUTE — it counts rows the deletion vector masks, which is what makes
    /// repeated DV deletes compose. After deleting the FIRST row of a file, its survivors keep indexes 1 and 2
    /// rather than renumbering to 0 and 1.</summary>
    [Fact]
    public async Task RowIndex_IsAbsolute_SoItSurvivesADeletionVector()
    {
        await using (var table = await CreateTrackedAsync())
        {
            var rows = await ReadMetaAsync(table);
            var target = rows.First(r => r.RowIndex == 0);
            await table.DeleteRowsAsync(RowSelection.ByPath(
                new Dictionary<string, IReadOnlyCollection<long>> { [target.FilePath] = new long[] { 0 } }));
        }

        await using var check = await OpenAsync();
        var after = await ReadMetaAsync(check);
        var touched = after.Where(r => r.RowIndex != 0).GroupBy(r => r.FilePath)
                           .First(g => g.Count() == 2 && after.Count(r => r.FilePath == g.Key) == 2);
        Assert.Equal(new long[] { 1, 2 }, touched.Select(r => r.RowIndex).OrderBy(x => x).ToArray());
    }

    /// <summary>THE LOAD-BEARING ONE. A rewrite relocates rows, so the LOCATOR changes — new file, new index —
    /// while the stable IDENTITY does not. That divergence is precisely why one struct must carry both, and it
    /// is invisible on a single-file table.</summary>
    [Fact]
    public async Task StableIdSurvivesARewrite_WhileTheLocatorDoesNot()
    {
        Dictionary<long, MetaRow> before;
        await using (var table = await CreateTrackedAsync())
        {
            before = (await ReadMetaAsync(table)).ToDictionary(r => r.Id);
            await table.CompactAsync();
        }

        await using var check = await OpenAsync();
        var after = (await ReadMetaAsync(check)).ToDictionary(r => r.Id);
        Assert.Equal(before.Keys.OrderBy(k => k), after.Keys.OrderBy(k => k));

        // compaction consolidated the three files into fewer — so the locator MOVED for at least some rows
        Assert.True(after.Values.Select(r => r.FilePath).Distinct().Count()
                    < before.Values.Select(r => r.FilePath).Distinct().Count(),
            "compaction should have reduced the file count, moving the locator");
        Assert.Contains(after.Keys, id => after[id].FilePath != before[id].FilePath);

        // ...while every row's STABLE id is unchanged. This is the property the locator cannot provide.
        foreach (var id in before.Keys)
        {
            Assert.NotNull(after[id].RowId);
            Assert.Equal(before[id].RowId, after[id].RowId);
        }
    }

    /// <summary>Without row tracking the LOCATOR is unaffected — a row always has a physical location — while
    /// the IDENTITY surface REFUSES the table rather than serving all-null columns, which would claim these
    /// rows have no identity when the truth is that this table does not track it.</summary>
    [Fact]
    public async Task WithoutRowTracking_TheLocatorStillWorks_AndTheIdentitySurfaceRefuses()
    {
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), BuildSchema());
        await table.WriteAsync([BuildBatch(1, 2)]);
        await table.WriteAsync([BuildBatch(11, 2)]);

        var rows = await ReadMetaAsync(table, withIdentity: false);
        Assert.Equal(4, rows.Count);
        Assert.All(rows, r => Assert.False(string.IsNullOrEmpty(r.FilePath)));
        Assert.Equal(2, rows.Select(r => r.FilePath).Distinct().Count());

        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await foreach (var _ in table.ReadAllWithRowTrackingAsync(columns: null, filter: null))
            {
            }
        });
    }

    /// <summary>THE ROUND TRIP, and the point of the whole surface: read with <c>_metadata</c>, change values,
    /// hand the batch straight back. The caller writes no substitution code and never sees a file ordinal or a
    /// packed rowid. Rows NOT handed back are untouched, and the stable ids of updated rows are preserved.</summary>
    [Fact]
    public async Task UpdateBySelection_FromAMetadataBatch_RoundTrips()
    {
        Dictionary<long, long?> idsBefore;
        await using (var table = await CreateTrackedAsync())
        {
            idsBefore = (await ReadMetaAsync(table)).ToDictionary(r => r.Id, r => r.RowId);

            // build an updates batch: _metadata (as read) + the new `id` values, for two rows in two files
            var rows = (await ReadMetaAsync(table)).Where(r => r.Id is 2 or 22).OrderBy(r => r.Id).ToList();
            Assert.Equal(2, rows.Count);

            var pathB = new StringArray.Builder();
            var idxB = new Int64Array.Builder();
            var newIds = new Int64Array.Builder();
            foreach (var r in rows)
            {
                pathB.Append(r.FilePath);
                idxB.Append(r.RowIndex);
                newIds.Append(r.Id + 1000);
            }
            var updSchema = new Apache.Arrow.Schema.Builder()
                .Field(new Field(MetadataPredicate.FilePathColumn, StringType.Default, false))
                .Field(new Field(MetadataPredicate.RowIndexColumn, Int64Type.Default, false))
                .Field(new Field("id", Int64Type.Default, false))
                .Build();
            var updates = new RecordBatch(updSchema,
                new IArrowArray[] { pathB.Build(), idxB.Build(), newIds.Build() }, rows.Count);

            await table.UpdateBySelectionAsync(updates);
        }

        await using var check = await OpenAsync();
        var after = await ReadMetaAsync(check);
        var ids = after.Select(r => r.Id).OrderBy(x => x).ToArray();
        // 2 -> 1002 and 22 -> 1022; everything else untouched
        Assert.Equal(new long[] { 1, 3, 11, 12, 13, 21, 23, 1002, 1022 }, ids);

        // row tracking survived the rewrite: the updated rows kept the stable ids their originals had
        var byId = after.ToDictionary(r => r.Id);
        Assert.Equal(idsBefore[2], byId[1002].RowId);
        Assert.Equal(idsBefore[22], byId[1022].RowId);
    }

    /// <summary>
    /// The PRIMITIVE overload's callback contract, which the RecordBatch form hides: the rewriter is invoked
    /// ONCE PER SELECTED FILE with that file's path, and <c>positionsPerBatch</c> is row-aligned with
    /// <c>sourceBatches</c> carrying ABSOLUTE in-file positions.
    /// </summary>
    /// <remarks>
    /// The table is given a DELETION VECTOR first, on purpose. Without one, an absolute position and a row's
    /// index within the emitted batch are the same number, so a callback handed in-batch indices instead of
    /// absolute positions would look correct — the same blind spot a single-file fixture has for ordinals.
    /// Masking row 0 makes the emitted batch's row 0 sit at absolute position 1, so the two disagree and the
    /// contract becomes testable.
    /// </remarks>
    [Fact]
    public async Task UpdateBySelection_Primitive_PassesThePathAndABSOLUTEPositions()
    {
        string targetFile;
        long survivingIdAtAbs2;
        await using (var table = await CreateTrackedAsync())
        {
            var rows = await ReadMetaAsync(table);
            // pick a file and mask its FIRST row, so absolute != in-batch index afterwards
            targetFile = rows.First(r => r.Id == 11).FilePath;
            long maskedAbs = rows.First(r => r.Id == 11).RowIndex;
            await table.DeleteRowsAsync(RowSelection.ByPath(
                new Dictionary<string, IReadOnlyCollection<long>> { [targetFile] = new long[] { maskedAbs } }));
            survivingIdAtAbs2 = (await ReadMetaAsync(table))
                .First(r => r.FilePath == targetFile && r.RowIndex == 2).Id;
        }

        var observedPaths = new List<string>();
        var observedPositions = new List<long>();
        int invocations = 0;

        await using (var table = await OpenAsync())
        {
            // target the row at ABSOLUTE position 2 of that file
            var selection = RowSelection.ByPath(new Dictionary<string, IReadOnlyCollection<long>>
            {
                [targetFile] = new long[] { 2 },
            });

            await table.UpdateBySelectionAsync(selection, (filePath, sourceBatches, positionsPerBatch) =>
            {
                invocations++;
                observedPaths.Add(filePath);
                Assert.Equal(sourceBatches.Count, positionsPerBatch.Count);   // row-aligned, per batch

                var result = new List<RecordBatch>(sourceBatches.Count);
                for (int b = 0; b < sourceBatches.Count; b++)
                {
                    var src = sourceBatches[b];
                    var pos = positionsPerBatch[b];
                    Assert.Equal(src.Length, pos.Length);                      // ...and per row
                    for (int i = 0; i < pos.Length; i++)
                        observedPositions.Add(pos.GetValue(i)!.Value);

                    // Substitute keyed on the ABSOLUTE position, exactly as a real caller would.
                    var ids = (Int64Array)src.Column("id");
                    var nb = new Int64Array.Builder();
                    for (int i = 0; i < src.Length; i++)
                        nb.Append(pos.GetValue(i)!.Value == 2 ? 7777L : ids.GetValue(i)!.Value);
                    result.Add(new RecordBatch(src.Schema, new IArrowArray[] { nb.Build() }, src.Length));
                }
                return result;
            });
        }

        Assert.Equal(1, invocations);                       // once per SELECTED file, not per batch
        Assert.Equal(new[] { targetFile }, observedPaths);  // and it is the file we selected

        // The masked row is gone from the emitted batch, so the positions the rewriter saw START AT 1.
        // Were they in-batch indices they would have started at 0 — that is the discriminating assertion.
        Assert.Equal(new long[] { 1, 2 }, observedPositions.OrderBy(x => x).ToArray());
        Assert.DoesNotContain(0L, observedPositions);

        // and the substitution landed on the row that really sat at absolute 2
        await using var check = await OpenAsync();
        var after = await ReadMetaAsync(check);
        Assert.Contains(7777L, after.Select(r => r.Id));
        Assert.DoesNotContain(survivingIdAtAbs2, after.Select(r => r.Id));
    }

    /// <summary>The primitive rejects a stale path too, and the rewriter is never invoked for it.</summary>
    [Fact]
    public async Task UpdateBySelection_Primitive_StalePath_ThrowsWithoutInvokingTheRewriter()
    {
        await using var table = await CreateTrackedAsync();
        bool invoked = false;
        var bogus = RowSelection.ByPath(new Dictionary<string, IReadOnlyCollection<long>>
        {
            ["part-vanished.parquet"] = new long[] { 0 },
        });

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await table.UpdateBySelectionAsync(bogus, (_, batches, _) => { invoked = true; return batches; }));
        Assert.Contains("part-vanished.parquet", ex.Message);
        Assert.False(invoked);
    }

    /// <summary>
    /// THE NATURAL USAGE, end to end and with no predicate anywhere: read with
    /// <see cref="DeltaTable.ReadAllWithMetadataAsync"/>, KEEP the rows you want to change (carrying their
    /// <c>_metadata</c> along), change a column, hand that batch back to
    /// <c>UpdateBySelectionAsync</c>. This is the documented flow, so it is worth a test rather than prose.
    /// </summary>
    /// <remarks>
    /// The one thing a caller must get right: hand back ONLY the rows being changed. Passing every row read
    /// would make the selection the whole file — semantically valid, but it rewrites everything and bumps every
    /// row's commit version. Filtering first is what keeps the update minimal.
    /// </remarks>
    [Fact]
    public async Task RoundTrip_ReadFilterModifyWriteBack_NeedsNoPredicate()
    {
        await using (var table = await CreateTrackedAsync())
        {
            var updateBatches = new List<RecordBatch>();
            await foreach (var batch in table.ReadAllWithMetadataAsync())
            {
                var ids = (Int64Array)batch.Column("id");
                // keep only the rows we intend to change (ids 2 and 22)
                var keep = new List<int>();
                for (int i = 0; i < batch.Length; i++)
                    if (ids.GetValue(i) is 2 or 22)
                        keep.Add(i);
                if (keep.Count == 0)
                    continue;

                // carry the locator columns through untouched; rewrite the value column
                var metaPath = EngineeredWood.Arrow.ArrowCompute.Take(
                    batch.Column(MetadataPredicate.FilePathColumn), keep);
                var metaIdx = EngineeredWood.Arrow.ArrowCompute.Take(
                    batch.Column(MetadataPredicate.RowIndexColumn), keep);
                var newIds = new Int64Array.Builder();
                foreach (int i in keep)
                    newIds.Append(ids.GetValue(i)!.Value * 10);

                updateBatches.Add(new RecordBatch(
                    new Apache.Arrow.Schema.Builder()
                        .Field(batch.Schema.GetFieldByName(MetadataPredicate.FilePathColumn)!)
                        .Field(batch.Schema.GetFieldByName(MetadataPredicate.RowIndexColumn)!)
                        .Field(new Field("id", Int64Type.Default, false))
                        .Build(),
                    new IArrowArray[] { metaPath, metaIdx, newIds.Build() }, keep.Count));
            }

            // one call per batch of changes; each addresses exactly its own rows
            foreach (var upd in updateBatches)
                await table.UpdateBySelectionAsync(upd);
        }

        await using var check = await OpenAsync();
        var after = (await ReadMetaAsync(check)).Select(r => r.Id).OrderBy(x => x).ToArray();
        Assert.Equal(new long[] { 1, 3, 11, 12, 13, 20, 21, 23, 220 }, after);
    }

    // ── merge-on-read UPDATE (UpdateBySelectionViaVectorsAsync) ───────────────────────────────────────────
    //
    // The cheap UPDATE shape: mask the old rows with a deletion vector and APPEND the new ones, instead of
    // rewriting the whole file. The two tests that justify it prove exactly that — the source file survives
    // (so it was not rewritten) and each moved row keeps its stable id.

    /// <summary>
    /// IT DOES NOT REWRITE. After the update the source file is STILL ACTIVE — carrying a deletion vector —
    /// and a small post-image file has been added beside it. Copy-on-write would instead have removed that path
    /// and replaced it, so "path still active + file count grew" is the discriminating evidence.
    /// </summary>
    [Fact]
    public async Task MergeOnReadUpdate_MasksAndAppends_WithoutRewritingTheFile()
    {
        await using var table = await CreateTrackedAsync();
        var before = table.CurrentSnapshot;
        int filesBefore = before.ActiveFiles.Count;

        var target = (await ReadMetaAsync(table)).First(r => r.Id == 12);
        var selection = RowSelection.ByPath(new Dictionary<string, IReadOnlyCollection<long>>
        {
            [target.FilePath] = new long[] { target.RowIndex },
        });

        var (rows, _) = await table.UpdateBySelectionViaVectorsAsync(selection, matched =>
        {
            Assert.Equal(1, matched.Length);
            Assert.Equal(12L, ((Int64Array)matched.Column("id")).GetValue(0)!.Value);
            return new RecordBatch(matched.Schema,
                new IArrowArray[] { new Int64Array.Builder().Append(120L).Build() }, 1);
        });
        Assert.Equal(1, rows);

        await using var check = await OpenAsync();
        var after = check.CurrentSnapshot;

        // the source file was NOT rewritten away — it is still active, now with a deletion vector
        var survivor = after.ActiveFiles.Values.FirstOrDefault(f => f.Path == target.FilePath);
        Assert.NotNull(survivor);
        Assert.NotNull(survivor!.DeletionVector);
        // ...and the post-image landed in a NEW file beside it
        Assert.Equal(filesBefore + 1, after.ActiveFiles.Count);

        Assert.Equal(new long[] { 1, 2, 3, 11, 13, 21, 22, 23, 120 },
            (await ReadMetaAsync(check)).Select(r => r.Id).OrderBy(x => x).ToArray());
    }

    /// <summary>
    /// THE CAPABILITY: the moved row keeps its ORIGINAL stable id. That is what merge-on-read buys over
    /// copy-on-write, whose rewrite re-derives ids for every row of the file it touches.
    /// </summary>
    [Fact]
    public async Task MergeOnReadUpdate_PreservesTheStableRowId()
    {
        long? idBefore;
        long targetPos;
        string targetPath;
        await using (var table = await CreateTrackedAsync())
        {
            var target = (await ReadMetaAsync(table)).First(r => r.Id == 22);
            idBefore = target.RowId;
            targetPos = target.RowIndex;
            targetPath = target.FilePath;
            Assert.NotNull(idBefore);

            await table.UpdateBySelectionViaVectorsAsync(
                RowSelection.ByPath(new Dictionary<string, IReadOnlyCollection<long>>
                {
                    [targetPath] = new long[] { targetPos },
                }),
                matched => new RecordBatch(matched.Schema,
                    new IArrowArray[] { new Int64Array.Builder().Append(2200L).Build() }, 1));
        }

        await using var check = await OpenAsync();
        var updated = (await ReadMetaAsync(check)).Single(r => r.Id == 2200);
        Assert.Equal(idBefore, updated.RowId);          // identity survived the move
        Assert.NotEqual(targetPath, updated.FilePath);  // ...even though the row is in a DIFFERENT file now
    }

    /// <summary>
    /// The KEYED overload's contract — the one a host-side join depends on, and the one the single-argument
    /// form cannot express. The updater receives the file's <c>add.path</c> and the matched rows' ABSOLUTE
    /// positions, row-aligned, so values can be looked up by identity instead of by emission order.
    /// </summary>
    /// <remarks>
    /// A deletion vector is applied first so that an absolute position and a row's index within the matched
    /// batch DISAGREE. Without that, handing the updater in-batch indices would look correct — the same blind
    /// spot as elsewhere in this file.
    /// </remarks>
    [Fact]
    public async Task MergeOnReadUpdate_KeyedOverload_PassesPathAndAbsolutePositions()
    {
        string targetFile;
        await using (var table = await CreateTrackedAsync())
        {
            var rows = await ReadMetaAsync(table);
            targetFile = rows.First(r => r.Id == 11).FilePath;
            // mask absolute 0 of that file, so its survivors sit at absolute 1 and 2
            await table.DeleteRowsAsync(RowSelection.ByPath(
                new Dictionary<string, IReadOnlyCollection<long>>
                {
                    [targetFile] = new long[] { rows.First(r => r.Id == 11).RowIndex },
                }));
        }

        var observedPaths = new List<string>();
        var observedPositions = new List<long>();

        await using (var table = await OpenAsync())
        {
            // select BOTH survivors, and key the new values by absolute position
            var newByPos = new Dictionary<long, long> { [1] = 777L, [2] = 888L };
            var selection = RowSelection.ByPath(new Dictionary<string, IReadOnlyCollection<long>>
            {
                [targetFile] = new long[] { 1, 2 },
            });

            var (rows, _) = await table.UpdateBySelectionViaVectorsAsync(selection,
                (filePath, matched, positions) =>
                {
                    observedPaths.Add(filePath);
                    Assert.Equal(matched.Length, positions.Length);      // row-aligned
                    var nb = new Int64Array.Builder();
                    for (int i = 0; i < matched.Length; i++)
                    {
                        long pos = positions.GetValue(i)!.Value;
                        observedPositions.Add(pos);
                        nb.Append(newByPos[pos]);                        // keyed, not ordered
                    }
                    return new RecordBatch(matched.Schema, new IArrowArray[] { nb.Build() }, matched.Length);
                });
            Assert.Equal(2, rows);
        }

        Assert.Equal(new[] { targetFile }, observedPaths.Distinct().ToArray());
        // ABSOLUTE: the masked row is absent, so positions start at 1 — in-batch indices would be {0,1}
        Assert.Equal(new long[] { 1, 2 }, observedPositions.OrderBy(x => x).ToArray());
        Assert.DoesNotContain(0L, observedPositions);

        // the keyed substitution landed on the right rows
        await using var check = await OpenAsync();
        var after = (await ReadMetaAsync(check)).Select(r => r.Id).OrderBy(x => x).ToArray();
        Assert.Equal(new long[] { 1, 2, 3, 21, 22, 23, 777, 888 }, after);
    }

    /// <summary>Without deletion vectors it refuses cleanly and points at the copy-on-write form — never a
    /// silent fallback, since the two have very different IO costs.</summary>
    [Fact]
    public async Task MergeOnReadUpdate_WithoutDeletionVectors_RefusesCleanly()
    {
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), BuildSchema());   // no DVs
        await table.WriteAsync([BuildBatch(1, 2)]);
        await table.WriteAsync([BuildBatch(11, 2)]);
        var path = table.CurrentSnapshot.ActiveFiles.Values.First().Path;

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await table.UpdateBySelectionViaVectorsAsync(
                RowSelection.ByPath(new Dictionary<string, IReadOnlyCollection<long>>
                {
                    [path] = new long[] { 0 },
                }),
                m => m));
        Assert.Contains("deletion vectors", ex.Message);
        Assert.Contains("UpdateBySelectionAsync", ex.Message);   // names the alternative
    }

    /// <summary>An updater returning the wrong row count is a caller error, caught rather than committed.</summary>
    [Fact]
    public async Task MergeOnReadUpdate_UpdaterReturningWrongRowCount_Throws()
    {
        await using var table = await CreateTrackedAsync();
        var target = (await ReadMetaAsync(table)).First(r => r.Id == 2);
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await table.UpdateBySelectionViaVectorsAsync(
                RowSelection.ByPath(new Dictionary<string, IReadOnlyCollection<long>>
                {
                    [target.FilePath] = new long[] { target.RowIndex },
                }),
                matched => new RecordBatch(matched.Schema,
                    new IArrowArray[] { new Int64Array.Builder().Append(1L).Append(2L).Build() }, 2)));
        Assert.Contains("one row per matched row", ex.Message);

        // nothing committed
        Assert.Equal(9, (await ReadMetaAsync(table)).Count);
    }

    /// <summary>An updates batch naming a file that is no longer active is an error, not a silent no-op.</summary>
    [Fact]
    public async Task UpdateBySelection_StalePath_Throws()
    {
        await using var table = await CreateTrackedAsync();
        var updates = new RecordBatch(
            new Apache.Arrow.Schema.Builder()
                .Field(new Field(MetadataPredicate.FilePathColumn, StringType.Default, false))
                .Field(new Field(MetadataPredicate.RowIndexColumn, Int64Type.Default, false))
                .Field(new Field("id", Int64Type.Default, false))
                .Build(),
            new IArrowArray[]
            {
                new StringArray.Builder().Append("part-not-here.parquet").Build(),
                new Int64Array.Builder().Append(0L).Build(),
                new Int64Array.Builder().Append(99L).Build(),
            }, 1);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await table.UpdateBySelectionAsync(updates));
        Assert.Contains("part-not-here.parquet", ex.Message);
    }

    /// <summary>A batch with no <c>_metadata</c> column is rejected with a message that says how to get one.</summary>
    [Fact]
    public async Task UpdateBySelection_WithoutMetadataColumn_IsRejected()
    {
        await using var table = await CreateTrackedAsync();
        var ex = await Assert.ThrowsAsync<ArgumentException>(
            async () => await table.UpdateBySelectionAsync(BuildBatch(1, 1)));
        Assert.Contains(nameof(DeltaTable.ReadAllWithMetadataAsync), ex.Message);
    }

    /// <summary>Delegating filesystem that counts opens of DATA parquet files (the log and its checkpoints
    /// excluded) — the instrument behind the zero-data-reads claim. Without it that claim would be an
    /// assertion about intent rather than a measurement.</summary>
    private sealed class CountingFileSystem(ITableFileSystem inner) : ITableFileSystem
    {
        public int DataParquetOpens;

        public IAsyncEnumerable<TableFileInfo> ListAsync(string prefix, CancellationToken ct = default)
            => inner.ListAsync(prefix, ct);

        public ValueTask<IRandomAccessFile> OpenReadAsync(string path, CancellationToken ct = default)
        {
            if (path.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase)
                && !path.StartsWith("_delta_log", StringComparison.Ordinal))
            {
                Interlocked.Increment(ref DataParquetOpens);
            }
            return inner.OpenReadAsync(path, ct);
        }

        public ValueTask<ISequentialFile> CreateAsync(string path, bool overwrite = false, CancellationToken ct = default)
            => inner.CreateAsync(path, overwrite, ct);
        public ValueTask<bool> RenameAsync(string sourcePath, string targetPath, CancellationToken ct = default)
            => inner.RenameAsync(sourcePath, targetPath, ct);
        public ValueTask DeleteAsync(string path, CancellationToken ct = default)
            => inner.DeleteAsync(path, ct);
        public ValueTask<bool> ExistsAsync(string path, CancellationToken ct = default)
            => inner.ExistsAsync(path, ct);
        public ValueTask<byte[]> ReadAllBytesAsync(string path, CancellationToken ct = default)
            => inner.ReadAllBytesAsync(path, ct);
        public ValueTask WriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken ct = default)
            => inner.WriteAllBytesAsync(path, data, ct);
    }

    /// <summary>THE CAPABILITY, measured rather than asserted: a DELETE whose predicate addresses rows only
    /// physically (<c>_metadata.file_path</c> + <c>_metadata.row_index</c>) lowers to a selection and commits
    /// with ZERO data-parquet opens — it needs the log and a deletion-vector write, nothing else.</summary>
    [Fact]
    public async Task MetadataPredicateDelete_ReadsNoDataFiles()
    {
        string targetPath;
        await using (var setup = await CreateTrackedAsync())
        {
            targetPath = (await ReadMetaAsync(setup)).First(r => r.Id == 12).FilePath;
        }

        var countingFs = new CountingFileSystem(new LocalTableFileSystem(_tempDir));
        await using (var table = await DeltaTable.OpenAsync(countingFs))
        {
            var pred = new EngineeredWood.Expressions.AndPredicate(new EngineeredWood.Expressions.Predicate[]
            {
                Ex.Equal(MetadataPredicate.FilePathColumn, targetPath),
                Ex.Equal(MetadataPredicate.RowIndexColumn, 1L),
            });
            var (deleted, _) = await table.DeleteAsync(pred);
            Assert.Equal(1, deleted);
        }

        Assert.Equal(0, countingFs.DataParquetOpens);   // the measurement that makes the claim real

        await using var check = await OpenAsync();
        var left = (await ReadMetaAsync(check)).Select(r => r.Id).OrderBy(x => x).ToArray();
        Assert.Equal(new long[] { 1, 2, 3, 11, 13, 21, 22, 23 }, left);
    }

    /// <summary>
    /// SCOPES the zero-read claim, so it cannot be over-read. Without deletion vectors the same lowered
    /// predicate routes to COPY-ON-WRITE, which must read and rewrite each affected file — so it opens data
    /// files, and is NOT a zero-read fast path. The lowering still helps (it names the files directly instead
    /// of evaluating a mask over pruning candidates), but the saving is different in kind.
    /// </summary>
    [Fact]
    public async Task MetadataPredicateDelete_WithoutDeletionVectors_IsCopyOnWrite_AndDoesReadData()
    {
        string targetPath;
        await using (var setup = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), BuildSchema()))   // NO deletion vectors
        {
            await setup.WriteAsync([BuildBatch(1, 3)]);
            await setup.WriteAsync([BuildBatch(11, 3)]);
            targetPath = (await ReadMetaAsync(setup)).First(r => r.Id == 12).FilePath;
        }

        var countingFs = new CountingFileSystem(new LocalTableFileSystem(_tempDir));
        await using (var table = await DeltaTable.OpenAsync(countingFs))
        {
            var pred = new EngineeredWood.Expressions.AndPredicate(new EngineeredWood.Expressions.Predicate[]
            {
                Ex.Equal(MetadataPredicate.FilePathColumn, targetPath),
                Ex.Equal(MetadataPredicate.RowIndexColumn, 1L),
            });
            var (deleted, _) = await table.DeleteAsync(pred);
            Assert.Equal(1, deleted);
        }

        // the rewrite had to read the affected file — this is the assertion that scopes the DV-path claim
        Assert.True(countingFs.DataParquetOpens > 0,
            "copy-on-write must read the file it rewrites; only the deletion-vector path is zero-read");

        await using var check = await OpenAsync();
        Assert.Equal(new long[] { 1, 2, 3, 11, 13 },
            (await ReadMetaAsync(check)).Select(r => r.Id).OrderBy(x => x).ToArray());
    }

    /// <summary>
    /// The other boundary: with Change Data Feed on, even the deletion-vector path must read the SELECTED
    /// files to capture the deleted rows' content for the feed — so it is not zero-read either. It still reads
    /// only the selected file, not the whole table, which is the actual guarantee.
    /// </summary>
    [Fact]
    public async Task MetadataPredicateDelete_WithChangeDataFeed_ReadsOnlyTheSelectedFile()
    {
        string targetPath;
        await using (var setup = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), BuildSchema(),
            enableDeletionVectors: true,
            configuration: new Dictionary<string, string> { ["delta.enableChangeDataFeed"] = "true" }))
        {
            await setup.WriteAsync([BuildBatch(1, 3)]);
            await setup.WriteAsync([BuildBatch(11, 3)]);
            await setup.WriteAsync([BuildBatch(21, 3)]);
            targetPath = (await ReadMetaAsync(setup)).First(r => r.Id == 12).FilePath;
        }

        var countingFs = new CountingFileSystem(new LocalTableFileSystem(_tempDir));
        await using (var table = await DeltaTable.OpenAsync(countingFs))
        {
            var pred = new EngineeredWood.Expressions.AndPredicate(new EngineeredWood.Expressions.Predicate[]
            {
                Ex.Equal(MetadataPredicate.FilePathColumn, targetPath),
                Ex.Equal(MetadataPredicate.RowIndexColumn, 1L),
            });
            var (deleted, _) = await table.DeleteAsync(pred);
            Assert.Equal(1, deleted);
        }

        // reads happened (for the feed) but were confined to the ONE selected file out of three
        Assert.True(countingFs.DataParquetOpens > 0, "CDF capture must read the selected rows' content");
        Assert.True(countingFs.DataParquetOpens <= 2,
            $"only the selected file should be read, not all three — saw {countingFs.DataParquetOpens} opens");
    }

    /// <summary>An IN set names several positions, and OR combines several files — the shapes the lowering
    /// supports. Still zero data reads.</summary>
    [Fact]
    public async Task MetadataPredicateDelete_InSet_AndOrAcrossFiles()
    {
        string fileA, fileB;
        await using (var setup = await CreateTrackedAsync())
        {
            var rows = await ReadMetaAsync(setup);
            fileA = rows.First(r => r.Id == 1).FilePath;
            fileB = rows.First(r => r.Id == 21).FilePath;
        }

        var countingFs = new CountingFileSystem(new LocalTableFileSystem(_tempDir));
        await using (var table = await DeltaTable.OpenAsync(countingFs))
        {
            // (fileA AND row_index IN (0,1)) OR (fileB AND row_index = 2)
            var pred = new EngineeredWood.Expressions.OrPredicate(new EngineeredWood.Expressions.Predicate[]
            {
                new EngineeredWood.Expressions.AndPredicate(new EngineeredWood.Expressions.Predicate[]
                {
                    Ex.Equal(MetadataPredicate.FilePathColumn, fileA),
                    Ex.In(MetadataPredicate.RowIndexColumn, new EngineeredWood.Expressions.LiteralValue[] { 0L, 1L }),
                }),
                new EngineeredWood.Expressions.AndPredicate(new EngineeredWood.Expressions.Predicate[]
                {
                    Ex.Equal(MetadataPredicate.FilePathColumn, fileB),
                    Ex.Equal(MetadataPredicate.RowIndexColumn, 2L),
                }),
            });
            var (deleted, _) = await table.DeleteAsync(pred);
            Assert.Equal(3, deleted);
        }
        Assert.Equal(0, countingFs.DataParquetOpens);

        await using var check = await OpenAsync();
        Assert.Equal(6, (await ReadMetaAsync(check)).Count);
    }

    /// <summary>A predicate that MENTIONS <c>_metadata</c> but cannot be lowered is REJECTED loudly. It must
    /// never fall through to the row mask, which binds data columns only and would mis-evaluate it —
    /// silently deleting the wrong rows.</summary>
    [Fact]
    public async Task MetadataPredicateThatCannotLower_IsRejected_NotMisEvaluated()
    {
        await using var table = await CreateTrackedAsync();
        // mixing a metadata reference with a data column is not a supported lowering
        var pred = new EngineeredWood.Expressions.AndPredicate(new EngineeredWood.Expressions.Predicate[]
        {
            Ex.Equal(MetadataPredicate.FilePathColumn, "whatever.parquet"),
            Ex.Equal("id", 2L),
        });
        var ex = await Assert.ThrowsAsync<NotSupportedException>(async () => await table.DeleteAsync(pred));
        Assert.Contains("_metadata", ex.Message);

        // and nothing was deleted
        Assert.Equal(9, (await ReadMetaAsync(table)).Count);
    }

    /// <summary>UpdateAsync is now SYMMETRIC with DeleteAsync: a physically-addressing predicate lowers to a
    /// selection, and the updater sees only the MATCHED rows. Non-matched rows pass through untouched.</summary>
    [Fact]
    public async Task MetadataPredicateUpdate_LowersAndUpdatesOnlyTheMatchedRows()
    {
        string targetFile;
        await using (var setup = await CreateTrackedAsync())
        {
            targetFile = (await ReadMetaAsync(setup)).First(r => r.Id == 12).FilePath;
        }

        await using (var table = await OpenAsync())
        {
            var pred = new EngineeredWood.Expressions.AndPredicate(new EngineeredWood.Expressions.Predicate[]
            {
                Ex.Equal(MetadataPredicate.FilePathColumn, targetFile),
                Ex.Equal(MetadataPredicate.RowIndexColumn, 1L),
            });
            var (rows, _) = await table.UpdateAsync(pred, matched =>
            {
                // exactly the one selected row reaches the updater
                Assert.Equal(1, matched.Length);
                var ids = (Int64Array)matched.Column("id");
                Assert.Equal(12L, ids.GetValue(0)!.Value);
                return new RecordBatch(matched.Schema,
                    new IArrowArray[] { new Int64Array.Builder().Append(9999L).Build() }, 1);
            });
            Assert.Equal(1, rows);
        }

        await using var check = await OpenAsync();
        var after = (await ReadMetaAsync(check)).Select(r => r.Id).OrderBy(x => x).ToArray();
        Assert.Equal(new long[] { 1, 2, 3, 11, 13, 21, 22, 23, 9999 }, after);
    }

    /// <summary>THE DEFECT THIS FIXES: before UpdateAsync got the guard, a `_metadata` predicate it could not
    /// lower fell through to a row mask that binds DATA columns only — so it would mis-evaluate rather than
    /// refuse. DeleteAsync rejected it; UpdateAsync did not. Now both do.</summary>
    [Fact]
    public async Task MetadataPredicateUpdate_ThatCannotLower_IsRejected_LikeDelete()
    {
        await using var table = await CreateTrackedAsync();
        var pred = new EngineeredWood.Expressions.AndPredicate(new EngineeredWood.Expressions.Predicate[]
        {
            Ex.Equal(MetadataPredicate.FilePathColumn, "somewhere.parquet"),
            Ex.Equal("id", 2L),
        });
        var ex = await Assert.ThrowsAsync<NotSupportedException>(
            async () => await table.UpdateAsync(pred, b => b));
        Assert.Contains("_metadata", ex.Message);
        Assert.Contains("UPDATE", ex.Message);

        // untouched
        Assert.Equal(9, (await ReadMetaAsync(table)).Count);
    }

    /// <summary>A pushed filter prunes files, and the surviving rows still carry a correct locator — the
    /// metadata read goes through the same planner as every other read.</summary>
    [Fact]
    public async Task Metadata_HonoursAPushedFilter()
    {
        await using var table = await CreateTrackedAsync();
        var all = await ReadMetaAsync(table);

        var filtered = new List<MetaRow>();
        var pred = EngineeredWood.Expressions.Expressions.GreaterThanOrEqual("id", 21L);
        await foreach (var batch in table.ReadAllWithMetadataAsync(null, pred))
        {
            var ids = (Int64Array)batch.Column("id");
            var path = (StringArray)batch.Column(MetadataPredicate.FilePathColumn);
            for (int i = 0; i < batch.Length; i++)
                filtered.Add(new MetaRow(ids.GetValue(i)!.Value, path.GetString(i), 0, null, null));
        }

        // superset-safe: pruning may keep a whole file, but it must not lose the matching rows
        Assert.All(new long[] { 21, 22, 23 }, id => Assert.Contains(id, filtered.Select(r => r.Id)));
        Assert.True(filtered.Count <= all.Count);
        var active = table.CurrentSnapshot.ActiveFiles.Values.Select(a => a.Path).ToHashSet(StringComparer.Ordinal);
        Assert.All(filtered, r => Assert.Contains(r.FilePath, active));
    }
}
