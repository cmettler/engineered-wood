// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;

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

    private static async Task<List<MetaRow>> ReadMetaAsync(DeltaTable table)
    {
        var rows = new List<MetaRow>();
        await foreach (var batch in table.ReadAllWithMetadataAsync())
        {
            var ids = (Int64Array)batch.Column("id");
            var meta = (StructArray)batch.Column(DeltaTable.MetadataColumnName);
            var path = (StringArray)meta.Fields[0];
            var idx = (Int64Array)meta.Fields[1];
            var rid = (Int64Array)meta.Fields[2];
            var ver = (Int64Array)meta.Fields[3];
            for (int i = 0; i < batch.Length; i++)
            {
                rows.Add(new MetaRow(
                    ids.GetValue(i)!.Value, path.GetString(i), idx.GetValue(i)!.Value,
                    rid.IsNull(i) ? null : rid.GetValue(i), ver.IsNull(i) ? null : ver.GetValue(i)));
            }
        }
        return rows;
    }

    /// <summary>The struct has exactly the four documented members, in order, with the nullability split that
    /// distinguishes locator from identity.</summary>
    [Fact]
    public async Task Metadata_HasFourMembers_WithTheLocatorNonNullAndTheIdsNullable()
    {
        await using var table = await CreateTrackedAsync();
        await foreach (var batch in table.ReadAllWithMetadataAsync())
        {
            var f = batch.Schema.GetFieldByName(DeltaTable.MetadataColumnName);
            Assert.NotNull(f);
            var st = (StructType)f!.DataType;
            Assert.Equal(
                new[] { "file_path", "row_index", "row_id", "row_commit_version" },
                st.Fields.Select(x => x.Name).ToArray());
            Assert.False(st.Fields[0].IsNullable);   // locator
            Assert.False(st.Fields[1].IsNullable);   // locator
            Assert.True(st.Fields[2].IsNullable);    // identity — NULL when underivable
            Assert.True(st.Fields[3].IsNullable);
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

    /// <summary>The locator round-trips into a <see cref="FileRowSelection"/> that deletes exactly the intended
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
            var (deleted, _) = await table.DeleteBySelectionViaVectorsAsync(new FileRowSelection(byFile));
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
            await table.DeleteBySelectionViaVectorsAsync(new FileRowSelection(
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

    /// <summary>Without row tracking the SHAPE is unchanged — still four members — and the identity half is
    /// simply all-null, so a consumer binds one schema regardless of table configuration.</summary>
    [Fact]
    public async Task WithoutRowTracking_ShapeIsUnchanged_AndTheIdsAreNull()
    {
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), BuildSchema());
        await table.WriteAsync([BuildBatch(1, 2)]);
        await table.WriteAsync([BuildBatch(11, 2)]);

        var rows = await ReadMetaAsync(table);
        Assert.Equal(4, rows.Count);
        Assert.All(rows, r => Assert.Null(r.RowId));
        Assert.All(rows, r => Assert.Null(r.Version));
        // the locator half is still fully populated and usable
        Assert.All(rows, r => Assert.False(string.IsNullOrEmpty(r.FilePath)));
        Assert.Equal(2, rows.Select(r => r.FilePath).Distinct().Count());
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
            var meta = (StructArray)batch.Column(DeltaTable.MetadataColumnName);
            var path = (StringArray)meta.Fields[0];
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
