// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// <see cref="DeltaTable.PlanFiles"/> — the scan-planning API a host uses when it reads the active set
/// through its own reader. The properties under test are the ones that make the returned ordinal usable
/// as a row address: it is assigned BEFORE pruning, it is stable across filters, and it is the SAME
/// ordinal the transient row id encodes and the DML paths decode.
/// </summary>
public class PlanFilesTests : IDisposable
{
    private readonly string _tempDir;

    public PlanFilesTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_planfiles_{Guid.NewGuid():N}");
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

    /// <summary>Three commits => three files with disjoint id ranges: 0-9, 100-109, 200-209. Disjoint so a
    /// range predicate prunes on min/max stats alone, and spread so exactly one file survives each bound.</summary>
    private async Task WriteThreeFilesAsync()
    {
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdSchema);
        await table.WriteAsync([Batch(0, 10)]);
        await table.WriteAsync([Batch(100, 10)]);
        await table.WriteAsync([Batch(200, 10)]);
    }

    [Fact]
    public async Task PlanFiles_NoFilter_ReturnsEveryActiveFileInPathSortedOrderWithDenseOrdinals()
    {
        await WriteThreeFilesAsync();
        await using var table = await OpenAsync();

        var plan = table.PlanFiles();

        Assert.Equal(3, plan.Count);
        // Ordinals are dense 0..n-1 when nothing is pruned, and the paths ascend ordinally — the sort
        // the row-id contract depends on.
        Assert.Equal([0, 1, 2], plan.Select(p => p.Ordinal).ToArray());
        var paths = plan.Select(p => p.File.Path).ToArray();
        Assert.Equal(paths.OrderBy(p => p, StringComparer.Ordinal).ToArray(), paths);
    }

    [Fact]
    public async Task PlanFiles_Pruned_KeepsThePrePruneOrdinalAndLeavesAGap()
    {
        await WriteThreeFilesAsync();
        await using var table = await OpenAsync();

        var all = table.PlanFiles();
        // Which file holds ids >= 200 is layout-dependent (the paths are uuids, so the path sort is not
        // the write order) — so find it in the unfiltered plan rather than assuming an ordinal.
        var expected = await OrdinalHoldingAsync(table, 200);

        var plan = table.PlanFiles(EngineeredWood.Expressions.Expressions.GreaterThanOrEqual("id", 200L));

        var single = Assert.Single(plan);
        // THE property: the ordinal is a position in the full active set, not an index into this list.
        // Were it post-prune it would be 0 whenever exactly one file survives.
        Assert.Equal(expected, single.Ordinal);
        Assert.Equal(3, all.Count);
    }

    [Fact]
    public async Task PlanFiles_OrdinalsAgreeAcrossFilters()
    {
        await WriteThreeFilesAsync();
        await using var table = await OpenAsync();

        var unfiltered = table.PlanFiles().ToDictionary(p => p.File.Path, p => p.Ordinal);

        // Every filter that keeps a file must report that file's SAME ordinal — otherwise adding a
        // predicate would silently change what a row id means.
        foreach (long lowerBound in new long[] { 0, 100, 200 })
        {
            var filtered = table.PlanFiles(
                EngineeredWood.Expressions.Expressions.GreaterThanOrEqual("id", lowerBound));
            Assert.NotEmpty(filtered);
            foreach (var (file, ordinal) in filtered)
                Assert.Equal(unfiltered[file.Path], ordinal);
        }
    }

    [Fact]
    public async Task PlanFiles_OrdinalDecodedFromARowIdIdentifiesTheFileThatHoldsTheRow()
    {
        await WriteThreeFilesAsync();
        await using var table = await OpenAsync();

        // The contract a host reading the active set itself must reproduce: `rowid >> 40` names a file in
        // the plan, and it is the file the row actually LIVES in. Checked against file CONTENT (each
        // file's recorded min/max must bracket the row's id) rather than against another ordinal, so the
        // assertion does not just compare PlanFiles with itself — the three id ranges are disjoint, so
        // exactly one file can bracket each row and an off-by-one ordinal cannot pass.
        var byOrdinal = table.PlanFiles().ToDictionary(p => p.Ordinal, p => p.File);
        int rowsChecked = 0;

        await foreach (var batch in table.ReadAllWithRowIdsAsync(null, null))
        {
            var ids = (Int64Array)batch.Column("id");
            var rids = (Int64Array)batch.Column("_metadata.row_id");
            for (int i = 0; i < batch.Length; i++)
            {
                long id = ids.GetValue(i)!.Value;
                int ordinal = (int)(rids.GetValue(i)!.Value >> 40);

                Assert.True(byOrdinal.ContainsKey(ordinal), $"rowid decoded ordinal {ordinal} is not in the plan");
                var stats = EngineeredWood.DeltaLake.Actions.ColumnStats.Parse(byOrdinal[ordinal].Stats);
                Assert.NotNull(stats);
                Assert.Equal(id / 100 * 100, stats!.MinValues!["id"].GetInt64());   // 0, 100 or 200
                rowsChecked++;
            }
        }

        Assert.Equal(30, rowsChecked);
    }

    [Fact]
    public async Task PlanFiles_CallerSuppliedSnapshot_PlansThatVersionsFileSet()
    {
        await WriteThreeFilesAsync();
        await using var table = await OpenAsync();

        // Commit 0 is CreateAsync's schema-only commit, so the three appends are versions 1..3 and each
        // adds one file. Planning against a caller-supplied snapshot is what lets a rewrite list and
        // commit against ONE version, so a writer landing between two opens cannot manufacture a conflict.
        Assert.Empty(table.PlanFiles(snapshot: await table.GetSnapshotAtVersionAsync(0)));
        Assert.Single(table.PlanFiles(snapshot: await table.GetSnapshotAtVersionAsync(1)));
        Assert.Equal(2, table.PlanFiles(snapshot: await table.GetSnapshotAtVersionAsync(2)).Count);
        Assert.Equal(3, table.PlanFiles().Count);   // no snapshot => CurrentSnapshot
    }

    [Fact]
    public async Task PlanFiles_PruneSchemaOverride_DecidesWhetherAReferenceResolves()
    {
        await WriteThreeFilesAsync();
        await using var table = await OpenAsync();

        var filter = EngineeredWood.Expressions.Expressions.GreaterThanOrEqual("id", 200L);

        // Against the real schema the predicate resolves and two of three files prune away.
        Assert.Single(table.PlanFiles(filter));

        // Against a schema in which "id" does not exist the reference is unresolvable, so pruning must
        // keep every file — pruning never guesses. That the count changes is the proof the parameter is
        // consulted rather than ignored.
        var unrelated = new EngineeredWood.DeltaLake.Schema.StructType
        {
            Fields =
            [
                new EngineeredWood.DeltaLake.Schema.StructField
                {
                    Name = "other",
                    Type = new EngineeredWood.DeltaLake.Schema.PrimitiveType { TypeName = "long" },
                    Nullable = true,
                },
            ],
        };
        Assert.Equal(3, table.PlanFiles(filter, pruneSchema: unrelated).Count);
    }

    [Fact]
    public async Task PlanFiles_TableWithNoActiveFiles_ReturnsEmpty()
    {
        await using (var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdSchema))
        {
            await table.WriteAsync([Batch(0, 10)]);
            await table.DeleteAsync(EngineeredWood.Expressions.Expressions.GreaterThanOrEqual("id", 0L));
        }

        await using var reader = await OpenAsync();
        Assert.Empty(reader.PlanFiles());
    }

    /// <summary>The ordinal encoded in the transient rowids of the rows whose id is <paramref name="id"/>.</summary>
    private static async Task<int> OrdinalHoldingAsync(DeltaTable table, long id)
    {
        await foreach (var batch in table.ReadAllWithRowIdsAsync(null, null))
        {
            var ids = (Int64Array)batch.Column("id");
            var rids = (Int64Array)batch.Column("_metadata.row_id");
            for (int i = 0; i < batch.Length; i++)
                if (ids.GetValue(i)!.Value == id)
                    return (int)(rids.GetValue(i)!.Value >> 40);
        }
        throw new InvalidOperationException($"no row with id {id}");
    }
}
