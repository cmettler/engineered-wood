// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// <see cref="DeltaTable.CheckLogicalRebaseAsync"/> — Spark ConflictChecker parity for buffered
/// (multi-statement) transactions: a transaction whose changes were computed against a BASE snapshot may
/// commit on top of concurrent writers when those commits COMMUTE; real conflicts throw. The isolation
/// split matches Delta's two levels:
///
/// <list type="bullet">
/// <item><b>WriteSerializable</b> (<c>serializable: false</c>, the Spark default): blind appends may be
/// logically reordered before the transaction — a concurrent append never conflicts with the
/// transaction's READS unless the committing writer also removed/changed something.</item>
/// <item><b>Serializable</b> (<c>serializable: true</c>): commit order is the logical order — a blind
/// append MATCHING the transaction's read predicates conflicts too.</item>
/// </list>
///
/// Read-predicate-vs-file matching is stats-based (<c>DeltaFilePruner</c>): a predicate that provably
/// excludes the concurrent file's rows does not conflict even under Serializable. Compaction
/// (<c>dataChange=false</c>) rearranges rows without changing them and is exempt from the read checks.
/// </summary>
public class LogicalRebaseTests : IDisposable
{
    private readonly string _tempDir;

    public LogicalRebaseTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_rebase_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static readonly Dictionary<string, string> DvConfig = new()
    {
        ["delta.enableDeletionVectors"] = "true",
    };

    private static Apache.Arrow.Schema BuildSchema() => new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("value", StringType.Default, true))
        .Build();

    private static RecordBatch BuildBatch(long startId, int count)
    {
        var ids = new Int64Array.Builder();
        var values = new StringArray.Builder();
        for (int i = 0; i < count; i++)
        {
            ids.Append(startId + i);
            values.Append("v" + (startId + i));
        }
        return new RecordBatch(BuildSchema(), [ids.Build(), values.Build()], count);
    }

    /// <summary>Table with rows 1..10 in one file; returns the handle (its CurrentSnapshot = the base).</summary>
    private async Task<DeltaTable> CreateTableAsync()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var table = await DeltaTable.CreateAsync(fs, BuildSchema(), configuration: DvConfig);
        await table.WriteAsync([BuildBatch(1, 10)]);
        return table;
    }

    private Task<DeltaTable> OpenAsync() => DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir)).AsTask();

    [Fact]
    public async Task BlindAppend_MatchingReads_Passes_WriteSerializable()
    {
        await using var table = await CreateTableAsync();
        var baseSnapshot = table.CurrentSnapshot;

        await using (var racer = await OpenAsync())
        {
            await racer.WriteAsync([BuildBatch(100, 5)]); // blind append, ids 100..104
        }

        await using var committer = await OpenAsync();
        // the transaction READ rows the append matches — WriteSerializable reorders the append before us
        await committer.CheckLogicalRebaseAsync(baseSnapshot, [],
            readPredicates: [Ex.Equal("id", 102L)], serializable: false);
    }

    [Fact]
    public async Task BlindAppend_MatchingReads_Conflicts_Serializable()
    {
        await using var table = await CreateTableAsync();
        var baseSnapshot = table.CurrentSnapshot;

        await using (var racer = await OpenAsync())
        {
            await racer.WriteAsync([BuildBatch(100, 5)]);
        }

        await using var committer = await OpenAsync();
        await Assert.ThrowsAsync<DeltaConflictException>(async () =>
            await committer.CheckLogicalRebaseAsync(baseSnapshot, [],
                readPredicates: [Ex.Equal("id", 102L)], serializable: true));
    }

    [Fact]
    public async Task BlindAppend_NonMatchingPredicate_Passes_Serializable()
    {
        await using var table = await CreateTableAsync();
        var baseSnapshot = table.CurrentSnapshot;

        await using (var racer = await OpenAsync())
        {
            await racer.WriteAsync([BuildBatch(100, 5)]); // ids 100..104
        }

        await using var committer = await OpenAsync();
        // the file's stats (min 100, max 104) provably exclude id=5 — no conflict even under Serializable
        await committer.CheckLogicalRebaseAsync(baseSnapshot, [],
            readPredicates: [Ex.Equal("id", 5L)], serializable: true);
    }

    [Fact]
    public async Task ConcurrentDeleteOfReadFile_Conflicts()
    {
        await using var table = await CreateTableAsync();
        var baseSnapshot = table.CurrentSnapshot;

        await using (var racer = await OpenAsync())
        {
            // a DV delete swaps the file: remove(dataChange) + add — invalidates what the transaction read
            await racer.DeleteByRowIdsViaVectorsAsync([3L]); // ordinal 0, position 3
        }

        await using var committer = await OpenAsync();
        await Assert.ThrowsAsync<DeltaConflictException>(async () =>
            await committer.CheckLogicalRebaseAsync(baseSnapshot, [], readWholeTable: true));
    }

    [Fact]
    public async Task DeleteDelete_SameFile_Conflicts()
    {
        await using var table = await CreateTableAsync();
        var baseSnapshot = table.CurrentSnapshot;

        // the transaction's planned DV pair, resolved against the BASE snapshot
        var (planned, _) = await table.ComputeDeletionVectorActionsAsync(
            new Dictionary<int, IReadOnlyCollection<long>> { [0] = new long[] { 1 } },
            resolveAgainst: baseSnapshot);

        await using (var racer = await OpenAsync())
        {
            await racer.DeleteByRowIdsViaVectorsAsync([5L]); // same file, different row — DV changed
        }

        await using var committer = await OpenAsync();
        // strict file-level check: the planned remove's (path, DV) is no longer active unchanged.
        // (The write_serializable caller resolves this with RebaseDvDmlActionsAsync instead — see
        // RowLevelConcurrencyTests.)
        await Assert.ThrowsAsync<DeltaConflictException>(async () =>
            await committer.CheckLogicalRebaseAsync(baseSnapshot, planned));
    }

    [Fact]
    public async Task ConcurrentMetadataChange_Conflicts()
    {
        await using var table = await CreateTableAsync();
        var baseSnapshot = table.CurrentSnapshot;

        await using (var racer = await OpenAsync())
        {
            await racer.AddColumnAsync(new Field("extra", Int32Type.Default, true));
        }

        await using var committer = await OpenAsync();
        await Assert.ThrowsAsync<DeltaConflictException>(async () =>
            await committer.CheckLogicalRebaseAsync(baseSnapshot, []));
    }

    [Fact]
    public async Task Compaction_ExemptFromReadChecks()
    {
        await using var table = await CreateTableAsync();
        await table.WriteAsync([BuildBatch(11, 5)]); // second file so compaction has work
        var baseSnapshot = table.CurrentSnapshot;

        await using (var racer = await OpenAsync())
        {
            await racer.CompactAsync(new CompactionOptions { MinFileSize = long.MaxValue });
        }

        await using var committer = await OpenAsync();
        // the compaction removed BOTH files the transaction read — but dataChange=false rearranges rows
        // without changing them, so the read checks pass (Spark parity)
        await committer.CheckLogicalRebaseAsync(baseSnapshot, [], readWholeTable: true);
    }
}
