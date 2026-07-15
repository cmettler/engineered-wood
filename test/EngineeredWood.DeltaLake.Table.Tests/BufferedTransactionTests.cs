// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The BUFFERED (multi-statement) transaction seams: a host can hold a whole transaction's changes —
/// schema ALTERs, appends, deletion-vector DML — and commit them as ONE atomic Delta version, the same
/// OptimisticTransaction shape Spark/delta-rs use. The seams composed here:
///
/// <list type="bullet">
/// <item><see cref="DeltaTable.ComputeAddColumn"/> (+ the Compute* family) — the compute-only halves of
/// the schema ALTERs: metaData + protocol-upgrade actions, chainable against a pending base;</item>
/// <item><see cref="DeltaTable.WriteDataFilesAsync"/> — the write-no-commit half of the batch path
/// (files land on storage at statement time; a rollback simply abandons them as orphans);</item>
/// <item><see cref="DeltaTable.ComputeDeletionVectorActionsAsync"/> — the deferred half of the DV
/// delete, resolved against the transaction's PINNED snapshot;</item>
/// <item><see cref="DeltaTable.CommitDataFilesAsync"/> with <c>extraActions</c> +
/// <c>expectedVersion</c> — everything fuses into one commit; a concurrent writer turns the commit into
/// a conflict-abort instead of a silent append-retry (first-committer-wins snapshot isolation);</item>
/// <item><see cref="DeltaTable.ReconcileBatchToFields"/> — the read side of "read your own (pending)
/// schema": committed batches reconciled to a not-yet-committed schema (typed-NULL backfill);</item>
/// <item><see cref="DeltaTable.ReadRowsByRowIdsAsync"/> with <c>atVersion</c> — exact-row read-back for
/// UPDATE post-image construction, pinned to the snapshot the rowids were scanned against;</item>
/// <item><see cref="TransactionId"/> actions — Delta application transactions (idempotent-producer
/// versions), committed atomically with the data.</item>
/// </list>
/// </summary>
public class BufferedTransactionTests : IDisposable
{
    private readonly string _tempDir;

    public BufferedTransactionTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_buftxn_{Guid.NewGuid():N}");
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

    private async Task<DeltaTable> CreateTableAsync()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var table = await DeltaTable.CreateAsync(fs, BuildSchema(), configuration: DvConfig);
        await table.WriteAsync([BuildBatch(1, 5)]);
        return table;
    }

    private Task<DeltaTable> OpenAsync() => DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir)).AsTask();

    [Fact]
    public async Task FusedCommit_AlterInsertDelete_IsOneAtomicVersion()
    {
        await using var table = await CreateTableAsync();
        var pinned = table.CurrentSnapshot; // v1: ids 1..5, one file

        // "ALTER TABLE ADD COLUMN extra INT" — computed, not committed
        var change = table.ComputeAddColumn(new Field("extra", Int32Type.Default, true));

        // "INSERT" under the pending schema — the file is written NOW, the add is deferred
        var widened = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("value", StringType.Default, true))
            .Field(new Field("extra", Int32Type.Default, true))
            .Build();
        var insertBatch = new RecordBatch(widened,
        [
            new Int64Array.Builder().Append(6).Append(7).Build(),
            new StringArray.Builder().Append("v6").Append("v7").Build(),
            new Int32Array.Builder().Append(60).Append(70).Build(),
        ], 2);
        var files = await table.WriteDataFilesAsync([insertBatch], schemaOverride: change.NewSchema);

        // "DELETE WHERE id = 2" — position 1 of ordinal 0 in the PINNED snapshot
        var (dvActions, rowsDeleted) = await table.ComputeDeletionVectorActionsAsync(
            new Dictionary<int, IReadOnlyCollection<long>> { [0] = new long[] { 1 } },
            resolveAgainst: pinned);
        Assert.Equal(1, rowsDeleted);

        // COMMIT: everything in ONE version
        var extra = new List<DeltaAction>();
        extra.AddRange(change.Actions);
        extra.AddRange(dvActions);
        long committed = await table.CommitDataFilesAsync(files, DeltaWriteMode.Append,
            extraActions: extra, expectedVersion: pinned.Version, operation: "TRANSACTION");
        Assert.Equal(pinned.Version + 1, committed);

        // read back through a fresh handle: old rows NULL-backfilled, new rows carry values, id 2 gone
        await using var check = await OpenAsync();
        var seen = new Dictionary<long, int?>();
        await foreach (var batch in check.ReadAllAsync())
        {
            var ids = (Int64Array)batch.Column(0);
            var extras = (Int32Array)batch.Column(2);
            for (int i = 0; i < batch.Length; i++)
                seen[ids.GetValue(i)!.Value] = extras.IsNull(i) ? null : extras.GetValue(i);
        }
        Assert.Equal(new long[] { 1, 3, 4, 5, 6, 7 }, seen.Keys.OrderBy(k => k).ToArray());
        Assert.Null(seen[1]);
        Assert.Equal(60, seen[6]);
        Assert.Equal(70, seen[7]);

        // the history shows ONE commit with the transaction's operation
        var history = new List<DeltaTable.DeltaHistoryEntry>();
        await foreach (var entry in check.GetHistoryAsync())
            history.Add(entry);
        Assert.Equal(committed, history[^1].Version);
        Assert.Equal("TRANSACTION", history[^1].Operation);
    }

    [Fact]
    public async Task ChainedComputes_SecondAddComposesOnPendingSchema()
    {
        await using var table = await CreateTableAsync();
        var pinned = table.CurrentSnapshot;

        // two ALTERs in one transaction: the second composes on the FIRST's pending metadata — the
        // fused commit carries only the FINAL metaData action (a commit must not carry two).
        var c1 = table.ComputeAddColumn(new Field("e1", Int32Type.Default, true));
        var c2 = table.ComputeAddColumn(new Field("e2", StringType.Default, true), c1.Metadata, c1.ProtocolUpgrade);

        long committed = await table.CommitDataFilesAsync(
            [], DeltaWriteMode.Append, extraActions: c2.Actions,
            expectedVersion: pinned.Version, operation: "ALTER TABLE");
        Assert.Equal(pinned.Version + 1, committed);

        await using var check = await OpenAsync();
        var names = check.ArrowSchema.FieldsList.Select(f => f.Name).ToArray();
        Assert.Contains("e1", names);
        Assert.Contains("e2", names);
    }

    [Fact]
    public async Task ReconcileBatchToFields_BackfillsPendingColumn()
    {
        // "read your own (pending) schema": committed 2-column batches served under a pending 3-column
        // schema — the added column appears as a typed all-NULL array.
        await using var table = await CreateTableAsync();
        var pendingFields = new List<Field>
        {
            new("id", Int64Type.Default, false),
            new("value", StringType.Default, true),
            new("extra", Int32Type.Default, true),
        };

        await foreach (var batch in table.ReadAllAsync())
        {
            var reconciled = DeltaTable.ReconcileBatchToFields(batch, pendingFields);
            Assert.Equal(3, reconciled.ColumnCount);
            Assert.Equal(batch.Length, reconciled.Length);
            var extra = Assert.IsType<Int32Array>(reconciled.Column(2));
            for (int i = 0; i < extra.Length; i++)
                Assert.True(extra.IsNull(i));
        }
    }

    [Fact]
    public async Task ReadRowsByRowIds_AtVersion_ExactReadBack()
    {
        await using var table = await CreateTableAsync();
        long pinnedVersion = table.CurrentSnapshot.Version;

        // a concurrent append moves the table (and could shift path-sorted ordinals)
        await using (var racer = await OpenAsync())
        {
            await racer.WriteAsync([BuildBatch(100, 5)]);
        }

        // rowids (ordinal 0, positions 1 and 3 = ids 2 and 4) resolve against the PINNED snapshot
        await using var reader = await OpenAsync();
        var ids = new List<long>();
        await foreach (var batch in reader.ReadRowsByRowIdsAsync([1L, 3L], atVersion: pinnedVersion))
        {
            var col = (Int64Array)batch.Column(0);
            for (int i = 0; i < batch.Length; i++)
                ids.Add(col.GetValue(i)!.Value);
        }
        ids.Sort();
        Assert.Equal(new long[] { 2, 4 }, ids);
    }

    [Fact]
    public async Task ExpectedVersion_ConcurrentWriter_ConflictAborts()
    {
        await using var table = await CreateTableAsync();
        var pinned = table.CurrentSnapshot;
        var (dvActions, _) = await table.ComputeDeletionVectorActionsAsync(
            new Dictionary<int, IReadOnlyCollection<long>> { [0] = new long[] { 1 } },
            resolveAgainst: pinned);

        await using (var racer = await OpenAsync())
        {
            await racer.WriteAsync([BuildBatch(100, 2)]);
        }

        // expectedVersion turns the append OCC retry into a conflict-ABORT — snapshot-coupled actions
        // must not silently land on a moved table (the caller re-validates/rebases and retries itself).
        await Assert.ThrowsAsync<DeltaConflictException>(async () =>
            await table.CommitDataFilesAsync([], DeltaWriteMode.Append,
                extraActions: dvActions, expectedVersion: pinned.Version, operation: "DELETE"));
    }

    [Fact]
    public async Task AppTransactionAction_RoundTrips()
    {
        // Delta application transactions (the `txn` action): an idempotent producer commits its
        // application-level version ATOMICALLY with the data; the snapshot exposes the high-water mark.
        await using var table = await CreateTableAsync();
        long nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await table.CommitDataFilesAsync([], DeltaWriteMode.Append,
            extraActions: [new TransactionId { AppId = "producer-1", Version = 42, LastUpdated = nowMs }],
            expectedVersion: table.CurrentSnapshot.Version, operation: "WRITE");

        await using var check = await OpenAsync();
        Assert.True(check.CurrentSnapshot.AppTransactions.TryGetValue("producer-1", out var txn));
        Assert.Equal(42, txn!.Version);
    }
}
