// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Timestamp time travel on PLAIN tables (no <c>inCommitTimestamp</c> feature): every commit carries an
/// always-on <c>commitInfo</c> with a <c>timestamp</c> field, and
/// <see cref="DeltaTable.GetSnapshotAtTimestampAsync"/> resolves through
/// <c>inCommitTimestamp ?? commitInfo.timestamp</c> — so timestamp travel works without the writer-v7
/// feature (which then only adds the in-protocol monotonicity guarantee for Spark/Fabric interop).
/// The ICT-enabled resolution edges live in <see cref="InCommitTimestampIntegrationTests"/>.
/// </summary>
public class TimestampResolutionTests : IDisposable
{
    private readonly string _tempDir;

    public TimestampResolutionTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_tsres_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static RecordBatch Batch(long id)
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();
        return new RecordBatch(schema, [new Int64Array.Builder().Append(id).Build()], 1);
    }

    [Fact]
    public async Task PlainTable_ResolvesViaCommitInfoTimestamp()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();
        await using var table = await DeltaTable.CreateAsync(fs, schema); // v0, plain (writer v2)
        await table.WriteAsync([Batch(1)]);                               // v1
        await Task.Delay(20);                                             // distinct wall-clock ms
        await table.WriteAsync([Batch(2)]);                               // v2

        // ground truth from the history view (the same commitInfo timestamps the resolver reads)
        var history = new List<DeltaTable.DeltaHistoryEntry>();
        await foreach (var entry in table.GetHistoryAsync())
            history.Add(entry);
        Assert.All(history, e => Assert.NotNull(e.TimestampMs)); // always-on commitInfo, plain writer v2

        long v1Ts = history[1].TimestampMs!.Value;

        // exactly v1's instant → v1 (at-or-before semantics)
        var atV1 = await table.GetSnapshotAtTimestampAsync(DateTimeOffset.FromUnixTimeMilliseconds(v1Ts));
        Assert.Equal(1, atV1.Version);

        // just before v2 → still v1
        long v2Ts = history[2].TimestampMs!.Value;
        var beforeV2 = await table.GetSnapshotAtTimestampAsync(DateTimeOffset.FromUnixTimeMilliseconds(v2Ts - 1));
        Assert.Equal(1, beforeV2.Version);

        // far future → latest
        var future = await table.GetSnapshotAtTimestampAsync(DateTimeOffset.UtcNow.AddDays(1));
        Assert.Equal(2, future.Version);

        // before commit-0 → a clean error, never a silent current-data result
        await Assert.ThrowsAnyAsync<Exception>(async () =>
            await table.GetSnapshotAtTimestampAsync(DateTimeOffset.FromUnixTimeMilliseconds(0)));
    }
}
