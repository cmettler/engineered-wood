// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// A table written through DML must checkpoint on the interval, exactly as a batch-written one does.
///
/// <para>These pin upstream issue #86. <c>CommitOccAsync</c> — the loop behind the transaction commit, the
/// row-level DML, the copy-on-write rewrites, compaction and the schema changes — used to pass
/// <c>WriteCheckpointOnInterval = false</c>, so only <c>CommitWriteAsync</c> and
/// <c>CommitDataFilesAsync</c> ever produced one. A table written exclusively through DML therefore never
/// got a checkpoint and never got a <c>_last_checkpoint</c>, which costs three compounding things: every
/// open replays the log from v0; foreign readers get no resume hint; and commits accumulate without bound,
/// because log cleanup is defined in terms of what a checkpoint subsumes.</para>
///
/// <para><b>⚠ The DELETE case is the load-bearing one.</b> A test that only checks a batch APPEND passes
/// with the defect fully present — that path always checkpointed. What distinguishes them is the commit
/// loop, so the assertion has to name a version that a DML statement produced.</para>
/// </summary>
public class DmlCheckpointTests : IDisposable
{
    private readonly string _tempDir;

    public DmlCheckpointTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_dmlckpt_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static Apache.Arrow.Schema IdSchema() =>
        new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

    private static RecordBatch Rows(Apache.Arrow.Schema schema, params long[] ids)
    {
        var b = new Int64Array.Builder();
        foreach (long id in ids)
            b.Append(id);
        return new RecordBatch(schema, [b.Build()], ids.Length);
    }

    /// <summary>
    /// The regression itself: drive the version to a multiple of the interval using DELETEs and assert the
    /// checkpoint for THAT version exists. A checkpoint interval of 4 keeps the test to a handful of
    /// commits; the version is asserted rather than assumed, so the test cannot pass by landing somewhere
    /// else.
    /// </summary>
    [Fact]
    public async Task DeleteCommits_Checkpoint_OnTheInterval()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();
        var options = new DeltaTableOptions { CheckpointInterval = 4 };

        // Deletion vectors ON: these DELETEs remove SOME rows of one file, which is the row-level path our
        // host takes and the one that goes through CommitOccAsync. Without them EW refuses the partial
        // delete outright and the test would never reach the commit loop it exists to check.
        await using var table = await DeltaTable.CreateAsync(
            fs, schema, options, enableDeletionVectors: true);                     // v0
        await table.WriteAsync([Rows(schema, 1, 2, 3, 4, 5, 6)]);                  // v1

        // v2, v3, v4 — all through DeleteAsync, i.e. CommitOccAsync.
        long version = 0;
        for (long target = 1; target <= 3; target++)
        {
            long capture = target;
            (_, version) = await table.DeleteAsync(batch =>
            {
                var ids = (Int64Array)batch.Column(0);
                var mask = new BooleanArray.Builder();
                for (int i = 0; i < ids.Length; i++)
                    mask.Append(ids.GetValue(i) == capture);
                return mask.Build();
            });
        }

        // The assertion is only meaningful if a DELETE actually produced the interval version.
        Assert.Equal(4, version);
        Assert.True(
            await fs.ExistsAsync(DeltaVersion.CheckpointPath(4)),
            "a DELETE landed on the checkpoint interval but wrote no checkpoint — issue #86");

        // ...and the table is still readable through it, so this is a checkpoint and not just a file.
        int rows = 0;
        await foreach (var b in table.ReadAllAsync())
            rows += b.Length;
        Assert.Equal(3, rows); // 6 written, 3 deleted
    }

    /// <summary>
    /// POSITIVE CONTROL. The batch write path always checkpointed, so if this fails the mechanism is broken
    /// generally (interval, writer wiring) rather than specifically on the DML loop — which is a different
    /// bug, and without this test the DML assertion above could not tell the two apart.
    /// </summary>
    [Fact]
    public async Task WriteCommits_Checkpoint_OnTheInterval()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();
        var options = new DeltaTableOptions { CheckpointInterval = 4 };

        await using var table = await DeltaTable.CreateAsync(fs, schema, options); // v0
        for (long i = 1; i <= 4; i++)
            await table.WriteAsync([Rows(schema, i)]);                             // v1..v4

        Assert.Equal(4, table.CurrentSnapshot.Version);
        Assert.True(await fs.ExistsAsync(DeltaVersion.CheckpointPath(4)));
    }

    /// <summary>
    /// A table's own <c>delta.checkpointInterval</c> is what it gets checkpointed at — the property is part
    /// of the Delta spec and is the table's statement about a cost it pays per commit. Reading only
    /// <see cref="DeltaTableOptions.CheckpointInterval"/> meant a table declaring 25 was still checkpointed
    /// every 10, i.e. honouring someone else's declaration incorrectly rather than neutrally.
    /// </summary>
    [Fact]
    public async Task CheckpointInterval_ComesFromTheTableProperty_WhenItDeclaresOne()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();

        await using (var created = await DeltaTable.CreateAsync(
            fs, schema,
            configuration: new Dictionary<string, string> { ["delta.checkpointInterval"] = "4" }))
        {
        }

        // Re-opened so the property is read from the snapshot, which is where a foreign writer's would be.
        await using var table = await DeltaTable.OpenAsync(fs, new DeltaTableOptions { CheckpointInterval = 10 });
        for (long i = 1; i <= 4; i++)
            await table.WriteAsync([Rows(schema, i)]);

        Assert.Equal(4, table.CurrentSnapshot.Version);
        Assert.True(
            await fs.ExistsAsync(DeltaVersion.CheckpointPath(4)),
            "the table declares delta.checkpointInterval = 4 and was not checkpointed at v4");
    }

    /// <summary>
    /// THE CONTROL. Without it the test above passes equally if the caller's option had simply started
    /// being ignored in favour of a hardcoded 4 — a different bug with the same symptom on one table.
    /// </summary>
    [Fact]
    public async Task CheckpointInterval_FallsBackToTheCallerOption_WhenTheTableDeclaresNone()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, new DeltaTableOptions { CheckpointInterval = 4 });
        for (long i = 1; i <= 4; i++)
            await table.WriteAsync([Rows(schema, i)]);

        Assert.Equal(4, table.CurrentSnapshot.Version);
        Assert.True(await fs.ExistsAsync(DeltaVersion.CheckpointPath(4)));
    }

    /// <summary>
    /// <c>CheckpointInterval = 0</c> means "never checkpoint" and is an ABSOLUTE caller override: a host
    /// that owns checkpointing on its own schedule must not have a table property switch it back on.
    /// </summary>
    [Fact]
    public async Task CheckpointInterval_Zero_StaysDisabled_EvenWhenTheTableDeclaresOne()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();

        await using (var created = await DeltaTable.CreateAsync(
            fs, schema,
            configuration: new Dictionary<string, string> { ["delta.checkpointInterval"] = "2" }))
        {
        }

        await using var table = await DeltaTable.OpenAsync(fs, new DeltaTableOptions { CheckpointInterval = 0 });
        for (long i = 1; i <= 4; i++)
            await table.WriteAsync([Rows(schema, i)]);

        Assert.False(await fs.ExistsAsync(DeltaVersion.CheckpointPath(2)));
        Assert.False(await fs.ExistsAsync(DeltaVersion.CheckpointPath(4)));
    }

    /// <summary>
    /// The hint file is what a FOREIGN reader uses to skip the replay — a checkpoint nobody can find helps
    /// only us. Asserted on the DML path for the same reason as above.
    /// </summary>
    [Fact]
    public async Task DeleteCommits_Publish_LastCheckpointHint()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();
        var options = new DeltaTableOptions { CheckpointInterval = 2 };

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, options, enableDeletionVectors: true);                     // v0
        await table.WriteAsync([Rows(schema, 1, 2, 3)]);                           // v1

        (_, long version) = await table.DeleteAsync(batch =>                       // v2
        {
            var ids = (Int64Array)batch.Column(0);
            var mask = new BooleanArray.Builder();
            for (int i = 0; i < ids.Length; i++)
                mask.Append(ids.GetValue(i) == 1);
            return mask.Build();
        });

        Assert.Equal(2, version);
        Assert.True(await fs.ExistsAsync(DeltaVersion.LastCheckpointPath));
    }
}
