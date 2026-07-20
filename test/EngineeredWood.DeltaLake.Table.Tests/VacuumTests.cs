// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.DeletionVectors;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

public class VacuumTests : IDisposable
{
    private readonly string _tempDir;

    public VacuumTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_vacuum_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    [Fact]
    public async Task Vacuum_DryRun_ListsFilesToDelete()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        // Write data then overwrite (leaving orphaned file)
        var batch1 = new RecordBatch(schema,
            [new Int64Array.Builder().Append(1).Build()], 1);
        await table.WriteAsync([batch1]);

        var batch2 = new RecordBatch(schema,
            [new Int64Array.Builder().Append(2).Build()], 1);
        await table.WriteAsync([batch2], DeltaWriteMode.Overwrite);

        // Vacuum with zero retention (all unreferenced files eligible)
        var result = await table.VacuumAsync(
            retentionPeriod: TimeSpan.Zero,
            dryRun: true);

        // Should find the orphaned file from the first write
        Assert.NotEmpty(result.FilesToDelete);
        Assert.Equal(0, result.FilesDeleted); // Dry run → nothing deleted
    }

    [Fact]
    public async Task Vacuum_DeletesOrphanedFiles()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        // Write then overwrite
        var batch1 = new RecordBatch(schema,
            [new Int64Array.Builder().Append(1).Build()], 1);
        await table.WriteAsync([batch1]);

        var batch2 = new RecordBatch(schema,
            [new Int64Array.Builder().Append(2).Build()], 1);
        await table.WriteAsync([batch2], DeltaWriteMode.Overwrite);

        // Vacuum with zero retention — actually delete
        var result = await table.VacuumAsync(
            retentionPeriod: TimeSpan.Zero,
            dryRun: false);

        Assert.NotEmpty(result.FilesToDelete);
        Assert.Equal(result.FilesToDelete.Count, result.FilesDeleted);

        // Verify deleted files no longer exist
        foreach (string path in result.FilesToDelete)
        {
            Assert.False(File.Exists(Path.Combine(_tempDir, path)));
        }

        // Table data should still be readable (only the active file remains)
        int totalRows = 0;
        await foreach (var b in table.ReadAllAsync())
            totalRows += b.Length;
        Assert.Equal(1, totalRows);
    }

    [Fact]
    public async Task Vacuum_RespectsRetention()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        var batch1 = new RecordBatch(schema,
            [new Int64Array.Builder().Append(1).Build()], 1);
        await table.WriteAsync([batch1]);

        var batch2 = new RecordBatch(schema,
            [new Int64Array.Builder().Append(2).Build()], 1);
        await table.WriteAsync([batch2], DeltaWriteMode.Overwrite);

        // Vacuum with 7-day retention — files just created won't qualify
        var result = await table.VacuumAsync(
            retentionPeriod: TimeSpan.FromDays(7),
            dryRun: true);

        // Recently created files should not be eligible for deletion
        Assert.Empty(result.FilesToDelete);
    }

    [Fact]
    public async Task Vacuum_HonorsDeletedFileRetentionDurationProperty()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        // The table property sets the DEFAULT retention to zero, so a RETAIN-less vacuum collects a
        // just-orphaned file. Without honoring the property, DeltaTableOptions.VacuumRetention (days)
        // would protect it and FilesToDelete would be empty.
        await using var table = await DeltaTable.CreateAsync(fs, schema,
            configuration: new Dictionary<string, string>
            {
                ["delta.deletedFileRetentionDuration"] = "interval 0 seconds",
            });
        await table.WriteAsync([new RecordBatch(schema,
            [new Int64Array.Builder().Append(1).Build()], 1)]);
        await table.WriteAsync([new RecordBatch(schema,
            [new Int64Array.Builder().Append(2).Build()], 1)], DeltaWriteMode.Overwrite);

        var result = await table.VacuumAsync(dryRun: false); // NO explicit retention → property drives it
        Assert.NotEmpty(result.FilesToDelete);
    }

    [Fact]
    public async Task Vacuum_CollectsOrphanBinDv_AndProtectsLiveDv()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();
        await using var table = await DeltaTable.CreateAsync(fs, schema);

        var idb = new Int64Array.Builder();
        for (long i = 0; i < 2000; i++) idb.Append(i);
        await table.WriteAsync([new RecordBatch(schema, [idb.Build()], 2000)]);
        var addFile = table.CurrentSnapshot.ActiveFiles.Values.First();

        // A large scattered delete spills to an ON-DISK ("u") deletion vector (past the 1 KB inline
        // threshold) — the writer materializes a deletion_vector_<uuid>.bin at the table root.
        var deleted = Enumerable.Range(0, 600).Select(i => (long)(i * 3)).ToList();
        var liveDv = await new DeletionVectorWriter(fs).CreateAsync(deleted, deleted.Count);
        Assert.Equal("u", liveDv.StorageType); // confirm on-disk, not inline
        string liveBin = Directory.GetFiles(_tempDir, "*.bin", SearchOption.AllDirectories).Single();

        // Reference the on-disk DV from the (only) active add.
        var log = new TransactionLog(fs);
        await log.WriteCommitAsync(table.CurrentSnapshot.Version + 1, new List<DeltaAction>
        {
            new RemoveFile
            {
                Path = addFile.Path, DataChange = false,
                DeletionTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            },
            new AddFile
            {
                Path = addFile.Path, PartitionValues = addFile.PartitionValues, Size = addFile.Size,
                ModificationTime = addFile.ModificationTime, DataChange = false, Stats = addFile.Stats,
                DeletionVector = liveDv,
            },
        });
        await table.RefreshAsync();

        // Drop an ORPHAN .bin beside the live one (a superseded DV left behind by an earlier delete).
        string orphanBin = Path.Combine(_tempDir, "deletion_vector_orphaned.bin");
        File.WriteAllBytes(orphanBin, new byte[] { 1, 2, 3, 4 });

        // RETAIN 0 → every unreferenced file is eligible; the live DV must SURVIVE, the orphan must go.
        await table.VacuumAsync(retentionPeriod: TimeSpan.Zero, dryRun: false);

        Assert.False(File.Exists(orphanBin), "orphan .bin should be collected");
        Assert.True(File.Exists(liveBin), "live on-disk deletion vector must be protected");

        // The live DV is intact + readable after the vacuum: 2000 - 600 rows remain.
        int rows = 0;
        await foreach (var b in table.ReadAllAsync()) rows += b.Length;
        Assert.Equal(1400, rows);
    }

    [Fact]
    public async Task Vacuum_RefusesAbsolutePathDeletionVector()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();
        await using var table = await DeltaTable.CreateAsync(fs, schema);
        await table.WriteAsync([new RecordBatch(schema,
            [new Int64Array.Builder().Append(1).Append(2).Build()], 2)]);
        var addFile = table.CurrentSnapshot.ActiveFiles.Values.First();

        // An absolute-path ("p") deletion vector — its file cannot be proven to lie inside the swept dir.
        var log = new TransactionLog(fs);
        await log.WriteCommitAsync(table.CurrentSnapshot.Version + 1, new List<DeltaAction>
        {
            new RemoveFile
            {
                Path = addFile.Path, DataChange = false,
                DeletionTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            },
            new AddFile
            {
                Path = addFile.Path, PartitionValues = addFile.PartitionValues, Size = addFile.Size,
                ModificationTime = addFile.ModificationTime, DataChange = false, Stats = addFile.Stats,
                DeletionVector = new DeletionVector
                {
                    StorageType = "p", PathOrInlineDv = "/abs/deletion_vector_x.bin",
                    SizeInBytes = 4, Cardinality = 1,
                },
            },
        });
        await table.RefreshAsync();

        await Assert.ThrowsAsync<NotSupportedException>(
            () => table.VacuumAsync(TimeSpan.Zero).AsTask());
    }
}
