// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

public class CompactionTests : IDisposable
{
    private readonly string _tempDir;

    public CompactionTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_compact_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    [Fact]
    public async Task Compact_MergesSmallFiles()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        // Use very small target to force compaction eligibility
        var options = new DeltaTableOptions
        {
            CheckpointInterval = 0, // Disable auto-checkpoint
        };

        await using var table = await DeltaTable.CreateAsync(fs, schema, options);

        // Write 5 small files (one row each)
        for (int i = 0; i < 5; i++)
        {
            var batch = new RecordBatch(schema,
                [new Int64Array.Builder().Append(i).Build()], 1);
            await table.WriteAsync([batch]);
        }

        Assert.Equal(5, table.CurrentSnapshot.FileCount);

        // Compact with a very large MinFileSize so all files qualify
        var compactResult = await table.CompactAsync(new CompactionOptions
        {
            MinFileSize = long.MaxValue, // All files are "small"
            TargetFileSize = long.MaxValue,
        });

        Assert.NotNull(compactResult);
        // After compaction, should have fewer files
        Assert.True(table.CurrentSnapshot.FileCount < 5);

        // Data integrity check: should still have 5 rows
        int totalRows = 0;
        await foreach (var b in table.ReadAllAsync())
            totalRows += b.Length;
        Assert.Equal(5, totalRows);
    }

    [Fact]
    public async Task Compact_PartitionedTable_CompactsPerPartition()
    {
        // THE BUG THIS PINS: compaction mixed EVERY partition's rows into one file stamped with the FIRST
        // candidate's partitionValues — after OPTIMIZE all rows read that one partition value (silent
        // corruption of the partition column). Per-partition grouping compacts each partition into its own
        // file, carrying that partition's values + landing in its Hive directory.
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("region", StringType.Default, false))
            .Field(new Field("id", Int64Type.Default, false))
            .Build();
        await using var table = await DeltaTable.CreateAsync(
            fs, schema, new DeltaTableOptions { CheckpointInterval = 0 },
            partitionColumns: ["region"]);

        // 2 appends x 2 partitions => 2 small files per partition
        for (int i = 0; i < 2; i++)
        {
            var regions = new StringArray.Builder();
            var ids = new Int64Array.Builder();
            for (int r = 0; r < 6; r++)
            {
                regions.Append(r % 2 == 0 ? "US" : "EU");
                ids.Append(i * 6 + r);
            }
            var batch = new RecordBatch(schema, [regions.Build(), ids.Build()], 6);
            await table.WriteAsync([batch]);
        }
        Assert.Equal(4, table.CurrentSnapshot.FileCount);

        var v = await table.CompactAsync(new CompactionOptions
        {
            MinFileSize = long.MaxValue,
            TargetFileSize = long.MaxValue,
        });
        Assert.NotNull(v);

        // One compacted file PER PARTITION, each carrying ITS partition's values in ITS Hive directory.
        var active = table.CurrentSnapshot.ActiveFiles.Values.ToList();
        Assert.Equal(2, active.Count);
        var byRegion = active.ToDictionary(a => a.PartitionValues.Values.Single());
        Assert.Contains("US", byRegion.Keys);
        Assert.Contains("EU", byRegion.Keys);
        Assert.All(active, a => Assert.Contains("/", a.Path)); // files live under their partition dirs

        // Values are EXACT per partition (the old behavior read every row as the first partition).
        int us = 0, eu = 0, total = 0;
        await foreach (var b in table.ReadAllAsync())
        {
            var regionCol = (StringArray)b.Column(b.Schema.GetFieldIndex("region"));
            for (int r = 0; r < b.Length; r++)
            {
                total++;
                if (regionCol.GetString(r) == "US") us++;
                else if (regionCol.GetString(r) == "EU") eu++;
            }
            b.Dispose();
        }
        Assert.Equal(12, total);
        Assert.Equal(6, us);
        Assert.Equal(6, eu);
    }

    [Fact]
    public async Task Compact_PartitionedTable_SingleFilePartitionLeftAlone()
    {
        // A partition with only ONE small file is not worth compacting — its file must stay ACTIVE
        // (the old list-level `candidates.Count < 2` check does not translate to groups).
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("region", StringType.Default, false))
            .Field(new Field("id", Int64Type.Default, false))
            .Build();
        await using var table = await DeltaTable.CreateAsync(
            fs, schema, new DeltaTableOptions { CheckpointInterval = 0 },
            partitionColumns: ["region"]);

        // US gets 2 files, APAC only 1
        RecordBatch Batch(string region, long id)
        {
            return new RecordBatch(schema,
                [new StringArray.Builder().Append(region).Build(), new Int64Array.Builder().Append(id).Build()], 1);
        }
        await table.WriteAsync([Batch("US", 1)]);
        await table.WriteAsync([Batch("US", 2)]);
        await table.WriteAsync([Batch("APAC", 3)]);
        Assert.Equal(3, table.CurrentSnapshot.FileCount);

        var v = await table.CompactAsync(new CompactionOptions
        {
            MinFileSize = long.MaxValue,
            TargetFileSize = long.MaxValue,
        });
        Assert.NotNull(v);

        var active = table.CurrentSnapshot.ActiveFiles.Values.ToList();
        Assert.Equal(2, active.Count); // US compacted to 1, APAC's single file untouched
        long rows = 0;
        await foreach (var b in table.ReadAllAsync())
        {
            rows += b.Length;
            b.Dispose();
        }
        Assert.Equal(3, rows);
    }

    [Fact]
    public async Task Compact_NoOpWhenNotNeeded()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        // Write one file
        var batch = new RecordBatch(schema,
            [new Int64Array.Builder().Append(1).Build()], 1);
        await table.WriteAsync([batch]);

        // Compact with default settings — single file won't be compacted
        var result = await table.CompactAsync(new CompactionOptions
        {
            MinFileSize = long.MaxValue,
        });

        // Single file → no compaction needed
        Assert.Null(result);
    }
}
