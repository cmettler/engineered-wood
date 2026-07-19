// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.IO.Local;
using ArrowStructType = Apache.Arrow.Types.StructType;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Stats for STRUCT columns: collected per leaf as nested JSON objects mirroring the schema, parsed back into
/// dotted keys ("s.a"), and consumed by the pruner so a predicate on a nested field prunes files like a
/// top-level column would.
/// </summary>
public class NestedStatsPruningTests : IDisposable
{
    private readonly string _tempDir;

    public NestedStatsPruningTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_nstats_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static Apache.Arrow.Schema NestedSchema() =>
        new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("s", new ArrowStructType(
            [
                new Field("a", Int64Type.Default, true),
                new Field("b", StringType.Default, true),
            ]), true))
            .Build();

    private static RecordBatch Batch(Apache.Arrow.Schema schema, long id, long a, string b)
    {
        var structType = (ArrowStructType)schema.FieldsList[1].DataType;
        var nested = new StructArray(structType, 1,
        [
            new Int64Array.Builder().Append(a).Build(),
            new StringArray.Builder().Append(b).Build(),
        ], ArrowBuffer.Empty);
        return new RecordBatch(schema,
            [new Int64Array.Builder().Append(id).Build(), nested], 1);
    }

    [Fact]
    public async Task StatsCollector_EmitsNestedMinMaxAndNullCount()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = NestedSchema();

        await using var table = await DeltaTable.CreateAsync(fs, schema);
        await table.WriteAsync([Batch(schema, 1, 42, "hello")]);

        string stats = table.CurrentSnapshot.ActiveFiles.Values.Single().Stats!;
        using var doc = JsonDocument.Parse(stats);
        var root = doc.RootElement;

        // Nested stats mirror the schema: minValues.s.a, not minValues["s.a"].
        var minS = root.GetProperty("minValues").GetProperty("s");
        Assert.Equal(42L, minS.GetProperty("a").GetInt64());
        Assert.Equal("hello", minS.GetProperty("b").GetString());

        var maxS = root.GetProperty("maxValues").GetProperty("s");
        Assert.Equal(42L, maxS.GetProperty("a").GetInt64());

        var nullS = root.GetProperty("nullCount").GetProperty("s");
        Assert.Equal(0L, nullS.GetProperty("a").GetInt64());
    }

    [Fact]
    public void ColumnStats_FlattensNestedKeysIncludingNullCount()
    {
        string json = """
        {
          "numRecords": 2,
          "minValues": {"id": 1, "s": {"a": 10, "b": "x"}},
          "maxValues": {"id": 2, "s": {"a": 20, "b": "y"}},
          "nullCount": {"id": 0, "s": {"a": 1, "b": 0}}
        }
        """;

        var stats = ColumnStats.Parse(json)!;

        Assert.Equal(2L, stats.NumRecords);
        Assert.Equal(10L, stats.MinValues!["s.a"].GetInt64());
        Assert.Equal(20L, stats.MaxValues!["s.a"].GetInt64());
        Assert.Equal("y", stats.MaxValues!["s.b"].GetString());
        // Nested nullCount objects used to be dropped entirely at parse.
        Assert.Equal(1L, stats.NullCount!["s.a"]);
        Assert.Equal(0L, stats.NullCount!["id"]);
    }

    // A literal dotted column name colliding with a struct leaf path is ambiguous — pruning must never guess,
    // so the key is dropped and the file is simply kept.
    [Fact]
    public void ColumnStats_CollidingDottedKeyIsPoisoned()
    {
        string json = """
        {
          "numRecords": 1,
          "minValues": {"s.a": 99, "s": {"a": 10}},
          "nullCount": {"s.a": 5, "s": {"a": 1}}
        }
        """;

        var stats = ColumnStats.Parse(json)!;

        Assert.False(stats.MinValues!.ContainsKey("s.a"));
        Assert.False(stats.NullCount!.ContainsKey("s.a"));
    }

    // The point of all of it: a predicate on a nested leaf prunes files.
    [Fact]
    public async Task Read_NestedPredicate_PrunesNonMatchingFiles()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = NestedSchema();
        var options = new DeltaTableOptions { CheckpointInterval = 0 };

        await using var table = await DeltaTable.CreateAsync(fs, schema, options);
        await table.WriteAsync([Batch(schema, 1, 10, "low")]);
        await table.WriteAsync([Batch(schema, 2, 500, "high")]);
        Assert.Equal(2, table.CurrentSnapshot.FileCount);

        // s.a = 500 lies outside file 1's [10,10] bounds, so that file is pruned on stats alone.
        var rows = new List<long>();
        await foreach (var b in table.ReadAllAsync(columns: null, Ex.Equal("s.a", 500L)))
        {
            var ids = (Int64Array)b.Column(0);
            for (int i = 0; i < b.Length; i++)
                rows.Add(ids.GetValue(i)!.Value);
        }

        Assert.Equal([2L], rows);
    }
}
