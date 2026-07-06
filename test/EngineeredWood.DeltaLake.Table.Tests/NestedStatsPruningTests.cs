// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Nested (struct-leaf) statistics pruning: <see cref="ColumnStats.Parse"/> flattens nested
/// minValues/maxValues/nullCount objects into dotted keys, and <see cref="DeltaFilePruner"/>
/// registers struct leaves under their dotted path — so a predicate referencing "s.a" prunes
/// files exactly like a top-level column. Pruning is superset-safe: an unresolvable reference
/// evaluates Unknown and the file is kept.
/// </summary>
public class NestedStatsPruningTests
{
    private static AddFile MakeAdd(string stats) => new()
    {
        Path = "part-0.parquet",
        PartitionValues = new Dictionary<string, string>(),
        Size = 1,
        ModificationTime = 0,
        DataChange = true,
        Stats = stats,
    };

    private static StructType NestedSchema(IReadOnlyDictionary<string, string>? sMeta = null,
                                           IReadOnlyDictionary<string, string>? aMeta = null) => new()
    {
        Fields = new[]
        {
            new StructField { Name = "id", Type = new PrimitiveType { TypeName = "long" }, Nullable = false },
            new StructField
            {
                Name = "s",
                Type = new StructType
                {
                    Fields = new[]
                    {
                        new StructField
                        {
                            Name = "a", Type = new PrimitiveType { TypeName = "integer" },
                            Nullable = true, Metadata = aMeta,
                        },
                        new StructField
                        {
                            Name = "b", Type = new PrimitiveType { TypeName = "string" }, Nullable = true,
                        },
                    },
                },
                Nullable = true,
                Metadata = sMeta,
            },
        },
    };

    private const string NestedStats =
        "{\"numRecords\":10,\"minValues\":{\"id\":1,\"s\":{\"a\":5,\"b\":\"aaa\"}}," +
        "\"maxValues\":{\"id\":10,\"s\":{\"a\":9,\"b\":\"zzz\"}}," +
        "\"nullCount\":{\"id\":0,\"s\":{\"a\":0,\"b\":10}}}";

    [Fact]
    public void ColumnStats_FlattensNestedLeaves()
    {
        var stats = ColumnStats.Parse(NestedStats)!;
        Assert.Equal(5, stats.MinValues!["s.a"].GetInt32());
        Assert.Equal(9, stats.MaxValues!["s.a"].GetInt32());
        Assert.Equal("zzz", stats.MaxValues["s.b"].GetString());
        Assert.Equal(0, stats.NullCount!["s.a"]);
        Assert.Equal(10, stats.NullCount["s.b"]);
        // Top-level keys unchanged.
        Assert.Equal(1, stats.MinValues["id"].GetInt32());
    }

    [Fact]
    public void NestedComparison_PrunesAndKeeps()
    {
        var pruner = new DeltaFilePruner(NestedSchema(), Array.Empty<string>());
        var add = MakeAdd(NestedStats);

        // s.a in [5, 9]:
        Assert.False(pruner.ShouldInclude(add, Expressions.Expressions.Equal("s.a", LiteralValue.Of(4))));
        Assert.True(pruner.ShouldInclude(add, Expressions.Expressions.Equal("s.a", LiteralValue.Of(7))));
        Assert.False(pruner.ShouldInclude(add, Expressions.Expressions.GreaterThan("s.a", LiteralValue.Of(9))));
        Assert.True(pruner.ShouldInclude(add, Expressions.Expressions.LessThan("s.a", LiteralValue.Of(6))));
        // String leaf byte-order bounds:
        Assert.False(pruner.ShouldInclude(add, Expressions.Expressions.Equal("s.b", LiteralValue.Of("zzzz"))));
    }

    [Fact]
    public void NestedNullCount_Prunes()
    {
        var pruner = new DeltaFilePruner(NestedSchema(), Array.Empty<string>());
        var add = MakeAdd(NestedStats);

        // s.a has no nulls; s.b is all-null (10 of 10).
        Assert.False(pruner.ShouldInclude(add, Expressions.Expressions.IsNull("s.a")));
        Assert.True(pruner.ShouldInclude(add, Expressions.Expressions.IsNull("s.b")));
        Assert.False(pruner.ShouldInclude(add, Expressions.Expressions.IsNotNull("s.b")));
    }

    [Fact]
    public void ColumnMapping_ResolvesDottedPhysicalKeys()
    {
        // Stats keyed by PHYSICAL names at every level; predicate references logical "s.a".
        var pruner = new DeltaFilePruner(
            NestedSchema(
                sMeta: new Dictionary<string, string> { [ColumnMapping.PhysicalNameKey] = "col-s" },
                aMeta: new Dictionary<string, string> { [ColumnMapping.PhysicalNameKey] = "col-a" }),
            Array.Empty<string>());
        var add = MakeAdd(
            "{\"numRecords\":10,\"minValues\":{\"col-s\":{\"col-a\":5}}," +
            "\"maxValues\":{\"col-s\":{\"col-a\":9}},\"nullCount\":{\"col-s\":{\"col-a\":0}}}");

        Assert.False(pruner.ShouldInclude(add, Expressions.Expressions.Equal("s.a", LiteralValue.Of(4))));
        Assert.True(pruner.ShouldInclude(add, Expressions.Expressions.Equal("s.a", LiteralValue.Of(7))));
    }

    [Fact]
    public void DottedNameCollision_IsPoisonedNotGuessed()
    {
        // A literal top-level column "s.a" alongside struct leaf s.a: ambiguous — never prune on it.
        var schema = new StructType
        {
            Fields = new[]
            {
                new StructField { Name = "s.a", Type = new PrimitiveType { TypeName = "integer" }, Nullable = true },
                new StructField
                {
                    Name = "s",
                    Type = new StructType
                    {
                        Fields = new[]
                        {
                            new StructField
                            {
                                Name = "a", Type = new PrimitiveType { TypeName = "integer" }, Nullable = true,
                            },
                        },
                    },
                    Nullable = true,
                },
            },
        };
        var pruner = new DeltaFilePruner(schema, Array.Empty<string>());
        // Stats that would prune under either interpretation — must still be kept (Unknown).
        var add = MakeAdd(
            "{\"numRecords\":10,\"minValues\":{\"s.a\":100,\"s\":{\"a\":100}}," +
            "\"maxValues\":{\"s.a\":200,\"s\":{\"a\":200}},\"nullCount\":{\"s.a\":0,\"s\":{\"a\":0}}}");

        Assert.True(pruner.ShouldInclude(add, Expressions.Expressions.Equal("s.a", LiteralValue.Of(4))));
    }

    [Fact]
    public void MissingNestedStats_KeepsFile()
    {
        var pruner = new DeltaFilePruner(NestedSchema(), Array.Empty<string>());
        var add = MakeAdd("{\"numRecords\":10,\"minValues\":{\"id\":1},\"maxValues\":{\"id\":10},\"nullCount\":{\"id\":0}}");

        Assert.True(pruner.ShouldInclude(add, Expressions.Expressions.Equal("s.a", LiteralValue.Of(4))));
    }
}
