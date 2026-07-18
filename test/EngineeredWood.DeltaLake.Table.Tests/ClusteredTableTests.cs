// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Clustered (liquid-clustering) tables — the <c>clustering</c> writer feature. The feature is advisory
/// LAYOUT: the spec permits plain (unclustered) appends and DML by writers that don't implement
/// clustering; a later clustering OPTIMIZE reclusters them. What a non-clustering writer MUST do is
/// preserve the clustering metadata: the <c>delta.clustering</c> system domain (the clustering-columns
/// spec) has to survive every commit and — the sharp edge — CHECKPOINTS, and <c>add.clusteringProvider</c>
/// must round-trip through log replay. These tests pin exactly that against a synthetic clustered table
/// (protocol v7 + <c>clustering</c>/<c>domainMetadata</c> + the domain action), the shape OSS delta-spark
/// creates for <c>CREATE TABLE … CLUSTER BY</c>.
/// </summary>
public class ClusteredTableTests : IDisposable
{
    private const string ClusteringDomain = "delta.clustering";
    private const string ClusteringConfig = /*lang=json,strict*/ "{\"clusteringColumns\":[[\"id\"]]}";

    private readonly string _tempDir;

    public ClusteredTableTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_clustered_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private async Task<DeltaTable> CreateClusteredTableAsync(DeltaTableOptions? options = null)
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);
        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction
            {
                MinReaderVersion = 1,
                MinWriterVersion = 7,
                WriterFeatures = ["clustering", "domainMetadata"],
            },
            new MetadataAction
            {
                Id = "clustered-table",
                Format = Format.Parquet,
                SchemaString = @"{""type"":""struct"",""fields"":[" +
                               @"{""name"":""id"",""type"":""long"",""nullable"":true,""metadata"":{}}," +
                               @"{""name"":""v"",""type"":""string"",""nullable"":true,""metadata"":{}}]}",
                PartitionColumns = [],
                Configuration = new Dictionary<string, string>(),
            },
            new DomainMetadata
            {
                Domain = ClusteringDomain,
                Configuration = ClusteringConfig,
                Removed = false,
            },
        });
        return options is null
            ? await DeltaTable.OpenAsync(fs)
            : await DeltaTable.OpenAsync(fs, options);
    }

    private static RecordBatch Batch(long startId, int count)
    {
        var ids = new Int64Array.Builder();
        var values = new StringArray.Builder();
        for (int i = 0; i < count; i++)
        {
            ids.Append(startId + i);
            values.Append("v" + (startId + i));
        }
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, true))
            .Field(new Field("v", StringType.Default, true))
            .Build();
        return new RecordBatch(schema, [ids.Build(), values.Build()], count);
    }

    [Fact]
    public async Task AppendToClusteredTable_Works_AndPreservesClusteringDomain()
    {
        // Before "clustering" joined SupportedWriterFeatures this threw
        // "unsupported writer features: [clustering]".
        await using var table = await CreateClusteredTableAsync();
        long v1 = await table.WriteAsync([Batch(1, 5)]);
        long v2 = await table.WriteAsync([Batch(6, 5)]);
        Assert.Equal(1, v1);
        Assert.Equal(2, v2);

        Assert.True(table.CurrentSnapshot.DomainMetadata.TryGetValue(ClusteringDomain, out var dm));
        Assert.Equal(ClusteringConfig, dm!.Configuration);
        Assert.False(dm.Removed);

        long rows = 0;
        await foreach (var b in table.ReadAllAsync())
        {
            rows += b.Length;
            b.Dispose();
        }
        Assert.Equal(10, rows);
    }

    [Fact]
    public async Task ClusteringDomain_SurvivesCheckpoint()
    {
        // The sharp edge: checkpoints REBUILD the log state — a checkpoint that dropped domainMetadata
        // would silently destroy the table's clustering spec (and Spark's ability to recluster it).
        await using (var table = await CreateClusteredTableAsync(new DeltaTableOptions { CheckpointInterval = 2 }))
        {
            for (int i = 0; i < 5; i++)
            {
                await table.WriteAsync([Batch(i * 10, 3)]);
            }
        }
        Assert.NotEmpty(Directory.GetFiles(Path.Combine(_tempDir, "_delta_log"), "*.checkpoint.parquet"));

        // A FRESH open resolves through the checkpoint — the domain must still be there, data exact.
        await using var reopened = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        Assert.True(reopened.CurrentSnapshot.DomainMetadata.TryGetValue(ClusteringDomain, out var dm));
        Assert.Equal(ClusteringConfig, dm!.Configuration);

        long rows = 0;
        await foreach (var b in reopened.ReadAllAsync())
        {
            rows += b.Length;
            b.Dispose();
        }
        Assert.Equal(15, rows);
    }

    [Fact]
    public async Task ClusteringProvider_RoundTripsThroughLogReplay()
    {
        // add.clusteringProvider (a Databricks/Spark clustering writer tags its clustered files) must
        // survive our reading of the log — a rewrite that dropped it would degrade incremental
        // reclustering. Written directly as an action here (we never WRITE the tag ourselves).
        var fs = new LocalTableFileSystem(_tempDir);
        await using (var table = await CreateClusteredTableAsync())
        {
            await table.WriteAsync([Batch(1, 3)]);
        }
        var log = new TransactionLog(fs);
        var commit1 = await log.ReadCommitAsync(1);
        var add = commit1.OfType<AddFile>().Single();
        await log.WriteCommitAsync(2, new List<DeltaAction>
        {
            add with { Path = add.Path, ClusteringProvider = "liquid" },
        });

        await using var reopened = await DeltaTable.OpenAsync(fs);
        var active = reopened.CurrentSnapshot.ActiveFiles.Values.Single();
        Assert.Equal("liquid", active.ClusteringProvider);
    }
}
