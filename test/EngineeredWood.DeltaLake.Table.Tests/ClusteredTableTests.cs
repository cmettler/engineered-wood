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
    public async Task CreateAsync_WithClusteringColumns_DeclaresTheFeatureAndDomain()
    {
        // The create-side counterpart: CreateAsync(clusteringColumns:) declares the table LIQUID-CLUSTERED
        // — writer-v7 `clustering` (+ its domainMetadata dependency) and the delta.clustering domain in
        // commit-0, byte-shaped like Spark's own (paths + the redundant domainName field) — so a clustering
        // engine's OPTIMIZE (Spark) reclusters tables created here.
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, true))
            .Field(new Field("v", StringType.Default, true))
            .Build();
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), schema, clusteringColumns: ["id"]);

        var protocol = table.CurrentSnapshot.Protocol;
        Assert.Equal(7, protocol.MinWriterVersion);
        Assert.Contains("clustering", protocol.WriterFeatures!);
        Assert.Contains("domainMetadata", protocol.WriterFeatures!);

        Assert.True(table.CurrentSnapshot.DomainMetadata.TryGetValue(ClusteringDomain, out var dm));
        Assert.Equal(/*lang=json,strict*/ "{\"clusteringColumns\":[[\"id\"]],\"domainName\":\"delta.clustering\"}",
                     dm!.Configuration);

        // and writes work as usual (unclustered appends — the spec-legal ingest shape)
        long v = await table.WriteAsync([Batch(1, 5)]);
        Assert.Equal(1, v);
    }

    [Fact]
    public async Task CreateAsync_WithClusteringColumns_UnderColumnMapping_StoresPhysicalNames()
    {
        // The spec stores PHYSICAL column names in the delta.clustering domain: OSS Spark's
        // ClusteringColumnInfo resolves the domain's paths against the schema's physical names and
        // None.get-crashes on a logical name under column mapping (observed live on Fabric Spark 4.1 —
        // DESCRIBE DETAIL and OPTIMIZE both failed on a domain carrying logical names). The caller
        // supplies LOGICAL names; CreateAsync must resolve them through the mapping-assigned schema.
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("grp", Int64Type.Default, true))
            .Field(new Field("id", Int64Type.Default, true))
            .Field(new Field("v", StringType.Default, true))
            .Build();
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), schema,
            columnMappingMode: Schema.ColumnMappingMode.Name,
            clusteringColumns: ["grp", "id"]);

        Assert.True(table.CurrentSnapshot.DomainMetadata.TryGetValue(ClusteringDomain, out var dm));
        var config = dm!.Configuration;

        // The domain must carry the mapped col-<guid> physical names, never the logical ones.
        var grpPhysical = Schema.ColumnMapping.GetPhysicalName(
            table.CurrentSnapshot.Schema.Fields.Single(f => f.Name == "grp"), Schema.ColumnMappingMode.Name);
        var idPhysical = Schema.ColumnMapping.GetPhysicalName(
            table.CurrentSnapshot.Schema.Fields.Single(f => f.Name == "id"), Schema.ColumnMappingMode.Name);
        Assert.StartsWith("col-", grpPhysical);
        Assert.StartsWith("col-", idPhysical);
        Assert.Equal(
            $"{{\"clusteringColumns\":[[\"{grpPhysical}\"],[\"{idPhysical}\"]],\"domainName\":\"delta.clustering\"}}",
            config);
        Assert.DoesNotContain("[\"grp\"]", config);
    }

    [Fact]
    public async Task CreateAsync_ClusteringPlusPartitioning_Throws()
    {
        // Liquid clustering and partitioning are mutually exclusive (Spark's CLUSTER BY REPLACES
        // PARTITIONED BY) — declaring both would put readers' clustering-info paths in undefined territory.
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("region", StringType.Default, true))
            .Field(new Field("id", Int64Type.Default, true))
            .Build();
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () => await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), schema,
            partitionColumns: ["region"], clusteringColumns: ["id"]));
        Assert.Contains("mutually exclusive", ex.Message);
    }

    [Fact]
    public async Task CreateAsync_WithUnknownClusteringColumn_Throws()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, true))
            .Build();
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () => await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), schema, clusteringColumns: ["nope"]));
        Assert.Contains("nope", ex.Message);
    }

    [Fact]
    public async Task CommitDataFiles_RewriteShape_DataChangeFalseAndClusteringProvider()
    {
        // A clustering OPTIMIZE commits Overwrite-shaped with dataChange=false on BOTH sides (CDF/append-only
        // legality) and clusteringProvider on the adds — pin the action shape.
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await CreateClusteredTableAsync();
        await table.WriteAsync([Batch(1, 3)]);
        await table.WriteAsync([Batch(4, 3)]);

        long v = await table.CommitDataFilesAsync(
            [new WrittenDataFile("clustered-rewrite.parquet", 1234, 6, null, null)],
            DeltaWriteMode.Overwrite,
            expectedVersion: table.CurrentSnapshot.Version,
            operation: "OPTIMIZE",
            dataChange: false,
            clusteringProvider: "liquid");
        Assert.Equal(3, v);

        var log = new TransactionLog(fs);
        var commit = await log.ReadCommitAsync(3);
        var removes = commit.OfType<RemoveFile>().ToList();
        Assert.Equal(2, removes.Count);
        Assert.All(removes, r => Assert.False(r.DataChange));
        var add = commit.OfType<AddFile>().Single();
        Assert.False(add.DataChange);
        Assert.Equal("liquid", add.ClusteringProvider);
        // The clustering domain still survives the rewrite commit.
        Assert.True(table.CurrentSnapshot.DomainMetadata.ContainsKey(ClusteringDomain));
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
