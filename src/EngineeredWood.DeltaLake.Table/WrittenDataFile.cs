// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Describes a parquet data file that was written OUTSIDE engineered-wood (e.g. streamed straight to disk by
/// DuckDB's native COPY) and is to be committed to the Delta log via
/// <see cref="DeltaTable.CommitDataFilesAsync(System.Collections.Generic.IReadOnlyList{WrittenDataFile},
/// DeltaWriteMode, System.Threading.CancellationToken)"/>.
/// </summary>
/// <param name="RelativePath">The file path relative to the table root (forward-slashed, e.g. <c>abc.parquet</c>
/// or <c>region=US/abc.parquet</c>).</param>
/// <param name="SizeBytes">The file size in bytes (the Delta <c>add.size</c>).</param>
/// <param name="NumRecords">The row count (REQUIRED — the row-tracking high-water mark is derived from
/// <c>baseRowId + numRecords</c>).</param>
/// <param name="PartitionValues">The Hive partition values for this file, or null/empty when unpartitioned.</param>
/// <param name="StatsJson">The Delta stats JSON (<c>{numRecords, minValues, maxValues, nullCount}</c>). When null,
/// <see cref="DeltaTable.CommitDataFilesAsync"/> emits minimal <c>{"numRecords":N}</c> stats.</param>
/// <param name="Tags">Optional <c>add.tags</c> for the file (e.g. a clustering OPTIMIZE's <c>ZCUBE_ID</c> —
/// Spark's incremental-clustering cube identity). Null = no tags.</param>
public readonly record struct WrittenDataFile(
    string RelativePath,
    long SizeBytes,
    long NumRecords,
    System.Collections.Generic.IReadOnlyDictionary<string, string>? PartitionValues,
    string? StatsJson,
    System.Collections.Generic.IReadOnlyDictionary<string, string>? Tags = null);
