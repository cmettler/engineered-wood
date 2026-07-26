// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Actions;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// One file a scan has to read, as returned by
/// <see cref="DeltaTable.PlanFiles(EngineeredWood.Expressions.Predicate, Snapshot.Snapshot, Schema.StructType)"/>:
/// the <c>add</c> action, plus that file's ordinal in the snapshot's PATH-SORTED active set.
///
/// <para><see cref="Ordinal"/> is assigned BEFORE pruning, so it is a position in the FULL active set
/// and not an index into the returned list. That is what makes it usable as a row address — the
/// transient row id is <c>(Ordinal &lt;&lt; 40) | absolute-in-file position</c>, and adding a filter must
/// not change what a row id means. Pruning therefore leaves GAPS in the ordinals of a plan, and the
/// ordinals of two plans over the same snapshot agree regardless of their filters.</para>
///
/// <para>A struct rather than a class record (unlike Iceberg's <c>ScanResult</c>) because a plan holds
/// one per file — thousands on a large table — and there is no identity to preserve.</para>
/// </summary>
/// <param name="File">The <c>add</c> action: path, partition values, stats, deletion vector, row-tracking ids.</param>
/// <param name="Ordinal">Zero-based position in the snapshot's path-sorted active set, assigned before pruning.</param>
public readonly record struct PlannedFile(AddFile File, int Ordinal);
