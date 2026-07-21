// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// The lowered form of a metadata predicate: for each data file — keyed by its log <c>add.path</c>
/// (the URL-ENCODED form, exactly as it appears in the snapshot) — the ABSOLUTE physical row positions
/// selected by the caller. Positions are parquet row indexes counting rows already masked by the file's
/// deletion vector (Spark's <c>_metadata.row_index</c> semantics; the same convention
/// <see cref="DeltaTable.ReadAllWithMetadataAsync"/> emits and the DELETE machinery records in
/// <c>DeleteDvEdit</c>). Produced by decoding <c>_metadata.file_path</c>/<c>_metadata.row_index</c>
/// predicates, or directly by an engine whose scan carried <c>_metadata</c>.
/// </summary>
public sealed record FileRowSelection(
    IReadOnlyDictionary<string, IReadOnlyCollection<long>> RowsByFile);
