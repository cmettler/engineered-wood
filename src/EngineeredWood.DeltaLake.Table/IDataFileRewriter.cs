// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Collections.Generic;
using System.Threading;
using Apache.Arrow;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Pluggable copy-on-write <b>rewrite reader</b>. When set on <see cref="DeltaTableOptions.DataFileRewriter"/>,
/// the Delta table delegates the <i>read + row-level transform</i> of a copy-on-write DELETE/UPDATE to this
/// reader instead of reading the source parquet with its own reader and applying the transform in-process. The
/// implementation reads the source file itself (e.g. via DuckDB's native <c>read_parquet(..., file_row_number =&gt;
/// true)</c>), drops the excluded physical positions, and — for an UPDATE — substitutes the SET columns entirely
/// in the host engine (retiring the in-process typed value substitution). It returns the resulting rows as
/// logical-schema Arrow batches; <b>everything else stays in the Delta layer</b> — column-mapping rename, the
/// stats collection (<c>StatsCollector</c>), the pluggable data-file <i>writer</i> (<see cref="IDataFileWriter"/>),
/// row-tracking materialization, the <c>remove</c>/<c>add</c> actions, CDF change files, and the commit.
///
/// <para>This is the read/transform half of the "inversion" (the data bytes are read AND written by the host's
/// native parquet engine; only the <c>_delta_log</c> protocol stays engineered-wood). It is only used when the
/// table's shape permits a clean <c>read_parquet</c> rewrite — the Delta layer falls back to its own reader for
/// column-mapping tables and schema-evolved files (missing-column NULL backfill), which this seam does not
/// handle. It composes with <see cref="IDataFileWriter"/>: the reader produces the transformed batches, the
/// writer emits their parquet bytes.</para>
/// </summary>
public interface IDataFileRewriter
{
    /// <summary>
    /// Reads the copy-on-write result for the source file at <paramref name="sourceRelativePath"/> (relative to
    /// the table root): every physical row except those in <paramref name="excludePositions"/> (the deleted
    /// positions for a DELETE, plus the file's existing deletion-vector positions for an UPDATE), with the
    /// UPDATE substitution — if this rewriter was created for an UPDATE — applied to the rows the implementation
    /// matches by transient rowid within <paramref name="fileOrdinal"/> (<c>(fileOrdinal &lt;&lt; 40) | position</c>).
    /// The returned batches carry the table's logical user schema (no trailing rowid, no partition columns —
    /// exactly what the Delta write path expects for a fresh data file). The Delta layer does NOT open the
    /// source file when this is used.
    ///
    /// <para>When <paramref name="rowTracking"/> is non-null (a row-tracking table with materialized columns
    /// declared), the returned batches ADDITIONALLY carry two trailing nullable BIGINT columns
    /// <c>__delta_row_id</c> and <c>__delta_row_commit_version</c>: each row's ORIGINAL stable id (the source
    /// file's materialized value where present, else <see cref="RowTrackingRewrite.SourceBaseRowId"/> +
    /// position) and commit version (an UPDATE-substituted row gets
    /// <see cref="RowTrackingRewrite.NewCommitVersion"/>; others keep the source's materialized value else
    /// <see cref="RowTrackingRewrite.SourceDefaultCommitVersion"/>). A value may be NULL when underivable
    /// (a source file predating row tracking) — readers then fall back to the NEW file's baseRowId +
    /// position, i.e. a fresh id for that row.</para>
    /// </summary>
    IAsyncEnumerable<RecordBatch> ReadRewriteAsync(
        int fileOrdinal, string sourceRelativePath, IReadOnlyCollection<long> excludePositions,
        CancellationToken cancellationToken, RowTrackingRewrite? rowTracking = null);
}

/// <summary>Per-source-file inputs for materializing row tracking through a copy-on-write rewrite —
/// see <see cref="IDataFileRewriter.ReadRewriteAsync"/>.</summary>
public sealed record RowTrackingRewrite(
    long? SourceBaseRowId, long? SourceDefaultCommitVersion, long NewCommitVersion);
