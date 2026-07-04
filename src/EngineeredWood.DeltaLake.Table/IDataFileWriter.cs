// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Pluggable data-file writer. When set on <see cref="DeltaTableOptions.DataFileWriter"/>, the Delta table
/// delegates the production of each parquet data file to this writer instead of using its built-in
/// <c>ParquetFileWriter</c> — everything else (partition split, row tracking, column mapping, stats collection,
/// the <c>add</c> action, and the commit) stays in the Delta layer. This is the seam that lets a host embed its
/// own parquet writer (e.g. DuckDB's native <c>COPY … TO … (FORMAT parquet)</c>) for the data bytes while the
/// engineered-wood <c>_delta_log</c> layer owns the protocol.
/// </summary>
public interface IDataFileWriter
{
    /// <summary>Writes <paramref name="batches"/> as a single parquet file at <paramref name="relativePath"/>
    /// (relative to the table root, including any partition subdirectory), and returns the written file's byte
    /// size (stored on the <c>add</c> action). A fresh write passes one batch; a copy-on-write rewrite passes the
    /// surviving batches of one source file. The implementation is responsible for placing the bytes at the
    /// location the table's filesystem maps <paramref name="relativePath"/> to.</summary>
    ValueTask<long> WriteAsync(IReadOnlyList<RecordBatch> batches, string relativePath,
                               CancellationToken cancellationToken);
}
