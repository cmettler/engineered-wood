// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The `_metadata` DML prototype: <see cref="DeltaTable.ReadAllWithMetadataAsync"/> (batches carry a
/// `_metadata` struct — file_path + ABSOLUTE row_index, Spark semantics) and
/// <see cref="DeltaTable.DeleteAsync(FileRowSelection, CancellationToken)"/> (delete exactly the named
/// (file, position) rows — the lowered form of a metadata predicate — with ZERO data reads on a non-CDF
/// table, proven here by a parquet-open-counting filesystem).
/// </summary>
public class MetadataDmlTests : IDisposable
{
    private readonly string _tempDir;

    public MetadataDmlTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_metadml_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static Apache.Arrow.Schema IdSchema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Build();

    private static RecordBatch IdBatch(params long[] ids) =>
        new(IdSchema, [new Int64Array.Builder().AppendRange(ids).Build()], ids.Length);

    private static async Task<List<long>> ReadIdsAsync(DeltaTable table)
    {
        var ids = new List<long>();
        await foreach (var b in table.ReadAllAsync())
        {
            var col = (Int64Array)b.Column(0);
            for (int i = 0; i < b.Length; i++)
                ids.Add(col.GetValue(i)!.Value);
        }
        ids.Sort();
        return ids;
    }

    /// <summary>Delegating filesystem counting opens of DATA parquet files (log/checkpoint excluded) —
    /// the instrument behind the zero-data-reads assertion.</summary>
    private sealed class CountingFileSystem(ITableFileSystem inner) : ITableFileSystem
    {
        public int DataParquetOpens;

        public IAsyncEnumerable<TableFileInfo> ListAsync(string prefix, CancellationToken ct = default)
            => inner.ListAsync(prefix, ct);

        public ValueTask<IRandomAccessFile> OpenReadAsync(string path, CancellationToken ct = default)
        {
            if (path.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase)
                && !path.StartsWith("_delta_log", StringComparison.Ordinal))
            {
                Interlocked.Increment(ref DataParquetOpens);
            }
            return inner.OpenReadAsync(path, ct);
        }

        public ValueTask<ISequentialFile> CreateAsync(string path, bool overwrite = false, CancellationToken ct = default)
            => inner.CreateAsync(path, overwrite, ct);
        public ValueTask<bool> RenameAsync(string sourcePath, string targetPath, CancellationToken ct = default)
            => inner.RenameAsync(sourcePath, targetPath, ct);
        public ValueTask DeleteAsync(string path, CancellationToken ct = default)
            => inner.DeleteAsync(path, ct);
        public ValueTask<bool> ExistsAsync(string path, CancellationToken ct = default)
            => inner.ExistsAsync(path, ct);
        public ValueTask<byte[]> ReadAllBytesAsync(string path, CancellationToken ct = default)
            => inner.ReadAllBytesAsync(path, ct);
        public ValueTask WriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken ct = default)
            => inner.WriteAllBytesAsync(path, data, ct);
    }

    [Fact]
    public async Task DeleteBySelection_PartialFile_ZeroDataReads_AndUnions()
    {
        var setupFs = new LocalTableFileSystem(_tempDir);
        await using (var setup = await DeltaTable.CreateAsync(setupFs, IdSchema, enableDeletionVectors: true))
        {
            await setup.WriteAsync([IdBatch(10, 20, 30, 40, 50)]);
        }

        // A fresh handle over the COUNTING fs: every data-parquet open from here on is observed.
        var countingFs = new CountingFileSystem(new LocalTableFileSystem(_tempDir));
        await using var table = await DeltaTable.OpenAsync(countingFs);
        string path = table.CurrentSnapshot.ActiveFiles.Values.Single().Path;

        // Delete positions 1 and 3 (ids 20, 40) — by position, no predicate, NO data read.
        var (rows, v1) = await table.DeleteAsync(new FileRowSelection(
            new Dictionary<string, IReadOnlyCollection<long>> { [path] = new long[] { 1, 3 } }));
        Assert.Equal(2, rows);
        Assert.Equal(0, countingFs.DataParquetOpens); // the whole point: DV union + commit only

        // A second selection UNIONS with the existing DV (position 0 = id 10); re-deleting 1 is a no-op.
        var (rows2, v2) = await table.DeleteAsync(new FileRowSelection(
            new Dictionary<string, IReadOnlyCollection<long>> { [path] = new long[] { 0, 1 } }));
        Assert.Equal(1, rows2);
        Assert.True(v2 > v1);
        Assert.Equal(0, countingFs.DataParquetOpens);

        Assert.Equal([30L, 50L], await ReadIdsAsync(table));
    }

    [Fact]
    public async Task DeleteBySelection_AllPositions_DropsTheWholeFile()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema, enableDeletionVectors: true);
        await table.WriteAsync([IdBatch(1, 2, 3)]);
        string path = table.CurrentSnapshot.ActiveFiles.Values.Single().Path;

        var (rows, _) = await table.DeleteAsync(new FileRowSelection(
            new Dictionary<string, IReadOnlyCollection<long>> { [path] = new long[] { 0, 1, 2 } }));

        Assert.Equal(3, rows);
        // Whole-file: a plain remove — the file leaves the active set instead of gaining a DV.
        Assert.Empty(table.CurrentSnapshot.ActiveFiles);
        Assert.Empty(await ReadIdsAsync(table));
    }

    [Fact]
    public async Task DeleteBySelection_StaleFilePath_ThrowsClearError()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema, enableDeletionVectors: true);
        await table.WriteAsync([IdBatch(1)]);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await table.DeleteAsync(new FileRowSelection(
                new Dictionary<string, IReadOnlyCollection<long>> { ["no-such-file.parquet"] = new long[] { 0 } })));
        Assert.Contains("not active", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task DeleteBySelection_OutOfRangePosition_Throws()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema, enableDeletionVectors: true);
        await table.WriteAsync([IdBatch(1, 2)]);
        string path = table.CurrentSnapshot.ActiveFiles.Values.Single().Path;

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await table.DeleteAsync(new FileRowSelection(
                new Dictionary<string, IReadOnlyCollection<long>> { [path] = new long[] { 99 } })));
    }

    private static Func<RecordBatch, RecordBatch> AddToIds(long delta) => batch =>
    {
        var ids = (Int64Array)batch.Column(0);
        var b = new Int64Array.Builder();
        for (int i = 0; i < batch.Length; i++)
            b.Append(ids.GetValue(i)!.Value + delta);
        return new RecordBatch(batch.Schema, [b.Build()], batch.Length);
    };

    [Fact]
    public async Task UpdateBySelection_RewritesMatched_KeepsRest_AndAppliesTheDv()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema, enableDeletionVectors: true);
        await table.WriteAsync([IdBatch(10, 20, 30, 40, 50)]);
        string path = table.CurrentSnapshot.ActiveFiles.Values.Single().Path;

        // DV-delete position 1 (id 20) first, then UPDATE position 3 (id 40 -> 1040) by selection.
        await table.DeleteAsync(new FileRowSelection(
            new Dictionary<string, IReadOnlyCollection<long>> { [path] = new long[] { 1 } }));
        var (rows, _) = await table.UpdateAsync(new FileRowSelection(
            new Dictionary<string, IReadOnlyCollection<long>> { [path] = new long[] { 3 } }), AddToIds(1000));

        Assert.Equal(1, rows);
        // Matched row transformed, others kept, DV-deleted row NOT resurrected by the rewrite.
        Assert.Equal([10L, 30L, 50L, 1040L], await ReadIdsAsync(table));
        // The rewrite replaced the file: one active file, a new path, no deletion vector.
        var add = table.CurrentSnapshot.ActiveFiles.Values.Single();
        Assert.NotEqual(path, add.Path);
        Assert.Null(add.DeletionVector);
    }

    [Fact]
    public async Task UpdateBySelection_DvDeletedPosition_MatchesNothing()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema, enableDeletionVectors: true);
        await table.WriteAsync([IdBatch(1, 2, 3)]);
        string path = table.CurrentSnapshot.ActiveFiles.Values.Single().Path;
        await table.DeleteAsync(new FileRowSelection(
            new Dictionary<string, IReadOnlyCollection<long>> { [path] = new long[] { 1 } }));

        long before = table.CurrentSnapshot.Version;
        var (rows, version) = await table.UpdateAsync(new FileRowSelection(
            new Dictionary<string, IReadOnlyCollection<long>> { [path] = new long[] { 1 } }), AddToIds(1000));

        // The concurrently-deleted-row semantics: nothing matched, nothing committed.
        Assert.Equal(0, rows);
        Assert.Equal(before, version);
        Assert.Equal([1L, 3L], await ReadIdsAsync(table));
    }

    [Fact]
    public async Task MetadataPredicate_Delete_LowersToSelection_ZeroDataReads()
    {
        var setupFs = new LocalTableFileSystem(_tempDir);
        await using (var setup = await DeltaTable.CreateAsync(setupFs, IdSchema, enableDeletionVectors: true))
        {
            await setup.WriteAsync([IdBatch(10, 20, 30, 40, 50)]);
        }

        var countingFs = new CountingFileSystem(new LocalTableFileSystem(_tempDir));
        await using var table = await DeltaTable.OpenAsync(countingFs);
        string path = table.CurrentSnapshot.ActiveFiles.Values.Single().Path;

        // DELETE WHERE _metadata.file_path = <path> AND _metadata.row_index IN (1, 3)
        var predicate = new Expressions.AndPredicate(
        [
            new Expressions.ComparisonPredicate(
                new Expressions.UnboundReference(MetadataPredicate.FilePathColumn),
                Expressions.ComparisonOperator.Equal,
                new Expressions.LiteralExpression(Expressions.LiteralValue.Of(path))),
            new Expressions.SetPredicate(
                new Expressions.UnboundReference(MetadataPredicate.RowIndexColumn),
                [Expressions.LiteralValue.Of(1L), Expressions.LiteralValue.Of(3L)],
                Expressions.SetOperator.In),
        ]);

        var (rows, _) = await table.DeleteAsync(predicate);

        Assert.Equal(2, rows);
        Assert.Equal(0, countingFs.DataParquetOpens); // the lowering engaged — no scan, DV union only
        Assert.Equal([10L, 30L, 50L], await ReadIdsAsync(table));
    }

    [Fact]
    public async Task MetadataPredicate_OrAcrossFiles_LowersAndUnions()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema, enableDeletionVectors: true);
        await table.WriteAsync([IdBatch(10, 20, 30)]); // file A
        await table.WriteAsync([IdBatch(40, 50)]);     // file B
        var paths = table.CurrentSnapshot.ActiveFiles.Values
            .Select(a => a.Path).OrderBy(p => p, StringComparer.Ordinal).ToList();

        static Expressions.Predicate ForFile(string p, long idx) => new Expressions.AndPredicate(
        [
            new Expressions.ComparisonPredicate(
                new Expressions.UnboundReference(MetadataPredicate.FilePathColumn),
                Expressions.ComparisonOperator.Equal,
                new Expressions.LiteralExpression(Expressions.LiteralValue.Of(p))),
            new Expressions.ComparisonPredicate(
                new Expressions.UnboundReference(MetadataPredicate.RowIndexColumn),
                Expressions.ComparisonOperator.Equal,
                new Expressions.LiteralExpression(Expressions.LiteralValue.Of(idx))),
        ]);

        // one position in EACH file, OR-combined
        var (rows, _) = await table.DeleteAsync(
            new Expressions.OrPredicate([ForFile(paths[0], 0), ForFile(paths[1], 0)]));

        Assert.Equal(2, rows);
        Assert.Equal(3, (await ReadIdsAsync(table)).Count);
    }

    [Fact]
    public async Task MetadataPredicate_MixedWithDataColumn_IsRejectedLoudly()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema, enableDeletionVectors: true);
        await table.WriteAsync([IdBatch(1, 2)]);
        string path = table.CurrentSnapshot.ActiveFiles.Values.Single().Path;

        // _metadata.file_path = X AND id = 2 — metadata mixed with a data column must not silently
        // fall through to the row mask (which cannot bind _metadata columns).
        var mixed = new Expressions.AndPredicate(
        [
            new Expressions.ComparisonPredicate(
                new Expressions.UnboundReference(MetadataPredicate.FilePathColumn),
                Expressions.ComparisonOperator.Equal,
                new Expressions.LiteralExpression(Expressions.LiteralValue.Of(path))),
            new Expressions.ComparisonPredicate(
                new Expressions.UnboundReference("id"),
                Expressions.ComparisonOperator.Equal,
                new Expressions.LiteralExpression(Expressions.LiteralValue.Of(2L))),
        ]);

        await Assert.ThrowsAsync<NotSupportedException>(async () => await table.DeleteAsync(mixed));
    }

    [Fact]
    public async Task MetadataPredicate_Update_LowersToSelection()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema, enableDeletionVectors: true);
        await table.WriteAsync([IdBatch(10, 20, 30)]);
        string path = table.CurrentSnapshot.ActiveFiles.Values.Single().Path;

        var predicate = new Expressions.AndPredicate(
        [
            new Expressions.ComparisonPredicate(
                new Expressions.UnboundReference(MetadataPredicate.FilePathColumn),
                Expressions.ComparisonOperator.Equal,
                new Expressions.LiteralExpression(Expressions.LiteralValue.Of(path))),
            new Expressions.ComparisonPredicate(
                new Expressions.UnboundReference(MetadataPredicate.RowIndexColumn),
                Expressions.ComparisonOperator.Equal,
                new Expressions.LiteralExpression(Expressions.LiteralValue.Of(1L))),
        ]);

        var (rows, _) = await table.UpdateAsync(predicate, AddToIds(1000));
        Assert.Equal(1, rows);
        Assert.Equal([10L, 30L, 1020L], await ReadIdsAsync(table));
    }

    [Fact]
    public async Task ReadAllWithMetadata_EmitsFilePath_AndAbsoluteRowIndex_AcrossDvDeletes()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema, enableDeletionVectors: true);
        await table.WriteAsync([IdBatch(10, 20, 30, 40, 50)]); // file A
        await table.WriteAsync([IdBatch(60, 70)]);             // file B

        // Round-trip: read metadata → build a selection from it → delete → re-read metadata.
        var byId = new Dictionary<long, (string Path, long Index)>();
        await foreach (var b in table.ReadAllWithMetadataAsync())
        {
            int metaIdx = b.Schema.GetFieldIndex("_metadata");
            var meta = (StructArray)b.Column(metaIdx);
            var paths = (StringArray)meta.Fields[0];
            var ridx = (Int64Array)meta.Fields[1];
            var ids = (Int64Array)b.Column(0);
            for (int i = 0; i < b.Length; i++)
                byId[ids.GetValue(i)!.Value] = (paths.GetString(i), ridx.GetValue(i)!.Value);
        }
        Assert.Equal(7, byId.Count);
        Assert.Equal(2, byId.Values.Select(v => v.Path).Distinct().Count());
        Assert.Equal(1, byId[20].Index); // positions are per-file parquet row indexes
        Assert.Equal(0, byId[60].Index);

        // Delete ids 20 and 40 via the metadata they reported.
        var sel = new[] { 20L, 40L }
            .Select(id => byId[id])
            .GroupBy(x => x.Path)
            .ToDictionary(g => g.Key, g => (IReadOnlyCollection<long>)g.Select(x => x.Index).ToList());
        var (rows, _) = await table.DeleteAsync(new FileRowSelection(sel));
        Assert.Equal(2, rows);

        // Survivors keep their ABSOLUTE indexes (30 stays at 2, 50 at 4 — the DV never shifts positions).
        var after = new Dictionary<long, long>();
        await foreach (var b in table.ReadAllWithMetadataAsync())
        {
            int metaIdx = b.Schema.GetFieldIndex("_metadata");
            var meta = (StructArray)b.Column(metaIdx);
            var ridx = (Int64Array)meta.Fields[1];
            var ids = (Int64Array)b.Column(0);
            for (int i = 0; i < b.Length; i++)
                after[ids.GetValue(i)!.Value] = ridx.GetValue(i)!.Value;
        }
        Assert.Equal([10L, 30L, 50L, 60L, 70L], after.Keys.OrderBy(x => x).ToArray());
        Assert.Equal(2, after[30]);
        Assert.Equal(4, after[50]);
    }
}
