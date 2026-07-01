// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Checkpoint;
using EngineeredWood.DeltaLake.DeletionVectors;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.DeltaLake.Snapshot;
using EngineeredWood.IO;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Main entry point for Delta Lake table operations.
/// Supports reading and writing Arrow <see cref="RecordBatch"/> data,
/// time travel, compaction, and vacuum.
/// </summary>
public sealed class DeltaTable : IAsyncDisposable, IDisposable
{
    private readonly ITableFileSystem _fs;
    private readonly DeltaTableOptions _options;
    private readonly TransactionLog _log;
    private readonly CheckpointReader _checkpointReader;
    private readonly CheckpointWriter _checkpointWriter;
    private readonly DeletionVectorReader _dvReader;
    private Snapshot.Snapshot? _currentSnapshot;
    private bool _disposed;

    private DeltaTable(
        ITableFileSystem fileSystem,
        DeltaTableOptions options,
        Snapshot.Snapshot? snapshot)
    {
        _fs = fileSystem;
        _options = options;
        _log = new TransactionLog(fileSystem);
        _checkpointReader = new CheckpointReader(fileSystem);
        _dvReader = new DeletionVectorReader(fileSystem);
        _checkpointWriter = new CheckpointWriter(fileSystem, options.ParquetWriteOptions);
        _currentSnapshot = snapshot;
    }

    /// <summary>The current point-in-time table state.</summary>
    public Snapshot.Snapshot CurrentSnapshot =>
        _currentSnapshot ?? throw new InvalidOperationException("Table not initialized.");

    /// <summary>The Arrow schema of the table.</summary>
    public Apache.Arrow.Schema ArrowSchema => CurrentSnapshot.ArrowSchema;

    /// <summary>
    /// Opens an existing Delta table.
    /// </summary>
    public static async ValueTask<DeltaTable> OpenAsync(
        ITableFileSystem fileSystem,
        DeltaTableOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        options ??= DeltaTableOptions.Default;
        var log = new TransactionLog(fileSystem);

        long latestVersion = await log.GetLatestVersionAsync(cancellationToken)
            .ConfigureAwait(false);

        if (latestVersion < 0)
            throw new DeltaFormatException("No Delta table found (no commits in _delta_log/).");

        var checkpointReader = new CheckpointReader(fileSystem);
        var snapshot = await SnapshotBuilder.BuildAsync(
            log, checkpointReader, atVersion: null, cancellationToken)
            .ConfigureAwait(false);

        // Validate protocol compatibility
        ProtocolVersions.ValidateReadSupport(snapshot.Protocol);

        return new DeltaTable(fileSystem, options, snapshot);
    }

    /// <summary>
    /// Creates a new Delta table with the given Arrow schema.
    /// </summary>
    /// <param name="fileSystem">The filesystem rooted at the table directory.</param>
    /// <param name="schema">The Arrow schema for the table.</param>
    /// <param name="options">Table options.</param>
    /// <param name="partitionColumns">Ordered list of partition column names.</param>
    /// <param name="columnMappingMode">
    /// Column mapping mode. When set to <see cref="ColumnMappingMode.Name"/> or
    /// <see cref="ColumnMappingMode.Id"/>, the protocol is upgraded to
    /// Reader v2 / Writer v5 and column mapping metadata is assigned.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public static async ValueTask<DeltaTable> CreateAsync(
        ITableFileSystem fileSystem,
        Apache.Arrow.Schema schema,
        DeltaTableOptions? options = null,
        IReadOnlyList<string>? partitionColumns = null,
        ColumnMappingMode columnMappingMode = ColumnMappingMode.None,
        IReadOnlyDictionary<string, string>? configuration = null,
        CancellationToken cancellationToken = default)
    {
        options ??= DeltaTableOptions.Default;
        var log = new TransactionLog(fileSystem);

        // Check that the table doesn't already exist
        long latestVersion = await log.GetLatestVersionAsync(cancellationToken)
            .ConfigureAwait(false);

        if (latestVersion >= 0)
            throw new InvalidOperationException("Delta table already exists.");

        // Convert Arrow schema to Delta schema
        var deltaSchema = SchemaConverter.FromArrowSchema(schema);

        // Set protocol versions based on column mapping mode
        int minReaderVersion = 1;
        int minWriterVersion = 2;
        // Start from any caller-supplied table properties (e.g. delta.enableRowTracking).
        var config = new Dictionary<string, string>(StringComparer.Ordinal);
        if (configuration is not null)
        {
            foreach (var kv in configuration)
                config[kv.Key] = kv.Value;
        }

        if (columnMappingMode != ColumnMappingMode.None)
        {
            minReaderVersion = 2;
            minWriterVersion = 5;

            // Assign column mapping IDs and physical names
            var (mappedSchema, maxId) = ColumnMapping.AssignColumnMapping(deltaSchema);
            deltaSchema = mappedSchema;

            string modeStr = columnMappingMode switch
            {
                ColumnMappingMode.Id => "id",
                ColumnMappingMode.Name => "name",
                _ => "none",
            };

            config[ColumnMapping.ModeKey] = modeStr;
            config[ColumnMapping.MaxColumnIdKey] = maxId.ToString();
        }

        // Declare table features (writer-version-7 / reader-version-3) for any capabilities the configuration
        // enables, so the commits that use them are protocol-compliant (strict readers — Fabric's OneLake
        // converter, delta-kernel — reject a table that uses a feature its protocol does not list). In
        // table-features mode (reader v3 / writer v7) EVERY enabled feature must appear in the feature lists.
        var writerFeatureSet = new List<string>();
        List<string>? readerFeatureList = null;
        if (DeltaLake.RowTracking.RowTrackingConfig.IsEnabled(config))
        {
            minWriterVersion = 7;
            writerFeatureSet.Add("rowTracking");
            writerFeatureSet.Add("domainMetadata"); // rowTracking depends on domainMetadata (high-water-mark)
        }
        if (config.TryGetValue("delta.enableDeletionVectors", out var dv) &&
            string.Equals(dv, "true", StringComparison.OrdinalIgnoreCase))
        {
            minReaderVersion = 3;
            minWriterVersion = 7;
            writerFeatureSet.Add("deletionVectors");
            readerFeatureList = new List<string> { "deletionVectors" }; // DVs are a reader feature too
        }
        if (config.TryGetValue("delta.enableInCommitTimestamps", out var ict) &&
            string.Equals(ict, "true", StringComparison.OrdinalIgnoreCase))
        {
            // in-commit timestamps: a WRITER-only feature (readers read normally), so it does not bump the
            // reader version. Enabled at creation (version 0), so no inCommitTimestampEnablementVersion/Timestamp
            // properties are required (every commit carries the timestamp). EnsureCommitInfo writes the field.
            minWriterVersion = 7;
            writerFeatureSet.Add("inCommitTimestamp");
        }
        if (config.TryGetValue("delta.enableChangeDataFeed", out var cdf) &&
            string.Equals(cdf, "true", StringComparison.OrdinalIgnoreCase))
        {
            // change data feed: a WRITER-only feature (readers read data normally; the change feed is opt-in via
            // the reader). Writes _change_data files on DELETE/UPDATE so table_changes / ReadChangesAsync return a
            // correct feed.
            minWriterVersion = 7;
            writerFeatureSet.Add("changeDataFeed");
        }
        IReadOnlyList<string>? writerFeatures = writerFeatureSet.Count > 0 ? writerFeatureSet : null;
        IReadOnlyList<string>? readerFeatures = readerFeatureList;

        string schemaString = DeltaSchemaSerializer.Serialize(deltaSchema);
        Dictionary<string, string>? configuration2 = config.Count > 0 ? config : null;

        var actions = new List<DeltaAction>
        {
            new ProtocolAction
            {
                MinReaderVersion = minReaderVersion,
                MinWriterVersion = minWriterVersion,
                ReaderFeatures = readerFeatures,
                WriterFeatures = writerFeatures,
            },
            new MetadataAction
            {
                Id = Guid.NewGuid().ToString(),
                Format = Format.Parquet,
                SchemaString = schemaString,
                PartitionColumns = partitionColumns ?? [],
                Configuration = configuration2,
                CreatedTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            },
        };

        // Record a commitInfo on the create commit too (operation "CREATE TABLE" + timestamp), so version 0
        // appears in the history with metadata like every other commit.
        var createActions = Log.InCommitTimestamp.EnsureCommitInfo(actions, configuration2, "CREATE TABLE");

        await log.WriteCommitAsync(0, createActions, cancellationToken).ConfigureAwait(false);

        var snapshot = await SnapshotBuilder.BuildAsync(
            log, checkpointReader: null, atVersion: 0, cancellationToken)
            .ConfigureAwait(false);

        return new DeltaTable(fileSystem, options, snapshot);
    }

    /// <summary>
    /// Opens an existing Delta table, or creates a new one if it doesn't exist.
    /// </summary>
    public static async ValueTask<DeltaTable> OpenOrCreateAsync(
        ITableFileSystem fileSystem,
        Apache.Arrow.Schema schema,
        DeltaTableOptions? options = null,
        IReadOnlyList<string>? partitionColumns = null,
        IReadOnlyDictionary<string, string>? configuration = null,
        CancellationToken cancellationToken = default)
    {
        options ??= DeltaTableOptions.Default;
        var log = new TransactionLog(fileSystem);

        long latestVersion = await log.GetLatestVersionAsync(cancellationToken)
            .ConfigureAwait(false);

        if (latestVersion >= 0)
            return await OpenAsync(fileSystem, options, cancellationToken)
                .ConfigureAwait(false);

        return await CreateAsync(fileSystem, schema, options, partitionColumns,
            configuration: configuration, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Refreshes the snapshot to the latest version.
    /// </summary>
    public async ValueTask RefreshAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        _currentSnapshot = await SnapshotBuilder.UpdateAsync(
            CurrentSnapshot, _log, cancellationToken).ConfigureAwait(false);
    }

    #region Domain Metadata

    /// <summary>
    /// Gets all active domain metadata entries.
    /// </summary>
    public IReadOnlyDictionary<string, DomainMetadata> GetDomainMetadata() =>
        CurrentSnapshot.DomainMetadata;

    /// <summary>
    /// Gets the configuration for a specific domain, or null if not set.
    /// </summary>
    public string? GetDomainMetadata(string domain) =>
        CurrentSnapshot.DomainMetadata.TryGetValue(domain, out var dm) ? dm.Configuration : null;

    /// <summary>
    /// Sets domain metadata. User domains are unrestricted; system domains
    /// (starting with <c>delta.</c>) can only be modified by implementations
    /// that understand them.
    /// </summary>
    public async ValueTask<long> SetDomainMetadataAsync(
        string domain, string configuration,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);
        DomainMetadataValidation.ValidateUserModification(domain);

        IReadOnlyList<DeltaAction> actions = new List<DeltaAction>
        {
            new DomainMetadata
            {
                Domain = domain,
                Configuration = configuration,
                Removed = false,
            },
        };

        actions = Log.InCommitTimestamp.EnsureCommitInfo(
            actions, CurrentSnapshot.Metadata.Configuration, "SET DOMAIN METADATA");

        long newVersion = CurrentSnapshot.Version + 1;
        await _log.WriteCommitAsync(newVersion, actions, cancellationToken)
            .ConfigureAwait(false);

        _currentSnapshot = await SnapshotBuilder.UpdateAsync(
            CurrentSnapshot, _log, cancellationToken).ConfigureAwait(false);

        return newVersion;
    }

    /// <summary>
    /// Removes domain metadata by setting a tombstone. User domains are unrestricted;
    /// system domains can only be removed by implementations that understand them.
    /// </summary>
    public async ValueTask<long> RemoveDomainMetadataAsync(
        string domain,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);
        DomainMetadataValidation.ValidateUserModification(domain);

        if (!CurrentSnapshot.DomainMetadata.ContainsKey(domain))
            throw new InvalidOperationException(
                $"Domain '{domain}' does not exist in the table metadata.");

        IReadOnlyList<DeltaAction> actions = new List<DeltaAction>
        {
            new DomainMetadata
            {
                Domain = domain,
                Configuration = "",
                Removed = true,
            },
        };

        actions = Log.InCommitTimestamp.EnsureCommitInfo(
            actions, CurrentSnapshot.Metadata.Configuration, "REMOVE DOMAIN METADATA");

        long newVersion = CurrentSnapshot.Version + 1;
        await _log.WriteCommitAsync(newVersion, actions, cancellationToken)
            .ConfigureAwait(false);

        _currentSnapshot = await SnapshotBuilder.UpdateAsync(
            CurrentSnapshot, _log, cancellationToken).ConfigureAwait(false);

        return newVersion;
    }

    #endregion

    /// <summary>
    /// Schema evolution — appends a nullable column. Writes a metadata-only commit (a new
    /// <see cref="MetadataAction"/> whose schema = the current schema ++ <paramref name="newColumn"/>); NO data
    /// files are rewritten. Old files lack the column, so the read path backfills it as all-NULL
    /// (<see cref="BackfillMissingColumns"/>). The column must be nullable (existing rows have no value for it).
    /// Only supported for tables WITHOUT column mapping (the default); a column-mapping table would need a new
    /// field id, which this does not assign. Returns the new version.
    /// </summary>
    public async ValueTask<long> AddColumnAsync(Field newColumn, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);

        if (!newColumn.IsNullable)
            throw new InvalidOperationException(
                $"ADD COLUMN '{newColumn.Name}' must be nullable — existing rows have no value for a new column.");

        var snapshot = CurrentSnapshot;
        var config = snapshot.Metadata.Configuration;
        if (config is not null
            && config.TryGetValue(ColumnMapping.ModeKey, out var mode)
            && !string.Equals(mode, "none", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                "ADD COLUMN is not supported on a column-mapping table (no field id is assigned).");
        }

        var current = snapshot.ArrowSchema;
        foreach (var f in current.FieldsList)
        {
            if (string.Equals(f.Name, newColumn.Name, StringComparison.Ordinal))
                throw new InvalidOperationException($"Column '{newColumn.Name}' already exists.");
        }

        var fields = new List<Field>(current.FieldsList) { newColumn };
        var newArrowSchema = new Apache.Arrow.Schema(fields, current.Metadata);
        var newDeltaSchema = SchemaConverter.FromArrowSchema(newArrowSchema);
        string newSchemaString = DeltaSchemaSerializer.Serialize(newDeltaSchema);

        IReadOnlyList<DeltaAction> actions = new List<DeltaAction>
        {
            snapshot.Metadata with { SchemaString = newSchemaString },
        };
        actions = Log.InCommitTimestamp.EnsureCommitInfo(
            actions, snapshot.Metadata.Configuration, "ADD COLUMNS");

        long newVersion = snapshot.Version + 1;
        await _log.WriteCommitAsync(newVersion, actions, cancellationToken).ConfigureAwait(false);

        _currentSnapshot = await SnapshotBuilder.UpdateAsync(
            snapshot, _log, cancellationToken).ConfigureAwait(false);

        return newVersion;
    }

    /// <summary>
    /// Replaces the table's schema wholesale with <paramref name="newSchema"/> as a metadata-only commit
    /// (a new <c>metaData</c> action; no data files are rewritten). Unlike <see cref="AddColumnAsync"/> this can
    /// add, drop, or retype columns — it is the "schema overwrite" primitive used by a true CREATE OR REPLACE
    /// (adopt exactly the incoming schema). Callers are responsible for aligning the data (typically paired with
    /// an <c>Overwrite</c> write that removes the old-schema files immediately after). Only for tables WITHOUT
    /// column mapping (no field ids to assign/preserve). Returns the new version. No-op (returns the current
    /// version) if the schema is already identical.
    /// </summary>
    public async ValueTask<long> SetSchemaAsync(
        Apache.Arrow.Schema newSchema, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);

        var snapshot = CurrentSnapshot;
        var config = snapshot.Metadata.Configuration;
        if (config is not null
            && config.TryGetValue(ColumnMapping.ModeKey, out var mode)
            && !string.Equals(mode, "none", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                "SetSchema is not supported on a column-mapping table (field ids are not reassigned).");
        }

        var newDeltaSchema = SchemaConverter.FromArrowSchema(newSchema);
        string newSchemaString = DeltaSchemaSerializer.Serialize(newDeltaSchema);
        if (string.Equals(newSchemaString, snapshot.Metadata.SchemaString, StringComparison.Ordinal))
        {
            return snapshot.Version; // identical schema — nothing to commit
        }

        IReadOnlyList<DeltaAction> actions = new List<DeltaAction>
        {
            snapshot.Metadata with { SchemaString = newSchemaString },
        };
        actions = Log.InCommitTimestamp.EnsureCommitInfo(
            actions, snapshot.Metadata.Configuration, "CHANGE COLUMNS");

        long newVersion = snapshot.Version + 1;
        await _log.WriteCommitAsync(newVersion, actions, cancellationToken).ConfigureAwait(false);

        _currentSnapshot = await SnapshotBuilder.UpdateAsync(
            snapshot, _log, cancellationToken).ConfigureAwait(false);

        return newVersion;
    }

    /// <summary>
    /// Gets a snapshot at a specific version (time travel).
    /// </summary>
    public async ValueTask<Snapshot.Snapshot> GetSnapshotAtVersionAsync(
        long version, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        return await SnapshotBuilder.BuildAsync(
            _log, _checkpointReader, atVersion: version, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Gets a snapshot at the latest version whose in-commit timestamp
    /// is at or before the specified timestamp.
    /// Requires <c>delta.enableInCommitTimestamps</c> to be enabled.
    /// </summary>
    public async ValueTask<Snapshot.Snapshot> GetSnapshotAtTimestampAsync(
        DateTimeOffset timestamp, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        long targetMs = timestamp.ToUnixTimeMilliseconds();

        // Scan commits to find the latest version at or before the target timestamp
        long? bestVersion = null;

        await foreach (long version in _log.ListVersionsAsync(0, cancellationToken)
            .ConfigureAwait(false))
        {
            var actions = await _log.ReadCommitAsync(version, cancellationToken)
                .ConfigureAwait(false);

            long? commitTs = Log.InCommitTimestamp.GetTimestampFromActions(actions);

            if (commitTs.HasValue && commitTs.Value <= targetMs)
                bestVersion = version;
            else if (commitTs.HasValue && commitTs.Value > targetMs)
                break; // Timestamps are monotonically increasing
        }

        if (bestVersion is null)
            throw new DeltaFormatException(
                "No commit found at or before the specified timestamp. " +
                "Ensure the table has in-commit timestamps enabled.");

        return await GetSnapshotAtVersionAsync(bestVersion.Value, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Reads data at the latest version whose in-commit timestamp
    /// is at or before the specified timestamp.
    /// </summary>
    public async IAsyncEnumerable<RecordBatch> ReadAtTimestampAsync(
        DateTimeOffset timestamp,
        IReadOnlyList<string>? columns = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        var snapshot = await GetSnapshotAtTimestampAsync(timestamp, cancellationToken)
            .ConfigureAwait(false);

        foreach (var addFile in snapshot.ActiveFiles.Values)
        {
            await foreach (var batch in ReadFileAsync(
                addFile, columns, snapshot, cancellationToken).ConfigureAwait(false))
            {
                yield return batch;
            }
        }
    }

    /// <summary>One row of the table's commit history (the snapshots/versions view). <see cref="TimestampMs"/>
    /// is the commit's inCommitTimestamp (or the commitInfo <c>timestamp</c> field) in epoch ms, null on a plain
    /// table that records neither. <see cref="OperationParameters"/> is the raw JSON of commitInfo.operationParameters.</summary>
    public readonly record struct DeltaHistoryEntry(long Version, long? TimestampMs, string? Operation, string? OperationParameters);

    /// <summary>Enumerates the table's commit history (every version + its commitInfo), oldest first — the
    /// snapshots/versions view. Reads the Delta log only (no data files).</summary>
    public async IAsyncEnumerable<DeltaHistoryEntry> GetHistoryAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        await foreach (long version in _log.ListVersionsAsync(0, cancellationToken).ConfigureAwait(false))
        {
            var actions = await _log.ReadCommitAsync(version, cancellationToken).ConfigureAwait(false);
            long? ts = null;
            string? op = null;
            string? opParams = null;
            foreach (var action in actions)
            {
                if (action is CommitInfo ci)
                {
                    ts = Log.InCommitTimestamp.GetTimestamp(ci);
                    if (ts is null && ci.GetValue("timestamp") is
                        { ValueKind: System.Text.Json.JsonValueKind.Number } t)
                    {
                        ts = t.GetInt64();
                    }
                    if (ci.GetValue("operation") is { ValueKind: System.Text.Json.JsonValueKind.String } o)
                    {
                        op = o.GetString();
                    }
                    var p = ci.GetValue("operationParameters");
                    opParams = p.HasValue ? p.Value.GetRawText() : null;
                    break;
                }
            }
            yield return new DeltaHistoryEntry(version, ts, op, opParams);
        }
    }

    #region Delete and Update

    /// <summary>
    /// Deletes rows matching the predicate using deletion vectors.
    /// The predicate receives each batch (with logical column names) and returns
    /// a <see cref="BooleanArray"/> where <c>true</c> means the row should be deleted.
    /// Returns the number of rows deleted and the committed version.
    /// </summary>
    public async ValueTask<(long RowsDeleted, long Version)> DeleteAsync(
        Func<RecordBatch, BooleanArray> predicate,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);

        var snapshot = CurrentSnapshot;
        var dvWriter = new DeletionVectors.DeletionVectorWriter(_fs);
        var actions = new List<DeltaAction>();
        long totalDeleted = 0;
        bool cdfEnabled = DeltaLake.ChangeDataFeed.CdfConfig.IsEnabled(
            snapshot.Metadata.Configuration);

        foreach (var addFile in snapshot.ActiveFiles.Values)
        {
            var rawDeletedRows = addFile.DeletionVector is not null
                ? await _dvReader.ReadAsync(addFile.DeletionVector, cancellationToken)
                    .ConfigureAwait(false)
                : new HashSet<long>();

            var newDeletedIndices = new List<long>();
            var deletedRowBatches = new List<RecordBatch>(); // For CDC
            long rowOffset = 0;

            await using var file = await _fs.OpenReadAsync(addFile.Path, cancellationToken)
                .ConfigureAwait(false);
            using var reader = new Parquet.ParquetFileReader(
                file, ownsFile: false, _options.ParquetReadOptions);

            var mappingMode = ColumnMapping.GetMode(snapshot.Metadata.Configuration);
            var physicalToLogical = ColumnMapping.BuildPhysicalToLogicalMap(
                snapshot.Schema, mappingMode);

            await foreach (var batch in reader.ReadAllAsync(
                cancellationToken: cancellationToken).ConfigureAwait(false))
            {
                var logicalBatch = ColumnMapping.RenameColumns(batch, physicalToLogical);
                var mask = predicate(logicalBatch);
                var matchRows = new List<int>();

                for (int i = 0; i < batch.Length; i++)
                {
                    long absIdx = rowOffset + i;
                    if (rawDeletedRows.Contains(absIdx))
                        continue;

                    if (!mask.IsNull(i) && mask.GetValue(i) == true)
                    {
                        newDeletedIndices.Add(absIdx);
                        matchRows.Add(i);
                    }
                }

                // Collect deleted row data for CDC
                if (cdfEnabled && matchRows.Count > 0)
                    deletedRowBatches.Add(TakeRowsFromBatch(logicalBatch, matchRows));

                rowOffset += batch.Length;
            }

            if (newDeletedIndices.Count == 0)
                continue;

            var allDeleted = new HashSet<long>(rawDeletedRows);
            foreach (long idx in newDeletedIndices)
                allDeleted.Add(idx);

            totalDeleted += newDeletedIndices.Count;

            var newDv = await dvWriter.CreateAsync(
                allDeleted, allDeleted.Count, cancellationToken).ConfigureAwait(false);

            actions.Add(new RemoveFile
            {
                Path = addFile.Path,
                DeletionTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                DataChange = true,
                DeletionVector = addFile.DeletionVector,
            });

            actions.Add(addFile with
            {
                DeletionVector = newDv,
                DataChange = true,
            });

            // Write CDC file for deleted rows
            if (cdfEnabled)
            {
                foreach (var deletedBatch in deletedRowBatches)
                {
                    var cdcAction = await ChangeDataFeed.CdfWriter.WriteAsync(
                        _fs, deletedBatch, DeltaLake.ChangeDataFeed.CdfConfig.Delete,
                        addFile.PartitionValues, _options.ParquetWriteOptions,
                        cancellationToken).ConfigureAwait(false);
                    actions.Add(cdcAction);
                }
            }
        }

        if (actions.Count == 0)
            return (0, snapshot.Version);

        var finalActions = Log.InCommitTimestamp.EnsureCommitInfo(
            actions, snapshot.Metadata.Configuration, "DELETE");

        long newVersion = snapshot.Version + 1;
        await _log.WriteCommitAsync(newVersion, finalActions, cancellationToken)
            .ConfigureAwait(false);

        _currentSnapshot = await SnapshotBuilder.UpdateAsync(
            snapshot, _log, cancellationToken).ConfigureAwait(false);

        return (totalDeleted, newVersion);
    }

    /// <summary>
    /// Deletes the rows addressed by the TRANSIENT rowids in <paramref name="rowIds"/> (each =
    /// <c>(fileOrdinal &lt;&lt; 40) | rowPosition</c>, as produced by <see cref="ReadAllWithRowIdsAsync"/> over the
    /// SAME snapshot) using <b>copy-on-write</b>: each affected file is rewritten without the deleted rows and
    /// committed as plain <c>remove</c>/<c>add</c> actions — NO deletion vectors, NO row-tracking feature, so the
    /// result is maximally reader-compatible (Fabric OneLake conversion, Spark, delta-kernel all read it). The
    /// file ordinal is resolved against the path-sorted active set (<see cref="OrderedActiveFiles"/>), matching
    /// the read. Returns the number of rows deleted and the committed version.
    /// </summary>
    public async ValueTask<(long RowsDeleted, long Version)> DeleteByRowIdsAsync(
        IReadOnlyCollection<long> rowIds,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);

        var snapshot = CurrentSnapshot;
        if (rowIds.Count == 0)
            return (0, snapshot.Version);

        // Decode the transient rowids into a set of in-file positions per file ordinal.
        long posMask = (1L << RowIdPositionBits) - 1;
        var positionsByFile = new Dictionary<int, HashSet<long>>();
        foreach (var rid in rowIds)
        {
            int ordinal = (int)(rid >> RowIdPositionBits);
            if (!positionsByFile.TryGetValue(ordinal, out var set))
            {
                set = new HashSet<long>();
                positionsByFile[ordinal] = set;
            }
            set.Add(rid & posMask);
        }

        var ordered = OrderedActiveFiles(snapshot);
        var mappingMode = ColumnMapping.GetMode(snapshot.Metadata.Configuration);
        var logicalToPhysical = ColumnMapping.BuildLogicalToPhysicalMap(snapshot.Schema, mappingMode);
        bool cdfEnabled = DeltaLake.ChangeDataFeed.CdfConfig.IsEnabled(snapshot.Metadata.Configuration);
        var actions = new List<DeltaAction>();
        long totalDeleted = 0;

        foreach (var kvp in positionsByFile)
        {
            int ordinal = kvp.Key;
            var positions = kvp.Value;
            if (ordinal < 0 || ordinal >= ordered.Count)
                continue;
            var addFile = ordered[ordinal];

            // Read the file (logical) keeping only rows whose position is NOT targeted; rewrite the survivors.
            // When CDF is enabled, also collect the DELETED rows (logical) so we can emit a "delete" change file.
            var keptBatches = new List<RecordBatch>();
            var deletedBatches = new List<RecordBatch>();
            long pos = 0;
            long deletedHere = 0;
            await foreach (var batch in ReadFileAsync(addFile, null, snapshot, cancellationToken)
                               .ConfigureAwait(false))
            {
                var keepRows = new List<int>();
                var delRows = cdfEnabled ? new List<int>() : null;
                for (int i = 0; i < batch.Length; i++)
                {
                    if (positions.Contains(pos + i))
                    {
                        deletedHere++;
                        delRows?.Add(i);
                    }
                    else
                    {
                        keepRows.Add(i);
                    }
                }
                pos += batch.Length;
                if (keepRows.Count == batch.Length)
                    keptBatches.Add(batch);
                else if (keepRows.Count > 0)
                    keptBatches.Add(TakeRowsFromBatch(batch, keepRows));
                if (cdfEnabled && delRows!.Count > 0)
                    deletedBatches.Add(TakeRowsFromBatch(batch, delRows));
            }

            if (deletedHere == 0)
                continue;
            totalDeleted += deletedHere;

            actions.Add(new RemoveFile
            {
                Path = addFile.Path,
                DeletionTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                DataChange = true,
                ExtendedFileMetadata = true,
                PartitionValues = addFile.PartitionValues,
                Size = addFile.Size,
            });

            long keptCount = 0;
            foreach (var b in keptBatches)
                keptCount += b.Length;

            if (keptCount > 0)
            {
                string newFileName = $"{Guid.NewGuid():N}.parquet";
                long fileSize;
                await using (var file = await _fs.CreateAsync(newFileName, cancellationToken: cancellationToken)
                                 .ConfigureAwait(false))
                {
                    await using var writer = new Parquet.ParquetFileWriter(
                        file, ownsFile: false, _options.ParquetWriteOptions);
                    foreach (var b in keptBatches)
                    {
                        // Rebuild with a CLEAN schema (drop the parquet reader's field metadata, e.g. an existing
                        // PARQUET:field_id) so the re-write matches the initial write path — otherwise the writer
                        // emits a malformed footer (TProtocolException) on reader-sourced batches.
                        var cleanFields = new List<Field>(b.Schema.FieldsList.Count);
                        foreach (var f in b.Schema.FieldsList)
                            cleanFields.Add(new Field(f.Name, f.DataType, f.IsNullable));
                        var cleanArrays = new List<IArrowArray>(b.ColumnCount);
                        for (int c = 0; c < b.ColumnCount; c++)
                            cleanArrays.Add(b.Column(c));
                        var clean = new RecordBatch(new Apache.Arrow.Schema(cleanFields, null), cleanArrays, b.Length);

                        var physicalBatch = ColumnMapping.RenameToPhysical(clean, logicalToPhysical);
                        physicalBatch = ColumnMapping.SetParquetFieldIds(physicalBatch, snapshot.Schema, mappingMode);
                        await writer.WriteRowGroupAsync(physicalBatch, cancellationToken).ConfigureAwait(false);
                    }
                    await writer.DisposeAsync().ConfigureAwait(false);
                    fileSize = file.Position;
                }

                actions.Add(new AddFile
                {
                    Path = newFileName,
                    PartitionValues = addFile.PartitionValues,
                    Size = fileSize,
                    ModificationTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    DataChange = true,
                    Stats = Stats.StatsCollector.Collect(keptBatches),
                });
            }

            // Change Data Feed: record the deleted rows as a "delete" change file (reuses CdfWriter, the same
            // path the predicate DeleteAsync uses — so the rowid copy-on-write delete produces a correct feed
            // instead of the reader inferring whole-file deletes/inserts from the remove+add).
            if (cdfEnabled)
            {
                foreach (var deletedBatch in deletedBatches)
                {
                    var cdcAction = await ChangeDataFeed.CdfWriter.WriteAsync(
                        _fs, deletedBatch, DeltaLake.ChangeDataFeed.CdfConfig.Delete,
                        addFile.PartitionValues, _options.ParquetWriteOptions,
                        cancellationToken).ConfigureAwait(false);
                    actions.Add(cdcAction);
                }
            }
        }

        if (actions.Count == 0)
            return (0, snapshot.Version);

        var finalActions = Log.InCommitTimestamp.EnsureCommitInfo(
            actions, snapshot.Metadata.Configuration, "DELETE");

        long newVersion = snapshot.Version + 1;
        await _log.WriteCommitAsync(newVersion, finalActions, cancellationToken)
            .ConfigureAwait(false);

        _currentSnapshot = await SnapshotBuilder.UpdateAsync(
            snapshot, _log, cancellationToken).ConfigureAwait(false);

        return (totalDeleted, newVersion);
    }

    /// <summary>
    /// DELETE by TRANSIENT rowid using DELETION VECTORS (no file rewrite): each affected file's existing DV is
    /// unioned with the new in-file positions (decoded from <c>rowid &amp; posMask</c>) and a fresh DV is written;
    /// the commit is <c>remove</c> (old file+DV) + <c>add</c> (same file, new DV). The rowids MUST be ABSOLUTE
    /// positions (as <see cref="ReadAllWithRowIdsAsync"/> now emits) so repeated DV deletes compose. Requires the
    /// table to have <c>delta.enableDeletionVectors</c> (DeltaCatalog only calls this for such tables). The
    /// efficient alternative to <see cref="DeleteByRowIdsAsync"/> (copy-on-write). Returns rows newly deleted.
    /// </summary>
    public async ValueTask<(long RowsDeleted, long Version)> DeleteByRowIdsViaVectorsAsync(
        IReadOnlyCollection<long> rowIds,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);

        var snapshot = CurrentSnapshot;
        if (rowIds.Count == 0)
            return (0, snapshot.Version);

        long posMask = (1L << RowIdPositionBits) - 1;
        var positionsByFile = new Dictionary<int, HashSet<long>>();
        foreach (var rid in rowIds)
        {
            int ordinal = (int)(rid >> RowIdPositionBits);
            if (!positionsByFile.TryGetValue(ordinal, out var set))
            {
                set = new HashSet<long>();
                positionsByFile[ordinal] = set;
            }
            set.Add(rid & posMask);
        }

        var ordered = OrderedActiveFiles(snapshot);
        var dvWriter = new DeletionVectors.DeletionVectorWriter(_fs);
        bool cdfEnabled = DeltaLake.ChangeDataFeed.CdfConfig.IsEnabled(snapshot.Metadata.Configuration);
        var actions = new List<DeltaAction>();
        long totalDeleted = 0;

        foreach (var kvp in positionsByFile)
        {
            int ordinal = kvp.Key;
            if (ordinal < 0 || ordinal >= ordered.Count)
                continue;
            var addFile = ordered[ordinal];

            var allDeleted = addFile.DeletionVector is not null
                ? new HashSet<long>(await _dvReader.ReadAsync(addFile.DeletionVector, cancellationToken)
                    .ConfigureAwait(false))
                : new HashSet<long>();

            long newlyDeleted = 0;
            var newPositions = cdfEnabled ? new HashSet<long>() : null; // CDC: the rows deleted in THIS commit
            foreach (long p in kvp.Value)
                if (allDeleted.Add(p))
                {
                    newlyDeleted++;
                    newPositions?.Add(p);
                }
            if (newlyDeleted == 0)
                continue;
            totalDeleted += newlyDeleted;

            var newDv = await dvWriter.CreateAsync(allDeleted, allDeleted.Count, cancellationToken)
                .ConfigureAwait(false);

            actions.Add(new RemoveFile
            {
                Path = addFile.Path,
                DeletionTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                DataChange = true,
                DeletionVector = addFile.DeletionVector,
            });
            actions.Add(addFile with { DeletionVector = newDv, DataChange = true });

            // Change Data Feed: a DV delete doesn't rewrite data, so read the newly-deleted rows and emit a
            // "delete" change file. Read WITH the trailing rowid (absolute position) and match `rid & posMask`
            // against the newly-deleted ABSOLUTE positions — the survivor-sequential index would be wrong when
            // the file already carries a DV. Strip the rowid column from the emitted change rows.
            if (cdfEnabled)
            {
                var deletedBatches = new List<RecordBatch>();
                await foreach (var batch in ReadFileAsync(addFile, null, snapshot, cancellationToken,
                                                          fileOrdinal: ordinal).ConfigureAwait(false))
                {
                    var rids = batch.Column(batch.ColumnCount - 1) as Apache.Arrow.Int64Array; // trailing rowid
                    if (rids is null)
                        continue;
                    var delRows = new List<int>();
                    for (int i = 0; i < batch.Length; i++)
                        if (!rids.IsNull(i) && newPositions!.Contains(rids.GetValue(i)!.Value & posMask))
                            delRows.Add(i);
                    if (delRows.Count > 0)
                    {
                        var clean = DropVirtualRowId(batch);
                        deletedBatches.Add(TakeRowsFromBatch(clean, delRows));
                    }
                }
                foreach (var deletedBatch in deletedBatches)
                {
                    var cdcAction = await ChangeDataFeed.CdfWriter.WriteAsync(
                        _fs, deletedBatch, DeltaLake.ChangeDataFeed.CdfConfig.Delete,
                        addFile.PartitionValues, _options.ParquetWriteOptions, cancellationToken).ConfigureAwait(false);
                    actions.Add(cdcAction);
                }
            }
        }

        if (actions.Count == 0)
            return (0, snapshot.Version);

        var finalActions = Log.InCommitTimestamp.EnsureCommitInfo(
            actions, snapshot.Metadata.Configuration, "DELETE");
        long newVersion = snapshot.Version + 1;
        await _log.WriteCommitAsync(newVersion, finalActions, cancellationToken).ConfigureAwait(false);
        _currentSnapshot = await SnapshotBuilder.UpdateAsync(snapshot, _log, cancellationToken).ConfigureAwait(false);
        return (totalDeleted, newVersion);
    }

    /// <summary>
    /// Per-file copy-on-write UPDATE by TRANSIENT rowid (the companion to <see cref="DeleteByRowIdsAsync"/>).
    /// <paramref name="rowIds"/> = <c>(fileOrdinal &lt;&lt; 40) | rowPosition</c> (same encoding as
    /// <see cref="ReadAllWithRowIdsAsync"/>). Only the files containing a target row are rewritten: each such
    /// file's batches are read (in position order) and handed to <paramref name="rewriteFile"/> (which returns
    /// the same rows with the SET columns modified on the matched positions — the caller owns that typed logic),
    /// then re-written as plain <c>remove</c>+<c>add</c> with a CLEAN schema (so the parquet is standard-readable,
    /// like the delete path). Unaffected files are untouched. Returns the committed version (or the current
    /// version if nothing changed). The row count is the caller's (= number of distinct rowids).
    /// </summary>
    public async ValueTask<long> UpdateByRowIdsAsync(
        IReadOnlyCollection<long> rowIds,
        Func<long, IReadOnlyList<RecordBatch>, IReadOnlyList<RecordBatch>> rewriteFile,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);

        var snapshot = CurrentSnapshot;
        if (rowIds.Count == 0)
            return snapshot.Version;

        var affectedOrdinals = new HashSet<int>();
        foreach (var rid in rowIds)
            affectedOrdinals.Add((int)(rid >> RowIdPositionBits));

        var ordered = OrderedActiveFiles(snapshot);
        var mappingMode = ColumnMapping.GetMode(snapshot.Metadata.Configuration);
        var logicalToPhysical = ColumnMapping.BuildLogicalToPhysicalMap(snapshot.Schema, mappingMode);
        bool cdfEnabled = DeltaLake.ChangeDataFeed.CdfConfig.IsEnabled(snapshot.Metadata.Configuration);
        var rowIdSet = cdfEnabled ? (rowIds as HashSet<long> ?? new HashSet<long>(rowIds)) : null;
        var actions = new List<DeltaAction>();

        foreach (int ordinal in affectedOrdinals)
        {
            if (ordinal < 0 || ordinal >= ordered.Count)
                continue;
            var addFile = ordered[ordinal];

            // Read WITH the trailing _metadata.row_id column (absolute positions) so the caller matches rows by
            // rowid — correct even when the file already has a deletion vector (post-DV survivors keep their
            // absolute rowids). The caller returns USER-column batches (rowid stripped).
            var fileBatches = new List<RecordBatch>();
            await foreach (var batch in ReadFileAsync(addFile, null, snapshot, cancellationToken,
                                                      fileOrdinal: ordinal).ConfigureAwait(false))
            {
                fileBatches.Add(batch);
            }

            // The caller rebuilds the file's rows with the SET columns modified on matched positions.
            var rewritten = rewriteFile(ordinal, fileBatches);

            long keptCount = 0;
            foreach (var b in rewritten)
                keptCount += b.Length;

            string newFileName = $"{Guid.NewGuid():N}.parquet";
            long fileSize;
            await using (var file = await _fs.CreateAsync(newFileName, cancellationToken: cancellationToken)
                             .ConfigureAwait(false))
            {
                await using var writer = new Parquet.ParquetFileWriter(
                    file, ownsFile: false, _options.ParquetWriteOptions);
                foreach (var b in rewritten)
                {
                    // Clean schema (drop reader field metadata) before re-writing — same as the delete path,
                    // else the parquet footer is malformed for delta-kernel/Spark/Fabric.
                    var cleanFields = new List<Field>(b.Schema.FieldsList.Count);
                    foreach (var f in b.Schema.FieldsList)
                        cleanFields.Add(new Field(f.Name, f.DataType, f.IsNullable));
                    var cleanArrays = new List<IArrowArray>(b.ColumnCount);
                    for (int c = 0; c < b.ColumnCount; c++)
                        cleanArrays.Add(b.Column(c));
                    var clean = new RecordBatch(new Apache.Arrow.Schema(cleanFields, null), cleanArrays, b.Length);

                    var physicalBatch = ColumnMapping.RenameToPhysical(clean, logicalToPhysical);
                    physicalBatch = ColumnMapping.SetParquetFieldIds(physicalBatch, snapshot.Schema, mappingMode);
                    await writer.WriteRowGroupAsync(physicalBatch, cancellationToken).ConfigureAwait(false);
                }
                await writer.DisposeAsync().ConfigureAwait(false);
                fileSize = file.Position;
            }

            actions.Add(new RemoveFile
            {
                Path = addFile.Path,
                DeletionTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                DataChange = true,
                ExtendedFileMetadata = true,
                PartitionValues = addFile.PartitionValues,
                Size = addFile.Size,
                DeletionVector = addFile.DeletionVector, // match the active (path, DV) file so the remove takes effect
            });
            actions.Add(new AddFile
            {
                Path = newFileName,
                PartitionValues = addFile.PartitionValues,
                Size = fileSize,
                ModificationTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                DataChange = true,
                Stats = Stats.StatsCollector.Collect(rewritten),
            });

            // Change Data Feed: emit update_preimage (old rows) + update_postimage (new rows) for the matched
            // rows. The matched rows are those whose trailing _metadata.row_id is in the rowid set; the original
            // (pre) values come from fileBatches (rowid stripped), the new (post) values from the aligned
            // rewritten batches (the caller rebuilds in place, preserving batch/row structure).
            if (cdfEnabled && rewritten.Count == fileBatches.Count)
            {
                for (int bi = 0; bi < fileBatches.Count; bi++)
                {
                    var orig = fileBatches[bi];
                    var rew = rewritten[bi];
                    if (rew.Length != orig.Length || orig.ColumnCount == 0)
                        continue;
                    var rids = orig.Column(orig.ColumnCount - 1) as Apache.Arrow.Int64Array; // trailing rowid
                    if (rids is null)
                        continue;
                    var matched = new List<int>();
                    for (int i = 0; i < orig.Length; i++)
                    {
                        if (!rids.IsNull(i) && rowIdSet!.Contains(rids.GetValue(i)!.Value))
                            matched.Add(i);
                    }
                    if (matched.Count == 0)
                        continue;

                    var preFull = DropVirtualRowId(orig); // drop the trailing virtual rowid col
                    var preBatch = TakeRowsFromBatch(preFull, matched);
                    var postBatch = TakeRowsFromBatch(rew, matched);

                    var preCdc = await ChangeDataFeed.CdfWriter.WriteAsync(
                        _fs, preBatch, DeltaLake.ChangeDataFeed.CdfConfig.UpdatePreimage,
                        addFile.PartitionValues, _options.ParquetWriteOptions, cancellationToken).ConfigureAwait(false);
                    actions.Add(preCdc);
                    var postCdc = await ChangeDataFeed.CdfWriter.WriteAsync(
                        _fs, postBatch, DeltaLake.ChangeDataFeed.CdfConfig.UpdatePostimage,
                        addFile.PartitionValues, _options.ParquetWriteOptions, cancellationToken).ConfigureAwait(false);
                    actions.Add(postCdc);
                }
            }
        }

        if (actions.Count == 0)
            return snapshot.Version;

        var finalActions = Log.InCommitTimestamp.EnsureCommitInfo(
            actions, snapshot.Metadata.Configuration, "UPDATE");
        long newVersion = snapshot.Version + 1;
        await _log.WriteCommitAsync(newVersion, finalActions, cancellationToken).ConfigureAwait(false);
        _currentSnapshot = await SnapshotBuilder.UpdateAsync(snapshot, _log, cancellationToken).ConfigureAwait(false);
        return newVersion;
    }

    /// <summary>
    /// Updates rows matching the predicate. The <paramref name="updater"/> function
    /// receives matching rows and returns modified rows. Non-matching rows are
    /// preserved unchanged. Affected files are rewritten.
    /// Returns the number of rows updated and the committed version.
    /// </summary>
    public async ValueTask<(long RowsUpdated, long Version)> UpdateAsync(
        Func<RecordBatch, BooleanArray> predicate,
        Func<RecordBatch, RecordBatch> updater,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);

        var snapshot = CurrentSnapshot;
        var actions = new List<DeltaAction>();
        long totalUpdated = 0;
        bool cdfEnabled = DeltaLake.ChangeDataFeed.CdfConfig.IsEnabled(
            snapshot.Metadata.Configuration);

        var mappingMode = ColumnMapping.GetMode(snapshot.Metadata.Configuration);
        var logicalToPhysical = ColumnMapping.BuildLogicalToPhysicalMap(
            snapshot.Schema, mappingMode);
        var physicalToLogical = ColumnMapping.BuildPhysicalToLogicalMap(
            snapshot.Schema, mappingMode);

        foreach (var addFile in snapshot.ActiveFiles.Values)
        {
            // Read file data with DV filtering
            var batches = new List<RecordBatch>();
            await foreach (var batch in ReadFileAsync(
                addFile, null, snapshot, cancellationToken).ConfigureAwait(false))
            {
                batches.Add(batch);
            }

            if (batches.Count == 0)
                continue;

            // Evaluate predicate and apply updates
            bool fileModified = false;
            var outputBatches = new List<RecordBatch>();
            var preimages = new List<RecordBatch>();
            var postimages = new List<RecordBatch>();

            foreach (var batch in batches)
            {
                var mask = predicate(batch);
                int matchCount = CountTrue(mask);

                if (matchCount == 0)
                {
                    outputBatches.Add(batch);
                    continue;
                }

                fileModified = true;
                totalUpdated += matchCount;

                var matchRows = new List<int>();
                var keepRows = new List<int>();

                for (int i = 0; i < batch.Length; i++)
                {
                    if (!mask.IsNull(i) && mask.GetValue(i) == true)
                        matchRows.Add(i);
                    else
                        keepRows.Add(i);
                }

                if (matchRows.Count > 0)
                {
                    var matchBatch = TakeRowsFromBatch(batch, matchRows);
                    var updatedBatch = updater(matchBatch);
                    outputBatches.Add(updatedBatch);

                    // Collect preimage and postimage for CDC
                    if (cdfEnabled)
                    {
                        preimages.Add(matchBatch);
                        postimages.Add(updatedBatch);
                    }
                }

                if (keepRows.Count > 0)
                    outputBatches.Add(TakeRowsFromBatch(batch, keepRows));
            }

            if (!fileModified)
                continue;

            // Write new file with all output batches
            string newFileName = $"{Guid.NewGuid():N}.parquet";
            long fileSize;

            await using (var file = await _fs.CreateAsync(
                newFileName, cancellationToken: cancellationToken).ConfigureAwait(false))
            {
                await using var writer = new Parquet.ParquetFileWriter(
                    file, ownsFile: false, _options.ParquetWriteOptions);

                foreach (var batch in outputBatches)
                {
                    // Rename to physical names for Parquet storage
                    var physicalBatch = ColumnMapping.RenameToPhysical(batch, logicalToPhysical);
                    physicalBatch = ColumnMapping.SetParquetFieldIds(
                        physicalBatch, snapshot.Schema, mappingMode);

                    // Strip row tracking column if present
                    var (cleanBatch, _) = RowTracking.RowTrackingWriter.StripRowIdColumn(physicalBatch);
                    await writer.WriteRowGroupAsync(cleanBatch, cancellationToken)
                        .ConfigureAwait(false);
                }

                await writer.DisposeAsync().ConfigureAwait(false);
                fileSize = file.Position;
            }

            string? stats = Stats.StatsCollector.Collect(outputBatches);

            // Remove old, add new
            actions.Add(new RemoveFile
            {
                Path = addFile.Path,
                DeletionTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                DataChange = true,
                ExtendedFileMetadata = true,
                PartitionValues = addFile.PartitionValues,
                Size = addFile.Size,
            });

            actions.Add(new AddFile
            {
                Path = newFileName,
                PartitionValues = addFile.PartitionValues,
                Size = fileSize,
                ModificationTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                DataChange = true,
                Stats = stats,
            });

            // Write CDC files for update preimage/postimage
            if (cdfEnabled)
            {
                foreach (var pre in preimages)
                {
                    var cdcAction = await ChangeDataFeed.CdfWriter.WriteAsync(
                        _fs, pre, DeltaLake.ChangeDataFeed.CdfConfig.UpdatePreimage,
                        addFile.PartitionValues, _options.ParquetWriteOptions,
                        cancellationToken).ConfigureAwait(false);
                    actions.Add(cdcAction);
                }
                foreach (var post in postimages)
                {
                    var cdcAction = await ChangeDataFeed.CdfWriter.WriteAsync(
                        _fs, post, DeltaLake.ChangeDataFeed.CdfConfig.UpdatePostimage,
                        addFile.PartitionValues, _options.ParquetWriteOptions,
                        cancellationToken).ConfigureAwait(false);
                    actions.Add(cdcAction);
                }
            }
        }

        if (actions.Count == 0)
            return (0, snapshot.Version);

        var finalActions = Log.InCommitTimestamp.EnsureCommitInfo(
            actions, snapshot.Metadata.Configuration, "UPDATE");

        long newVersion = snapshot.Version + 1;
        await _log.WriteCommitAsync(newVersion, finalActions, cancellationToken)
            .ConfigureAwait(false);

        _currentSnapshot = await SnapshotBuilder.UpdateAsync(
            snapshot, _log, cancellationToken).ConfigureAwait(false);

        return (totalUpdated, newVersion);
    }

    private static int CountTrue(BooleanArray mask)
    {
        int count = 0;
        for (int i = 0; i < mask.Length; i++)
            if (!mask.IsNull(i) && mask.GetValue(i) == true)
                count++;
        return count;
    }

    private static RecordBatch TakeRowsFromBatch(RecordBatch batch, List<int> rows)
    {
        var columns = new IArrowArray[batch.ColumnCount];
        for (int col = 0; col < batch.ColumnCount; col++)
            columns[col] = DeletionVectors.DeletionVectorFilter.TakeRowsPublic(
                batch.Column(col), rows);
        return new RecordBatch(batch.Schema, columns, rows.Count);
    }

    // Drops the trailing VIRTUAL rowid column appended by ReadFileAsync(fileOrdinal). Unlike
    // RowTrackingWriter.StripRowIdColumn (which strips the PHYSICAL "__delta_row_id"), this removes
    // RowTrackingConfig.VirtualRowIdColumn ("_metadata.row_id"). Used before emitting CDC change rows
    // so the change feed's user-column schema matches the insert/delete/postimage batches.
    private static RecordBatch DropVirtualRowId(RecordBatch batch)
    {
        int last = batch.ColumnCount - 1;
        if (last < 0 ||
            batch.Schema.FieldsList[last].Name != DeltaLake.RowTracking.RowTrackingConfig.VirtualRowIdColumn)
            return batch;
        var fields = new List<Field>(last);
        var columns = new IArrowArray[last];
        for (int col = 0; col < last; col++)
        {
            fields.Add(batch.Schema.FieldsList[col]);
            columns[col] = batch.Column(col);
        }
        return new RecordBatch(new Apache.Arrow.Schema(fields, batch.Schema.Metadata), columns, batch.Length);
    }

    // True when `fileValues` matches every entry in `filter` (partition-overwrite file selection). A file matches
    // only if it carries each filter key with the exact same value (ordinal string compare — partition values are
    // stored as strings). Keys are validated to be partition columns before this is called.
    private static bool PartitionValuesMatch(
        IReadOnlyDictionary<string, string> fileValues, IReadOnlyDictionary<string, string> filter)
    {
        foreach (var kv in filter)
        {
            if (!fileValues.TryGetValue(kv.Key, out var v) || !string.Equals(v, kv.Value, StringComparison.Ordinal))
            {
                return false;
            }
        }
        return true;
    }

    #endregion

    /// <summary>
    /// Reads row-level changes between two versions using the Change Data Feed.
    /// Each batch includes <c>_change_type</c> ("insert", "delete", "update_preimage",
    /// "update_postimage"), <c>_commit_version</c>, and <c>_commit_timestamp</c> columns.
    /// For versions with CDC files, those are used directly. For versions without,
    /// changes are inferred from add/remove actions.
    /// </summary>
    public IAsyncEnumerable<RecordBatch> ReadChangesAsync(
        long startVersion, long endVersion,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        return ChangeDataFeed.CdfReader.ReadChangesAsync(
            _fs, _log, startVersion, endVersion,
            _options.ParquetReadOptions, cancellationToken);
    }

    /// <summary>
    /// Creates a log compaction file for a range of commits.
    /// Compacted files aggregate reconciled actions, allowing readers to
    /// skip individual commit files for faster snapshot construction.
    /// </summary>
    /// <param name="startVersion">Start of the commit range (inclusive).</param>
    /// <param name="endVersion">End of the commit range (inclusive). Must be greater than startVersion.</param>
    public async ValueTask CompactLogAsync(
        long startVersion, long endVersion,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        var logCompaction = new Log.LogCompaction(_fs, _log);
        await logCompaction.CompactRangeAsync(startVersion, endVersion, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Reads all data from the current snapshot as a stream of RecordBatches.
    /// </summary>
    public IAsyncEnumerable<RecordBatch> ReadAllAsync(
        IReadOnlyList<string>? columns = null,
        CancellationToken cancellationToken = default) =>
        ReadAllAsync(columns, filter: null, cancellationToken);

    /// <summary>
    /// Reads all data with an optional <see cref="EngineeredWood.Expressions.Predicate"/>
    /// filter. When set, files whose partition values or column statistics
    /// prove no rows can match are skipped before any data pages are read.
    /// The reader does NOT re-apply the predicate per row; callers wanting
    /// exact row-level filtering must do that on the returned batches.
    /// </summary>
    public async IAsyncEnumerable<RecordBatch> ReadAllAsync(
        IReadOnlyList<string>? columns,
        EngineeredWood.Expressions.Predicate? filter,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        var snapshot = CurrentSnapshot;
        var pruner = filter is null ? null : new DeltaFilePruner(
            snapshot.Schema, snapshot.Metadata.PartitionColumns);

        foreach (var addFile in snapshot.ActiveFiles.Values)
        {
            if (pruner is not null && !pruner.ShouldInclude(addFile, filter!))
                continue;

            await foreach (var batch in ReadFileAsync(
                addFile, columns, snapshot, cancellationToken).ConfigureAwait(false))
            {
                yield return batch;
            }
        }
    }

    /// <summary>The deterministic active-file ordering used to encode/decode the transient rowid (sorted by
    /// path). The same ordering at scan + delete guarantees a rowid's high bits map back to the same file.</summary>
    private static List<Actions.AddFile> OrderedActiveFiles(Snapshot.Snapshot snapshot)
    {
        var files = new List<Actions.AddFile>(snapshot.ActiveFiles.Values);
        files.Sort((a, b) => string.CompareOrdinal(a.Path, b.Path));
        return files;
    }

    // Transient rowid packing: (fileOrdinal << PositionBits) | rowPositionInFile. NOT a stable Delta row id —
    // valid only within one snapshot; encodes "which file + which row" so a copy-on-write DELETE can find rows.
    private const int RowIdPositionBits = 40; // up to ~1T rows/file and ~16M files

    /// <summary>
    /// Like <see cref="ReadAllAsync(IReadOnlyList{string}, EngineeredWood.Expressions.Predicate, CancellationToken)"/>
    /// but appends a trailing non-null Int64 <c>_metadata.row_id</c> = a TRANSIENT rowid
    /// <c>(fileOrdinal &lt;&lt; 40) | rowPosition</c> (file ordinal in the path-sorted active set; row position in
    /// the file). NOT a stable Delta row id — it round-trips to <see cref="DeleteByRowIdsAsync"/> within the same
    /// snapshot so a plain copy-on-write DELETE (no deletion vectors, no row-tracking feature → maximally
    /// reader-compatible, incl. Fabric/Spark) can locate the rows. The whole-row read order matches the rewrite.
    /// </summary>
    public async IAsyncEnumerable<RecordBatch> ReadAllWithRowIdsAsync(
        IReadOnlyList<string>? columns,
        EngineeredWood.Expressions.Predicate? filter,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        var snapshot = CurrentSnapshot;
        var pruner = filter is null ? null : new DeltaFilePruner(
            snapshot.Schema, snapshot.Metadata.PartitionColumns);

        var ordered = OrderedActiveFiles(snapshot);
        for (int ordinal = 0; ordinal < ordered.Count; ordinal++)
        {
            var addFile = ordered[ordinal];
            if (pruner is not null && !pruner.ShouldInclude(addFile, filter!))
                continue;

            await foreach (var batch in ReadFileAsync(
                addFile, columns, snapshot, cancellationToken, fileOrdinal: ordinal).ConfigureAwait(false))
            {
                yield return batch;
            }
        }
    }

    /// <summary>
    /// Reads data from a specific version (time travel).
    /// </summary>
    public IAsyncEnumerable<RecordBatch> ReadAtVersionAsync(
        long version,
        IReadOnlyList<string>? columns = null,
        CancellationToken cancellationToken = default) =>
        ReadAtVersionAsync(version, columns, filter: null, cancellationToken);

    /// <summary>
    /// Reads data from a specific version with an optional filter predicate.
    /// See <see cref="ReadAllAsync(IReadOnlyList{string}, EngineeredWood.Expressions.Predicate, CancellationToken)"/>
    /// for filter semantics.
    /// </summary>
    public async IAsyncEnumerable<RecordBatch> ReadAtVersionAsync(
        long version,
        IReadOnlyList<string>? columns,
        EngineeredWood.Expressions.Predicate? filter,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        var snapshot = await GetSnapshotAtVersionAsync(version, cancellationToken)
            .ConfigureAwait(false);
        var pruner = filter is null ? null : new DeltaFilePruner(
            snapshot.Schema, snapshot.Metadata.PartitionColumns);

        foreach (var addFile in snapshot.ActiveFiles.Values)
        {
            if (pruner is not null && !pruner.ShouldInclude(addFile, filter!))
                continue;

            await foreach (var batch in ReadFileAsync(
                addFile, columns, snapshot, cancellationToken).ConfigureAwait(false))
            {
                yield return batch;
            }
        }
    }

    /// <summary>
    /// Time travel WITH the transient rowid column — the version analog of
    /// <see cref="ReadAllWithRowIdsAsync"/>. Each batch carries the trailing <c>_metadata.row_id</c> =
    /// <c>(fileOrdinal &lt;&lt; 40) | rowPosition</c> over the version's path-sorted active files. Read-only
    /// (the rowid is consumed by DuckDB's count/scan, not by DML against a past snapshot).
    /// </summary>
    public async IAsyncEnumerable<RecordBatch> ReadAtVersionWithRowIdsAsync(
        long version,
        IReadOnlyList<string>? columns,
        EngineeredWood.Expressions.Predicate? filter,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        var snapshot = await GetSnapshotAtVersionAsync(version, cancellationToken).ConfigureAwait(false);
        var pruner = filter is null ? null : new DeltaFilePruner(
            snapshot.Schema, snapshot.Metadata.PartitionColumns);

        var ordered = OrderedActiveFiles(snapshot);
        for (int ordinal = 0; ordinal < ordered.Count; ordinal++)
        {
            var addFile = ordered[ordinal];
            if (pruner is not null && !pruner.ShouldInclude(addFile, filter!))
                continue;

            await foreach (var batch in ReadFileAsync(
                addFile, columns, snapshot, cancellationToken, fileOrdinal: ordinal).ConfigureAwait(false))
            {
                yield return batch;
            }
        }
    }

    /// <summary>
    /// Writes RecordBatch data as a new commit.
    /// Returns the committed version number.
    /// </summary>
    public ValueTask<long> WriteAsync(
        IReadOnlyList<RecordBatch> batches,
        DeltaWriteMode mode = DeltaWriteMode.Append,
        CancellationToken cancellationToken = default)
        => WriteCoreAsync(batches, mode, null, cancellationToken);

    /// <summary>
    /// Atomically overwrites one or more whole partitions in a SINGLE commit: removes exactly the active files
    /// whose partition values match every entry in <paramref name="overwritePartitions"/>, and adds
    /// <paramref name="batches"/> (which must fall within those partitions). This is delta-rs's static
    /// partition-overwrite / <c>replaceWhere</c>-on-partition-columns: the removal is file-exact (no rewrite)
    /// because the keys are partition columns, and the swap is one atomic Delta version. Files outside the target
    /// partitions are untouched. The keys must be partition columns of the table.
    /// </summary>
    public ValueTask<long> OverwritePartitionsAsync(
        IReadOnlyList<RecordBatch> batches,
        IReadOnlyDictionary<string, string> overwritePartitions,
        CancellationToken cancellationToken = default)
        => WriteCoreAsync(batches, DeltaWriteMode.Overwrite, overwritePartitions, cancellationToken);

    private async ValueTask<long> WriteCoreAsync(
        IReadOnlyList<RecordBatch> batches,
        DeltaWriteMode mode,
        IReadOnlyDictionary<string, string>? overwritePartitions,
        CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);

        var snapshot = CurrentSnapshot;

        // A partition-overwrite: the filter keys MUST be partition columns so file-level removal is exact (a
        // data-column predicate could partially match a file → deleting the whole file would drop other rows).
        if (overwritePartitions is { Count: > 0 })
        {
            foreach (var key in overwritePartitions.Keys)
            {
                if (!snapshot.Metadata.PartitionColumns.Contains(key))
                {
                    throw new DeltaFormatException(
                        $"OverwritePartitions: '{key}' is not a partition column of the table " +
                        $"(partition columns: {string.Join(", ", snapshot.Metadata.PartitionColumns)}).");
                }
            }
        }

        // Iceberg compatibility: validate constraints before writing
        var icebergVersion = Schema.IcebergCompat.GetVersion(snapshot.Metadata.Configuration);
        if (icebergVersion != Schema.IcebergCompatVersion.None)
        {
            Schema.IcebergCompat.Validate(icebergVersion, snapshot.Metadata, snapshot.Protocol);
        }

        var actions = new List<DeltaAction>();

        // For overwrite mode, remove existing files: ALL of them for a full overwrite, or only the files whose
        // partition values match `overwritePartitions` for an atomic partition-scoped overwrite (files outside
        // the target partitions are kept).
        if (mode == DeltaWriteMode.Overwrite)
        {
            long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            foreach (var existingFile in snapshot.ActiveFiles.Values)
            {
                if (overwritePartitions is { Count: > 0 } &&
                    !PartitionValuesMatch(existingFile.PartitionValues, overwritePartitions))
                {
                    continue; // keep files outside the target partition(s)
                }
                actions.Add(new RemoveFile
                {
                    Path = existingFile.Path,
                    DeletionTimestamp = now,
                    DataChange = true,
                    ExtendedFileMetadata = true,
                    PartitionValues = existingFile.PartitionValues,
                    Size = existingFile.Size,
                });
            }
        }

        // Row tracking: prepare high water mark
        bool rowTrackingEnabled = DeltaLake.RowTracking.RowTrackingConfig.IsEnabled(
            snapshot.Metadata.Configuration);
        long nextRowId = rowTrackingEnabled ? snapshot.RowIdHighWaterMark : 0;
        long newVersion = snapshot.Version + 1;

        // Identity columns: prepare configs
        var identityConfigs = new Dictionary<string, IdentityColumnConfig>();
        foreach (var field in snapshot.Schema.Fields)
        {
            var config = IdentityColumn.GetConfig(field);
            if (config is not null)
                identityConfigs[field.Name] = config;
        }
        var allIdentityUpdates = new List<(string Name, long HighWaterMark)>();

        // Column mapping: prepare logical-to-physical name mapping
        var partitionColumns = snapshot.Metadata.PartitionColumns;
        var mappingMode = ColumnMapping.GetMode(snapshot.Metadata.Configuration);
        var logicalToPhysical = ColumnMapping.BuildLogicalToPhysicalMap(
            snapshot.Schema, mappingMode);

        foreach (var batch in batches)
        {
            if (batch.Length == 0)
                continue;

            // Process identity columns: generate or validate values
            var processedBatch = batch;
            if (identityConfigs.Count > 0)
            {
                var (processed, updates) = IdentityColumns.IdentityColumnWriter.ProcessBatch(
                    batch, snapshot.Schema, ref identityConfigs);
                processedBatch = processed;
                allIdentityUpdates.AddRange(updates);
            }

            var partitions = Partitioning.PartitionUtils.SplitByPartition(
                processedBatch, partitionColumns);

            foreach (var (partValues, dataBatch) in partitions)
            {
                if (dataBatch.Length == 0)
                    continue;

                // Partition overwrite: the input must fall within the target partition(s) — otherwise we'd ADD
                // files in partitions we didn't clear, silently mixing overwrite + append semantics.
                if (overwritePartitions is { Count: > 0 } && !PartitionValuesMatch(partValues, overwritePartitions))
                {
                    throw new DeltaFormatException(
                        "OverwritePartitions: input data falls outside the target partition(s) " +
                        $"({string.Join(", ", overwritePartitions.Select(kv => kv.Key + "=" + kv.Value))}).");
                }

                // Rename logical columns to physical names for Parquet storage
                var physicalBatch = ColumnMapping.RenameToPhysical(dataBatch, logicalToPhysical);

                // IcebergCompat: materialize partition columns into Parquet file
                if (Schema.IcebergCompat.RequiresPartitionMaterialization(icebergVersion) &&
                    partValues.Count > 0)
                {
                    physicalBatch = Partitioning.PartitionUtils.AppendPartitionColumns(
                        physicalBatch, partValues, snapshot.Schema, partitionColumns,
                        logicalToPhysical);
                }

                // Set PARQUET:field_id metadata for column mapping (both id and name modes)
                physicalBatch = ColumnMapping.SetParquetFieldIds(
                    physicalBatch, snapshot.Schema, mappingMode);

                // Assign row IDs if row tracking is enabled
                long fileBaseRowId = nextRowId;
                if (rowTrackingEnabled)
                {
                    physicalBatch = RowTracking.RowTrackingWriter.AddRowIdColumn(
                        physicalBatch, fileBaseRowId);
                    nextRowId += dataBatch.Length;
                }

                // Build file path: partition subdirectory + UUID filename
                string partDir = Partitioning.PartitionUtils.BuildPartitionPath(partValues);
                string fileName = string.IsNullOrEmpty(partDir)
                    ? $"{Guid.NewGuid():N}.parquet"
                    : $"{partDir}/{Guid.NewGuid():N}.parquet";

                long fileSize;

                await using (var file = await _fs.CreateAsync(
                    fileName, cancellationToken: cancellationToken).ConfigureAwait(false))
                {
                    await using var writer = new ParquetFileWriter(
                        file, ownsFile: false, _options.ParquetWriteOptions);
                    await writer.WriteRowGroupAsync(physicalBatch, cancellationToken)
                        .ConfigureAwait(false);

                    // DisposeAsync writes the Parquet footer before we read Position
                    await writer.DisposeAsync().ConfigureAwait(false);
                    fileSize = file.Position;
                }

                // Collect stats from the data batch using logical names
                // IcebergCompat requires numRecords in stats regardless of options
                string? stats = null;
                if (_options.CollectStats ||
                    Schema.IcebergCompat.RequiresNumRecords(icebergVersion))
                    stats = CollectStats(dataBatch);

                actions.Add(new AddFile
                {
                    Path = fileName,
                    PartitionValues = partValues,
                    Size = fileSize,
                    ModificationTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    DataChange = true,
                    Stats = stats,
                    BaseRowId = rowTrackingEnabled ? fileBaseRowId : null,
                    DefaultRowCommitVersion = rowTrackingEnabled ? newVersion : null,
                });
            }
        }

        // If identity columns were updated, emit metadata action with new HWMs
        if (allIdentityUpdates.Count > 0)
        {
            var updatedSchema = snapshot.Schema;
            foreach (var (name, hwm) in allIdentityUpdates)
            {
                var updatedFields = updatedSchema.Fields.Select(f =>
                    f.Name == name ? IdentityColumn.UpdateHighWaterMark(f, hwm) : f).ToList();
                updatedSchema = new Schema.StructType { Fields = updatedFields };
            }

            string updatedSchemaString = DeltaSchemaSerializer.Serialize(updatedSchema);
            actions.Add(snapshot.Metadata with { SchemaString = updatedSchemaString });
        }

        // Prepend CommitInfo with inCommitTimestamp if enabled
        var finalActions = Log.InCommitTimestamp.EnsureCommitInfo(
            actions, snapshot.Metadata.Configuration, "WRITE");

        // Commit (newVersion computed earlier for row tracking)
        await _log.WriteCommitAsync(newVersion, finalActions, cancellationToken)
            .ConfigureAwait(false);

        // Refresh snapshot
        _currentSnapshot = await SnapshotBuilder.UpdateAsync(
            snapshot, _log, cancellationToken).ConfigureAwait(false);

        // Auto-checkpoint
        if (_options.CheckpointInterval > 0 &&
            newVersion % _options.CheckpointInterval == 0)
        {
            await _checkpointWriter.WriteCheckpointAsync(
                _currentSnapshot, cancellationToken).ConfigureAwait(false);
        }

        return newVersion;
    }

    /// <summary>
    /// Writes a stream of RecordBatch data as a new commit.
    /// </summary>
    public async ValueTask<long> WriteAsync(
        IAsyncEnumerable<RecordBatch> batches,
        DeltaWriteMode mode = DeltaWriteMode.Append,
        CancellationToken cancellationToken = default)
    {
        var batchList = new List<RecordBatch>();
        await foreach (var batch in batches.WithCancellation(cancellationToken)
            .ConfigureAwait(false))
        {
            batchList.Add(batch);
        }
        return await WriteAsync(batchList, mode, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Compacts small files into larger ones.
    /// Returns the committed version number, or null if no compaction was needed.
    /// </summary>
    public async ValueTask<long?> CompactAsync(
        CompactionOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);

        options ??= CompactionOptions.Default;
        var result = await Compaction.CompactionExecutor.ExecuteAsync(
            _fs, _log, CurrentSnapshot, options,
            _options.ParquetWriteOptions, _options.ParquetReadOptions,
            cancellationToken).ConfigureAwait(false);

        if (result.HasValue)
        {
            _currentSnapshot = await SnapshotBuilder.UpdateAsync(
                CurrentSnapshot, _log, cancellationToken).ConfigureAwait(false);
        }

        return result;
    }

    /// <summary>
    /// Deletes unreferenced data files older than the retention period.
    /// </summary>
    public async ValueTask<VacuumResult> VacuumAsync(
        TimeSpan? retentionPeriod = null,
        bool dryRun = false,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        // When vacuumProtocolCheck is enabled, validate we understand ALL features
        // before deleting any files — prevents deleting files needed by
        // features this implementation doesn't recognize.
        ProtocolVersions.ValidateVacuumSupport(CurrentSnapshot.Protocol);

        var retention = retentionPeriod ?? _options.VacuumRetention;
        return await Vacuum.VacuumExecutor.ExecuteAsync(
            _fs, CurrentSnapshot, retention, dryRun, cancellationToken)
            .ConfigureAwait(false);
    }

    private async IAsyncEnumerable<RecordBatch> ReadFileAsync(
        AddFile addFile,
        IReadOnlyList<string>? columns,
        Snapshot.Snapshot snapshot,
        [EnumeratorCancellation] CancellationToken cancellationToken,
        long fileOrdinal = -1)
    {
        // fileOrdinal >= 0 => append a trailing transient rowid column _metadata.row_id =
        // (fileOrdinal << RowIdPositionBits) | ABSOLUTE rowPositionInFile (the parquet row index, counting
        // DV-deleted rows too — NOT the post-filter index). Absolute positions are stable across repeated
        // deletion-vector deletes (the file is never rewritten), so the rowid round-trips to both
        // DeleteByRowIdsAsync (copy-on-write) and the DV-union delete.
        bool includeRowId = fileOrdinal >= 0;
        var partitionColumns = snapshot.Metadata.PartitionColumns;
        bool hasPartitions = partitionColumns.Count > 0 &&
            addFile.PartitionValues.Count > 0;

        // Column mapping setup
        var mappingMode = ColumnMapping.GetMode(snapshot.Metadata.Configuration);
        bool isIdMode = mappingMode == ColumnMappingMode.Id;

        var logicalToPhysical = ColumnMapping.BuildLogicalToPhysicalMap(
            snapshot.Schema, mappingMode);
        var physicalToLogical = ColumnMapping.BuildPhysicalToLogicalMap(
            snapshot.Schema, mappingMode);
        var fieldIdToLogical = isIdMode
            ? ColumnMapping.BuildFieldIdToLogicalMap(snapshot.Schema)
            : null;

        // Open the file and read its Parquet schema for field_id resolution
        await using var file = await _fs.OpenReadAsync(addFile.Path, cancellationToken)
            .ConfigureAwait(false);
        using var reader = new ParquetFileReader(
            file, ownsFile: false, _options.ParquetReadOptions);

        Parquet.Schema.SchemaDescriptor? parquetSchema = null;

        // Determine which columns to request from the Parquet file
        IReadOnlyList<string>? fileColumns = null;

        if (isIdMode)
        {
            // In ID mode: resolve field_ids from the Parquet schema to column names
            parquetSchema = await reader.GetSchemaAsync(cancellationToken)
                .ConfigureAwait(false);

            if (columns is not null)
            {
                var partSet = hasPartitions
                    ? new HashSet<string>(partitionColumns, StringComparer.Ordinal)
                    : new HashSet<string>();
                var logicalToFieldId = ColumnMapping.BuildLogicalToFieldIdMap(snapshot.Schema);

                // Map logical names → field_ids → Parquet column names
                var fieldIds = columns
                    .Where(c => !partSet.Contains(c))
                    .Where(c => logicalToFieldId.ContainsKey(c))
                    .Select(c => logicalToFieldId[c])
                    .ToList();
                var resolved = parquetSchema.ResolveFieldIds(fieldIds);
                fileColumns = resolved.Where(n => n is not null).Select(n => n!).ToList();
            }
            // else: read all columns, rename by field_id after
        }
        else
        {
            // Name mode or None mode: translate by physical name
            fileColumns = columns;
            if (columns is not null)
            {
                var partSet = hasPartitions
                    ? new HashSet<string>(partitionColumns, StringComparer.Ordinal)
                    : new HashSet<string>();

                fileColumns = columns
                    .Where(c => !partSet.Contains(c))
                    .Select(c => logicalToPhysical.TryGetValue(c, out var p) ? p : c)
                    .ToList();
            }
            else if (hasPartitions)
            {
                var partSet = new HashSet<string>(partitionColumns, StringComparer.Ordinal);
                fileColumns = snapshot.Schema.Fields
                    .Where(f => !partSet.Contains(f.Name))
                    .Select(f => ColumnMapping.GetPhysicalName(f, mappingMode))
                    .ToList();
            }
        }

        // Load deletion vector if present
        HashSet<long>? deletedRows = null;
        if (addFile.DeletionVector is not null)
        {
            deletedRows = await _dvReader.ReadAsync(
                addFile.DeletionVector, cancellationToken).ConfigureAwait(false);
        }

        if (parquetSchema is null && isIdMode)
        {
            parquetSchema = await reader.GetSchemaAsync(cancellationToken)
                .ConfigureAwait(false);
        }

        long batchStartRow = 0;

        await foreach (var batch in reader.ReadAllAsync(
            columnNames: fileColumns, cancellationToken: cancellationToken)
            .ConfigureAwait(false))
        {
            // Rename columns back to logical names
            RecordBatch result;
            if (isIdMode && fieldIdToLogical is not null && parquetSchema is not null)
            {
                result = ColumnMapping.RenameByFieldId(batch, fieldIdToLogical, parquetSchema);
            }
            else
            {
                result = ColumnMapping.RenameColumns(batch, physicalToLogical);
            }

            // Compute the ABSOLUTE parquet positions of the rows that survive DV filtering (in order), so the
            // rowid column reflects true file positions — captured BEFORE the filter drops rows.
            List<long>? survivorAbs = null;
            if (includeRowId)
            {
                survivorAbs = new List<long>(batch.Length);
                for (int i = 0; i < batch.Length; i++)
                {
                    long abs = batchStartRow + i;
                    if (deletedRows is null || !deletedRows.Contains(abs))
                        survivorAbs.Add(abs);
                }
            }

            // Apply deletion vector filtering
            if (deletedRows is not null)
            {
                result = DeletionVectorFilter.Filter(result, deletedRows, batchStartRow);
                batchStartRow += batch.Length;

                if (result.Length == 0)
                    continue; // All rows in this batch were deleted
            }
            else
            {
                batchStartRow += batch.Length; // track absolute position for the rowid even without a DV
            }

            // Apply type widening — convert narrow types from old files to current schema types
            if (Schema.TypeWidening.IsEnabled(snapshot.Metadata.Configuration) ||
                HasTypeChanges(snapshot.Schema))
            {
                var targetSchema = columns is not null
                    ? BuildProjectedSchema(snapshot.ArrowSchema, columns,
                        hasPartitions ? partitionColumns : null)
                    : BuildNonPartitionSchema(snapshot.ArrowSchema, partitionColumns);

                result = TypeWidening.ValueWidener.WidenBatch(result, targetSchema);
            }

            if (hasPartitions)
            {
                // Re-add partition columns as constant arrays
                var fullSchema = columns is not null
                    ? BuildProjectedSchema(snapshot.ArrowSchema, columns)
                    : snapshot.ArrowSchema;

                result = Partitioning.PartitionUtils.AddPartitionColumns(
                    result, fullSchema, addFile.PartitionValues, partitionColumns);
            }

            // Strip any internal row tracking column if present (no-op for plain tables).
            var (cleanResult, _) = RowTracking.RowTrackingWriter.StripRowIdColumn(result);

            // Schema evolution: a column ADDed after this file was written is absent from the parquet — backfill
            // it as all-NULL so every emitted batch matches the current schema (the expected output columns).
            var expectedFields = (columns is not null
                ? BuildProjectedSchema(snapshot.ArrowSchema, columns)
                : snapshot.ArrowSchema).FieldsList;
            cleanResult = BackfillMissingColumns(cleanResult, expectedFields);

            // Append the TRANSIENT rowid column = (fileOrdinal << bits) | rowPosition, advancing the per-file
            // position counter in read order so DeleteByRowIdsAsync can map each rowid back to (file, position).
            if (includeRowId)
            {
                long fileBase = fileOrdinal << RowIdPositionBits;
                var rowIdBuilder = new Int64Array.Builder();
                for (int i = 0; i < cleanResult.Length; i++)
                    rowIdBuilder.Append(fileBase | survivorAbs![i]); // absolute position (aligned with survivors)

                var fields = new List<Field>(cleanResult.Schema.FieldsList)
                {
                    new Field(DeltaLake.RowTracking.RowTrackingConfig.VirtualRowIdColumn,
                              Apache.Arrow.Types.Int64Type.Default, false),
                };
                var arrays = new List<IArrowArray>(cleanResult.ColumnCount + 1);
                for (int c = 0; c < cleanResult.ColumnCount; c++)
                    arrays.Add(cleanResult.Column(c));
                arrays.Add(rowIdBuilder.Build());

                var schemaBuilder = new Apache.Arrow.Schema.Builder();
                foreach (var f in fields)
                    schemaBuilder.Field(f);
                cleanResult = new RecordBatch(schemaBuilder.Build(), arrays, cleanResult.Length);
            }

            yield return cleanResult;
        }
    }

    private static Apache.Arrow.Schema BuildProjectedSchema(
        Apache.Arrow.Schema fullSchema, IReadOnlyList<string> columns)
    {
        var colSet = new HashSet<string>(columns, StringComparer.Ordinal);
        var builder = new Apache.Arrow.Schema.Builder();
        foreach (var field in fullSchema.FieldsList)
        {
            if (colSet.Contains(field.Name))
                builder.Field(field);
        }
        return builder.Build();
    }

    /// <summary>
    /// Schema evolution backfill: a column ADDed (via <see cref="AddColumnAsync"/>) after a data file was
    /// written is absent from that file's parquet, so a batch read from it lacks the column. Reconcile the
    /// batch to <paramref name="expectedFields"/> (the current schema's expected output columns) — present
    /// columns are taken by name, missing ones are filled with an all-NULL array of the field's type. No-op
    /// (returns the batch unchanged) when every expected field is already present.
    /// </summary>
    private static RecordBatch BackfillMissingColumns(RecordBatch batch, IReadOnlyList<Field> expectedFields)
    {
        var present = new Dictionary<string, int>(StringComparer.Ordinal);
        for (int i = 0; i < batch.Schema.FieldsList.Count; i++)
            present[batch.Schema.FieldsList[i].Name] = i;

        bool anyMissing = false;
        foreach (var f in expectedFields)
        {
            if (!present.ContainsKey(f.Name)) { anyMissing = true; break; }
        }
        if (!anyMissing)
            return batch; // common path — file has every current column, no rebuild.

        var arrays = new List<IArrowArray>(expectedFields.Count);
        var schemaBuilder = new Apache.Arrow.Schema.Builder();
        foreach (var f in expectedFields)
        {
            schemaBuilder.Field(f);
            arrays.Add(present.TryGetValue(f.Name, out int idx)
                ? batch.Column(idx)
                : MakeNullArray(f.DataType, batch.Length));
        }
        return new RecordBatch(schemaBuilder.Build(), arrays, batch.Length);
    }

    /// <summary>Builds an all-NULL array of the given Arrow type and length (for schema-evolution backfill).</summary>
    private static IArrowArray MakeNullArray(IArrowType type, int length)
    {
        switch (type)
        {
            case BooleanType:
            { var b = new BooleanArray.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
            case Int8Type:
            { var b = new Int8Array.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
            case Int16Type:
            { var b = new Int16Array.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
            case Int32Type:
            { var b = new Int32Array.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
            case Int64Type:
            { var b = new Int64Array.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
            case UInt8Type:
            { var b = new UInt8Array.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
            case UInt16Type:
            { var b = new UInt16Array.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
            case UInt32Type:
            { var b = new UInt32Array.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
            case UInt64Type:
            { var b = new UInt64Array.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
            case FloatType:
            { var b = new FloatArray.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
            case DoubleType:
            { var b = new DoubleArray.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
            case Decimal128Type dec:
            { var b = new Decimal128Array.Builder(dec); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
            case Date32Type:
            { var b = new Date32Array.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
            case TimestampType ts:
            { var b = new TimestampArray.Builder(ts); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
            case BinaryType:
            { var b = new BinaryArray.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
            case StringType:
            default:
            { var b = new StringArray.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
        }
    }

    private static Apache.Arrow.Schema BuildProjectedSchema(
        Apache.Arrow.Schema fullSchema, IReadOnlyList<string> columns,
        IReadOnlyList<string>? excludeColumns)
    {
        var colSet = new HashSet<string>(columns, StringComparer.Ordinal);
        var excludeSet = excludeColumns is not null
            ? new HashSet<string>(excludeColumns, StringComparer.Ordinal)
            : new HashSet<string>();
        var builder = new Apache.Arrow.Schema.Builder();
        foreach (var field in fullSchema.FieldsList)
        {
            if (colSet.Contains(field.Name) && !excludeSet.Contains(field.Name))
                builder.Field(field);
        }
        return builder.Build();
    }

    private static Apache.Arrow.Schema BuildNonPartitionSchema(
        Apache.Arrow.Schema fullSchema, IReadOnlyList<string> partitionColumns)
    {
        if (partitionColumns.Count == 0)
            return fullSchema;

        var partSet = new HashSet<string>(partitionColumns, StringComparer.Ordinal);
        var builder = new Apache.Arrow.Schema.Builder();
        foreach (var field in fullSchema.FieldsList)
        {
            if (!partSet.Contains(field.Name))
                builder.Field(field);
        }
        return builder.Build();
    }

    private static bool HasTypeChanges(EngineeredWood.DeltaLake.Schema.StructType schema)
    {
        foreach (var field in schema.Fields)
        {
            if (field.Metadata is not null &&
                field.Metadata.ContainsKey(EngineeredWood.DeltaLake.Schema.TypeWidening.TypeChangesKey))
                return true;
        }
        return false;
    }

    private static string? CollectStats(RecordBatch batch) =>
        Stats.StatsCollector.Collect(batch);

    private void ThrowIfDisposed()
    {
#if NET8_0_OR_GREATER
        ObjectDisposedException.ThrowIf(_disposed, this);
#else
        if (_disposed) throw new ObjectDisposedException(GetType().FullName);
#endif
    }

    public void Dispose()
    {
        _disposed = true;
    }

    public ValueTask DisposeAsync()
    {
        _disposed = true;
        return default;
    }
}
