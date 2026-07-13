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
using EngineeredWood.Expressions;
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
        Schema.StructType? preAssignedSchema = null,
        CancellationToken cancellationToken = default)
    {
        options ??= DeltaTableOptions.Default;
        var log = new TransactionLog(fileSystem);

        // Check that the table doesn't already exist
        long latestVersion = await log.GetLatestVersionAsync(cancellationToken)
            .ConfigureAwait(false);

        if (latestVersion >= 0)
            throw new InvalidOperationException("Delta table already exists.");

        // Convert Arrow schema to Delta schema. `preAssignedSchema` = a caller-supplied Delta schema whose
        // column-mapping ids + physical names were assigned BEFORE this create (data files referencing them
        // were already written — an eagerly-streamed buffered-transaction CTAS); physical names are random
        // GUIDs, so a re-assignment here would orphan those files.
        var deltaSchema = preAssignedSchema ?? SchemaConverter.FromArrowSchema(schema);

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

            // Assign column mapping IDs and physical names (a pre-assigned schema keeps its own).
            int maxId;
            if (preAssignedSchema is not null)
            {
                maxId = ColumnMapping.GetMaxColumnId(deltaSchema);
            }
            else
            {
                (deltaSchema, maxId) = ColumnMapping.AssignColumnMapping(deltaSchema);
            }

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
        // timestamp_ntz (a naive TIMESTAMP column) is itself a table feature: the spec requires the
        // 'timestampNtz' reader+writer feature whenever the schema contains the type — strict readers
        // (Spark, delta-kernel) reject the table otherwise.
        if (SchemaUsesTimestampNtz(deltaSchema))
        {
            minReaderVersion = 3;
            minWriterVersion = 7;
            writerFeatureSet.Add("timestampNtz");
            readerFeatureList ??= new List<string>();
            readerFeatureList.Add("timestampNtz");
        }
        // variant is likewise a reader+writer table feature ('variantType') whenever the schema contains it.
        if (SchemaUsesVariant(deltaSchema))
        {
            minReaderVersion = 3;
            minWriterVersion = 7;
            writerFeatureSet.Add("variantType");
            readerFeatureList ??= new List<string>();
            readerFeatureList.Add("variantType");
        }
        // Identity columns (delta.identity.* field metadata) are a WRITER-only feature ('identityColumns',
        // legacy writer v6) — readers see an ordinary long column. Declared whenever the schema carries one,
        // so a strict writer (Spark) recognizes the table's generation contract.
        if (deltaSchema.Fields.Any(IdentityColumn.IsIdentityColumn))
        {
            minWriterVersion = 7;
            writerFeatureSet.Add("identityColumns");
        }
        // Column mapping is BOTH a reader and writer feature. In table-features mode (reader v3 / writer v7 —
        // forced by deletionVectors/rowTracking/… above) it MUST be listed in BOTH feature lists, else a strict
        // reader (Spark) rejects the table ("feature enabled in metadata but not listed in protocol"). Absent any
        // other table feature, legacy versioning (reader v2 / writer v5, no lists — set above) is used instead.
        if (columnMappingMode != ColumnMappingMode.None)
        {
            bool tableFeatures = minWriterVersion >= 7 || minReaderVersion >= 3
                                 || writerFeatureSet.Count > 0 || readerFeatureList is not null;
            if (tableFeatures)
            {
                minReaderVersion = Math.Max(minReaderVersion, 3);
                minWriterVersion = 7;
                if (!writerFeatureSet.Contains("columnMapping"))
                    writerFeatureSet.Add("columnMapping");
                readerFeatureList ??= new List<string>();
                if (!readerFeatureList.Contains("columnMapping"))
                    readerFeatureList.Add("columnMapping");
            }
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
        ColumnMappingMode columnMappingMode = ColumnMappingMode.None,
        Schema.StructType? preAssignedSchema = null,
        CancellationToken cancellationToken = default)
    {
        options ??= DeltaTableOptions.Default;
        var log = new TransactionLog(fileSystem);

        long latestVersion = await log.GetLatestVersionAsync(cancellationToken)
            .ConfigureAwait(false);

        if (latestVersion >= 0)
            return await OpenAsync(fileSystem, options, cancellationToken)
                .ConfigureAwait(false);

        // New table: honor the requested column-mapping mode (name/id assigns physical names + bumps the protocol).
        return await CreateAsync(fileSystem, schema, options, partitionColumns,
            columnMappingMode: columnMappingMode, configuration: configuration,
            preAssignedSchema: preAssignedSchema,
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes a Change-Data-Feed <c>_change_data</c> parquet file for <paramref name="rows"/> (no commit —
    /// the returned <see cref="CdcFile"/> action is the caller's to include in a later commit). Serves a
    /// buffered (multi-statement) transaction's eager CDC capture: the rows are in hand at statement time,
    /// the action fuses into the transaction's single commit at flush. Unpartitioned rows by default
    /// (<paramref name="partitionValues"/> empty). Column mapping + the <c>_change_type</c> column are
    /// handled exactly like the per-statement CDC writes (<see cref="ChangeDataFeed.CdfWriter"/>).
    /// </summary>
    public async ValueTask<IReadOnlyList<CdcFile>> WriteChangeDataFileAsync(
        RecordBatch rows, string changeType,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        return await ChangeDataFeed.CdfWriter.WriteSplitAsync(
            _fs, CurrentSnapshot, rows, changeType,
            _options.ParquetWriteOptions, cancellationToken).ConfigureAwait(false);
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
    /// On a column-mapping table the new field is assigned a fresh column id (maxColumnId + 1) and physical name,
    /// and <c>delta.columnMapping.maxColumnId</c> is bumped. Returns the new version.
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
        var mappingMode = ColumnMapping.GetMode(config);

        foreach (var f in snapshot.Schema.Fields)
        {
            if (string.Equals(f.Name, newColumn.Name, StringComparison.Ordinal))
                throw new InvalidOperationException($"Column '{newColumn.Name}' already exists.");
        }

        // Convert the incoming Arrow field to a Delta field (via a one-field schema — reuses the type mapping).
        var newDeltaField = SchemaConverter.FromArrowSchema(
            new Apache.Arrow.Schema(new[] { newColumn }, null)).Fields[0];

        string newSchemaString;
        var newConfig = config;
        if (mappingMode == ColumnMappingMode.None)
        {
            // Plain table: append the field; old files backfill NULL on read.
            var fields = new List<StructField>(snapshot.Schema.Fields) { newDeltaField };
            newSchemaString = DeltaSchemaSerializer.Serialize(
                new EngineeredWood.DeltaLake.Schema.StructType { Fields = fields });
        }
        else
        {
            // Column-mapping table: assign the new field a fresh column id + physical name — RECURSIVELY
            // (AssignColumnMapping, the create-time assigner), so a struct/array/map-typed column arrives
            // with ids on every descendant (a top-level-only assignment would commit spec-violating
            // metadata that strict readers reject) — and bump maxColumnId past the last assigned id.
            // Existing fields keep their id/physicalName. Old files lack the column → the read path
            // backfills NULL by name/field-id.
            var (mappedField, lastId) = AssignMappedField(snapshot, config, newDeltaField);
            var fields = new List<StructField>(snapshot.Schema.Fields) { mappedField };
            newSchemaString = DeltaSchemaSerializer.Serialize(
                new EngineeredWood.DeltaLake.Schema.StructType { Fields = fields });
            var cfg = config is null
                ? new Dictionary<string, string>()
                : config.ToDictionary(kv => kv.Key, kv => kv.Value);
            cfg[ColumnMapping.MaxColumnIdKey] = lastId.ToString();
            newConfig = cfg;
        }

        // Adding a column whose type requires a schema-driven table feature (timestampNtz / variantType)
        // to a table whose protocol lacks it requires a protocol upgrade in the same commit.
        var protocolUpgrade =
            UpgradeProtocolForFeatures(snapshot.Protocol, RequiredSchemaFeatures(newDeltaField.Type));

        var actionList = new List<DeltaAction>();
        if (protocolUpgrade is not null)
            actionList.Add(protocolUpgrade);
        actionList.Add(snapshot.Metadata with { SchemaString = newSchemaString, Configuration = newConfig });
        IReadOnlyList<DeltaAction> actions = actionList;
        actions = Log.InCommitTimestamp.EnsureCommitInfo(
            actions, snapshot.Metadata.Configuration, "ADD COLUMNS");

        long newVersion = snapshot.Version + 1;
        await _log.WriteCommitAsync(newVersion, actions, cancellationToken).ConfigureAwait(false);

        _currentSnapshot = await SnapshotBuilder.UpdateAsync(
            snapshot, _log, cancellationToken).ConfigureAwait(false);

        return newVersion;
    }

    /// <summary>The deferred (compute-only) form of a schema change, for a buffered multi-statement
    /// transaction: <see cref="Actions"/> = the optional protocol upgrade + the new <c>metaData</c> action,
    /// to be fused into ONE commit via <see cref="CommitDataFilesAsync"/>' <c>extraActions</c>;
    /// <see cref="NewSchema"/> is the parsed new Delta schema (drives the caller's read overlays and
    /// schema-overridden writes).</summary>
    public readonly record struct DeferredSchemaChange(
        IReadOnlyList<DeltaAction> Actions,
        MetadataAction Metadata,
        ProtocolAction? ProtocolUpgrade,
        EngineeredWood.DeltaLake.Schema.StructType NewSchema);

    /// <summary>
    /// The compute-only counterpart of <see cref="AddColumnAsync"/>: builds the metadata (+ protocol
    /// upgrade) actions for appending a nullable column WITHOUT committing — for a buffered transaction
    /// that fuses its schema change with its data changes into one atomic commit. For CHAINED adds in one
    /// transaction pass the previous change's <paramref name="baseMetadata"/>/<paramref name="baseProtocol"/>
    /// so the second column composes on the first's pending schema/protocol. Pure computation, no IO.
    /// </summary>
    public DeferredSchemaChange ComputeAddColumn(
        Field newColumn, MetadataAction? baseMetadata = null, ProtocolAction? baseProtocol = null)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);

        if (!newColumn.IsNullable)
            throw new InvalidOperationException(
                $"ADD COLUMN '{newColumn.Name}' must be nullable — existing rows have no value for a new column.");

        var snapshot = CurrentSnapshot;
        var baseMeta = baseMetadata ?? snapshot.Metadata;
        var baseSchema = baseMetadata is null
            ? snapshot.Schema
            : DeltaSchemaSerializer.Parse(baseMetadata.SchemaString);
        var config = baseMeta.Configuration;
        var mappingMode = ColumnMapping.GetMode(config);

        foreach (var f in baseSchema.Fields)
        {
            if (string.Equals(f.Name, newColumn.Name, StringComparison.Ordinal))
                throw new InvalidOperationException($"Column '{newColumn.Name}' already exists.");
        }

        var newDeltaField = SchemaConverter.FromArrowSchema(
            new Apache.Arrow.Schema(new[] { newColumn }, null)).Fields[0];

        EngineeredWood.DeltaLake.Schema.StructType newSchema;
        var newConfig = config;
        if (mappingMode == ColumnMappingMode.None)
        {
            newSchema = new EngineeredWood.DeltaLake.Schema.StructType
            {
                Fields = new List<StructField>(baseSchema.Fields) { newDeltaField },
            };
        }
        else
        {
            // Fresh column id + physical name, recursively, continuing past the base's maxColumnId (the
            // base may itself be a pending change that already bumped it).
            var (mappedField, lastId) = AssignMappedFieldFor(baseSchema, config, newDeltaField);
            newSchema = new EngineeredWood.DeltaLake.Schema.StructType
            {
                Fields = new List<StructField>(baseSchema.Fields) { mappedField },
            };
            var cfg = config is null
                ? new Dictionary<string, string>()
                : config.ToDictionary(kv => kv.Key, kv => kv.Value);
            cfg[ColumnMapping.MaxColumnIdKey] = lastId.ToString();
            newConfig = cfg;
        }

        var protocolUpgrade = UpgradeProtocolForFeatures(
            baseProtocol ?? snapshot.Protocol, RequiredSchemaFeatures(newDeltaField.Type));

        var metadata = baseMeta with
        {
            SchemaString = DeltaSchemaSerializer.Serialize(newSchema),
            Configuration = newConfig,
        };
        var actions = new List<DeltaAction>();
        if (protocolUpgrade is not null)
            actions.Add(protocolUpgrade);
        actions.Add(metadata);
        return new DeferredSchemaChange(actions, metadata, protocolUpgrade, newSchema);
    }

    /// <summary>
    /// The compute-only counterpart of <see cref="RenameColumnAsync"/> — for a buffered transaction. Requires
    /// column mapping (checked against the base config). The renamed field keeps its column id + physical
    /// name; a renamed PARTITION column also updates <c>metaData.partitionColumns</c> (callers that cannot
    /// handle a mid-transaction partition-column rename should compare
    /// <see cref="DeferredSchemaChange.Metadata"/>.PartitionColumns against the base). No protocol change.
    /// </summary>
    public DeferredSchemaChange ComputeRenameColumn(
        string oldName, string newName, MetadataAction? baseMetadata = null)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);

        var snapshot = CurrentSnapshot;
        var baseMeta = baseMetadata ?? snapshot.Metadata;
        var baseSchema = baseMetadata is null
            ? snapshot.Schema
            : DeltaSchemaSerializer.Parse(baseMetadata.SchemaString);
        if (ColumnMapping.GetMode(baseMeta.Configuration) == ColumnMappingMode.None)
        {
            throw new InvalidOperationException(
                "RENAME COLUMN requires column mapping (enable it at table creation) — a plain table would need "
                + "to rewrite every data file since the logical name is the physical parquet column name.");
        }

        StructField? target = null;
        foreach (var f in baseSchema.Fields)
        {
            if (string.Equals(f.Name, newName, StringComparison.Ordinal))
                throw new InvalidOperationException($"Column '{newName}' already exists.");
            if (string.Equals(f.Name, oldName, StringComparison.Ordinal))
                target = f;
        }
        if (target is null)
            throw new InvalidOperationException($"Column '{oldName}' does not exist.");

        var newFields = new List<StructField>(baseSchema.Fields.Count);
        foreach (var f in baseSchema.Fields)
        {
            newFields.Add(ReferenceEquals(f, target)
                ? new StructField { Name = newName, Type = f.Type, Nullable = f.Nullable, Metadata = f.Metadata }
                : f);
        }
        var newSchema = new EngineeredWood.DeltaLake.Schema.StructType { Fields = newFields };

        var newPartitionColumns = baseMeta.PartitionColumns;
        if (newPartitionColumns.Contains(oldName))
        {
            newPartitionColumns = newPartitionColumns
                .Select(pc => string.Equals(pc, oldName, StringComparison.Ordinal) ? newName : pc)
                .ToList();
        }

        var metadata = baseMeta with
        {
            SchemaString = DeltaSchemaSerializer.Serialize(newSchema),
            PartitionColumns = newPartitionColumns,
        };
        return new DeferredSchemaChange(new List<DeltaAction> { metadata }, metadata, null, newSchema);
    }

    /// <summary>The compute-only counterpart of <see cref="DropColumnAsync"/> — for a buffered transaction.
    /// Requires column mapping; partition columns and the last column are rejected. No protocol change.</summary>
    public DeferredSchemaChange ComputeDropColumn(string name, MetadataAction? baseMetadata = null)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);

        var snapshot = CurrentSnapshot;
        var baseMeta = baseMetadata ?? snapshot.Metadata;
        var baseSchema = baseMetadata is null
            ? snapshot.Schema
            : DeltaSchemaSerializer.Parse(baseMetadata.SchemaString);
        if (ColumnMapping.GetMode(baseMeta.Configuration) == ColumnMappingMode.None)
        {
            throw new InvalidOperationException(
                "DROP COLUMN requires column mapping (enable it at table creation) — a plain table would need "
                + "to rewrite every data file since the logical name is the physical parquet column name.");
        }
        foreach (var pc in baseMeta.PartitionColumns)
        {
            if (string.Equals(pc, name, StringComparison.Ordinal))
                throw new InvalidOperationException($"Cannot drop partition column '{name}'.");
        }

        var newFields = new List<StructField>(baseSchema.Fields.Count);
        bool found = false;
        foreach (var f in baseSchema.Fields)
        {
            if (string.Equals(f.Name, name, StringComparison.Ordinal)) { found = true; continue; }
            newFields.Add(f);
        }
        if (!found)
            throw new InvalidOperationException($"Column '{name}' does not exist.");
        if (newFields.Count == 0)
            throw new InvalidOperationException("Cannot drop the table's only column.");
        var newSchema = new EngineeredWood.DeltaLake.Schema.StructType { Fields = newFields };

        var metadata = baseMeta with { SchemaString = DeltaSchemaSerializer.Serialize(newSchema) };
        return new DeferredSchemaChange(new List<DeltaAction> { metadata }, metadata, null, newSchema);
    }

    /// <summary>The compute-only counterpart of <see cref="AddFieldAsync"/> (nested ADD) — for a buffered
    /// transaction. Under column mapping the new field gets fresh recursive ids continuing past the base's
    /// maxColumnId; may carry a protocol upgrade for schema-driven features.</summary>
    public DeferredSchemaChange ComputeAddField(
        IReadOnlyList<string> containerPath, Field newField,
        MetadataAction? baseMetadata = null, ProtocolAction? baseProtocol = null)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);
        if (containerPath.Count == 0)
            throw new ArgumentException("containerPath must name the containing struct column.", nameof(containerPath));
        if (!newField.IsNullable)
            throw new InvalidOperationException(
                $"ADD COLUMN '{PathText(containerPath)}.{newField.Name}' must be nullable — existing rows have no value for a new field.");

        var snapshot = CurrentSnapshot;
        var baseMeta = baseMetadata ?? snapshot.Metadata;
        var baseSchema = baseMetadata is null
            ? snapshot.Schema
            : DeltaSchemaSerializer.Parse(baseMetadata.SchemaString);
        var config = baseMeta.Configuration;
        var mappingMode = ColumnMapping.GetMode(config);

        var newDeltaField = SchemaConverter.FromArrowSchema(
            new Apache.Arrow.Schema(new[] { newField }, null)).Fields[0];

        var newConfig = config;
        if (mappingMode != ColumnMappingMode.None)
        {
            var (mappedField, lastId) = AssignMappedFieldFor(baseSchema, config, newDeltaField);
            newDeltaField = mappedField;
            var cfg = config is null
                ? new Dictionary<string, string>()
                : config.ToDictionary(kv => kv.Key, kv => kv.Value);
            cfg[ColumnMapping.MaxColumnIdKey] = lastId.ToString();
            newConfig = cfg;
        }

        var addedField = newDeltaField;
        var newSchema = TransformStructAt(baseSchema, containerPath, 0, fields =>
        {
            foreach (var f in fields)
            {
                if (string.Equals(f.Name, addedField.Name, StringComparison.Ordinal))
                    throw new InvalidOperationException(
                        $"Field '{PathText(containerPath)}.{addedField.Name}' already exists.");
            }
            return new List<StructField>(fields) { addedField };
        });

        var protocolUpgrade = UpgradeProtocolForFeatures(
            baseProtocol ?? snapshot.Protocol, RequiredSchemaFeatures(newDeltaField.Type));

        var metadata = baseMeta with
        {
            SchemaString = DeltaSchemaSerializer.Serialize(newSchema),
            Configuration = newConfig,
        };
        var actions = new List<DeltaAction>();
        if (protocolUpgrade is not null)
            actions.Add(protocolUpgrade);
        actions.Add(metadata);
        return new DeferredSchemaChange(actions, metadata, protocolUpgrade, newSchema);
    }

    /// <summary>The compute-only counterpart of <see cref="DropFieldAsync"/> (nested DROP) — for a buffered
    /// transaction. Requires column mapping; the containing struct must not become empty.</summary>
    public DeferredSchemaChange ComputeDropField(
        IReadOnlyList<string> fieldPath, MetadataAction? baseMetadata = null)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);
        if (fieldPath.Count < 2)
            throw new ArgumentException("fieldPath must name a NESTED field (use ComputeDropColumn for top-level columns).");

        var snapshot = CurrentSnapshot;
        var baseMeta = baseMetadata ?? snapshot.Metadata;
        var baseSchema = baseMetadata is null
            ? snapshot.Schema
            : DeltaSchemaSerializer.Parse(baseMetadata.SchemaString);
        if (ColumnMapping.GetMode(baseMeta.Configuration) == ColumnMappingMode.None)
        {
            throw new InvalidOperationException(
                "DROP of a nested field requires column mapping (enable it at table creation) — a plain table "
                + "would need to rewrite every data file since the logical name is the physical parquet name.");
        }

        string name = fieldPath[fieldPath.Count - 1];
        var containerPath = fieldPath.Take(fieldPath.Count - 1).ToList();
        var newSchema = TransformStructAt(baseSchema, containerPath, 0, fields =>
        {
            var result = new List<StructField>(fields.Count);
            bool found = false;
            foreach (var f in fields)
            {
                if (string.Equals(f.Name, name, StringComparison.Ordinal)) { found = true; continue; }
                result.Add(f);
            }
            if (!found)
                throw new InvalidOperationException($"Field '{PathText(fieldPath)}' does not exist.");
            if (result.Count == 0)
                throw new InvalidOperationException(
                    $"Cannot drop the only field of struct '{PathText(containerPath)}'.");
            return result;
        });

        var metadata = baseMeta with { SchemaString = DeltaSchemaSerializer.Serialize(newSchema) };
        return new DeferredSchemaChange(new List<DeltaAction> { metadata }, metadata, null, newSchema);
    }

    /// <summary>Reconciles a logically-named batch to <paramref name="expectedFields"/> — the public form of
    /// the read path's RECURSIVE schema-evolution reconcile (<see cref="BackfillMissingColumns"/>): expected
    /// columns/struct members the batch lacks backfill as typed NULLs, extra ones drop, struct children
    /// recurse. A buffered transaction uses it to overlay its PENDING (uncommitted-ALTER) schema onto
    /// committed reads.</summary>
    public static RecordBatch ReconcileBatchToFields(RecordBatch batch, IReadOnlyList<Field> expectedFields)
        => BackfillMissingColumns(batch, expectedFields);

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
        var mappingMode = ColumnMapping.GetMode(config);

        var newDeltaSchema = SchemaConverter.FromArrowSchema(newSchema);
        var newConfig = config;
        if (mappingMode != ColumnMappingMode.None)
        {
            // A column-mapping table's SchemaString always differs (the current one carries field ids/physical
            // names the incoming Arrow schema lacks), so compare the LOGICAL shape (names + types, ids stripped)
            // to no-op when nothing actually changed — e.g. a fresh CTAS that just created the table with the
            // right schema+mapping. Only a genuine schema change re-assigns.
            if (string.Equals(LogicalSchemaString(snapshot.Schema),
                              LogicalSchemaString(newDeltaSchema), StringComparison.Ordinal))
            {
                return snapshot.Version;
            }
            // Full-replace adopts an arbitrary new schema, so assign FRESH field ids + physical names (continuing
            // from the current maxColumnId so ids are never reused across history) and bump maxColumnId. Sound for
            // a REPLACE because the old data files are removed by the paired Overwrite.
            int startId = ColumnMapping.GetMaxColumnId(snapshot.Schema);
            if (config is not null && config.TryGetValue(ColumnMapping.MaxColumnIdKey, out var maxStr)
                && int.TryParse(maxStr, out var cfgMax))
            {
                startId = System.Math.Max(startId, cfgMax);
            }
            var (mapped, newMax) = ColumnMapping.AssignColumnMapping(newDeltaSchema, startId);
            newDeltaSchema = mapped;
            var cfg = config is null
                ? new Dictionary<string, string>()
                : config.ToDictionary(kv => kv.Key, kv => kv.Value);
            cfg[ColumnMapping.MaxColumnIdKey] = newMax.ToString();
            newConfig = cfg;
        }

        string newSchemaString = DeltaSchemaSerializer.Serialize(newDeltaSchema);
        if (string.Equals(newSchemaString, snapshot.Metadata.SchemaString, StringComparison.Ordinal))
        {
            return snapshot.Version; // identical schema — nothing to commit
        }

        var protocolUpgrade = UpgradeProtocolForFeatures(snapshot.Protocol, RequiredSchemaFeatures(newDeltaSchema));

        var actionList = new List<DeltaAction>();
        if (protocolUpgrade is not null)
            actionList.Add(protocolUpgrade);
        actionList.Add(snapshot.Metadata with { SchemaString = newSchemaString, Configuration = newConfig });
        IReadOnlyList<DeltaAction> actions = actionList;
        actions = Log.InCommitTimestamp.EnsureCommitInfo(
            actions, snapshot.Metadata.Configuration, "CHANGE COLUMNS");

        long newVersion = snapshot.Version + 1;
        await _log.WriteCommitAsync(newVersion, actions, cancellationToken).ConfigureAwait(false);

        _currentSnapshot = await SnapshotBuilder.UpdateAsync(
            snapshot, _log, cancellationToken).ConfigureAwait(false);

        return newVersion;
    }

    // The schema's LOGICAL signature — field names + types + nullability, with column-mapping metadata (ids /
    // physical names) stripped RECURSIVELY (nested struct fields carry their own ids/physicalNames) — so two
    // schemas that differ only in assigned ids compare equal. Used to no-op SetSchema on a column-mapping table
    // when the logical shape is unchanged (without the recursive strip, a fresh nested CTAS falsely "differed"
    // and re-assigned every column id).
    private static string LogicalSchemaString(EngineeredWood.DeltaLake.Schema.StructType schema)
    {
        return DeltaSchemaSerializer.Serialize(StripMetadata(schema));
    }

    private static EngineeredWood.DeltaLake.Schema.StructType StripMetadata(
        EngineeredWood.DeltaLake.Schema.StructType schema)
    {
        var stripped = new List<StructField>(schema.Fields.Count);
        foreach (var f in schema.Fields)
            stripped.Add(new StructField
            {
                Name = f.Name, Type = StripMetadata(f.Type), Nullable = f.Nullable, Metadata = null,
            });
        return new EngineeredWood.DeltaLake.Schema.StructType { Fields = stripped };
    }

    private static DeltaDataType StripMetadata(DeltaDataType type) => type switch
    {
        EngineeredWood.DeltaLake.Schema.StructType st => StripMetadata(st),
        ArrayType at => new ArrayType { ElementType = StripMetadata(at.ElementType), ContainsNull = at.ContainsNull },
        EngineeredWood.DeltaLake.Schema.MapType mt => new EngineeredWood.DeltaLake.Schema.MapType
        {
            KeyType = StripMetadata(mt.KeyType), ValueType = StripMetadata(mt.ValueType),
            ValueContainsNull = mt.ValueContainsNull,
        },
        _ => type,
    };

    /// <summary>
    /// Renames a column as a metadata-only commit (a new <c>metaData</c> action changing only the field's logical
    /// name; NO data files are rewritten). ONLY supported on a <b>column-mapping</b> table: the field keeps its
    /// <c>delta.columnMapping.id</c> + <c>physicalName</c>, so the existing data files (stored under the physical
    /// name in name mode, or matched by field id in id mode) are read unchanged under the new logical name. A
    /// non-mapping table would have to rewrite every file (the logical name IS the physical parquet name), so it
    /// is rejected. Throws if <paramref name="oldName"/> is absent or <paramref name="newName"/> already exists.
    /// Returns the new version.
    /// </summary>
    public async ValueTask<long> RenameColumnAsync(
        string oldName, string newName, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);

        var snapshot = CurrentSnapshot;
        if (ColumnMapping.GetMode(snapshot.Metadata.Configuration) == ColumnMappingMode.None)
        {
            throw new InvalidOperationException(
                "RENAME COLUMN requires column mapping (enable it at table creation) — a plain table would need "
                + "to rewrite every data file since the logical name is the physical parquet column name.");
        }

        var schema = snapshot.Schema;
        StructField? target = null;
        foreach (var f in schema.Fields)
        {
            if (string.Equals(f.Name, newName, StringComparison.Ordinal))
                throw new InvalidOperationException($"Column '{newName}' already exists.");
            if (string.Equals(f.Name, oldName, StringComparison.Ordinal))
                target = f;
        }
        if (target is null)
            throw new InvalidOperationException($"Column '{oldName}' does not exist.");

        var newFields = new List<StructField>(schema.Fields.Count);
        foreach (var f in schema.Fields)
        {
            newFields.Add(ReferenceEquals(f, target)
                ? new StructField { Name = newName, Type = f.Type, Nullable = f.Nullable, Metadata = f.Metadata }
                : f);
        }
        string newSchemaString = DeltaSchemaSerializer.Serialize(
            new EngineeredWood.DeltaLake.Schema.StructType { Fields = newFields });

        // metaData.partitionColumns holds LOGICAL names (Spark convention) — renaming a partition column must
        // update it too, else the reader/writer treat the renamed column as a data column (and the committed
        // partitionValues, keyed by the stable PHYSICAL name, no longer resolve).
        var newPartitionColumns = snapshot.Metadata.PartitionColumns;
        if (newPartitionColumns.Contains(oldName))
        {
            newPartitionColumns = newPartitionColumns
                .Select(pc => string.Equals(pc, oldName, StringComparison.Ordinal) ? newName : pc)
                .ToList();
        }

        IReadOnlyList<DeltaAction> actions = new List<DeltaAction>
        {
            snapshot.Metadata with { SchemaString = newSchemaString, PartitionColumns = newPartitionColumns },
        };
        actions = Log.InCommitTimestamp.EnsureCommitInfo(
            actions, snapshot.Metadata.Configuration, "RENAME COLUMN");

        long newVersion = snapshot.Version + 1;
        await _log.WriteCommitAsync(newVersion, actions, cancellationToken).ConfigureAwait(false);

        _currentSnapshot = await SnapshotBuilder.UpdateAsync(
            snapshot, _log, cancellationToken).ConfigureAwait(false);

        return newVersion;
    }

    /// <summary>
    /// Drops a column as a metadata-only commit (a new <c>metaData</c> action removing the field from the schema;
    /// NO data files are rewritten — old files still carry the physical column, which readers reconcile away
    /// against the current schema). ONLY supported on a <b>column-mapping</b> table: without mapping, dropping a
    /// column would require rewriting every data file (the logical name IS the physical parquet column), and the
    /// name could not be safely reused. The dropped field's column id is retired (maxColumnId is NOT decremented),
    /// so a later ADD COLUMN never reuses it. Throws if the column is absent or is a partition column. Returns
    /// the new version.
    /// </summary>
    public async ValueTask<long> DropColumnAsync(string name, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);

        var snapshot = CurrentSnapshot;
        if (ColumnMapping.GetMode(snapshot.Metadata.Configuration) == ColumnMappingMode.None)
        {
            throw new InvalidOperationException(
                "DROP COLUMN requires column mapping (enable it at table creation) — a plain table would need "
                + "to rewrite every data file since the logical name is the physical parquet column name.");
        }
        foreach (var pc in snapshot.Metadata.PartitionColumns)
        {
            if (string.Equals(pc, name, StringComparison.Ordinal))
                throw new InvalidOperationException($"Cannot drop partition column '{name}'.");
        }

        var schema = snapshot.Schema;
        var newFields = new List<StructField>(schema.Fields.Count);
        bool found = false;
        foreach (var f in schema.Fields)
        {
            if (string.Equals(f.Name, name, StringComparison.Ordinal)) { found = true; continue; }
            newFields.Add(f);
        }
        if (!found)
            throw new InvalidOperationException($"Column '{name}' does not exist.");
        if (newFields.Count == 0)
            throw new InvalidOperationException("Cannot drop the table's only column.");

        string newSchemaString = DeltaSchemaSerializer.Serialize(
            new EngineeredWood.DeltaLake.Schema.StructType { Fields = newFields });

        IReadOnlyList<DeltaAction> actions = new List<DeltaAction>
        {
            snapshot.Metadata with { SchemaString = newSchemaString },
        };
        actions = Log.InCommitTimestamp.EnsureCommitInfo(
            actions, snapshot.Metadata.Configuration, "DROP COLUMNS");

        long newVersion = snapshot.Version + 1;
        await _log.WriteCommitAsync(newVersion, actions, cancellationToken).ConfigureAwait(false);

        _currentSnapshot = await SnapshotBuilder.UpdateAsync(
            snapshot, _log, cancellationToken).ConfigureAwait(false);

        return newVersion;
    }

    /// <summary>
    /// Adds a nullable field INSIDE a nested struct column as a metadata-only commit — the nested analog of
    /// <see cref="AddColumnAsync"/>. <paramref name="containerPath"/> names the CONTAINING struct (top-level
    /// column first, e.g. <c>["s","inner"]</c> adds a member to <c>s.inner</c>). Old files lack the member —
    /// the read path reconciles it to a typed NULL child (recursive
    /// <see cref="BackfillMissingColumns"/>). Under column mapping the new field gets a fresh column id +
    /// physical name (struct-typed additions are rejected there — their descendants would need ids too).
    /// </summary>
    public async ValueTask<long> AddFieldAsync(
        IReadOnlyList<string> containerPath, Field newField, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);
        if (containerPath.Count == 0)
            throw new ArgumentException("containerPath must name the containing struct column.", nameof(containerPath));
        if (!newField.IsNullable)
            throw new InvalidOperationException(
                $"ADD COLUMN '{PathText(containerPath)}.{newField.Name}' must be nullable — existing rows have no value for a new field.");

        var snapshot = CurrentSnapshot;
        var config = snapshot.Metadata.Configuration;
        var mappingMode = ColumnMapping.GetMode(config);

        var newDeltaField = SchemaConverter.FromArrowSchema(
            new Apache.Arrow.Schema(new[] { newField }, null)).Fields[0];

        var newConfig = config;
        if (mappingMode != ColumnMappingMode.None)
        {
            // Recursive id + physical-name assignment (AssignColumnMapping) — a struct/array/map-typed
            // field gets ids on every descendant, exactly like at create.
            var (mappedField, lastId) = AssignMappedField(snapshot, config, newDeltaField);
            newDeltaField = mappedField;
            var cfg = config is null
                ? new Dictionary<string, string>()
                : config.ToDictionary(kv => kv.Key, kv => kv.Value);
            cfg[ColumnMapping.MaxColumnIdKey] = lastId.ToString();
            newConfig = cfg;
        }

        var addedField = newDeltaField;
        var newSchema = TransformStructAt(snapshot.Schema, containerPath, 0, fields =>
        {
            foreach (var f in fields)
            {
                if (string.Equals(f.Name, addedField.Name, StringComparison.Ordinal))
                    throw new InvalidOperationException(
                        $"Field '{PathText(containerPath)}.{addedField.Name}' already exists.");
            }
            var result = new List<StructField>(fields) { addedField };
            return result;
        });
        string newSchemaString = DeltaSchemaSerializer.Serialize(newSchema);

        // A new field whose type requires a schema-driven table feature (timestampNtz / variantType) may
        // need a protocol upgrade in the same commit.
        var protocolUpgrade =
            UpgradeProtocolForFeatures(snapshot.Protocol, RequiredSchemaFeatures(newDeltaField.Type));

        var actionList = new List<DeltaAction>();
        if (protocolUpgrade is not null)
            actionList.Add(protocolUpgrade);
        actionList.Add(snapshot.Metadata with { SchemaString = newSchemaString, Configuration = newConfig });
        IReadOnlyList<DeltaAction> actions = actionList;
        actions = Log.InCommitTimestamp.EnsureCommitInfo(
            actions, snapshot.Metadata.Configuration, "ADD COLUMNS");

        long newVersion = snapshot.Version + 1;
        await _log.WriteCommitAsync(newVersion, actions, cancellationToken).ConfigureAwait(false);
        _currentSnapshot = await SnapshotBuilder.UpdateAsync(
            snapshot, _log, cancellationToken).ConfigureAwait(false);
        return newVersion;
    }

    /// <summary>
    /// Renames a field INSIDE a nested struct column as a metadata-only commit — the nested analog of
    /// <see cref="RenameColumnAsync"/>. <paramref name="fieldPath"/> is the FULL path of the field (length
    /// ≥ 2). Requires column mapping (the field keeps its column id + physical name, so old files keep
    /// resolving). Returns the new version.
    /// </summary>
    public async ValueTask<long> RenameFieldAsync(
        IReadOnlyList<string> fieldPath, string newName, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);
        if (fieldPath.Count < 2)
            throw new ArgumentException("fieldPath must name a NESTED field (use RenameColumnAsync for top-level columns).");

        var snapshot = CurrentSnapshot;
        if (ColumnMapping.GetMode(snapshot.Metadata.Configuration) == ColumnMappingMode.None)
        {
            throw new InvalidOperationException(
                "RENAME of a nested field requires column mapping (enable it at table creation) — a plain table "
                + "would need to rewrite every data file since the logical name is the physical parquet name.");
        }

        string oldName = fieldPath[fieldPath.Count - 1];
        var containerPath = fieldPath.Take(fieldPath.Count - 1).ToList();
        var newSchema = TransformStructAt(snapshot.Schema, containerPath, 0, fields =>
        {
            StructField? target = null;
            foreach (var f in fields)
            {
                if (string.Equals(f.Name, newName, StringComparison.Ordinal))
                    throw new InvalidOperationException($"Field '{PathText(containerPath)}.{newName}' already exists.");
                if (string.Equals(f.Name, oldName, StringComparison.Ordinal))
                    target = f;
            }
            if (target is null)
                throw new InvalidOperationException($"Field '{PathText(fieldPath)}' does not exist.");
            var result = new List<StructField>(fields.Count);
            foreach (var f in fields)
            {
                result.Add(ReferenceEquals(f, target)
                    ? new StructField { Name = newName, Type = f.Type, Nullable = f.Nullable, Metadata = f.Metadata }
                    : f);
            }
            return result;
        });
        string newSchemaString = DeltaSchemaSerializer.Serialize(newSchema);

        IReadOnlyList<DeltaAction> actions = new List<DeltaAction>
        {
            snapshot.Metadata with { SchemaString = newSchemaString },
        };
        actions = Log.InCommitTimestamp.EnsureCommitInfo(
            actions, snapshot.Metadata.Configuration, "RENAME COLUMN");

        long newVersion = snapshot.Version + 1;
        await _log.WriteCommitAsync(newVersion, actions, cancellationToken).ConfigureAwait(false);
        _currentSnapshot = await SnapshotBuilder.UpdateAsync(
            snapshot, _log, cancellationToken).ConfigureAwait(false);
        return newVersion;
    }

    /// <summary>
    /// Drops a field INSIDE a nested struct column as a metadata-only commit — the nested analog of
    /// <see cref="DropColumnAsync"/>. <paramref name="fieldPath"/> is the FULL path (length ≥ 2). Requires
    /// column mapping; the containing struct must not become empty; the retired column id is never reused
    /// (maxColumnId is not decremented). Old files still carry the physical column — readers reconcile it
    /// away. Returns the new version.
    /// </summary>
    public async ValueTask<long> DropFieldAsync(
        IReadOnlyList<string> fieldPath, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);
        if (fieldPath.Count < 2)
            throw new ArgumentException("fieldPath must name a NESTED field (use DropColumnAsync for top-level columns).");

        var snapshot = CurrentSnapshot;
        if (ColumnMapping.GetMode(snapshot.Metadata.Configuration) == ColumnMappingMode.None)
        {
            throw new InvalidOperationException(
                "DROP of a nested field requires column mapping (enable it at table creation) — a plain table "
                + "would need to rewrite every data file since the logical name is the physical parquet name.");
        }

        string name = fieldPath[fieldPath.Count - 1];
        var containerPath = fieldPath.Take(fieldPath.Count - 1).ToList();
        var newSchema = TransformStructAt(snapshot.Schema, containerPath, 0, fields =>
        {
            var result = new List<StructField>(fields.Count);
            bool found = false;
            foreach (var f in fields)
            {
                if (string.Equals(f.Name, name, StringComparison.Ordinal)) { found = true; continue; }
                result.Add(f);
            }
            if (!found)
                throw new InvalidOperationException($"Field '{PathText(fieldPath)}' does not exist.");
            if (result.Count == 0)
                throw new InvalidOperationException(
                    $"Cannot drop the only field of struct '{PathText(containerPath)}'.");
            return result;
        });
        string newSchemaString = DeltaSchemaSerializer.Serialize(newSchema);

        IReadOnlyList<DeltaAction> actions = new List<DeltaAction>
        {
            snapshot.Metadata with { SchemaString = newSchemaString },
        };
        actions = Log.InCommitTimestamp.EnsureCommitInfo(
            actions, snapshot.Metadata.Configuration, "DROP COLUMNS");

        long newVersion = snapshot.Version + 1;
        await _log.WriteCommitAsync(newVersion, actions, cancellationToken).ConfigureAwait(false);
        _currentSnapshot = await SnapshotBuilder.UpdateAsync(
            snapshot, _log, cancellationToken).ConfigureAwait(false);
        return newVersion;
    }

    // Rebuilds the schema with the struct at `containerPath` transformed via `transform` on its field list
    // (every non-terminal segment must be a struct field). Fields outside the path are untouched.
    private static EngineeredWood.DeltaLake.Schema.StructType TransformStructAt(
        EngineeredWood.DeltaLake.Schema.StructType current, IReadOnlyList<string> containerPath, int depth,
        Func<IReadOnlyList<StructField>, List<StructField>> transform)
    {
        if (depth == containerPath.Count)
            return new EngineeredWood.DeltaLake.Schema.StructType { Fields = transform(current.Fields) };

        string segment = containerPath[depth];
        var newFields = new List<StructField>(current.Fields.Count);
        bool found = false;
        foreach (var f in current.Fields)
        {
            if (!found && string.Equals(f.Name, segment, StringComparison.Ordinal))
            {
                found = true;
                if (f.Type is not EngineeredWood.DeltaLake.Schema.StructType st)
                    throw new InvalidOperationException(
                        $"'{PathText(containerPath.Take(depth + 1).ToList())}' is not a STRUCT column.");
                var newSt = TransformStructAt(st, containerPath, depth + 1, transform);
                newFields.Add(new StructField
                {
                    Name = f.Name, Type = newSt, Nullable = f.Nullable, Metadata = f.Metadata,
                });
            }
            else
            {
                newFields.Add(f);
            }
        }
        if (!found)
            throw new InvalidOperationException(
                $"Column '{PathText(containerPath.Take(depth + 1).ToList())}' does not exist.");
        return new EngineeredWood.DeltaLake.Schema.StructType { Fields = newFields };
    }

    // Assigns column-mapping metadata (id + physical name) to a NEW field being added to a mapped table —
    // RECURSIVELY, via the create-time assigner, so struct/array/map descendants all get their own ids.
    // Ids continue past the table's current maxColumnId (schema-derived OR the config key, whichever is
    // higher). Returns the mapped field + the last assigned id (the new maxColumnId).
    private static (StructField Field, int LastId) AssignMappedField(
        Snapshot.Snapshot snapshot, IReadOnlyDictionary<string, string>? config, StructField field)
        => AssignMappedFieldFor(snapshot.Schema, config, field);

    // Base-schema form: a buffered transaction chains adds against its PENDING schema/config (whose
    // maxColumnId the previous pending add already bumped), not the committed snapshot's.
    private static (StructField Field, int LastId) AssignMappedFieldFor(
        EngineeredWood.DeltaLake.Schema.StructType baseSchema,
        IReadOnlyDictionary<string, string>? config, StructField field)
    {
        int maxId = ColumnMapping.GetMaxColumnId(baseSchema);
        if (config is not null && config.TryGetValue(ColumnMapping.MaxColumnIdKey, out var maxStr)
            && int.TryParse(maxStr, out var cfgMax))
        {
            maxId = System.Math.Max(maxId, cfgMax);
        }
        var (assigned, lastId) = ColumnMapping.AssignColumnMapping(
            new EngineeredWood.DeltaLake.Schema.StructType { Fields = new[] { field } }, maxId);
        return (assigned.Fields[0], lastId);
    }

    private static string PathText(IReadOnlyList<string> path) => string.Join(".", path);

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

            await using var file = await _fs.OpenReadAsync(DeltaPath.Decode(addFile.Path), cancellationToken)
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
                if (ColumnMappingRecursive.HasNestedFields(snapshot.Schema))
                    logicalBatch = ColumnMappingRecursive.ToLogical(logicalBatch, snapshot.Schema, mappingMode);
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
                    actions.AddRange(await ChangeDataFeed.CdfWriter.WriteSplitAsync(
                        _fs, snapshot, deletedBatch, DeltaLake.ChangeDataFeed.CdfConfig.Delete,
                        _options.ParquetWriteOptions, cancellationToken).ConfigureAwait(false));
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
        HonorWriterFeatures(isAppend: false);
        ThrowIfVariantRewrite("copy-on-write DELETE");

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
        // Fully-native rewrite is used only for the clean shape a plain read_parquet can reproduce: no column
        // mapping, no partition columns, no type widening, and no CDF (CDF needs the deleted rows in-process).
        // Schema-evolution NULL backfill is handled inside the rewriter (it probes the file's columns).
        bool nativeRewrite = _options.DataFileRewriter is not null
            && mappingMode == ColumnMappingMode.None
            && snapshot.Metadata.PartitionColumns.Count == 0
            && !cdfEnabled
            && !(Schema.TypeWidening.IsEnabled(snapshot.Metadata.Configuration) || HasTypeChanges(snapshot.Schema));
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
            long deletedHere;
            if (nativeRewrite)
            {
                // DuckDB reads the source parquet and drops the deleted positions (WHERE file_row_number NOT IN …);
                // engineered-wood keeps stats/write/commit. deletedHere = the positions targeted in this file (all
                // present — the rowids came from a scan of this same snapshot).
                deletedHere = positions.Count;
                await foreach (var batch in _options.DataFileRewriter!
                                   .ReadRewriteAsync(ordinal, DeltaPath.Decode(addFile.Path), positions, cancellationToken)
                                   .ConfigureAwait(false))
                    keptBatches.Add(batch);
            }
            else
            {
                long pos = 0;
                deletedHere = 0;
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

                // Build the physical batches for the rewritten file (clean schema + column mapping) once — the
                // built-in writer and the pluggable host writer consume the same list.
                var physicalBatches = new List<RecordBatch>(keptBatches.Count);
                foreach (var b in keptBatches)
                {
                    // Rebuild with a CLEAN schema (drop the parquet reader's field metadata, e.g. an existing
                    // PARQUET:field_id) so the re-write matches the initial write path — otherwise the writer
                    // emits a malformed footer (TProtocolException) on reader-sourced batches.
                    var cleanFields = new List<Field>(b.Schema.FieldsList.Count);
                    foreach (var f in b.Schema.FieldsList)
                        cleanFields.Add(CleanField(f));
                    var cleanArrays = new List<IArrowArray>(b.ColumnCount);
                    for (int c = 0; c < b.ColumnCount; c++)
                        cleanArrays.Add(b.Column(c));
                    var clean = new RecordBatch(new Apache.Arrow.Schema(cleanFields, null), cleanArrays, b.Length);

                    var physicalBatch = ColumnMappingRecursive.ToPhysical(clean, snapshot.Schema, mappingMode);
                    physicalBatches.Add(physicalBatch);
                }

                if (_options.DataFileWriter is { } dataFileWriter)
                {
                    // Host writer (e.g. DuckDB's native COPY) produces the rewritten file; the _delta_log
                    // remove(old)+add(new) below is unchanged → standard-readable copy-on-write output.
                    fileSize = await dataFileWriter.WriteAsync(physicalBatches, newFileName, cancellationToken)
                        .ConfigureAwait(false);
                }
                else
                {
                    await using (var file = await _fs.CreateAsync(newFileName, cancellationToken: cancellationToken)
                                     .ConfigureAwait(false))
                    {
                        await using var writer = new Parquet.ParquetFileWriter(
                            file, ownsFile: false, _options.ParquetWriteOptions);
                        foreach (var physicalBatch in physicalBatches)
                            await writer.WriteRowGroupAsync(physicalBatch, cancellationToken).ConfigureAwait(false);
                        await writer.DisposeAsync().ConfigureAwait(false);
                        fileSize = file.Position;
                    }
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
                    actions.AddRange(await ChangeDataFeed.CdfWriter.WriteSplitAsync(
                        _fs, snapshot, deletedBatch, DeltaLake.ChangeDataFeed.CdfConfig.Delete,
                        _options.ParquetWriteOptions, cancellationToken).ConfigureAwait(false));
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
        HonorWriterFeatures(isAppend: false);

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
            actions.Add(addFile with
            {
                DeletionVector = newDv,
                DataChange = true,
                Stats = StatsWithLooseBounds(addFile.Stats),
            });

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
                    actions.AddRange(await ChangeDataFeed.CdfWriter.WriteSplitAsync(
                        _fs, snapshot, deletedBatch, DeltaLake.ChangeDataFeed.CdfConfig.Delete,
                        _options.ParquetWriteOptions, cancellationToken).ConfigureAwait(false));
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
        HonorWriterFeatures(isAppend: false);
        ThrowIfVariantRewrite("UPDATE");

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
        // Fully-native rewrite (DuckDB reads the source + applies the SET substitution via a LEFT JOIN) — only for
        // the clean shape (no column mapping, no partitions, no type widening, no CDF). The file's existing
        // deletion-vector positions are excluded by the rewriter (passed as excludePositions); schema-evolution
        // backfill is handled inside the rewriter.
        bool nativeRewrite = _options.DataFileRewriter is not null
            && mappingMode == ColumnMappingMode.None
            && snapshot.Metadata.PartitionColumns.Count == 0
            && !cdfEnabled
            && !(Schema.TypeWidening.IsEnabled(snapshot.Metadata.Configuration) || HasTypeChanges(snapshot.Schema));

        // MERGE-ON-READ UPDATE (deletion vectors): on a DV-enabled table, DV-delete the matched OLD rows (no
        // file rewrite) and APPEND their post-image rows as small new files — instead of rewriting the whole
        // file (copy-on-write). Big write-amplification win for a small update on a large file, and it changes
        // fewer row-tracking ids than copy-on-write (which re-ids every row in the rewritten file). COLUMN
        // MAPPING (name + id) is handled (the read side arrives logical-named; the append converts back via
        // ColumnMappingRecursive.ToPhysical — physical names + field ids, physical-keyed stats); PARTITIONS
        // are handled (the post-image append routes through WriteDataFilesAsync's partition split — Hive dirs,
        // per-file partitionValues; a SET of the partition column simply lands the row in its new partition);
        // CDF is handled (update_preimage/update_postimage change files emitted per affected file, exactly
        // like the copy-on-write path — a commit carrying cdc actions is read cdc-only, so the DV re-add and
        // the post-image add never double-count in the feed). Still gated: CDF × PARTITIONED (the cdc
        // partitionValues/column semantics — same corner as the buffered path), type widening, IcebergCompat.
        // Otherwise fall through to CoW.
        bool dvEnabled = snapshot.Metadata.Configuration is { } dvCfg
            && dvCfg.TryGetValue("delta.enableDeletionVectors", out var dvFlag)
            && string.Equals(dvFlag, "true", StringComparison.OrdinalIgnoreCase);
        if (dvEnabled && !IsIcebergCompat
            && !(Schema.TypeWidening.IsEnabled(snapshot.Metadata.Configuration) || HasTypeChanges(snapshot.Schema)))
        {
            return await UpdateViaVectorsAsync(snapshot, rowIds, rewriteFile, cancellationToken)
                .ConfigureAwait(false);
        }

        var rowIdSet = cdfEnabled ? (rowIds as HashSet<long> ?? new HashSet<long>(rowIds)) : null;
        var actions = new List<DeltaAction>();

        foreach (int ordinal in affectedOrdinals)
        {
            if (ordinal < 0 || ordinal >= ordered.Count)
                continue;
            var addFile = ordered[ordinal];

            IReadOnlyList<RecordBatch> rewritten;
            var fileBatches = new List<RecordBatch>(); // pre-image source rows (CDF only; empty on the native path)
            if (nativeRewrite)
            {
                // DuckDB reads the source and applies the SET substitution; engineered-wood keeps stats/write/
                // commit. The file's existing DV positions are excluded so only live rows are rewritten (matching
                // the reader path); the rewriter matches the update rows by transient rowid within this ordinal.
                var dvPositions = addFile.DeletionVector is not null
                    ? await _dvReader.ReadAsync(addFile.DeletionVector, cancellationToken).ConfigureAwait(false)
                    : (IReadOnlyCollection<long>)System.Array.Empty<long>();
                var nativeBatches = new List<RecordBatch>();
                await foreach (var batch in _options.DataFileRewriter!
                                   .ReadRewriteAsync(ordinal, DeltaPath.Decode(addFile.Path), dvPositions, cancellationToken)
                                   .ConfigureAwait(false))
                    nativeBatches.Add(batch);
                rewritten = nativeBatches;
            }
            else
            {
                // Read WITH the trailing _metadata.row_id column (absolute positions) so the caller matches rows by
                // rowid — correct even when the file already has a deletion vector (post-DV survivors keep their
                // absolute rowids). The caller returns USER-column batches (rowid stripped).
                await foreach (var batch in ReadFileAsync(addFile, null, snapshot, cancellationToken,
                                                          fileOrdinal: ordinal).ConfigureAwait(false))
                {
                    fileBatches.Add(batch);
                }

                // The caller rebuilds the file's rows with the SET columns modified on matched positions.
                rewritten = rewriteFile(ordinal, fileBatches);
            }

            long keptCount = 0;
            foreach (var b in rewritten)
                keptCount += b.Length;

            string newFileName = $"{Guid.NewGuid():N}.parquet";
            long fileSize;

            // Build the physical batches (clean schema + column mapping) once for either writer.
            var physicalBatches = new List<RecordBatch>(rewritten.Count);
            foreach (var b in rewritten)
            {
                // Clean schema (drop reader field metadata) before re-writing — same as the delete path,
                // else the parquet footer is malformed for delta-kernel/Spark/Fabric.
                var cleanFields = new List<Field>(b.Schema.FieldsList.Count);
                foreach (var f in b.Schema.FieldsList)
                    cleanFields.Add(CleanField(f));
                var cleanArrays = new List<IArrowArray>(b.ColumnCount);
                for (int c = 0; c < b.ColumnCount; c++)
                    cleanArrays.Add(b.Column(c));
                var clean = new RecordBatch(new Apache.Arrow.Schema(cleanFields, null), cleanArrays, b.Length);

                // Recursive: physical names + field ids at EVERY level (a substituted nested struct arrives
                // logical-named; pass-through columns read from data files are already physical — the
                // tolerant matching stamps their ids without renaming).
                var physicalBatch = ColumnMappingRecursive.ToPhysical(clean, snapshot.Schema, mappingMode);
                physicalBatches.Add(physicalBatch);
            }

            if (_options.DataFileWriter is { } dataFileWriter)
            {
                // Host writer (e.g. DuckDB's native COPY) produces the rewritten file; remove(old)+add(new) below
                // is unchanged → standard-readable copy-on-write UPDATE output.
                fileSize = await dataFileWriter.WriteAsync(physicalBatches, newFileName, cancellationToken)
                    .ConfigureAwait(false);
            }
            else
            {
                await using (var file = await _fs.CreateAsync(newFileName, cancellationToken: cancellationToken)
                                 .ConfigureAwait(false))
                {
                    await using var writer = new Parquet.ParquetFileWriter(
                        file, ownsFile: false, _options.ParquetWriteOptions);
                    foreach (var physicalBatch in physicalBatches)
                        await writer.WriteRowGroupAsync(physicalBatch, cancellationToken).ConfigureAwait(false);
                    await writer.DisposeAsync().ConfigureAwait(false);
                    fileSize = file.Position;
                }
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

                    actions.AddRange(await ChangeDataFeed.CdfWriter.WriteSplitAsync(
                        _fs, snapshot, preBatch, DeltaLake.ChangeDataFeed.CdfConfig.UpdatePreimage,
                        _options.ParquetWriteOptions, cancellationToken).ConfigureAwait(false));
                    actions.AddRange(await ChangeDataFeed.CdfWriter.WriteSplitAsync(
                        _fs, snapshot, postBatch, DeltaLake.ChangeDataFeed.CdfConfig.UpdatePostimage,
                        _options.ParquetWriteOptions, cancellationToken).ConfigureAwait(false));
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
    /// MERGE-ON-READ UPDATE by transient rowid (the deletion-vector companion to the copy-on-write
    /// <see cref="UpdateByRowIdsAsync"/>): the matched OLD rows are marked deleted in each source file's deletion
    /// vector (NO file rewrite), and their post-image rows (produced by <paramref name="rewriteFile"/>, matched by
    /// rowid) are APPENDED as one new file. Committed atomically as, per affected file, <c>remove</c>(old path+DV)
    /// + <c>add</c>(same path, new DV) + one <c>add</c>(the appended post-image file). Only used for the clean
    /// shape (no column mapping / partitions / type widening / CDF — the caller gates this). Non-updated rows stay
    /// in their original file (their ids/versions untouched); the appended rows get fresh row-tracking ids (stable-
    /// id preservation across UPDATE needs materialized row-id columns — a separate slice).
    /// </summary>
    private async ValueTask<long> UpdateViaVectorsAsync(
        Snapshot.Snapshot snapshot,
        IReadOnlyCollection<long> rowIds,
        Func<long, IReadOnlyList<RecordBatch>, IReadOnlyList<RecordBatch>> rewriteFile,
        CancellationToken cancellationToken)
    {
        long posMask = (1L << RowIdPositionBits) - 1;
        var affectedOrdinals = new HashSet<int>();
        foreach (var rid in rowIds)
            affectedOrdinals.Add((int)(rid >> RowIdPositionBits));

        var ordered = OrderedActiveFiles(snapshot);
        var rowIdSet = rowIds as HashSet<long> ?? new HashSet<long>(rowIds);
        var dvWriter = new DeletionVectors.DeletionVectorWriter(_fs);
        bool rowTrackingEnabled = DeltaLake.RowTracking.RowTrackingConfig.IsEnabled(snapshot.Metadata.Configuration);
        bool cdfEnabled = DeltaLake.ChangeDataFeed.CdfConfig.IsEnabled(snapshot.Metadata.Configuration);
        var mappingMode = ColumnMapping.GetMode(snapshot.Metadata.Configuration);
        var actions = new List<DeltaAction>();
        var appendBatches = new List<RecordBatch>(); // matched post-image rows, appended as one new file
        // When the table declares materialized row tracking, each appended row must carry its ORIGINAL stable id
        // (source baseRowId + position) so an UPDATE preserves the row id (Spark reads the materialized column).
        string? matColumn = snapshot.Metadata.Configuration is { } matCfg
            && matCfg.TryGetValue("delta.rowTracking.materializedRowIdColumnName", out var mc) ? mc : null;
        var appendRowIds = matColumn is not null ? new List<long>() : null;
        long totalUpdated = 0;

        foreach (int ordinal in affectedOrdinals)
        {
            if (ordinal < 0 || ordinal >= ordered.Count)
                continue;
            var addFile = ordered[ordinal];

            // Read the file WITH the trailing rowid (absolute positions), substitute the SET columns over the
            // WHOLE file (rewriteFile), then take only the MATCHED rows for the append + their positions for the DV.
            // When ids are materialized, also capture the SOURCE file's stripped __delta_row_id per batch
            // (row-aligned; null when the file has none): a file produced WITH materialized ids (a compacted
            // file, an earlier update's post-image) carries the row's ORIGINAL id there — baseRowId + position
            // arithmetic would resolve the file-local id instead and silently change the row's identity.
            var fileBatches = new List<RecordBatch>();
            var sourceMatIds = appendRowIds is not null ? new List<Int64Array?>() : null;
            await foreach (var batch in ReadFileAsync(addFile, null, snapshot, cancellationToken,
                                                      fileOrdinal: ordinal, strippedRowIdsOut: sourceMatIds)
                               .ConfigureAwait(false))
                fileBatches.Add(batch);
            var rewritten = rewriteFile(ordinal, fileBatches);
            if (rewritten.Count != fileBatches.Count)
                continue; // defensive: the caller preserves batch/row structure

            var matchedPositions = new HashSet<long>();
            for (int bi = 0; bi < fileBatches.Count; bi++)
            {
                var orig = fileBatches[bi];
                var rew = rewritten[bi];
                if (orig.ColumnCount == 0 || rew.Length != orig.Length)
                    continue;
                var rids = orig.Column(orig.ColumnCount - 1) as Int64Array; // trailing rowid
                if (rids is null)
                    continue;
                var matched = new List<int>();
                for (int i = 0; i < orig.Length; i++)
                {
                    if (rids.IsNull(i))
                        continue;
                    long rid = rids.GetValue(i)!.Value;
                    if (rowIdSet.Contains(rid))
                    {
                        matched.Add(i);
                        matchedPositions.Add(rid & posMask);
                    }
                }
                if (matched.Count > 0)
                {
                    var postRows = TakeRowsFromBatch(rew, matched); // rew is USER columns (rowid stripped)
                    appendBatches.Add(postRows);
                    if (appendRowIds is not null)
                    {
                        // ORIGINAL stable id: the source file's materialized value when present (row-aligned
                        // stripped column), else baseRowId + position.
                        long baseId = addFile.BaseRowId ?? 0;
                        var matIds = sourceMatIds is not null && bi < sourceMatIds.Count ? sourceMatIds[bi] : null;
                        foreach (int i in matched)
                        {
                            long? mat = matIds is not null && !matIds.IsNull(i) ? matIds.GetValue(i) : null;
                            appendRowIds.Add(mat ?? (baseId + (rids.GetValue(i)!.Value & posMask)));
                        }
                    }
                    if (cdfEnabled)
                    {
                        // Change Data Feed: update_preimage (the matched OLD rows, values from the source
                        // batch) + update_postimage (the substituted rows) — same shapes as the copy-on-write
                        // path. The commit then carries cdc actions, so the feed reads cdc-ONLY for it (the
                        // DV re-add + the post-image add never double-count).
                        var preBatch = TakeRowsFromBatch(DropVirtualRowId(orig), matched);
                        actions.AddRange(await ChangeDataFeed.CdfWriter.WriteSplitAsync(
                            _fs, snapshot, preBatch, DeltaLake.ChangeDataFeed.CdfConfig.UpdatePreimage,
                            _options.ParquetWriteOptions, cancellationToken).ConfigureAwait(false));
                        actions.AddRange(await ChangeDataFeed.CdfWriter.WriteSplitAsync(
                            _fs, snapshot, postRows, DeltaLake.ChangeDataFeed.CdfConfig.UpdatePostimage,
                            _options.ParquetWriteOptions, cancellationToken).ConfigureAwait(false));
                    }
                }
            }

            if (matchedPositions.Count == 0)
                continue;

            // DV-delete the matched OLD rows: union into the file's existing DV, write a fresh DV, and
            // remove(old path+DV) + add(same path, new DV). No data-file rewrite.
            var allDeleted = addFile.DeletionVector is not null
                ? new HashSet<long>(await _dvReader.ReadAsync(addFile.DeletionVector, cancellationToken)
                    .ConfigureAwait(false))
                : new HashSet<long>();
            foreach (long p in matchedPositions)
                if (allDeleted.Add(p))
                    totalUpdated++;
            var newDv = await dvWriter.CreateAsync(allDeleted, allDeleted.Count, cancellationToken)
                .ConfigureAwait(false);
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
                Stats = StatsWithLooseBounds(addFile.Stats),
            });
        }

        if (actions.Count == 0)
            return snapshot.Version;

        // Append the matched post-image rows (baseRowId assigned fresh from the high-water mark when row
        // tracking is on; DuckDB's native writer produces the bytes when a DataFileWriter is set).
        long appendRows = 0;
        foreach (var b in appendBatches)
            appendRows += b.Length;
        if (appendRows > 0 && snapshot.Metadata.PartitionColumns.Count > 0)
        {
            // PARTITIONED: route the post-images through WriteDataFilesAsync — partition split (Hive dirs +
            // per-file partitionValues, physical-keyed under mapping), recursive physical rename, per-file
            // stats, the IDataFileWriter seam, and the materialized ORIGINAL ids riding the split. The
            // post-image batches carry the partition columns (the read re-adds them from partitionValues),
            // so a SET of the partition column lands the row in its new partition. Row tracking is assigned
            // on the adds here, exactly as CommitDataFilesAsync does.
            // identityValuesPreGenerated: the post-image rows carry their EXISTING identity values —
            // regeneration would reassign them; the flag routes them through as-is.
            var written = await WriteDataFilesAsync(appendBatches, cancellationToken,
                identityValuesPreGenerated: true,
                materializedRowIds: appendRowIds).ConfigureAwait(false);
            long nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            long nextRowId = rowTrackingEnabled ? snapshot.RowIdHighWaterMark : 0;
            foreach (var f in written)
            {
                actions.Add(new AddFile
                {
                    Path = DeltaPath.Encode(f.RelativePath),
                    PartitionValues = f.PartitionValues ?? new Dictionary<string, string>(),
                    Size = f.SizeBytes,
                    ModificationTime = nowMs,
                    DataChange = true,
                    Stats = f.StatsJson ?? $"{{\"numRecords\":{f.NumRecords}}}",
                    BaseRowId = rowTrackingEnabled ? nextRowId : null,
                    DefaultRowCommitVersion = rowTrackingEnabled ? snapshot.Version + 1 : null,
                });
                if (rowTrackingEnabled)
                    nextRowId += f.NumRecords;
            }
            if (rowTrackingEnabled)
            {
                actions.Add(DeltaLake.RowTracking.RowTrackingConfig.BuildHighWaterMarkAction(nextRowId));
            }
        }
        else if (appendRows > 0)
        {
            var physicalBatches = new List<RecordBatch>(appendBatches.Count);
            // Stats are collected over the PHYSICAL-named batches (spec: stats keys are physical under column
            // mapping), BEFORE the internal __delta_row_id column is appended (it must not appear in stats).
            var statsBatches = new List<RecordBatch>(appendBatches.Count);
            long fileBaseRowId = rowTrackingEnabled ? snapshot.RowIdHighWaterMark : 0;
            int matOffset = 0; // running index into appendRowIds (aligned with the flat appended rows)
            foreach (var b in appendBatches)
            {
                var cleanFields = new List<Field>(b.Schema.FieldsList.Count);
                foreach (var f in b.Schema.FieldsList)
                    cleanFields.Add(CleanField(f));
                var cleanArrays = new List<IArrowArray>(b.ColumnCount);
                for (int c = 0; c < b.ColumnCount; c++)
                    cleanArrays.Add(b.Column(c));
                var clean = new RecordBatch(new Apache.Arrow.Schema(cleanFields, null), cleanArrays, b.Length);
                // Recursive: physical names + field ids at EVERY level (a substituted nested struct arrives
                // logical-named; pass-through columns read from data files are already physical — the
                // tolerant matching stamps their ids without renaming).
                var physicalBatch = ColumnMappingRecursive.ToPhysical(clean, snapshot.Schema, mappingMode);
                statsBatches.Add(physicalBatch);
                if (appendRowIds is not null)
                {
                    // Materialize the ORIGINAL stable ids (preserve row id across UPDATE) — the declared
                    // materialized column overrides baseRowId + position for a spec reader.
                    var idb = new Int64Array.Builder();
                    for (int r = 0; r < b.Length; r++)
                        idb.Append(appendRowIds[matOffset + r]);
                    matOffset += b.Length;
                    physicalBatch = RowTracking.RowTrackingWriter.AddRowIdColumn(physicalBatch, idb.Build());
                }
                else if (rowTrackingEnabled)
                {
                    physicalBatch = RowTracking.RowTrackingWriter.AddRowIdColumn(physicalBatch, fileBaseRowId);
                    fileBaseRowId += physicalBatch.Length;
                }
                physicalBatches.Add(physicalBatch);
            }

            string newFileName = $"{Guid.NewGuid():N}.parquet";
            long fileSize;
            if (_options.DataFileWriter is { } dataFileWriter)
            {
                fileSize = await dataFileWriter.WriteAsync(physicalBatches, newFileName, cancellationToken)
                    .ConfigureAwait(false);
            }
            else
            {
                await using var file = await _fs.CreateAsync(newFileName, cancellationToken: cancellationToken)
                    .ConfigureAwait(false);
                await using var writer = new Parquet.ParquetFileWriter(file, ownsFile: false, _options.ParquetWriteOptions);
                foreach (var pb in physicalBatches)
                    await writer.WriteRowGroupAsync(pb, cancellationToken).ConfigureAwait(false);
                await writer.DisposeAsync().ConfigureAwait(false);
                fileSize = file.Position;
            }

            actions.Add(new AddFile
            {
                Path = newFileName,
                PartitionValues = new Dictionary<string, string>(),
                Size = fileSize,
                ModificationTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                DataChange = true,
                Stats = Stats.StatsCollector.Collect(statsBatches),
                BaseRowId = rowTrackingEnabled ? snapshot.RowIdHighWaterMark : null,
                DefaultRowCommitVersion = rowTrackingEnabled ? snapshot.Version + 1 : null,
            });
            if (rowTrackingEnabled)
            {
                actions.Add(DeltaLake.RowTracking.RowTrackingConfig.BuildHighWaterMarkAction(
                    snapshot.RowIdHighWaterMark + appendRows));
            }
        }

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
                    var physicalBatch = ColumnMappingRecursive.ToPhysical(batch, snapshot.Schema, mappingMode);

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
                    actions.AddRange(await ChangeDataFeed.CdfWriter.WriteSplitAsync(
                        _fs, snapshot, pre, DeltaLake.ChangeDataFeed.CdfConfig.UpdatePreimage,
                        _options.ParquetWriteOptions, cancellationToken).ConfigureAwait(false));
                }
                foreach (var post in postimages)
                {
                    actions.AddRange(await ChangeDataFeed.CdfWriter.WriteSplitAsync(
                        _fs, snapshot, post, DeltaLake.ChangeDataFeed.CdfConfig.UpdatePostimage,
                        _options.ParquetWriteOptions, cancellationToken).ConfigureAwait(false));
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

    // The canonical identity of ONE partition (for dynamic partition overwrite set membership): the
    // sorted "key=value" pairs joined with U+0001, with every key translated to its PHYSICAL name when the
    // table has column mapping — so a physical-keyed entry (the spec convention) and a logical-keyed one
    // (older engineered-wood commits) canonicalize identically. A null value (Delta's "row is null in this
    // partition column") is marked distinctly from an empty string.
    private static string CanonicalPartitionKey(
        IReadOnlyDictionary<string, string> values,
        IReadOnlyDictionary<string, string>? logicalToPhysical)
    {
        var parts = new List<string>(values.Count);
        foreach (var kv in values)
        {
            string key = logicalToPhysical is not null && logicalToPhysical.TryGetValue(kv.Key, out var phys)
                ? phys : kv.Key;
            parts.Add(key + "=" + (kv.Value is null ? "\u0000<null>" : kv.Value));
        }
        parts.Sort(StringComparer.Ordinal);
        return string.Join("\u0001", parts);
    }

    // True when `fileValues` matches every entry in `filter` (partition-overwrite file selection). A file matches
    // only if it carries each filter key with the exact same value (ordinal string compare — partition values are
    // stored as strings). Keys are validated to be partition columns before this is called. `filter` keys are the
    // user-facing LOGICAL names; under column mapping a file's partitionValues are keyed by the PHYSICAL name
    // (the Delta-spec convention — physical keys survive a partition-column rename), while files written before
    // that convention are logical-keyed — so each filter key is tried under BOTH names.
    private static bool PartitionValuesMatch(
        IReadOnlyDictionary<string, string> fileValues, IReadOnlyDictionary<string, string> filter,
        IReadOnlyDictionary<string, string>? logicalToPhysical = null)
    {
        foreach (var kv in filter)
        {
            if (!fileValues.TryGetValue(kv.Key, out var v)
                && (logicalToPhysical is null || !logicalToPhysical.TryGetValue(kv.Key, out var phys)
                    || !fileValues.TryGetValue(phys, out v)))
            {
                return false;
            }
            if (!string.Equals(v, kv.Value, StringComparison.Ordinal))
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
        // The CURRENT snapshot names/types the feed's user columns: the column-mapping physical→logical rename,
        // the partition-column re-add for rows inferred from data files (which exclude them), and the per-file
        // deletion-vector exclusion all derive from it inside CdfReader.
        return ChangeDataFeed.CdfReader.ReadChangesAsync(
            _fs, _log, startVersion, endVersion,
            _options.ParquetReadOptions, CurrentSnapshot, cancellationToken);
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
    /// <summary>
    /// The active files' <c>baseRowId</c>s in TRANSIENT-ROWID ORDINAL order (the path-sorted active set
    /// — the same ordering the rowid encoding uses), for the CURRENT snapshot. A buffered transaction's
    /// eager UPDATE resolves each matched row's ORIGINAL stable id as
    /// <c>baseRowId[ordinal] + position</c> (the same rule the merge-on-read update applies).
    /// </summary>
    public async ValueTask<IReadOnlyList<long?>> OrderedActiveBaseRowIdsAsync(
        long? atVersion = null, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        // atVersion: pin the ordinal ordering to the snapshot the rowids came from (see
        // ReadRowsByRowIdsAsync) — a concurrent append must not shift the base-id resolution.
        var snapshot = atVersion is { } v && v != CurrentSnapshot.Version
            ? await GetSnapshotAtVersionAsync(v, cancellationToken).ConfigureAwait(false)
            : CurrentSnapshot;
        var ordered = OrderedActiveFiles(snapshot);
        var ids = new List<long?>(ordered.Count);
        foreach (var f in ordered)
        {
            ids.Add(f.BaseRowId);
        }
        return ids;
    }

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
    /// <para><paramref name="repartitionTo"/> (Overwrite only): change the table's partition columns as part
    /// of the SAME atomic commit — the Delta-protocol-legal way to repartition (a new <c>metaData</c> with
    /// the new <c>partitionColumns</c> is only valid when every active file is removed in the same commit,
    /// which a full Overwrite does; Spark exposes this as <c>overwriteSchema=true</c> + a new
    /// <c>partitionBy</c>). The new data is Hive-split by the NEW columns. Ignored when equal to the current
    /// partitioning; empty list = departition.</para>
    /// </summary>
    public ValueTask<long> WriteAsync(
        IReadOnlyList<RecordBatch> batches,
        DeltaWriteMode mode = DeltaWriteMode.Append,
        CancellationToken cancellationToken = default,
        IReadOnlyList<string>? repartitionTo = null)
        => WriteCoreAsync(batches, mode, null, cancellationToken, repartitionTo: repartitionTo);

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

    /// <summary>
    /// DYNAMIC partition overwrite (Spark <c>partitionOverwriteMode=dynamic</c>): atomically replaces exactly the
    /// partitions PRESENT IN <paramref name="batches"/> in a SINGLE commit — their currently-active files are
    /// removed and the new files added; partitions the input does not touch are unaffected. Unlike
    /// <see cref="OverwritePartitionsAsync"/> the target set is derived from the data, not supplied. Requires a
    /// partitioned table (throws otherwise — an unpartitioned "dynamic overwrite" would be a full replace in
    /// disguise; use Overwrite explicitly for that).
    /// </summary>
    public ValueTask<long> DynamicOverwriteAsync(
        IReadOnlyList<RecordBatch> batches,
        CancellationToken cancellationToken = default)
        => WriteCoreAsync(batches, DeltaWriteMode.Append, null, cancellationToken,
                          dynamicPartitionOverwrite: true);

    /// <summary>
    /// Honors the writer-enforcement features a writer-v7 table LISTS but must enforce only when ACTIVE:
    ///   • <c>appendOnly</c> — when <c>delta.appendOnly=true</c>, only appends are permitted (a non-append write
    ///     throws). Most tables merely LIST the feature (the v7-upgrade enumerates legacy features) without setting
    ///     the property, so this is a no-op there.
    ///   • <c>invariants</c> / <c>checkConstraints</c> — arbitrary column/table CHECK expressions this writer
    ///     cannot evaluate; if a table actually DECLARES one, the write is REJECTED (rather than silently writing
    ///     possibly-violating data — Delta constraints are enforced at write time only). NOT NULL is schema
    ///     nullability, separate and unaffected.
    /// </summary>
    private void HonorWriterFeatures(bool isAppend)
    {
        var cfg = CurrentSnapshot.Metadata.Configuration;
        if (cfg is not null)
        {
            if (!isAppend && cfg.TryGetValue("delta.appendOnly", out var ao)
                && string.Equals(ao, "true", StringComparison.OrdinalIgnoreCase))
            {
                throw new DeltaFormatException(
                    "Table is append-only (delta.appendOnly=true): overwrite/delete/update are not permitted.");
            }
            foreach (var key in cfg.Keys)
            {
                if (key.StartsWith("delta.constraints.", StringComparison.Ordinal))
                {
                    throw new DeltaFormatException(
                        $"Table declares CHECK constraint '{key}' which this writer cannot evaluate; write rejected.");
                }
            }
        }
        foreach (var field in CurrentSnapshot.Schema.Fields)
        {
            if (field.Metadata is not null && field.Metadata.ContainsKey("delta.invariants"))
            {
                throw new DeltaFormatException(
                    $"Column '{field.Name}' declares an invariant expression this writer cannot evaluate; write rejected.");
            }
            if (field.Metadata is not null && field.Metadata.ContainsKey("delta.generationExpression"))
            {
                throw new DeltaFormatException(
                    $"Column '{field.Name}' declares a generation expression this writer cannot evaluate; write rejected.");
            }
        }
    }

    /// <summary>
    /// A file carrying a deletion vector has stats computed over ALL physical rows, so min/max/nullCount may
    /// reference DV-deleted rows — the spec marks such stats <c>tightBounds=false</c> (readers then treat the
    /// bounds as loose supersets). numRecords stays the physical row count.
    /// </summary>
    private static string? StatsWithLooseBounds(string? stats)
    {
        if (string.IsNullOrEmpty(stats))
            return stats;
        try
        {
            using var doc = System.Text.Json.JsonDocument.Parse(stats!);
            using var stream = new MemoryStream();
            using (var writer = new System.Text.Json.Utf8JsonWriter(stream))
            {
                writer.WriteStartObject();
                writer.WriteBoolean("tightBounds", false);
                foreach (var prop in doc.RootElement.EnumerateObject())
                {
                    if (!prop.NameEquals("tightBounds"))
                        prop.WriteTo(writer);
                }
                writer.WriteEndObject();
            }
            return System.Text.Encoding.UTF8.GetString(stream.ToArray());
        }
        catch (System.Text.Json.JsonException)
        {
            return stats;
        }
    }

    /// <summary>True when the schema contains a <c>timestamp_ntz</c> column at any nesting depth.</summary>
    private static bool SchemaUsesTimestampNtz(DeltaDataType type) => type switch
    {
        PrimitiveType p => string.Equals(p.TypeName, "timestamp_ntz", StringComparison.Ordinal),
        EngineeredWood.DeltaLake.Schema.StructType st => st.Fields.Any(f => SchemaUsesTimestampNtz(f.Type)),
        ArrayType at => SchemaUsesTimestampNtz(at.ElementType),
        EngineeredWood.DeltaLake.Schema.MapType mt =>
            SchemaUsesTimestampNtz(mt.KeyType) || SchemaUsesTimestampNtz(mt.ValueType),
        _ => false,
    };

    /// <summary>
    /// Rejects a data-rewriting operation on a table with VARIANT columns. The rewrite READ half goes through
    /// the codec parquet reader, which is blind to the parquet VARIANT logical-type annotation — a rewritten
    /// file would carry a plain struct group, silently stripping variant-ness for spec readers (Spark,
    /// delta-kernel). Deletion-vector DELETE is exempt (it writes no data file). Lifting this requires a
    /// variant-aware read half (e.g. the host's native rewriter) on every affected path.
    /// </summary>
    private void ThrowIfVariantRewrite(string operation)
    {
        if (_options.DataFileReader is not null && _options.DataFileWriter is not null)
        {
            // Fully host-codec rewrite: the read AND write halves both go through the pluggable seams, which
            // preserve the parquet VARIANT annotation end to end (the host decodes/encodes the annotated
            // group; the transport marker survives the clean rebuild via CleanField) — no gate needed.
            return;
        }
        if (SchemaUsesVariant(CurrentSnapshot.Schema))
        {
            throw new DeltaFormatException(
                $"{operation} on a table with VARIANT columns is not supported yet (the rewrite would strip "
                + "the parquet VARIANT annotation). Deletion-vector DELETE works; for updates, delete + insert.");
        }
    }

    /// <summary>True when the schema contains a <c>variant</c> column at any nesting depth.</summary>
    private static bool SchemaUsesVariant(DeltaDataType type) => type switch
    {
        PrimitiveType p => string.Equals(p.TypeName, "variant", StringComparison.Ordinal),
        EngineeredWood.DeltaLake.Schema.StructType st => st.Fields.Any(f => SchemaUsesVariant(f.Type)),
        ArrayType at => SchemaUsesVariant(at.ElementType),
        EngineeredWood.DeltaLake.Schema.MapType mt =>
            SchemaUsesVariant(mt.KeyType) || SchemaUsesVariant(mt.ValueType),
        _ => false,
    };

    /// <summary>
    /// The schema-driven reader+writer table features <paramref name="type"/> requires per the Delta spec
    /// (<c>timestampNtz</c> for a naive timestamp, <c>variantType</c> for a variant column).
    /// </summary>
    private static List<string> RequiredSchemaFeatures(DeltaDataType type)
    {
        var features = new List<string>();
        if (SchemaUsesTimestampNtz(type))
            features.Add("timestampNtz");
        if (SchemaUsesVariant(type))
            features.Add("variantType");
        return features;
    }

    /// <summary>
    /// Builds the protocol action that adds the given reader+writer features, or null when the current
    /// protocol already declares them all (or none are required). Upgrading a LEGACY-versioned protocol to
    /// table-features mode (reader 3 / writer 7) must enumerate every feature the legacy version implied,
    /// else those capabilities are silently lost on the upgraded table.
    /// </summary>
    private static ProtocolAction? UpgradeProtocolForFeatures(
        ProtocolAction current, IReadOnlyList<string> features)
    {
        var missing = features.Where(f =>
            current.ReaderFeatures?.Contains(f) != true
            || current.WriterFeatures?.Contains(f) != true).ToList();
        if (missing.Count == 0)
            return null;

        var writerFeatures = new List<string>(
            current.WriterFeatures ?? LegacyWriterFeatures(current.MinWriterVersion));
        var readerFeatures = new List<string>(
            current.ReaderFeatures ?? LegacyReaderFeatures(current.MinReaderVersion));
        foreach (var feature in missing)
        {
            if (!writerFeatures.Contains(feature))
                writerFeatures.Add(feature);
            if (!readerFeatures.Contains(feature))
                readerFeatures.Add(feature);
        }

        return new ProtocolAction
        {
            MinReaderVersion = 3,
            MinWriterVersion = 7,
            ReaderFeatures = readerFeatures,
            WriterFeatures = writerFeatures,
        };
    }

    /// <summary>Writer features implied by a legacy writer version (Delta spec upgrade table).</summary>
    private static List<string> LegacyWriterFeatures(int minWriterVersion)
    {
        var features = new List<string>();
        if (minWriterVersion >= 2) { features.Add("appendOnly"); features.Add("invariants"); }
        if (minWriterVersion >= 3) { features.Add("checkConstraints"); }
        if (minWriterVersion >= 4) { features.Add("changeDataFeed"); features.Add("generatedColumns"); }
        if (minWriterVersion >= 5) { features.Add("columnMapping"); }
        if (minWriterVersion >= 6) { features.Add("identityColumns"); }
        return features;
    }

    /// <summary>Reader features implied by a legacy reader version (Delta spec upgrade table).</summary>
    private static List<string> LegacyReaderFeatures(int minReaderVersion)
    {
        var features = new List<string>();
        if (minReaderVersion >= 2) { features.Add("columnMapping"); }
        return features;
    }

    private async ValueTask<long> WriteCoreAsync(
        IReadOnlyList<RecordBatch> batches,
        DeltaWriteMode mode,
        IReadOnlyDictionary<string, string>? overwritePartitions,
        CancellationToken cancellationToken,
        bool dynamicPartitionOverwrite = false,
        IReadOnlyList<string>? repartitionTo = null)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);
        // A dynamic partition overwrite removes files, so it is NOT an append for appendOnly enforcement.
        HonorWriterFeatures(mode == DeltaWriteMode.Append && !dynamicPartitionOverwrite);

        var snapshot = CurrentSnapshot;

        // Repartition-on-overwrite: changing partitionColumns is protocol-legal ONLY when every active file
        // is removed in the same commit — i.e. a FULL overwrite (a partition-scoped or dynamic overwrite
        // keeps files that would no longer conform to the new partition schema).
        bool repartitioned = false;
        if (repartitionTo is not null)
        {
            if (mode != DeltaWriteMode.Overwrite || overwritePartitions is { Count: > 0 } || dynamicPartitionOverwrite)
            {
                throw new DeltaFormatException(
                    "Repartitioning requires a FULL overwrite (the new partition schema is only valid when "
                    + "every active file is replaced in the same commit).");
            }
            foreach (var col in repartitionTo)
            {
                if (!snapshot.Schema.Fields.Any(f => f.Name == col))
                {
                    throw new DeltaFormatException(
                        $"Repartition: '{col}' is not a column of the table.");
                }
            }
            repartitioned = !repartitionTo.SequenceEqual(snapshot.Metadata.PartitionColumns);
        }

        if (dynamicPartitionOverwrite && snapshot.Metadata.PartitionColumns.Count == 0)
        {
            throw new DeltaFormatException(
                "Dynamic partition overwrite requires a partitioned table (the table has no partition columns).");
        }

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

        // Column mapping: prepare logical-to-physical name mapping (also used to match/emit partitionValues,
        // which are keyed by the PHYSICAL column name under mapping — the Delta-spec convention).
        // A repartitioning overwrite splits by the NEW columns (the metaData swap is emitted below).
        var partitionColumns = repartitioned ? repartitionTo! : snapshot.Metadata.PartitionColumns;
        var mappingMode = ColumnMapping.GetMode(snapshot.Metadata.Configuration);
        var logicalToPhysical = ColumnMapping.BuildLogicalToPhysicalMap(
            snapshot.Schema, mappingMode);

        // Dynamic partition overwrite: collect the canonical partition keys the INPUT touches while writing;
        // the matching active files are removed after the write loop (one atomic commit).
        var touchedPartitions = dynamicPartitionOverwrite ? new HashSet<string>(StringComparer.Ordinal) : null;

        // For overwrite mode, remove existing files: ALL of them for a full overwrite, or only the files whose
        // partition values match `overwritePartitions` for an atomic partition-scoped overwrite (files outside
        // the target partitions are kept).
        if (mode == DeltaWriteMode.Overwrite)
        {
            long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            foreach (var existingFile in snapshot.ActiveFiles.Values)
            {
                if (overwritePartitions is { Count: > 0 } &&
                    !PartitionValuesMatch(existingFile.PartitionValues, overwritePartitions, logicalToPhysical))
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
                    // Must match the ACTIVE (path, DV) entry: without the DV a remove of a
                    // deletion-vector-carrying file never reconciles and the file stays active forever
                    // (duplicated rows after an Overwrite of a DV-deleted table). The
                    // CommitDataFilesAsync + dynamic-overwrite branches already carry it.
                    DeletionVector = existingFile.DeletionVector,
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

                // Rename logical columns to physical names + stamp field ids, at EVERY level (nested struct
                // children included — the top-level-only pair left them logical-named/id-less).
                var physicalBatch = ColumnMappingRecursive.ToPhysical(dataBatch, snapshot.Schema, mappingMode);

                // IcebergCompat: materialize partition columns into Parquet file
                if (Schema.IcebergCompat.RequiresPartitionMaterialization(icebergVersion) &&
                    partValues.Count > 0)
                {
                    physicalBatch = Partitioning.PartitionUtils.AppendPartitionColumns(
                        physicalBatch, partValues, snapshot.Schema, partitionColumns,
                        logicalToPhysical);
                }

                // (field ids already stamped recursively above; IcebergCompat-appended partition columns are
                // physical-named by AppendPartitionColumns and carry no mapping ids of their own)

                // Assign row IDs if row tracking is enabled
                long fileBaseRowId = nextRowId;
                if (rowTrackingEnabled)
                {
                    physicalBatch = RowTracking.RowTrackingWriter.AddRowIdColumn(
                        physicalBatch, fileBaseRowId);
                    nextRowId += dataBatch.Length;
                }

                // Under column mapping the tracked partitionValues are keyed by the PHYSICAL column name (Delta
                // spec: "track partition values with the physical name" — Spark does the same; physical keys
                // survive a partition-column RENAME, which never rewrites add actions). The Hive-style directory
                // follows the same (physical) keys; readers treat paths as opaque and take values from the log.
                var trackedPartValues = partValues;
                if (mappingMode != ColumnMappingMode.None && partValues.Count > 0)
                {
                    trackedPartValues = new Dictionary<string, string>(partValues.Count);
                    foreach (var kv in partValues)
                    {
                        trackedPartValues[logicalToPhysical.TryGetValue(kv.Key, out var p) ? p : kv.Key] = kv.Value;
                    }
                }

                touchedPartitions?.Add(CanonicalPartitionKey(trackedPartValues, logicalToPhysical));

                // Build file path: partition subdirectory + UUID filename
                string partDir = Partitioning.PartitionUtils.BuildPartitionPath(trackedPartValues);
                string fileName = string.IsNullOrEmpty(partDir)
                    ? $"{Guid.NewGuid():N}.parquet"
                    : $"{partDir}/{Guid.NewGuid():N}.parquet";

                long fileSize;

                if (_options.DataFileWriter is { } dataFileWriter)
                {
                    // Delegate the parquet bytes to the host writer (e.g. DuckDB's native COPY); it places the
                    // file at the location the table filesystem maps `fileName` to and returns its byte size.
                    fileSize = await dataFileWriter.WriteAsync(
                        new[] { physicalBatch }, fileName, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    // Variant columns arrive as the transport blob; the codec writer takes VariantArray
                    // columns, emitting the spec VARIANT-annotated group (see VariantTransport).
                    var codecBatch = VariantTransport.ToVariantArrays(physicalBatch);
                    await using (var file = await _fs.CreateAsync(
                        fileName, cancellationToken: cancellationToken).ConfigureAwait(false))
                    {
                        await using var writer = new ParquetFileWriter(
                            file, ownsFile: false, _options.ParquetWriteOptions);
                        await writer.WriteRowGroupAsync(codecBatch, cancellationToken)
                            .ConfigureAwait(false);

                        // DisposeAsync writes the Parquet footer before we read Position
                        await writer.DisposeAsync().ConfigureAwait(false);
                        fileSize = file.Position;
                    }
                }

                // Collect stats from the data batch. Under column mapping the Delta-spec convention keys the
                // per-file stats by the PHYSICAL column names (matching what the streaming writer emits and what
                // spec readers use for data skipping) — collect over the top-level-renamed batch (stats cover
                // top-level primitives only, so the flat rename suffices).
                // IcebergCompat requires numRecords in stats regardless of options
                string? stats = null;
                if (_options.CollectStats ||
                    Schema.IcebergCompat.RequiresNumRecords(icebergVersion))
                    // Stats keys are PHYSICAL at every level under mapping (nested struct leaves included).
                    stats = CollectStats(ColumnMappingRecursive.ToPhysical(dataBatch, snapshot.Schema, mappingMode));

                actions.Add(new AddFile
                {
                    // add.path is the URL-encoded form of the on-disk relative path (spec / Spark).
                    Path = DeltaPath.Encode(fileName),
                    PartitionValues = trackedPartValues,
                    Size = fileSize,
                    ModificationTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    DataChange = true,
                    Stats = stats,
                    BaseRowId = rowTrackingEnabled ? fileBaseRowId : null,
                    DefaultRowCommitVersion = rowTrackingEnabled ? newVersion : null,
                });
            }
        }

        // Dynamic partition overwrite: remove every currently-active file whose partition matches one the input
        // touched (canonical physical-keyed comparison, tolerating older logical-keyed commits). Files in
        // untouched partitions are kept. Same commit as the adds -> the swap is atomic per touched partition.
        if (touchedPartitions is { Count: > 0 })
        {
            long removeNow = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            foreach (var existingFile in snapshot.ActiveFiles.Values)
            {
                if (!touchedPartitions.Contains(CanonicalPartitionKey(existingFile.PartitionValues, logicalToPhysical)))
                    continue;
                actions.Add(new RemoveFile
                {
                    Path = existingFile.Path,
                    DeletionTimestamp = removeNow,
                    DataChange = true,
                    ExtendedFileMetadata = true,
                    PartitionValues = existingFile.PartitionValues,
                    Size = existingFile.Size,
                    DeletionVector = existingFile.DeletionVector, // match the active (path, DV) entry
                });
            }
        }

        // If identity columns were updated, emit metadata action with new HWMs. A commit must not carry two
        // conflicting metaData actions, so the identity metadata also carries a repartition's new
        // partitionColumns; a repartition WITHOUT identity updates emits its own metaData below.
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
            actions.Add(snapshot.Metadata with
            {
                SchemaString = updatedSchemaString,
                PartitionColumns = partitionColumns,
            });
        }
        else if (repartitioned)
        {
            // Repartition-on-overwrite: the new partitionColumns commit atomically with the full file swap —
            // every add in this commit already conforms to the new partition schema, every old file is
            // removed above, so no reader ever sees a nonconforming active file.
            actions.Add(snapshot.Metadata with { PartitionColumns = partitionColumns });
        }

        // Row tracking: persist the advanced high-water mark as the delta.rowTracking domainMetadata (the
        // spec-required source of truth; deriving it from active files alone under-counts after removes, so
        // a mixed writer could reassign already-used row ids).
        if (rowTrackingEnabled && nextRowId > snapshot.RowIdHighWaterMark)
        {
            actions.Add(DeltaLake.RowTracking.RowTrackingConfig.BuildHighWaterMarkAction(nextRowId));
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
    /// Commits data files that were written OUTSIDE engineered-wood (e.g. streamed straight to parquet by
    /// DuckDB's native COPY, bounded-memory) as one Delta commit — the commit-only half of <see cref="WriteCoreAsync"/>:
    /// it builds the <c>add</c> actions (+ <c>remove</c>s for Overwrite), assigns row-tracking
    /// <c>baseRowId</c>/<c>defaultRowCommitVersion</c> per file, prepends the commitInfo, writes the commit with
    /// optimistic-concurrency retry, and refreshes the snapshot + auto-checkpoint. The caller has ALREADY written
    /// the parquet files (relative to the table root) and supplies their size/rowcount/partitionValues/stats.
    /// <para>NOT supported (throws — the caller must fall back to <see cref="WriteAsync(IReadOnlyList{RecordBatch},
    /// DeltaWriteMode, CancellationToken)"/>): column mapping, identity columns, or IcebergCompat, because those
    /// need per-row processing at write time that an external writer did not perform.</para>
    /// </summary>
    /// <summary>
    /// True when <see cref="CommitDataFilesAsync"/> is usable for this table — i.e. the external writer can produce
    /// the data files without engineered-wood's per-row processing. <b>Column-mapping tables (both modes) are
    /// supported, with a caller contract</b>: the external writer must write the data files under the PHYSICAL
    /// column names and stamp each column's parquet <c>field_id</c> (the Delta-spec file layout for both modes —
    /// see <see cref="Schema.ColumnMapping.BuildLogicalToPhysicalMap"/> / <c>BuildLogicalToFieldIdMap</c>), and any
    /// per-file stats it supplies must be keyed by the physical names. Identity columns and IcebergCompat are NOT
    /// supported (they need write-time per-row processing). A caller checks this BEFORE writing files externally so
    /// it can fall back to the batch path without leaving an orphan. (Partitioning is a separate check — the caller
    /// inspects <c>CurrentSnapshot.Metadata.PartitionColumns</c>.)
    /// </summary>
    /// <summary>True when the table declares IcebergCompat (requires the committing write path).</summary>
    public bool IsIcebergCompat =>
        Schema.IcebergCompat.GetVersion(CurrentSnapshot.Metadata.Configuration)
        != Schema.IcebergCompatVersion.None;

    /// <summary>True when any column carries identity metadata.</summary>
    public bool HasIdentityColumns
    {
        get
        {
            foreach (var f in CurrentSnapshot.Schema.Fields)
            {
                if (IdentityColumn.GetConfig(f) is not null)
                    return true;
            }
            return false;
        }
    }

    /// <summary>
    /// Generates identity-column values for a buffered (multi-statement) transaction's eagerly-written
    /// appends: the configs seed from the CURRENT snapshot's schema, overridden by
    /// <paramref name="chainedHighWaterMarks"/> (the transaction's pending marks from earlier
    /// statements, so values chain across statements without a commit between them). Returns the
    /// processed batches + the new per-column high-water marks; the caller fuses them into its commit
    /// via <see cref="BuildIdentityMetadataAction"/>. Concurrency: a concurrent identity-consuming
    /// commit necessarily carries a metaData action (the HWM lives in schema metadata), so the caller's
    /// rebase metadata check aborts the transaction — Spark's own concurrent-identity policy; values
    /// baked here are never committed on top of a moved HWM.
    /// </summary>
    public (IReadOnlyList<RecordBatch> Batches, IReadOnlyDictionary<string, long> HighWaterMarks)
        GenerateIdentityValues(IReadOnlyList<RecordBatch> batches,
                               IReadOnlyDictionary<string, long>? chainedHighWaterMarks = null)
    {
        ThrowIfDisposed();
        return GenerateIdentityValuesForSchema(CurrentSnapshot.Schema, batches, chainedHighWaterMarks);
    }

    /// <summary>
    /// The schema-seeded form of <see cref="GenerateIdentityValues"/> — for a table that does NOT exist
    /// yet (a buffered transaction's pending CREATE: the identity configs come from the parked schema's
    /// <c>delta.identity.*</c> field metadata, values chain across the transaction's statements, and
    /// the flush bakes the final marks into commit-0's schema). No concurrency concern: nobody can
    /// consume ids from a table that has never been committed.
    /// </summary>
    public static (IReadOnlyList<RecordBatch> Batches, IReadOnlyDictionary<string, long> HighWaterMarks)
        GenerateIdentityValuesForSchema(Schema.StructType schema, IReadOnlyList<RecordBatch> batches,
                                        IReadOnlyDictionary<string, long>? chainedHighWaterMarks = null)
    {
        var configs = new Dictionary<string, IdentityColumnConfig>();
        foreach (var f in schema.Fields)
        {
            if (IdentityColumn.GetConfig(f) is { } cfg)
            {
                configs[f.Name] = chainedHighWaterMarks is not null
                                  && chainedHighWaterMarks.TryGetValue(f.Name, out var h)
                    ? cfg with { HighWaterMark = h }
                    : cfg;
            }
        }
        if (configs.Count == 0)
        {
            return (batches, new Dictionary<string, long>());
        }
        var outBatches = new List<RecordBatch>(batches.Count);
        foreach (var b in batches)
        {
            var (processed, _) = IdentityColumns.IdentityColumnWriter.ProcessBatch(b, schema, ref configs);
            outBatches.Add(processed);
        }
        var marks = new Dictionary<string, long>();
        foreach (var kv in configs)
        {
            if (kv.Value.HighWaterMark is { } hwm)
                marks[kv.Key] = hwm;
        }
        return (outBatches, marks);
    }

    /// <summary>
    /// Builds the metaData action carrying updated identity high-water marks, based on
    /// <paramref name="baseMetadata"/> (default: the current snapshot's — a buffered ALTER's pending
    /// metadata composes so one commit never carries two metaData actions).
    /// </summary>
    public MetadataAction BuildIdentityMetadataAction(
        IReadOnlyDictionary<string, long> highWaterMarks, MetadataAction? baseMetadata = null)
    {
        ThrowIfDisposed();
        var meta = baseMetadata ?? CurrentSnapshot.Metadata;
        var schema = baseMetadata is null
            ? CurrentSnapshot.Schema
            : DeltaSchemaSerializer.Parse(baseMetadata.SchemaString);
        var fields = new List<StructField>(schema.Fields.Count);
        foreach (var f in schema.Fields)
        {
            fields.Add(highWaterMarks.TryGetValue(f.Name, out var hwm)
                ? IdentityColumn.UpdateHighWaterMark(f, hwm)
                : f);
        }
        var updated = new Schema.StructType { Fields = fields };
        return meta with { SchemaString = DeltaSchemaSerializer.Serialize(updated) };
    }

    public bool SupportsExternalDataFileCommit
    {
        get
        {
            var cfg = CurrentSnapshot.Metadata.Configuration;
            if (Schema.IcebergCompat.GetVersion(cfg) != Schema.IcebergCompatVersion.None)
                return false;
            foreach (var f in CurrentSnapshot.Schema.Fields)
            {
                if (IdentityColumn.GetConfig(f) is not null)
                    return false;
            }
            return true;
        }
    }

    public async ValueTask<long> CommitDataFilesAsync(
        IReadOnlyList<WrittenDataFile> files,
        DeltaWriteMode mode,
        bool dynamicPartitionOverwrite = false,
        CancellationToken cancellationToken = default,
        IReadOnlyList<DeltaAction>? extraActions = null,
        long? expectedVersion = null,
        string operation = "WRITE",
        bool identityValuesPreGenerated = false,
        IReadOnlyDictionary<int, IReadOnlyCollection<long>>? deletedPositionsByFileIndex = null)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);
        // A dynamic partition overwrite removes files, so it is NOT an append for appendOnly enforcement.
        // extraActions (a buffered transaction's deletion-vector remove/add pairs) likewise make this a
        // non-append.
        HonorWriterFeatures(mode == DeltaWriteMode.Append && !dynamicPartitionOverwrite &&
                            extraActions is not { Count: > 0 });

        if (dynamicPartitionOverwrite)
        {
            if (mode != DeltaWriteMode.Append)
                throw new DeltaFormatException(
                    "Dynamic partition overwrite is append-shaped (a full Overwrite already removes everything).");
            if (CurrentSnapshot.Metadata.PartitionColumns.Count == 0)
                throw new DeltaFormatException(
                    "Dynamic partition overwrite requires a partitioned table (the table has no partition columns).");
        }

        // Reject configurations that require write-time per-row processing the external writer did not do
        // (the caller should have checked SupportsExternalDataFileCommit first). Only relevant when data
        // FILES are being committed — a deletion-vector-only or metadata-only fused flush (extraActions
        // with no files) involves no write-time processing.
        var cfg = CurrentSnapshot.Metadata.Configuration;
        if (files.Count > 0 && !SupportsExternalDataFileCommit
            && !(identityValuesPreGenerated && !IsIcebergCompat))
            throw new NotSupportedException(
                "CommitDataFilesAsync: table has identity columns or IcebergCompat — "
                + "these require engineered-wood's own writer.");

        bool rowTrackingEnabled = DeltaLake.RowTracking.RowTrackingConfig.IsEnabled(cfg);

        for (int attempt = 1; ; attempt++)
        {
            var snapshot = CurrentSnapshot;
            // Buffered-transaction commit: the caller's extraActions are snapshot-coupled (deletion-vector
            // ordinals/positions computed against expectedVersion), so a concurrent commit invalidates them —
            // conflict-ABORT instead of the append retry (first-committer-wins snapshot isolation).
            if (expectedVersion is { } expected && snapshot.Version != expected)
            {
                throw new DeltaConflictException(
                    snapshot.Version,
                    $"Transaction conflict: the table moved from version {expected} to {snapshot.Version} "
                    + "while the transaction was open — the buffered changes were rolled back; retry the "
                    + "transaction.");
            }
            var actions = new List<DeltaAction>();
            long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            // Overwrite: remove every currently-active file (full replace; STATIC partition-scoped overwrite is
            // not handled here — the caller keeps replace_where on the batch path). DYNAMIC partition overwrite:
            // remove only the active files whose partition matches one of the written files' partitions
            // (canonical physical-keyed comparison; the externally-written files' partitionValues are already
            // keyed physical under column mapping, per the SupportsExternalDataFileCommit caller contract).
            if (mode == DeltaWriteMode.Overwrite)
            {
                foreach (var existingFile in snapshot.ActiveFiles.Values)
                {
                    actions.Add(new RemoveFile
                    {
                        Path = existingFile.Path,
                        DeletionTimestamp = now,
                        DataChange = true,
                        ExtendedFileMetadata = true,
                        PartitionValues = existingFile.PartitionValues,
                        Size = existingFile.Size,
                        DeletionVector = existingFile.DeletionVector,
                    });
                }
            }
            else if (dynamicPartitionOverwrite)
            {
                var logicalToPhysical = ColumnMapping.BuildLogicalToPhysicalMap(
                    snapshot.Schema, ColumnMapping.GetMode(snapshot.Metadata.Configuration));
                var touched = new HashSet<string>(StringComparer.Ordinal);
                foreach (var f in files)
                {
                    if (f.PartitionValues is { Count: > 0 } pv)
                        touched.Add(CanonicalPartitionKey(pv, logicalToPhysical));
                }
                foreach (var existingFile in snapshot.ActiveFiles.Values)
                {
                    if (!touched.Contains(CanonicalPartitionKey(existingFile.PartitionValues, logicalToPhysical)))
                        continue;
                    actions.Add(new RemoveFile
                    {
                        Path = existingFile.Path,
                        DeletionTimestamp = now,
                        DataChange = true,
                        ExtendedFileMetadata = true,
                        PartitionValues = existingFile.PartitionValues,
                        Size = existingFile.Size,
                        DeletionVector = existingFile.DeletionVector,
                    });
                }
            }

            long nextRowId = rowTrackingEnabled ? snapshot.RowIdHighWaterMark : 0;
            long newVersion = snapshot.Version + 1;
            for (int fi = 0; fi < files.Count; fi++)
            {
                var f = files[fi];
                // deletedPositionsByFileIndex: rows of THIS not-yet-committed file that a buffered
                // transaction deleted after inserting them (same-transaction DML) — the add is born with
                // an inline deletion vector, so the rows never appear in any committed version. Stats
                // stay physical-row stats, marked tightBounds=false per the spec (loose supersets).
                DeletionVector? dv = null;
                string? stats = f.StatsJson ?? $"{{\"numRecords\":{f.NumRecords}}}";
                if (deletedPositionsByFileIndex is not null
                    && deletedPositionsByFileIndex.TryGetValue(fi, out var deletedPositions)
                    && deletedPositions.Count > 0)
                {
                    var dvWriter = new DeletionVectors.DeletionVectorWriter(_fs);
                    dv = await dvWriter.CreateAsync(deletedPositions, deletedPositions.Count,
                        cancellationToken).ConfigureAwait(false);
                    stats = StatsWithLooseBounds(stats);
                }
                long fileBaseRowId = nextRowId;
                actions.Add(new AddFile
                {
                    Path = DeltaPath.Encode(f.RelativePath),
                    PartitionValues = f.PartitionValues ?? new Dictionary<string, string>(),
                    Size = f.SizeBytes,
                    ModificationTime = now,
                    DataChange = true,
                    // numRecords is REQUIRED (row-tracking high-water mark is derived from baseRowId + numRecords);
                    // a caller that has full stats passes StatsJson, else we emit the minimal numRecords-only stats.
                    Stats = stats,
                    BaseRowId = rowTrackingEnabled ? fileBaseRowId : null,
                    DefaultRowCommitVersion = rowTrackingEnabled ? newVersion : null,
                    DeletionVector = dv,
                });
                if (rowTrackingEnabled)
                    nextRowId += f.NumRecords;
            }

            if (rowTrackingEnabled && nextRowId > snapshot.RowIdHighWaterMark)
            {
                actions.Add(DeltaLake.RowTracking.RowTrackingConfig.BuildHighWaterMarkAction(nextRowId));
            }

            // A buffered transaction's deletion-vector remove/add pairs join the SAME commit (atomic
            // DML + append flush).
            if (extraActions is { Count: > 0 })
                actions.AddRange(extraActions);

            var finalActions = Log.InCommitTimestamp.EnsureCommitInfo(actions, cfg, operation);
            try
            {
                await _log.WriteCommitAsync(newVersion, finalActions, cancellationToken).ConfigureAwait(false);
            }
            catch (DeltaConflictException) when (attempt < 16 && expectedVersion is null)
            {
                // A concurrent writer took our version — refresh the snapshot (recomputes the Overwrite removes +
                // the row-tracking high-water mark) and retry. The already-written data files are reused as-is.
                _currentSnapshot = await SnapshotBuilder.UpdateAsync(snapshot, _log, cancellationToken).ConfigureAwait(false);
                continue;
            }

            _currentSnapshot = await SnapshotBuilder.UpdateAsync(snapshot, _log, cancellationToken).ConfigureAwait(false);
            if (_options.CheckpointInterval > 0 && newVersion % _options.CheckpointInterval == 0)
                await _checkpointWriter.WriteCheckpointAsync(_currentSnapshot, cancellationToken).ConfigureAwait(false);
            return newVersion;
        }
    }

    /// <summary>
    /// Computes the deletion-vector actions for the given deleted positions WITHOUT committing — the
    /// deferred half of <see cref="DeleteByRowIdsViaVectorsAsync"/>, for a buffered (multi-statement)
    /// transaction that fuses its DML + appends into one commit via
    /// <see cref="CommitDataFilesAsync"/>' <c>extraActions</c>. Positions are keyed by the CURRENT
    /// snapshot's path-sorted file ordinal (the transient-rowid ordinal) and are ABSOLUTE in-file row
    /// positions; each touched file's existing DV is unioned with the new positions and the result is a
    /// <c>remove</c>(old path+DV) + <c>add</c>(same path, new DV) pair. Inline DVs → pure metadata, no
    /// storage write. Change Data Feed is NOT captured here (the caller must gate CDF tables to the
    /// committing path). Returns the actions + the count of NEWLY deleted rows.
    /// </summary>
    public async ValueTask<(IReadOnlyList<DeltaAction> Actions, long RowsDeleted)> ComputeDeletionVectorActionsAsync(
        IReadOnlyDictionary<int, IReadOnlyCollection<long>> positionsByOrdinal,
        CancellationToken cancellationToken = default,
        Snapshot.Snapshot? resolveAgainst = null)
    {
        ThrowIfDisposed();
        // `resolveAgainst` (rebase support): the ordinals + old DVs were captured against the transaction's
        // PINNED snapshot — resolve there, not against a possibly-advanced current snapshot (whose
        // path-sorted ordering may differ after concurrent appends). The caller must run
        // CheckLogicalRebase before committing the result on a newer snapshot.
        var snapshot = resolveAgainst ?? CurrentSnapshot;
        var ordered = OrderedActiveFiles(snapshot);
        var dvWriter = new DeletionVectors.DeletionVectorWriter(_fs);
        var actions = new List<DeltaAction>();
        long totalDeleted = 0;

        foreach (var kvp in positionsByOrdinal)
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
            foreach (long p in kvp.Value)
                if (allDeleted.Add(p))
                    newlyDeleted++;
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
            actions.Add(addFile with
            {
                DeletionVector = newDv,
                DataChange = true,
                Stats = StatsWithLooseBounds(addFile.Stats),
            });
        }

        return (actions, totalDeleted);
    }

    /// <summary>
    /// Spark-style LOGICAL REBASE conflict check: decides whether a transaction whose changes were computed
    /// against <paramref name="baseSnapshot"/> may commit on top of the CURRENT snapshot even though other
    /// writers committed in between. Real conflicts throw <see cref="DeltaConflictException"/>:
    /// <list type="bullet">
    /// <item>a concurrent METADATA change (schema / partitioning / configuration) — the transaction's
    /// reads, chained DDL computations and written files all assumed the base metadata;</item>
    /// <item>a concurrent PROTOCOL change;</item>
    /// <item>a concurrent remove / rewrite / DV-modification of a file this transaction deletes from or
    /// updates (delete/delete — the transaction's DV union was computed against the base file's DV), as
    /// detected by any planned <see cref="RemoveFile"/> whose (path, DV) is no longer active unchanged;</item>
    /// <item>with a READ SET supplied (<paramref name="readPredicates"/> — the pushed predicate of every
    /// scan the transaction ran, a superset of the rows consumed — or <paramref name="readWholeTable"/>):
    /// a concurrent data-changing REMOVE of a file the transaction read (Spark's
    /// concurrentDeleteReadCheck), and a concurrent data-changing ADD that could match the reads (Spark's
    /// concurrentAppendCheck) — the append check runs for every non-blind-append commit, and under
    /// <paramref name="serializable"/> for blind appends too (Spark's Serializable vs the default
    /// WriteSerializable, where blind appends may be logically reordered before the transaction).</item>
    /// </list>
    /// Predicate-vs-file matching uses <see cref="DeltaFilePruner"/> over the base snapshot's schema
    /// (partition values exactly, stats conservatively — can't rule it out ⇒ it matches). Commits are
    /// classified by walking the concurrent range (<c>base+1..latest</c>): blind append = no
    /// remove/metaData/protocol action. OPTIMIZE-style actions (<c>dataChange=false</c>) rearrange rows
    /// without changing them, so they are exempt from the READ checks; a compaction touching a file the
    /// transaction MODIFIES still conflicts via the delete/delete check. Row-id/identity high-water marks
    /// are re-derived at commit time from the snapshot committed onto, so appends need no compensation.
    /// </summary>
    public async ValueTask CheckLogicalRebaseAsync(
        Snapshot.Snapshot baseSnapshot,
        IReadOnlyList<DeltaAction> plannedActions,
        IReadOnlyList<Expressions.Predicate>? readPredicates = null,
        bool readWholeTable = false,
        bool serializable = false,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        var latest = CurrentSnapshot;
        if (latest.Version == baseSnapshot.Version)
        {
            return;
        }
        if (!MetadataEquals(baseSnapshot.Metadata, latest.Metadata))
        {
            throw new DeltaConflictException(latest.Version,
                "concurrent metadata change (schema/partitioning/configuration) — cannot rebase the transaction");
        }
        if (!ProtocolEquals(baseSnapshot.Protocol, latest.Protocol))
        {
            throw new DeltaConflictException(latest.Version,
                "concurrent protocol change — cannot rebase the transaction");
        }
        // delete/delete: every file the transaction removes (DV remove+add pairs, rewrites) must still be
        // active UNCHANGED — same path with the same deletion vector (DeletionVector is a record: value
        // equality; both null = plain file).
        Dictionary<string, AddFile>? latestByPath = null;
        foreach (var action in plannedActions)
        {
            if (action is not RemoveFile remove)
            {
                continue;
            }
            if (latestByPath is null)
            {
                latestByPath = new Dictionary<string, AddFile>(latest.ActiveFiles.Count, StringComparer.Ordinal);
                foreach (var f in latest.ActiveFiles.Values)
                {
                    latestByPath[f.Path] = f;
                }
            }
            if (!latestByPath.TryGetValue(remove.Path, out var current)
                || !Equals(current.DeletionVector, remove.DeletionVector))
            {
                throw new DeltaConflictException(latest.Version,
                    $"concurrent delete/rewrite of file '{remove.Path}' this transaction also modifies — "
                    + "cannot rebase the transaction");
            }
        }

        // Read-set checks (skipped when the caller recorded no reads — pure delete/delete mode).
        bool hasReads = readWholeTable || readPredicates is { Count: > 0 };
        if (!hasReads)
        {
            return;
        }
        var pruner = new DeltaFilePruner(baseSnapshot.Schema, baseSnapshot.Metadata.PartitionColumns);
        bool ReadsMatch(AddFile file)
        {
            if (readWholeTable)
            {
                return true;
            }
            foreach (var predicate in readPredicates!)
            {
                if (pruner.ShouldInclude(file, predicate))
                {
                    return true;
                }
            }
            return false;
        }
        var baseByPath = new Dictionary<string, AddFile>(baseSnapshot.ActiveFiles.Count, StringComparer.Ordinal);
        foreach (var f in baseSnapshot.ActiveFiles.Values)
        {
            baseByPath[f.Path] = f;
        }
        for (long v = baseSnapshot.Version + 1; v <= latest.Version; v++)
        {
            var actions = await _log.ReadCommitAsync(v, cancellationToken).ConfigureAwait(false);
            bool blindAppend = true;
            foreach (var a in actions)
            {
                if (a is RemoveFile or MetadataAction or ProtocolAction)
                {
                    blindAppend = false;
                    break;
                }
            }
            foreach (var a in actions)
            {
                switch (a)
                {
                    case RemoveFile removed when removed.DataChange:
                        // concurrentDeleteReadCheck: the file existed in our base snapshot and our reads
                        // could have consumed rows from it (its DV change / rewrite / delete invalidates
                        // what the transaction read). dataChange=false (compaction) is exempt.
                        if (baseByPath.TryGetValue(removed.Path, out var readFile) && ReadsMatch(readFile))
                        {
                            throw new DeltaConflictException(latest.Version,
                                $"concurrent delete/rewrite of file '{removed.Path}' this transaction read "
                                + $"(commit v{v}) — cannot rebase the transaction");
                        }
                        break;
                    case AddFile added when added.DataChange && (!blindAppend || serializable):
                        // concurrentAppendCheck: rows appeared that the transaction's reads would have
                        // consumed. Blind appends are exempt under WriteSerializable (they may be logically
                        // reordered before the transaction); under Serializable commit order is logical
                        // order, so they conflict too.
                        if (ReadsMatch(added))
                        {
                            throw new DeltaConflictException(latest.Version,
                                $"concurrent append of file '{added.Path}' matching this transaction's reads "
                                + $"(commit v{v}) — cannot rebase the transaction");
                        }
                        break;
                }
            }
        }
    }

    private static bool MetadataEquals(MetadataAction a, MetadataAction b)
    {
        if (!string.Equals(a.Id, b.Id, StringComparison.Ordinal)
            || !string.Equals(a.SchemaString, b.SchemaString, StringComparison.Ordinal)
            || !a.PartitionColumns.SequenceEqual(b.PartitionColumns, StringComparer.Ordinal))
        {
            return false;
        }
        var ca = a.Configuration;
        var cb = b.Configuration;
        if ((ca?.Count ?? 0) != (cb?.Count ?? 0))
        {
            return false;
        }
        if (ca is not null && cb is not null)
        {
            foreach (var kv in ca)
            {
                if (!cb.TryGetValue(kv.Key, out var v) || !string.Equals(kv.Value, v, StringComparison.Ordinal))
                {
                    return false;
                }
            }
        }
        return true;
    }

    private static bool ProtocolEquals(ProtocolAction a, ProtocolAction b)
    {
        if (a.MinReaderVersion != b.MinReaderVersion || a.MinWriterVersion != b.MinWriterVersion)
        {
            return false;
        }
        static bool FeaturesEqual(IReadOnlyList<string>? x, IReadOnlyList<string>? y)
        {
            var sx = new HashSet<string>(x ?? System.Array.Empty<string>(), StringComparer.Ordinal);
            var sy = new HashSet<string>(y ?? System.Array.Empty<string>(), StringComparer.Ordinal);
            return sx.SetEquals(sy);
        }
        return FeaturesEqual(a.ReaderFeatures, b.ReaderFeatures) && FeaturesEqual(a.WriterFeatures, b.WriterFeatures);
    }

    /// <summary>
    /// Writes <paramref name="batches"/> as data files WITHOUT committing — the write half of the batch
    /// path, for a buffered transaction that commits everything at once via
    /// <see cref="CommitDataFilesAsync"/>. Append-shaped: partition split, recursive column-mapping
    /// physical rename + field-id stamping, variant transport, the <see cref="IDataFileWriter"/> seam and
    /// per-file stats all apply; row-tracking <c>baseRowId</c> is NOT materialized into the files (the
    /// commit assigns it, like the streaming writer). Identity columns and IcebergCompat need write-time
    /// per-row processing tied to the commit — callers must check
    /// <see cref="SupportsExternalDataFileCommit"/> first. The written files are invisible orphans until
    /// committed (rollback = never reference them; vacuum cleans).
    /// </summary>
    public async ValueTask<IReadOnlyList<WrittenDataFile>> WriteDataFilesAsync(
        IReadOnlyList<RecordBatch> batches,
        CancellationToken cancellationToken = default,
        EngineeredWood.DeltaLake.Schema.StructType? schemaOverride = null,
        bool identityValuesPreGenerated = false,
        IReadOnlyList<long>? materializedRowIds = null)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);
        if (IsIcebergCompat)
            throw new NotSupportedException(
                "WriteDataFilesAsync: IcebergCompat tables require the committing write path.");
        if (HasIdentityColumns && !identityValuesPreGenerated)
            throw new NotSupportedException(
                "WriteDataFilesAsync: table has identity columns — generate their values first "
                + "(GenerateIdentityValues) and pass identityValuesPreGenerated, or use the "
                + "committing write path.");

        var snapshot = CurrentSnapshot;
        // schemaOverride: a buffered transaction's PENDING (ALTERed) schema — the batches carry columns the
        // committed snapshot doesn't know yet; the pending schema (whose added columns already carry their
        // column-mapping ids/physical names) drives the physical rename + stats keying. The paired commit
        // includes the matching metaData action.
        var writeSchema = schemaOverride ?? snapshot.Schema;
        var partitionColumns = snapshot.Metadata.PartitionColumns;
        var mappingMode = ColumnMapping.GetMode(snapshot.Metadata.Configuration);
        var logicalToPhysical = ColumnMapping.BuildLogicalToPhysicalMap(writeSchema, mappingMode);
        var files = new List<WrittenDataFile>();

        // materializedRowIds: the rows' ORIGINAL stable row ids, flat + aligned with the batches' rows
        // (UPDATE post-images on a materialized-row-tracking table — the declared __delta_row_id column
        // overrides baseRowId + position for a spec reader, preserving identity across the update).
        // Partitioned tables work too: the id column is attached BEFORE the partition split so each row
        // keeps its id through the regrouping.
        int matOffset = 0;

        foreach (var batch in batches)
        {
            if (batch.Length == 0)
                continue;

            var work = batch;
            if (materializedRowIds is not null)
            {
                var idb = new Int64Array.Builder();
                for (int r = 0; r < batch.Length; r++)
                    idb.Append(materializedRowIds[matOffset + r]);
                matOffset += batch.Length;
                work = RowTracking.RowTrackingWriter.AddRowIdColumn(batch, idb.Build());
            }

            var partitions = Partitioning.PartitionUtils.SplitByPartition(work, partitionColumns);
            foreach (var (partValues, dataBatch) in partitions)
            {
                if (dataBatch.Length == 0)
                    continue;

                // The id column (when attached) is NOT part of the table schema: convert AROUND it —
                // strip, physical-rename the user columns, re-append — and keep it out of the stats.
                RecordBatch statsSource = dataBatch;
                RecordBatch physicalBatch;
                if (materializedRowIds is not null)
                {
                    var (noId, ids) = RowTracking.RowTrackingWriter.StripRowIdColumn(dataBatch);
                    statsSource = noId;
                    physicalBatch = ColumnMappingRecursive.ToPhysical(noId, writeSchema, mappingMode);
                    if (ids is not null)
                        physicalBatch = RowTracking.RowTrackingWriter.AddRowIdColumn(physicalBatch, ids);
                }
                else
                {
                    physicalBatch = ColumnMappingRecursive.ToPhysical(dataBatch, writeSchema, mappingMode);
                }

                // partitionValues keyed by the PHYSICAL column name under mapping (the spec convention).
                var trackedPartValues = partValues;
                if (mappingMode != ColumnMappingMode.None && partValues.Count > 0)
                {
                    trackedPartValues = new Dictionary<string, string>(partValues.Count);
                    foreach (var kv in partValues)
                    {
                        trackedPartValues[logicalToPhysical.TryGetValue(kv.Key, out var p) ? p : kv.Key] = kv.Value;
                    }
                }

                string partDir = Partitioning.PartitionUtils.BuildPartitionPath(trackedPartValues);
                string fileName = string.IsNullOrEmpty(partDir)
                    ? $"{Guid.NewGuid():N}.parquet"
                    : $"{partDir}/{Guid.NewGuid():N}.parquet";

                long fileSize;
                if (_options.DataFileWriter is { } dataFileWriter)
                {
                    fileSize = await dataFileWriter.WriteAsync(
                        new[] { physicalBatch }, fileName, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    var codecBatch = VariantTransport.ToVariantArrays(physicalBatch);
                    await using (var file = await _fs.CreateAsync(
                        fileName, cancellationToken: cancellationToken).ConfigureAwait(false))
                    {
                        await using var writer = new ParquetFileWriter(
                            file, ownsFile: false, _options.ParquetWriteOptions);
                        await writer.WriteRowGroupAsync(codecBatch, cancellationToken).ConfigureAwait(false);
                        await writer.DisposeAsync().ConfigureAwait(false);
                        fileSize = file.Position;
                    }
                }

                string? stats = _options.CollectStats
                    ? CollectStats(ColumnMappingRecursive.ToPhysical(statsSource, writeSchema, mappingMode))
                    : null;

                files.Add(new WrittenDataFile(
                    fileName, fileSize, dataBatch.Length,
                    trackedPartValues.Count > 0 ? trackedPartValues : null, stats));
            }
        }

        return files;
    }

    /// <summary>
    /// Reads exactly the rows identified by the given transient rowids (<c>(fileOrdinal &lt;&lt; 40) |
    /// absolutePosition</c> against the CURRENT snapshot), yielding logical-named batches WITH the trailing
    /// virtual <c>_metadata.row_id</c> column so the caller can pair each row with its id — the read-back
    /// step of a buffered UPDATE (post-image construction). Deletion-vector-excluded rows never match
    /// (<see cref="ReadFileAsync"/> filters them), and files without a requested position are not read.
    /// </summary>
    public async IAsyncEnumerable<RecordBatch> ReadRowsByRowIdsAsync(
        IReadOnlyCollection<long> rowIds,
        [EnumeratorCancellation] CancellationToken cancellationToken = default,
        long? atVersion = null)
    {
        ThrowIfDisposed();
        // atVersion: the snapshot the rowids were SCANNED against (a buffered transaction's pinned
        // version). Ordinals are path-sort positions in THAT snapshot's active set — resolving them
        // against a moved CurrentSnapshot would read the wrong files after a concurrent commuting
        // append shifts the ordering.
        var snapshot = atVersion is { } v && v != CurrentSnapshot.Version
            ? await GetSnapshotAtVersionAsync(v, cancellationToken).ConfigureAwait(false)
            : CurrentSnapshot;
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
        foreach (var kvp in positionsByFile.OrderBy(k => k.Key))
        {
            if (kvp.Key < 0 || kvp.Key >= ordered.Count)
                continue;
            await foreach (var batch in ReadFileAsync(ordered[kvp.Key], null, snapshot, cancellationToken,
                                                      fileOrdinal: kvp.Key).ConfigureAwait(false))
            {
                if (batch.Column(batch.ColumnCount - 1) is not Apache.Arrow.Int64Array rids)
                    continue;
                var rows = new List<int>();
                for (int i = 0; i < batch.Length; i++)
                    if (!rids.IsNull(i) && kvp.Value.Contains(rids.GetValue(i)!.Value & posMask))
                        rows.Add(i);
                if (rows.Count > 0)
                    yield return TakeRowsFromBatch(batch, rows);
            }
        }
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
        ThrowIfVariantRewrite("OPTIMIZE (compaction)");

        options ??= CompactionOptions.Default;
        var result = await Compaction.CompactionExecutor.ExecuteAsync(
            _fs, _log, CurrentSnapshot, options,
            _options.ParquetWriteOptions, _options.ParquetReadOptions,
            cancellationToken, _options.DataFileWriter, _options.DataFileReader).ConfigureAwait(false);

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
        var result = await Vacuum.VacuumExecutor.ExecuteAsync(
            _fs, _log, CurrentSnapshot, retention, dryRun, cancellationToken)
            .ConfigureAwait(false);
        if (!dryRun)
        {
            // A non-dry-run vacuum wrote the VACUUM START/END history commits — pick them up so the next
            // operation's version allocation starts past them.
            _currentSnapshot = await SnapshotBuilder.UpdateAsync(
                CurrentSnapshot, _log, cancellationToken).ConfigureAwait(false);
        }
        return result;
    }

    private async IAsyncEnumerable<RecordBatch> ReadFileAsync(
        AddFile addFile,
        IReadOnlyList<string>? columns,
        Snapshot.Snapshot snapshot,
        [EnumeratorCancellation] CancellationToken cancellationToken,
        long fileOrdinal = -1,
        List<Int64Array?>? strippedRowIdsOut = null)
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

        // Load the deletion vector first — it is independent of the byte source.
        HashSet<long>? deletedRows = null;
        if (addFile.DeletionVector is not null)
        {
            deletedRows = await _dvReader.ReadAsync(
                addFile.DeletionVector, cancellationToken).ConfigureAwait(false);
        }

        if (_options.DataFileReader is { } dataFileReader)
        {
            // Pluggable codec read: raw physical batches in file order (DV rows included). Projection resolves
            // by PHYSICAL NAME in every mode — id-mode field-id resolution needs the parquet footer, which the
            // seam deliberately hides; Delta-spec files carry physicalName in BOTH modes, so name resolution is
            // exact for spec-written files. parquetSchema stays null, so the logical rename in the pipeline
            // falls to the (equivalent for spec files) name-based path.
            IReadOnlyList<string>? seamColumns = null;
            if (columns is not null)
            {
                var partSet = hasPartitions
                    ? new HashSet<string>(partitionColumns, StringComparer.Ordinal)
                    : new HashSet<string>();
                seamColumns = columns
                    .Where(c => !partSet.Contains(c))
                    .Select(c => logicalToPhysical.TryGetValue(c, out var p) ? p : c)
                    .ToList();
            }
            else if (hasPartitions)
            {
                var partSet = new HashSet<string>(partitionColumns, StringComparer.Ordinal);
                seamColumns = snapshot.Schema.Fields
                    .Where(f => !partSet.Contains(f.Name))
                    .Select(f => ColumnMapping.GetPhysicalName(f, mappingMode))
                    .ToList();
            }
            var seamBatches = dataFileReader.ReadAsync(
                DeltaPath.Decode(addFile.Path), seamColumns, cancellationToken);
            await foreach (var processed in ProcessFileBatchesAsync(
                seamBatches, addFile, snapshot, columns, includeRowId, fileOrdinal, mappingMode, isIdMode,
                physicalToLogical, logicalToPhysical, fieldIdToLogical, parquetSchema: null, deletedRows,
                partitionColumns, hasPartitions, cancellationToken, strippedRowIdsOut).ConfigureAwait(false))
            {
                yield return processed;
            }
            yield break;
        }

        // Open the file and read its Parquet schema for field_id resolution
        await using var file = await _fs.OpenReadAsync(DeltaPath.Decode(addFile.Path), cancellationToken)
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

        // Tolerant projection: a requested column may be ABSENT from this file (a partial-column INSERT
        // wrote only the supplied columns, or the column was ADDed after the file was written). The parquet
        // reader throws on unknown names, so intersect with the file's actual top-level columns —
        // BackfillMissingColumns reconciles the absent ones to typed NULLs. An intersection that comes up
        // EMPTY falls back to a full read (the row count must still come from the file).
        if (fileColumns is not null)
        {
            parquetSchema ??= await reader.GetSchemaAsync(cancellationToken).ConfigureAwait(false);
            var present = new HashSet<string>(StringComparer.Ordinal);
            foreach (var child in parquetSchema.Root.Children)
                present.Add(child.Name);
            var kept = fileColumns.Where(present.Contains).ToList();
            fileColumns = kept.Count > 0 ? kept : null;
        }

        if (parquetSchema is null && isIdMode)
        {
            parquetSchema = await reader.GetSchemaAsync(cancellationToken)
                .ConfigureAwait(false);
        }

        await foreach (var processed in ProcessFileBatchesAsync(
            reader.ReadAllAsync(columnNames: fileColumns, cancellationToken: cancellationToken),
            addFile, snapshot, columns, includeRowId, fileOrdinal, mappingMode, isIdMode,
            physicalToLogical, logicalToPhysical, fieldIdToLogical, parquetSchema, deletedRows,
            partitionColumns, hasPartitions, cancellationToken, strippedRowIdsOut).ConfigureAwait(false))
        {
            yield return processed;
        }
    }

    // The per-batch read pipeline shared by the built-in ParquetFileReader and a pluggable IDataFileReader:
    // logical rename (field-id when a parquet schema is available, else by physical name), DV filtering with
    // absolute-position tracking, type widening, partition-column re-add, row-tracking strip, schema-evolution
    // backfill, and the transient rowid. Raw batches MUST arrive in file order (positions are counted).
    private async IAsyncEnumerable<RecordBatch> ProcessFileBatchesAsync(
        IAsyncEnumerable<RecordBatch> rawBatches,
        AddFile addFile,
        Snapshot.Snapshot snapshot,
        IReadOnlyList<string>? columns,
        bool includeRowId,
        long fileOrdinal,
        ColumnMappingMode mappingMode,
        bool isIdMode,
        Dictionary<string, string> physicalToLogical,
        Dictionary<string, string> logicalToPhysical,
        Dictionary<int, string>? fieldIdToLogical,
        Parquet.Schema.SchemaDescriptor? parquetSchema,
        HashSet<long>? deletedRows,
        IReadOnlyList<string> partitionColumns,
        bool hasPartitions,
        [EnumeratorCancellation] CancellationToken cancellationToken,
        List<Int64Array?>? strippedRowIdsOut = null)
    {
        long batchStartRow = 0;

        await foreach (var batch in rawBatches.ConfigureAwait(false))
        {
            // Rename columns back to logical names (flat, top level), then recursively for nested struct
            // children (the flat renames leave them under their physical names).
            RecordBatch result;
            if (isIdMode && fieldIdToLogical is not null && parquetSchema is not null)
            {
                result = ColumnMapping.RenameByFieldId(batch, fieldIdToLogical, parquetSchema);
            }
            else
            {
                result = ColumnMapping.RenameColumns(batch, physicalToLogical);
            }
            if (ColumnMappingRecursive.HasNestedFields(snapshot.Schema))
            {
                result = ColumnMappingRecursive.ToLogical(result, snapshot.Schema, mappingMode);
            }
            // Variant columns: the registry-less codec reader surfaces the VARIANT-annotated group as a bare
            // struct — convert back to the transport blob (a pluggable host reader already delivers the blob,
            // which passes through untouched). A 1:1 per-row transform, so positions below are unaffected.
            if (VariantTransport.SchemaHasVariant(snapshot.Schema))
            {
                result = VariantTransport.FromStorageStructs(result, snapshot.Schema);
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
                    result, fullSchema, addFile.PartitionValues, partitionColumns, logicalToPhysical);
            }

            // Strip any internal row tracking column if present (no-op for plain tables). The stripped ids
            // (null when the file has no materialized column) are surfaced per emitted batch, row-aligned —
            // the merge-on-read UPDATE uses them so a row's ORIGINAL id survives updating a file that was
            // itself produced with materialized ids (a compacted file, an earlier update's post-image).
            var (cleanResult, strippedRowIds) = RowTracking.RowTrackingWriter.StripRowIdColumn(result);
            strippedRowIdsOut?.Add(strippedRowIds);

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
    /// A field stripped to name/type/nullability for the clean-rebuild before a re-write: reader-carried
    /// metadata (e.g. an existing <c>PARQUET:field_id</c>) malforms the footer when the writer re-stamps ids,
    /// so it is dropped — EXCEPT the <c>ARROW:extension:*</c> transport markers (e.g. the variant
    /// discriminator), which type the column for a pluggable host codec and must survive every rewrite.
    /// </summary>
    internal static Field CleanField(Field f)
    {
        Dictionary<string, string>? kept = null;
        if (f.Metadata is { } md)
        {
            foreach (var kv in md)
            {
                if (kv.Key.StartsWith("ARROW:extension:", StringComparison.Ordinal))
                    (kept ??= new Dictionary<string, string>())[kv.Key] = kv.Value;
            }
        }
        return new Field(f.Name, f.DataType, f.IsNullable, kept);
    }

    /// <summary>
    /// Schema evolution reconcile: a column ADDed (via <see cref="AddColumnAsync"/>) after a data file was
    /// written is absent from that file's parquet — backfill it as an all-NULL array of the field's type; a
    /// column DROPped (via <see cref="DropColumnAsync"/>) still exists in old files — drop it from the batch.
    /// Reconciles the batch to exactly <paramref name="expectedFields"/> (the current schema's expected output
    /// columns), taking present columns by name. No-op (returns the batch unchanged) when the batch already
    /// matches the expected column set.
    /// </summary>
    internal static RecordBatch BackfillMissingColumns(RecordBatch batch, IReadOnlyList<Field> expectedFields)
    {
        var present = new Dictionary<string, int>(StringComparer.Ordinal);
        for (int i = 0; i < batch.Schema.FieldsList.Count; i++)
            present[batch.Schema.FieldsList[i].Name] = i;

        // Reconcile every expected column (recursing into STRUCT children — a field ADDed/DROPped inside a
        // nested struct after this file was written must be backfilled/removed at its nesting level too).
        bool changed = batch.Schema.FieldsList.Count != expectedFields.Count;
        var arrays = new List<IArrowArray>(expectedFields.Count);
        var schemaBuilder = new Apache.Arrow.Schema.Builder();
        foreach (var f in expectedFields)
        {
            schemaBuilder.Field(f);
            IArrowArray reconciled;
            if (present.TryGetValue(f.Name, out int idx))
            {
                var column = batch.Column(idx);
                reconciled = ReconcileColumn(column, f.DataType, batch.Length);
                if (!ReferenceEquals(reconciled, column))
                    changed = true;
            }
            else
            {
                reconciled = MakeNullArray(f.DataType, batch.Length);
                changed = true;
            }
            arrays.Add(reconciled);
        }
        if (!changed)
            return batch; // common path — file matches the current schema, no rebuild.
        return new RecordBatch(schemaBuilder.Build(), arrays, batch.Length);
    }

    // Reconciles ONE column against its expected type: a STRUCT whose child set differs from the expected
    // struct (nested ADD/DROP after the file was written) is rebuilt — missing children backfilled as typed
    // all-NULL arrays, extra children dropped, children recursed. Non-structs (and matching structs) pass
    // through unchanged (reference-equal). Struct children are NOT sliced with the parent, so backfilled
    // child arrays are sized to the PHYSICAL child length (parent offset + length; see the TakeRows
    // convention) and the parent's offset/validity are preserved on the rebuilt array.
    private static IArrowArray ReconcileColumn(IArrowArray column, IArrowType expectedType, int logicalLength)
    {
        if (expectedType is not Apache.Arrow.Types.StructType expectedStruct || column is not StructArray sa)
            return column;

        var actualStruct = (Apache.Arrow.Types.StructType)sa.Data.DataType;
        var childIndex = new Dictionary<string, int>(StringComparer.Ordinal);
        for (int i = 0; i < actualStruct.Fields.Count; i++)
            childIndex[actualStruct.Fields[i].Name] = i;

        int physicalLength = sa.Data.Offset + sa.Length;
        foreach (var child in sa.Fields)
            physicalLength = System.Math.Max(physicalLength, child.Length);

        bool changed = actualStruct.Fields.Count != expectedStruct.Fields.Count;
        var children = new List<IArrowArray>(expectedStruct.Fields.Count);
        for (int i = 0; i < expectedStruct.Fields.Count; i++)
        {
            var expectedChild = expectedStruct.Fields[i];
            IArrowArray reconciled;
            if (childIndex.TryGetValue(expectedChild.Name, out int idx))
            {
                if (idx != i)
                    changed = true; // reordered relative to the expected layout
                var child = sa.Fields[idx];
                reconciled = ReconcileColumn(child, expectedChild.DataType, child.Length);
                if (!ReferenceEquals(reconciled, child))
                    changed = true;
            }
            else
            {
                reconciled = MakeNullArray(expectedChild.DataType, physicalLength);
                changed = true;
            }
            children.Add(reconciled);
        }
        if (!changed)
            return column;

        return new StructArray(
            expectedStruct, sa.Length, children, sa.NullBitmapBuffer, sa.NullCount, sa.Data.Offset);
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
            case Decimal256Type dec:
            { var b = new Decimal256Array.Builder(dec); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
            case Date64Type:
            { var b = new Date64Array.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
            case Time32Type t32:
            { var b = new Time32Array.Builder(t32); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
            case Time64Type t64:
            { var b = new Time64Array.Builder(t64); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
            case Apache.Arrow.Types.StructType st:
            {
                // An all-null struct: zeroed validity + typed all-null children (children length == the
                // struct's own length; the caller passes the PHYSICAL length when backfilling a child).
                var children = new List<IArrowArray>(st.Fields.Count);
                foreach (var f in st.Fields)
                    children.Add(MakeNullArray(f.DataType, length));
                return new StructArray(st, length, children, AllNullBitmap(length), nullCount: length);
            }
            case ListType lt:
            {
                // An all-null list: zeroed validity + all-zero offsets over an empty values child.
                var offsets = new ArrowBuffer.Builder<int>(length + 1);
                for (int i = 0; i <= length; i++) offsets.Append(0);
                return new ListArray(lt, length, offsets.Build(), MakeNullArray(lt.ValueDataType, 0),
                                     AllNullBitmap(length), nullCount: length);
            }
            case StringType:
            { var b = new StringArray.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
            default:
                throw new NotSupportedException(
                    $"Schema-evolution backfill has no NULL-array builder for Arrow type '{type.Name}'.");
        }
    }

    private static ArrowBuffer AllNullBitmap(int length)
    {
        var bitmap = new ArrowBuffer.Builder<byte>((length + 7) / 8);
        for (int i = 0; i < (length + 7) / 8; i++) bitmap.Append(0);
        return bitmap.Build();
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
