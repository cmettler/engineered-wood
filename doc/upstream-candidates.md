# Upstream candidates — proposed PR slicing

This fork carries a body of correctness fixes and one architectural seam accumulated while embedding
engineered-wood as the Delta layer of a DuckDB extension (the `mssql_net` / ArrowNet project). Everything
below is validated three ways unless noted: engineered-wood's own suites (DeltaLake 168 + Table 141, all
TFMs), delta-kernel-rs (DuckDB's official `delta` extension reading our output), and live Fabric (Spark
writes → we read / we write → Spark + the OneLake conversion + SQL endpoint read it). Sliced for
independent review, roughly in ascending dependency order; each slice is default-preserving.

## 1. Parquet writer correctness (pure bugfixes — highest review confidence)

- **Null-struct child misalignment** (`NestedLevelWriter`): a null STRUCT row was treated like a null LIST
  (no child slot), but Arrow struct children are 1:1 with parent rows — every child value after a null
  struct row shifted one slot (def levels + values wrong → file unreadable by DuckDB/delta-kernel). Fixed
  by threading an explicit per-level value map from struct parents through struct/leaf/list/map
  decomposition, including the sliced-struct case (children are NOT sliced with the parent — the map bakes
  in `Data.Offset`).
- **`ExpandArray` default 8-byte stride**: unknown fixed-width leaf types expanded as `long`, corrupting
  Int8/Int16/Date32/Time32/… whenever expansion triggered. Now width-dispatched; genuinely unsupported
  types throw instead of corrupting.
- **All-null pages declared a delta encoding with a 0-byte payload**: DELTA_BINARY_PACKED /
  DELTA_LENGTH_BYTE_ARRAY require a header even for zero values → readers underrun ("Out of buffer"). An
  all-null page now declares PLAIN.
- **`VariantType.specification_version` written explicitly (= 1)**: the annotation was emitted as an
  empty thrift struct; Spark's parquet variant reader rejects that (a generic `FAILED_READ_FILE`), while
  delta-kernel/DuckDB tolerate it. Isolated live on Fabric Spark 4.1 (only variant-annotated files
  failed; setting the version fixed both test tables).
- ns-timestamp/time `converted_type` OMITTED (no ns variant exists — was mislabeled micros, a 1000×
  error for converted_type-trusting readers); deprecated `Statistics.min`/`max` restricted to
  signed-order-safe types (parquet-mr parity); `SchemaConverter.FromArrowField` preserves per-field
  metadata (dropping it lost column-mapping identities on any Arrow→Delta round-trip).

## 2. Deletion-vector serialization format (spec compliance — headline interop fix)

`RoaringBitmapWriter`/`Reader` diverged from the spec in two ways: the 64-bit `RoaringBitmapArray`
wrapper was omitted (`[magic][32-bit bitmap]` instead of `[magic][count][high-key + bitmap]…`), and the
inner bitmap used a non-standard no-run cookie instead of the CRoaring portable form. Consequence: no
external reader (Spark, delta-kernel, Fabric) could decode an engineered-wood DV. Fixed both (reader keeps
a legacy fallback); delta-kernel now reads DV tables, and Fabric's SQL endpoint queries them live. This
also fixes engineered-wood's own `DeleteAsync` output for external consumers.

Second pass — the ON-DISK (`storageType "u"`) form was broken in BOTH directions (never exercised:
the writer only produced inline DVs until a large delete crossed the 1 KB inline threshold, and reading
Spark's `.bin` DVs was first attempted against a live Fabric table). Three spec divergences, mirrored in
reader and writer: (1) resolved/written under `_delta_log/` instead of the TABLE ROOT (+ the optional
random-prefix directory from `pathOrInlineDv`'s leading characters); (2) the file name's UUID rendered
with .NET's little-endian `Guid(byte[])` instead of the canonical big-endian/Java form; (3) the on-disk
framing ignored — the spec shape is `<version:1><dataSize:4-byte BE><bitmap><CRC-32 BE>` with the offset
pointing at the size field. Both sides now write/read the spec shape (reader keeps a legacy fallback for
pre-fix tables; CRC written via System.IO.Hashing). Validated: Spark reads our large-DV deletes, we read
Spark's `u`-DV UPDATE output byte-for-byte, delta-kernel exact, and Fabric's SQL endpoint decodes the
on-disk `.bin` live.

## 3. Checkpoint correctness (deleted rows resurrected — critical)

`CheckpointWriter` dropped `add.deletionVector` (plus `baseRowId`/`defaultRowCommitVersion`) and the
protocol `readerFeatures`/`writerFeatures` — with DV DML and `CheckpointInterval=10`, **deleted rows
resurrected after 10 commits** (same session). Also: remove TOMBSTONES within the retention window are
now included (`delta.deletedFileRetentionDuration` honored), `add.tags` preserved, action structs made
NULLABLE per the spec schema and `metaData.format.options` emitted (delta-kernel rejects checkpoints
otherwise). Reader side hardened symmetrically.

## 4. Path encoding (`DeltaPath`)

Partition directories use Spark's `escapePathName` set; `add.path` is the URL-ENCODED form of the
on-disk relative path (spec), URL-decoded at every read site; vacuum protects both forms. Before this,
tables with partition values containing `:`/`%`/space were unreadable by Spark and vice versa.

## 5. Protocol / feature declarations

- `timestampNtz`: any naive-timestamp column now declares the required reader+writer feature at create;
  `AddColumnAsync`/`SetSchemaAsync` emit a protocol UPGRADE in the same commit (legacy versions upgraded
  to table-features mode with implied features enumerated). Previously Spark/delta-kernel rejected every
  naive-timestamp table. Generalized as `RequiredSchemaFeatures`/`UpgradeProtocolForFeatures`, which also
  serves `variantType` (schema type `"variant"` — transported as a marker-tagged binary at the Arrow
  boundary) and `identityColumns` (declared when `delta.identity.*` metadata is present — the existing
  `IdentityColumnWriter` machinery finally advertises itself).
- `appendOnly`/`invariants`/`checkConstraints` allowlisted + enforced only when ACTIVE
  (`HonorWriterFeatures`) — a v7-upgraded table merely LISTING them (the common case) writes normally;
  a write violating an active one is rejected instead of silently proceeding. Tests:
  `WriterFeatureEnforcementTests` (inactive-listed writes normally; active invariant / CHECK
  constraint / generation expression each reject) + the appendOnly arm in `SchemaWriteModesTests`.
- `delta.rowTracking` domainMetadata high-water mark emitted on every id-assigning commit and max()'d on
  snapshot rebuild (removes could regress the derived mark → row-id reuse); `tightBounds:false` on
  DV-carrying adds; Spark-style 32-char string stat truncation (max side last-char-incremented); nested
  struct-leaf stats (exact nullCount; min/max superset-safe under parent nulls).

## 6. Column mapping spec compliance

- **Id-mode files now use PHYSICAL names + parquet field_ids** (the spec requires physical names in BOTH
  modes; writing logical names made delta-kernel read all-NULLs). All write paths covered.
- `ColumnMappingRecursive` (`ToPhysical`/`ToLogical`): the recursive rename + field-id stamping at every
  nesting depth (the flat top-level renames silently broke nested structs); wired into every mapped write
  site and the read sites.
- RENAME/ADD/DROP COLUMN under mapping as metadata-only commits (fresh ids from `maxColumnId`;
  `metaData.partitionColumns` updated on a partition-column rename); partitioned convention matched to
  Spark empirically (`partitionColumns` logical, `add.partitionValues` keys physical).
- Compaction is mapping-aware (physical-renamed target schema, per-vintage backfill, ids re-stamped).

## 7. The pluggable codec seam (the architectural piece)

Three small interfaces on `DeltaTableOptions`, all default-null (built-in codec unchanged, byte-identical):

- **`IDataFileWriter`** — batches → one parquet file (partition split / row tracking / mapping / stats /
  the `add` / the commit stay in the Delta layer). Plus `CommitDataFilesAsync` (the commit-only half of
  the write path) for hosts that stream files themselves.
- **`IDataFileRewriter`** — the read+transform half of a copy-on-write DELETE/UPDATE for clean shapes
  (the host applies the row filter/SET substitution itself, e.g. in SQL).
- **`IDataFileReader`** — the general read half: RAW batches (physical names, FILE ORDER, DV rows
  included — every consumer is position-keyed) for `ReadFileAsync` and compaction; everything above the
  decode (mapping rename, DV filter, backfill, partitions, rowids) stays in the Delta layer.

Together these position engineered-wood as *the `_delta_log` engine with a bring-your-own parquet codec*:
the embedding host (in our case DuckDB's native parquet reader/writer) supplies the bytes, engineered-wood
owns the protocol. That is what made VARIANT columns (a parquet logical type the built-in codec doesn't
know) fully writable/readable/rewritable through the Delta layer without teaching the codec anything —
and it's the natural adapter point for Parquet.Net/ParquetSharp if anyone wants a maintained codec.
Supporting fix: the clean-rebuild before rewrites preserves `ARROW:extension:*` field metadata
(`CleanField`) so a host codec's column-type discriminators survive.

## 8. Misc reader/DML correctness

Decimal reads always surface `Decimal128`/`Decimal256` (narrow Decimal32/64 mishandled over the Arrow C
interface); `TakeRows`/`DeletionVectorFilter` cover every fixed-width type and THROW on unsupported
instead of silently returning unfiltered (wrong-length columns); parquet `TIME(micros/nanos)` maps to
`Time64`; `GetHistoryAsync`; OCC `DeltaConflictException` + retry-safe writes; `commitInfo` on every
commit (operation + timestamp — enables timestamp time travel on plain tables); struct-aware
`TakeRows`/`PartitionUtils` (offset-correct child indexing).

Maintenance history commits (Spark parity): `CompactionExecutor` now writes the always-on commitInfo
(`operation: OPTIMIZE`) like every other commit path — it was the ONLY silent one (history showed a
NULL operation, and timestamp time travel had no timestamp to resolve through a compaction commit);
`VacuumExecutor` writes the Spark `VACUUM START` (retention params + files/bytes-to-delete metrics) /
`VACUUM END` (`status: COMPLETED` + deleted metrics) commitInfo-only pair around the physical deletes
(dry run writes nothing; versions allocated with a bounded conflict-retry — commitInfo-only commits are
safe to re-attempt at the next version). Without the pair, another engine's history shows no trace of
why older versions stopped being physically readable. Verified live against a Fabric AUTOSET-VORDER
table: unknown configuration keys (`delta.parquet.vorder.enabled`, Spark's materialized row-tracking
column names) survive every metaData rewrite by construction (the schema-change paths copy the full
config dict) — checked key-for-key after ADD COLUMN, and Spark reconfirms the property afterwards.

Spark-style logical rebase for buffered transactions:
`CheckLogicalRebaseAsync(baseSnapshot, plannedActions, readPredicates, readWholeTable, serializable)` —
FULL ConflictChecker parity. Walks the concurrent commit range (base+1..latest) and throws
`DeltaConflictException` on: a concurrent metadata change, protocol change, delete/delete (any planned
RemoveFile whose (path, DV) is no longer active unchanged), a concurrent data-changing remove of a file
the transaction READ (concurrentDeleteReadCheck), and a concurrent data-changing add matching the
transaction's read predicates (concurrentAppendCheck — from non-blind-append commits always; from blind
appends only under `serializable`; blind append = no remove/metaData/protocol action in the commit).
Read-predicate-vs-file matching = `DeltaFilePruner.ShouldInclude` over the base schema (partition values
exact, stats conservative); `dataChange=false` actions (compaction) are exempt from the read checks —
rows unchanged. Paired with `ComputeDeletionVectorActionsAsync(resolveAgainst:)` so DV ordinals/old-DVs
resolve against the pinned snapshot (the newer snapshot's path-sorted ordinals differ after appends).
Tests: `LogicalRebaseTests` (Table.Tests) — WriteSerializable vs Serializable blind-append semantics,
stats-based read matching, deleteRead, delete/delete, metadata conflict, the compaction exemption.

Repartition-on-overwrite: `WriteAsync(repartitionTo:)` changes the table's partition columns as part of
the SAME atomic Overwrite commit — the only protocol-legal repartitioning shape (a new
`metaData.partitionColumns` is valid only when every active file is removed in the same commit, since
readers interpret each `add.partitionValues` against the current partition schema; Spark exposes this as
`overwriteSchema=true` + a new `partitionBy` and has no `ALTER TABLE … PARTITIONED BY`). Guarded to a
FULL overwrite (partition-scoped/dynamic overwrites keep files that would no longer conform); the input
is Hive-split by the NEW columns; coordinated with the identity-HWM metadata emission so a commit never
carries two conflicting metaData actions. delta-kernel reads the repartitioned result.

`TransactionLog.ListVersionsAsync` sorts ascending (cross-platform correctness): it previously yielded
commit versions in RAW directory-listing order — Windows/S3/ADLS list sorted, but Linux readdir returns
inode-hash order — and the callers assume ascending replay: `SnapshotBuilder`'s latest-wins
metadata/protocol reconciliation, `GetSnapshotAtTimestampAsync`'s monotonic early-break (the symptom
that surfaced it: a timestamp resolved to v0 with newer commits present), and the history view. The
versions are materialized + sorted (the log directory is bounded by the checkpoint interval).

Thrift WIRE-TYPE guards in `MetadataDecoder` (`case N when type == ThriftType.X`): field dispatch was
by id only, so a foreign writer reusing a field id with a DIFFERENT type desynchronized the whole
stream — Impala's `dict-page-offset-zero.parquet` (parquet-testing) carries a LIST at ColumnMetaData
field 15 (modern parquet.thrift: `bloom_filter_length: i32`); reading its list header as a varint made
the reader fail four suite tests with "unknown Thrift type 14". Mismatches now fall to `default` →
`Skip(type)`, exactly like Thrift-generated readers. Also: the ALP decoder is validated BIT-EXACT
against apache/parquet-testing PR #100's arade/spotify1 files (unmerged upstream — the tests fetch
notes are in `AlpDecoderTests`, and they no-op when the data is absent). Parquet.Tests: 585/585.

Nested-stats PRUNING (consumption side of the nested `StatsCollector` output): `ColumnStats.Parse`
flattens nested minValues/maxValues/nullCount objects into dotted keys ("s.a") — nested nullCount
objects were previously DROPPED at parse — and `DeltaFilePruner` registers struct leaves under their
dotted path (dual logical|physical under column mapping), so a predicate referencing `s.a` file-prunes
like a top-level column; the Parquet `StatisticsAccessor`/bloom probe already resolved dotted leaf paths.
A literal dotted column name colliding with a struct path is POISONED (removed — never guessed).
Tests: `NestedStatsPruningTests`.

Transactional (multi-statement) commit seams: `ComputeDeletionVectorActionsAsync` (the deferred half of
the DV delete — positions-per-ordinal in, remove/add pairs with unioned inline DVs out, no commit),
`WriteDataFilesAsync` (the write-no-commit half of the batch path — partition split, recursive mapping
rename + field ids, variant transport, the `IDataFileWriter` seam, per-file stats; row-tracking baseRowId
left to the commit, like the streaming writer), and `CommitDataFilesAsync(extraActions:, expectedVersion:,
operation:)` (caller-supplied actions join the one commit; `expectedVersion` turns the append OCC retry
into a conflict-ABORT for snapshot-coupled actions — first-committer-wins snapshot isolation), plus
`ReadRowsByRowIdsAsync` (exact-row read-back by transient rowid, for UPDATE post-image construction),
the `Compute*` family (`ComputeAddColumn`/`ComputeRenameColumn`/`ComputeDropColumn`/`ComputeAddField`/
`ComputeDropField` — the compute-only halves of the schema ALTERs: metaData + protocol-upgrade actions +
the parsed new schema, chainable against a pending base), `WriteDataFilesAsync(schemaOverride:)`, and the
public `ReconcileBatchToFields` export of the recursive schema-evolution reconcile (lets a host overlay a
pending schema onto committed reads).
Together these let a host buffer a whole multi-statement transaction (schema changes + appends + DV
deletes + updates) and commit it as ONE atomic Delta version — the same OptimisticTransaction shape
Spark/delta-rs use (fused metaData+protocol+DV+add commits validated against delta-kernel).
Tests: `BufferedTransactionTests` (Table.Tests) — the fused ALTER+INSERT+DELETE one-version commit,
chained Compute* composition, the ReconcileBatchToFields pending-schema overlay, ReadRowsByRowIdsAsync
`atVersion` read-back, the expectedVersion conflict-abort, and the `txn`-action round-trip; plus
`SchemaWriteModesTests` for SetSchemaAsync (adopt/no-op), repartition-on-overwrite, static + dynamic
partition overwrite, and `delta.appendOnly` enforcement; plus `IdentityTransactionSeamsTests` for the
identity split (GenerateIdentityValues chaining across statements, the fused
BuildIdentityMetadataAction commit with a persisting high-water mark, the schema-seeded pending-CREATE
form, and the un-valued-batches guard on WriteDataFilesAsync); `TimestampResolutionTests` pins
plain-table timestamp travel via the always-on commitInfo.timestamp.

Row-tracking preservation through EVERY rewrite (the row-tracking promise made real — `delta.
enableRowTracking` guarantees ids stable across rewrites, which only holds if every rewrite path
materializes them): merge-on-read UPDATE post-images bake each row's ORIGINAL `__delta_row_id` (+ the
new commit version); compaction materializes BOTH id and per-row ORIGINAL commit version (a single
baseRowId/defaultRowCommitVersion cannot represent rows merged from several sources); and copy-on-write
DELETE/UPDATE rewrites materialize survivor ids + versions on both byte paths (codec via
`strippedRowIdsOut`/`strippedVersionsOut` collectors on `ReadFileAsync`; the `IDataFileRewriter` seam
via an optional `RowTrackingRewrite` record — the host projects the two trailing columns itself) while
assigning the fresh `baseRowId`/`defaultRowCommitVersion` + HWM domain action the CoW add previously
lacked (a spec gap). Sources with their OWN materialized ids are honored everywhere (chained rewrites
carry through; `baseRowId + position` arithmetic on an already-rewritten file would silently change row
identity). The merge-on-read eligibility gates were also lifted to the full matrix — column mapping
(name+id; the old `mappingMode == None` requirement was stale), partitions (post-images route through
`WriteDataFilesAsync` for the Hive split; a SET of the partition column moves the row), and CDF
(per-file `update_preimage`/`update_postimage` cdc emission; partitioned via the new
`CdfWriter.WriteSplitAsync`, which all cdc emission sites now share — per-partition cdc files with
physical-keyed partitionValues, and `CdfReader` re-adds partition columns from the cdc action). Spark
reads `_metadata.row_id` preserved through all of it, and Spark's own writes back into these tables
honor the materialized declaration.

S3 conditional-writes correctness (`S3TableFileSystem.RenameAsync`): the commit rename used a
conditional CopyObject — which is SILENTLY UNGUARDED (AWS documents conditional writes for
PutObject/CompleteMultipartUpload only; MinIO happily copies over an existing target), so the
put-if-absent commit guard did not actually guard. Rewritten as GetObject(temp) → PutObject(target,
`If-None-Match: *`) → Delete(temp); a 412 maps to the rename-failed → `DeltaConflictException` path.
Validated with 4 racing processes × 10 commits on MinIO: 40/40 commits, zero lost.

Two more spec-correctness fixes found by an S3 (MinIO) test rig: (1) `WriteCoreAsync`'s
Overwrite-removes omitted the file's `deletionVector`, so an Overwrite of a table whose active file
carries a DV never reconciled the remove against the (path, DV)-keyed active set — the file stayed
active forever and every subsequent read DUPLICATED its rows (the CommitDataFilesAsync + dynamic-
overwrite branches already carried the DV). (2) `CheckpointReader.ExtractMetadata` dropped
`metaData.configuration` entirely (the writer emits the map; the reader never read it): after the first
checkpoint a table silently lost `delta.enableDeletionVectors` / `enableChangeDataFeed` /
`columnMapping.mode` / `maxColumnId` / retention settings — and the loss is VIRAL, because the next
checkpoint persists the config-less metadata (tables checkpointed by the buggy reader stay poisoned
even after the fix). Fixed with the existing `GetStringMapField`. And a parquet-layer sibling: the
page writer's `CompressTo` returned a ZERO-BYTE payload for an empty input, but a valid snappy stream of
nothing is the single `0x00` length varint — so an all-null DataPage-V2 values section (declared
`is_compressed`) was "corrupt snappy" to strict decoders. Delta CHECKPOINT files are full of all-null
column chunks, so SQL Server 2025 failed every table read that crossed a checkpoint ("19787: Corrupt
snappy compressed data" on the `.checkpoint.parquet`; DuckDB and delta-kernel tolerate the 0-byte form).
Fixed by letting the codec encode emptiness (Snappier emits the valid empty stream).

## 9. Row-level concurrency (v1 + v2 — the capability piece)

Concurrent DML touching the SAME data file composes instead of failing the file-level delete/delete
check — the Databricks-proprietary capability (OSS Spark, delta-rs and delta-kernel all conflict at file
granularity), and v2 goes past Databricks. Opt-in per call; default behavior byte-identical.
**Tests: `RowLevelConcurrencyTests` (Table.Tests) — self-contained xunit racers for every case below.**

- **v1 — deletion-vector re-union** (`RebaseDvDmlActionsAsync`): a DML action set computed against a
  base snapshot re-targets onto the latest one. Per remove+add DV pair: the path must still be active;
  THIS transaction's newly-deleted positions must be DISJOINT from the concurrent deletions (absolute
  in-file positions are stable across DV swaps — the parquet is never rewritten); disjoint ⇒
  remove(path, currentDV) + add(path, currentDV ∪ ours); same-row overlap ⇒ a row-level
  `DeltaConflictException` (first committer wins, no lost update). Post-image adds re-derive
  `baseRowId`/`defaultRowCommitVersion` + the HWM action from the target snapshot.
- **v2 — remap across rewrites** (`RemapRowsAcrossRewriteAsync`, automatic within the rebase): when a
  touched file was REPLACED (compaction / copy-on-write), the rows relocate by STABLE ROW ID — the
  tombstoned source (on storage until VACUUM) resolves the target ids + ORIGINAL commit versions; the
  post-rewrite files are scanned for them (dataChange=false candidates first, early exit; fresh appends
  are structurally excluded — their derived ids sit above the base HWM); the row's COMMIT VERSION is
  the concurrent-modification discriminator (relocated-untouched keeps its original version — which the
  slice-8 materialization work guarantees; concurrently updated carries the rewrite's version ⇒
  conflict; found nowhere ⇒ concurrently deleted ⇒ conflict). Databricks' row-level concurrency still
  throws on compaction — this is what the materialized ids buy.
- **Consumption**: autocommit-style callers pass `rowLevelRetry: true` to
  `DeleteByRowIdsViaVectorsAsync`/`UpdateByRowIdsAsync` (a bounded reload+rebase+retry loop,
  `CommitDvDmlWithRebaseAsync`); buffered/multi-statement hosts call `RebaseDvDmlActionsAsync`
  explicitly before `CommitDataFilesAsync(extraActions:, expectedVersion:)` and pass
  `rowLevelDml: true` to `CheckLogicalRebaseAsync` (which then skips the read-set checks — the
  row-level write validation subsumes them under WriteSerializable's reads-don't-serialize
  semantics; `serializable` callers leave both flags off and keep the strict file-level checks).

## 10. Clustered (liquid-clustering) tables — writer-feature support

- **What**: `"clustering"` added to `SupportedWriterFeatures` (`ProtocolVersions.cs`). The feature is
  advisory LAYOUT: the Delta spec permits plain (unclustered) appends and DML by writers that don't
  implement clustering — a later clustering OPTIMIZE reclusters them. The obligations a non-clustering
  writer has were ALREADY met: the `delta.clustering` system domain (the clustering-columns spec) is
  preserved through commits and CHECKPOINTS (SnapshotBuilder/CheckpointWriter carry all domains), and
  `add.clusteringProvider` round-trips (AddFile/ActionSerializer). Without the allowlist entry, EVERY
  write to a Databricks/Fabric-Spark `CREATE TABLE … CLUSTER BY` table failed ValidateWriteSupport with
  "unsupported writer features: [clustering]".
- **Tests**: `ClusteredTableTests` (3) — append to a synthetic clustered table (protocol v7 +
  clustering/domainMetadata + the domain action, the exact OSS delta-spark shape), the domain surviving a
  CHECKPOINT (the sharp edge — a checkpoint that dropped domainMetadata would silently destroy the
  clustering spec), and `clusteringProvider` round-tripping through log replay.
- **Validated live** (Fabric Spark 4.1): Spark `CREATE TABLE … CLUSTER BY (grp, id)` + OPTIMIZE →
  external append + DV DELETE through this library → Spark reads the exact result, `DESCRIBE DETAIL`
  still shows the clusteringColumns, and a further OPTIMIZE reclusters (incl. the foreign unclustered
  files). Writing CLUSTERED files (Hilbert layout + provider tagging) remains unimplemented — this slice
  is interop, not a clustering writer.

## Suggested order

1 → 2 → 3 are independent pure bugfixes (start there; each has a one-line repro). 4 and 5 are small and
self-contained. 6 is larger but purely spec-alignment. 7 is the strategic discussion — worth an issue
first ("would you take a pluggable codec seam?") since it shapes the project's positioning. 8 can trickle
in alongside. 9 depends on 7's commit seams + 8's row-tracking materialization and is the showcase piece —
`RowLevelConcurrencyTests` demonstrates the whole surface without any external harness.
