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
  a write violating an active one is rejected instead of silently proceeding.
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

## Suggested order

1 → 2 → 3 are independent pure bugfixes (start there; each has a one-line repro). 4 and 5 are small and
self-contained. 6 is larger but purely spec-alignment. 7 is the strategic discussion — worth an issue
first ("would you take a pluggable codec seam?") since it shapes the project's positioning. 8 can trickle
in alongside.
