# Landing cmettler PR #4 — progress & remaining work

Working notes for the effort to land Christoph Mettler's PR #4
(`https://github.com/CurtHagenlocher/engineered-wood/pull/4`, branch `pr-4`, 55 commits,
~9.7k additions) onto `master` **slice by slice**. The PR's own review decomposition is in
`doc/upstream-candidates.md` on the `pr-4` branch (9 slices). Overall the PR is high quality —
mostly spec-compliance / interop bug fixes validated against Spark 4.x, delta-kernel-rs and DuckDB;
the full `pr-4` branch is green (DeltaLake 168, Table 182, Parquet 585/585).

**Slices are a review lens, not commit boundaries.** The 55 commits interleave themes, so most
slices are landed by reconstructing the net diff of the relevant files (or hand-porting a
cross-cutting thread) and committing as one focused change — not a clean cherry-pick. Commits are
authored as Christoph Mettler with a `Co-Authored-By: Claude` trailer and a body note where the
change diverges from the original PR.

## Landed on master (as of 2026-07-18)

| Slice | Commit(s) | Notes |
|---|---|---|
| 1 — Parquet writer correctness | `ced8d34`, `a742796` | Verbatim cherry-picks (null-struct alignment, all-null page → PLAIN, ExpandArray widths, VariantType.specification_version, ns-timestamp, deprecated min/max). |
| 2 — Spec DV serialization | `c4a5a07` | 64-bit RoaringBitmapArray + on-disk `.bin` framing. **Both legacy fallbacks removed** (library never shipped → no legacy EW DVs; fallbacks were fragile/OOM-prone). |
| 3 — Spec checkpoint content + tombstone DVs | `b41f5ad` | Preserve add.deletionVector/baseRowId/features/config, NULLABLE action structs, retained tombstones. **Added `remove.deletionVector` round-trip** beyond the PR. Row-tracking HWM reconciliation deliberately excluded (deferred to slice 5). |
| 4 — Spec path encoding (`DeltaPath`) | `74bc1aa` | Hive-escaped partition dirs + URL-encoded `add.path`, decoded at every read site + vacuum. Hand-ported cross-cutting wiring. **See non-ASCII follow-up below.** |
| 5 — writer features + protocol declaration | `cc8d6fe`, `c1b1474`, `70d2384`, `8e3fa8d` | `cc8d6fe`: ToArrowField preserves field metadata (PR #21, prerequisite). `c1b1474`: allowlist appendOnly/invariants/checkConstraints/generatedColumns + `HonorWriterFeatures` on Write/Delete/Update. `70d2384`: declare schema-driven features (timestampNtz/identityColumns/columnMapping-in-v7) at `CreateAsync` + protocol upgrade on `AddColumnAsync`. `8e3fa8d`: `delta.rowTracking` HWM emission + `SnapshotBuilder` reconciliation. **Did NOT allowlist variantType/rowTracking** as supported features (need later slices). |
| 6 — column mapping | `aa3f0e2`, `7a327f0`, `4754a72` | `aa3f0e2`: physical names in BOTH modes + new `ColumnMappingRecursive.ToPhysical/ToLogical` (nested struct children) + numeric `delta.columnMapping.id`. `7a327f0`: compaction re-stamps field ids & widens against the physical-named target schema. `4754a72`: `AddColumnAsync`/`RenameColumnAsync`/`DropColumnAsync` metadata-only + `SchemaEvolution.BackfillMissingColumns` read-path reconcile. **See slice-6 leftovers below.** |
| 7 (PARTIAL) — pluggable codec seam | `9302723` | `IDataFileWriter` + `IDataFileReader` on `DeltaTableOptions`, both default-null. Wired into the write path, the CoW UPDATE rewrite, compaction, and `ReadFileAsync` (via the extracted `ProcessFileBatchesAsync`). `CleanField` preserves `ARROW:extension:*`. **`IDataFileRewriter` NOT landed — see below.** |
| 8 (PARTIAL) — misc reader/DML correctness | `adc3b44`, `8cbf8d2`, `e025880`, `ebeb841`, `e83a232`, `d79abc7`, `1883f51`, `7bf3f6c`, `e0567cb`, `4cd1f06` | Thrift wire-type guards (+ALP test gating; Parquet 585/585, was 573). Empty-page snappy stream. ListVersions ascending. S3 conditional rename. Row-filter type coverage. Nested stats end-to-end. TIME→Time64. DV-qualified removes + compaction DV exclusion. `DecimalOutputKind` read option. Always-on commitInfo + `GetHistoryAsync`. **See slice-8 leftovers below.** |

| 10 — clustered (liquid) table interop | `093185f` | Allowlist the `clustering` writer feature (every write to a Databricks/Fabric CLUSTER BY table failed before), `CreateAsync(clusteringColumns)`, `SetClusteringColumnsAsync`, clustering/partitioning mutual exclusion, `UpgradeProtocolForWriterFeatures`. **Domain stores PHYSICAL names** (OSS Delta `None.get`-crashes otherwise). Interop only — writing clustered layouts is not implemented. |
| 11 — partitioned compaction (bugfix) | `e26a88f` | Candidates group by partition; each group compacts into its own Hive dir; target schema excludes partition columns. **Pre-existing data-corruption bug** (reproduced at `8d7ef32`): merged across partitions, stamped every row with `candidates[0]`, and threw IndexOutOfRange in the common shape. |

Verification standard for each: builds on net10.0/net8.0/netstandard2.0, Delta suites green on both
TFMs. GOTCHA: `parquet-testing` is a git submodule — worktrees don't auto-populate it, and without it
~98 Parquet reader tests spuriously fail (`git submodule update --init parquet-testing`, or test in
the main checkout).

## The inflection point (slice 5 onward)

Slices 1–4 were cleanly separable from the PR's central `DeltaTable.cs` write-path refactor
(`WriteCoreAsync` + the commit seams). Slice 5 onward weave *into* that refactor and into each other —
but slices 5 and 6 were nonetheless landed piecemeal without taking the refactor, by hand-porting each
thread onto master's existing write path. Slice 5 is now **complete**; the three pieces that were
deferred as refactor-/slice-6-coupled all landed once slice 6 supplied `AddColumnAsync`:

- ~~Create-time feature declaration~~ — `70d2384`. `variantType` omitted (no variant support on master;
  the `RequiredSchemaFeatures` hook is in place for when there is).
- ~~Row-tracking HWM emission + `SnapshotBuilder` reconciliation~~ — `8e3fa8d`.
- ~~Protocol-upgrade-on-ALTER~~ — `70d2384` (on `AddColumnAsync`; `SetSchemaAsync` still doesn't exist —
  see slice-6 leftovers).

### Write-path refactor taken (`808b944`)

`WriteCoreAsync` is now the single write path (append / overwrite / static partition overwrite / dynamic
partition overwrite / repartition-on-overwrite). Taken **refactor-only**: the codec seam and the
buffered-transaction + logical-rebase machinery were stripped out of the ported body, so slices 7 and 9 are
still open decisions. A leakage check (`DataFileWriter|VariantTransport|CheckLogicalRebase|
CommitDataFilesAsync`) over the Table project returns nothing.

What this means for 7 and 9: the structural prerequisite is now in place, so both can be ported onto
master's own `WriteCoreAsync` rather than requiring pr-4's whole `DeltaTable.cs`. Note pr-4's file is a
superset of master's `DeltaTable.cs` work but its `CheckpointReader` (drops slice 3's
`remove.deletionVector`) and `CompactionExecutor` (regresses slice 4's path encoding) are NOT — never take
those wholesale.

### The seam question — REOPENED (2026-07-19)

The slice-7 rationale I first recorded was **wrong**, and the corrected version changes the decision:

- Master's **Parquet** layer already supports VARIANT (opt-in `arrow.parquet.variant` extension,
  `VariantArrayRoundTripTests`). `doc/known-issues.md` still says otherwise — **that file is stale**.
- The PR's variant support runs through **engineered-wood's own codec**, not DuckDB's:
  `VariantTransport`'s comment says *"this is what lets the BUILT-IN parquet codec read and write variant
  columns"*. The seam is NOT what makes VARIANT work.
- The actual gap is a **registration** gap: VARIANT decoding is gated on an `ExtensionTypeRegistry`, and the
  Delta rewrite path supplies none — so its reader sees a VARIANT-annotated group as a bare struct and a
  rewrite strips the annotation. That is what `ThrowIfVariantRewrite` guards, and why setting BOTH seams
  lifts the guard.
- **Therefore**: registering the variant extension on the Delta layer's internal reader may close the same
  hole with no new public API. Unverified — shredded reassembly may need more.
- The PR ships the seam with **no implementations and no tests anywhere**; the real consumer is the
  out-of-tree `mssql_net`/ArrowNet DuckDB extension. The `CodecSeamTests` on master are the first tests it
  has ever had.

Curt has asked cmettler for more detail. Working assumption: **the seam comes out** once the variant
extension is registered. `9302723` is a clean single commit to revert; the `ProcessFileBatchesAsync`
extraction underneath is behaviour-preserving and should be KEPT either way.

Two contract gaps found while auditing (fix or delete with the seam): the writer's `relativePath` never
states whether it is URL-encoded (call site passes the RAW name; the reader's doc says "URL-decoded" —
the asymmetry invites the wrong guess), and directory creation for partition subdirs is an unstated
obligation.

### Remaining work

1. **Variant**: register the extension on the Delta layer's reader; verify the rewrite gap closes; then
   revert `9302723` (keeping `ProcessFileBatchesAsync`). If it does NOT close, the seam earns its place.
2. **`VariantTransport`** (~316 lines) — shredded read/write at the Delta layer. Independent of the seam.
3. **Slice 9** (row-level concurrency — **STRATEGIC**) — absorbs slice 8's OCC/conflict-checker material.
4. **`IDataFileRewriter`** + row-tracking-through-rewrite — only if the seam survives (1).
5. **Clustering bits coupled to `CommitDataFilesAsync`** — `dataChange`/`clusteringProvider` params and
   `WrittenDataFile.Tags` -> `add.tags`. Need the buffered-transaction API from slice 9.
6. **Stale `doc/known-issues.md`** — the VARIANT entry is wrong; the writer-feature table needs
   `clustering` and `variantType` corrected too.

## Deferred follow-ups (do after the PR-landing work)

### A. VACUUM spec alignment (`VacuumExecutor.cs`)

EW's `VacuumExecutor` diverges from official Delta (under-deletes): it only considers `.parquet` files
(so orphaned `deletion_vector_*.bin` leak forever), protects only active-add data paths, has no
tombstone model, and is file-mtime-based (ignores `delta.deletedFileRetentionDuration`).

Official algorithm (verified against delta-io/delta `VacuumCommand.scala` + PROTOCOL.md): mark-and-sweep
driven by snapshot state — build the keep-set over every FileAction (active `AddFile`s AND unexpired
`RemoveFile` tombstones), adding each one's data path AND its deletion-vector path
(`getDeletionVectorRelativePathAndSize`); list the whole table dir recursively with NO extension filter;
delete anything not in the keep-set past the cutoff. Retention defaults to
`delta.deletedFileRetentionDuration` = 1 week; tombstone protection keyed on `delTimestamp`.

Rewrite plan: (1) drop the `.parquet`-only filter; (2) keep-set = active adds' data+DV paths PLUS
unexpired tombstones' data+DV paths (use `Snapshot.Tombstones`, added in slice 3 / `b41f5ad`); (3) honor
`delta.deletedFileRetentionDuration` (default 1 week), gate tombstoned files on `deletionTimestamp`.
Behavioral change to flag to Curt: EW VACUUM will then actually delete orphaned DV `.bin` and removed
data files past retention (it doesn't today). Correct/spec-matching; default 7-day retention protects
recent files. Ship with tests. Curt's preference: match the official implementation, not EW's.

### B. Percent-encode non-ASCII in `DeltaPath.Encode`

`DeltaPath.Encode` (`src/EngineeredWood.DeltaLake/DeltaPath.cs`) currently escapes only
`% space # ?` + control chars and leaves **non-ASCII characters literal**. Spark/delta-rs percent-encode
non-ASCII (as UTF-8 bytes) in `add.path`, so an EW-written table whose partition values / paths contain
non-ASCII may not be readable by strict foreign readers — an interop gap. EW's own reader round-trips
fine (`Uri.UnescapeDataString` is a no-op on literals), so this only matters for cross-engine reads.

Fix approach (needs a short research pass first, like the VACUUM one, to confirm Spark's exact behavior):
confirm Spark UTF-8 percent-encodes non-ASCII in `add.path`, then update `DeltaPath.Encode` to
percent-encode each non-ASCII char's UTF-8 bytes as `%XX`, and verify `Decode` (`Uri.UnescapeDataString`)
round-trips. Don't guess the encoding — a wrong "fix" is worse than the current faithful port.
