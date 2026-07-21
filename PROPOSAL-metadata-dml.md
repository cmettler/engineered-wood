> 🅿️ **PARKED, revisit later (2026-07-21) — no longer load-bearing, still a good idea.** Curt landed PR#4
> parity on clast/master (23 commits, `45cced1..e48f449`) including the rowid DML with our fork's names/
> encoding — so fabricator no longer NEEDS this to run on master. But the proposal's value was never only
> that: the spec-aligned `_metadata` struct (Spark's `file_path`/`row_index` vocabulary), the zero-read
> selection DELETE, and the symbolic predicate lowering remain a clean ADDITIVE layer over the landed
> rowid core (master now has predicate DML + rowid DML; this would be the third, spec-facing surface —
> and it retires the fossil `_metadata.row_id`-as-transient-locator naming at the engine boundary).
> Revisit after the fabricator re-pin migration settles. The prototype (this branch: `72f2d3d` +
> `2780334`, MetadataDmlTests 11/11, suite 339/339 at base `45cced1`) is the reference implementation;
> reviving it means rebasing over Curt's rowid landing (overlapping regions — real but bounded conflicts).

# Proposal: `_metadata` (file_path, row_index) — reads that carry it, DML that consumes it

Draft RFC for clast-project/engineered-wood (to be posted as an issue / PR description).
Prototype: branch `proto/metadata-dml` (TWO commits on top of master: `72f2d3d` read+delete,
`2780334` update+lowering) — builds green, `MetadataDmlTests` 11/11, full `DeltaLake.Table.Tests`
339/339. ALL THREE layers are now prototyped (the "follow-on" sections below are implemented).

## Summary

Expose the `(file, position)` layer the DELETE machinery **already computes internally** as a first-class,
Spark-aligned surface:

1. **Reads can carry `_metadata`** — a struct column of `file_path` (the log `add.path`) and `row_index`
   (the row's ABSOLUTE physical parquet index — Spark's `_metadata.row_index` semantics: DV-masked rows
   are excluded from the output but still counted, so a surviving row's index is stable across DV deletes).
2. **DML can consume positions directly** — `DeleteAsync(FileRowSelection)`: delete exactly the named
   `(file, positions)` rows. On a non-CDF table this needs **zero data reads** (deletion-vector union +
   commit); a CDF table reads only the *selected* files, to capture change-feed content.
3. (follow-on) **The predicate analyzer lowers `_metadata` conjuncts onto (2)** — `_metadata.file_path`
   equality → exact file selection with no read; `_metadata.row_index` sets → positions; mixed
   metadata+data predicates → metadata selects files, the data residual masks within them.

## Why

- **It's Spark's vocabulary.** Every Delta user knows `_metadata.file_path`/`row_index`; engineered-wood
  currently has no way to ask "which file/row did this come from" — useful for provenance, file-targeted
  repair, debugging, per-file sampling, and CDC-style tooling, independent of any one client.
- **The DELETE machinery is already position-based one layer down.** `ComputeDeleteActionsAsync` reads
  each candidate file, evaluates the mask, and collects `rowOffset + i` into `newDeletedIndices` → DV
  union → `remove` + `add` + `DeleteDvEdit(path, positions)`. The mask front-end is just one way to
  *produce* positions. Callers that already know exact positions (an engine whose scan carried
  `_metadata`; a host database that identifies delete targets by row) currently have no way to hand them
  over — they must re-express them as a predicate, which forces a **full re-read of every non-pruned
  active file** to re-derive information the caller already had.
- **The efficiency delta is structural, not incremental.** DELETE-by-positions: zero data reads.
  DELETE-by-predicate: O(active files) reads (stats pruning helps only for stats-expressible predicates —
  and a position set is not one). Even Spark's own predicate DML materializes `(file, row_index)`
  internally before writing DVs; `DeleteDvEdit` records exactly that pair for row-level concurrency.

## Design

### `FileRowSelection` — the lowered form
```csharp
public sealed record FileRowSelection(
    IReadOnlyDictionary<string, IReadOnlyCollection<long>> RowsByFile);
```
Keys = the log `add.path` (URL-encoded, as in the snapshot). Values = absolute physical row indexes.
**Position semantics pinned:** parquet row index counting DV-masked rows (matching both Spark and the
existing `rowOffset + i` in the delete loop) — this is load-bearing for DV union.

### `DeleteAsync(FileRowSelection)` / `DeltaTransaction.DeleteAsync(FileRowSelection)`
`ComputeDeleteActionsForSelectionAsync` = the mask path's tail, fed positions directly:
- resolve each path against the active set (stale path → clear error: "re-derive from a fresh scan");
- read the file's existing DV; skip requested positions already masked (union semantics, never
  double-counted — identical to the mask path's `rawDeletedRows` skip);
- bounds-check positions against stats `numRecords` (when present); whole-file detection likewise
  (all physical rows dead → plain `remove`, no DV — legal even with DVs disabled, same as the mask path);
- partial + DVs disabled → the same clear rejection as the mask path;
- emit `remove(path, oldDv)` + `add(path, unionedDv)` + `DeleteDvEdit(path, newPositions)` — byte-identical
  commit shape and row-level-concurrency records to a predicate delete;
- CDF: read ONLY the selected files (raw, absolute offsets) to capture deleted rows' content — O(selected),
  not O(active);
- staged/committed via the existing `DeltaTransaction` route (removed-file read-set, ConflictChecker,
  OCC) — nothing new in the concurrency story.

### `ReadAllWithMetadataAsync`
Per active file: raw read (absolute offsets) → logical renames → DV filter keeping survivor absolute
indexes → append the trailing `_metadata` struct. Values round-trip into a `FileRowSelection`.

### Follow-on layers (NOW IN THE PROTOTYPE, commit `2780334`)
- **UPDATE analog** — `UpdateAsync(FileRowSelection, updater)` + transaction overload: positions select
  files; each selected file is read RAW (not DV-filtered) so the caller's positions land on the right
  rows; DV-dropped / matched / kept split per batch; a DV-masked position matches nothing (the
  concurrently-deleted-row semantics — a no-match update commits nothing). The per-file rewrite tail was
  extracted VERBATIM from `ComputeUpdateActionsAsync` into a shared `RewriteUpdatedFileAsync` (identical
  commit shape: partition-dir placement, `DataFileWriter` seam, stats, remove+add, CDC pre/post) —
  behavior-preserving by construction (full suite green after the extraction).
- **Symbolic predicate lowering** — `MetadataPredicate.TryLower`: `(file_path = 'p' AND row_index IN (…)
  | = k)`, OR-combined per file (same file unions), wired at the head of `DeleteAsync(Predicate)` /
  `UpdateAsync(Predicate)`. A pure-metadata DELETE runs with zero data reads (test-pinned); a predicate
  that references `_metadata` but cannot lower is REJECTED loudly (the row mask binds data columns only —
  silent fallback would mis-evaluate). Mixed metadata+data residuals and a set-valued literal node
  (millions of positions ≠ a giant expression list) remain the open design points.
- **Production `_metadata` read** (still open): fold into `ReadFileAsync`/`ProcessFileBatchesAsync`
  (projection, partition-column re-add, schema-evolution backfill — the prototype's standalone loop
  mirrors the delete loop's read fidelity and documents these limits).

## What the prototype proves (tests)
- `DeleteBySelection_PartialFile_ZeroDataReads_AndUnions` — a parquet-open-counting filesystem observes
  **0 data-file opens** across two selection deletes; the second unions with the first's DV.
- `DeleteBySelection_AllPositions_DropsTheWholeFile` — whole-file detection via stats → plain remove.
- Stale path / out-of-range positions → clear errors.
- `ReadAllWithMetadata_EmitsFilePath_AndAbsoluteRowIndex_AcrossDvDeletes` — the full round-trip: read
  `_metadata` → build a selection from it → delete → survivors keep their ABSOLUTE indexes (30 stays at
  index 2, 50 at 4 after positions 1,3 are deleted).

## Open questions for review
1. Struct vs flat columns for the read (`_metadata` struct matches Spark; flat `_metadata.file_path`/
   `_metadata.row_index` columns are simpler for some consumers).
2. Where the row-tracking write fence applies: a selection DELETE is a pure DV operation (no rewrite, no
   row moved), so it arguably composes with `delta.enableRowTracking` even before the preservation port —
   worth deciding explicitly rather than inheriting the blanket `RejectRowTrackingWrite`.
3. `numRecords`-less adds (foreign tables without stats): the prototype skips bounds/whole-file checks
   there; alternative = read the footer's row count (one footer read, still no data pages).
4. Naming: `FileRowSelection` / `DeleteAsync(selection)` vs `DeleteByPositionsAsync`.
