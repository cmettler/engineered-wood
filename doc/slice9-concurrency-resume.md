# Resuming the Delta optimistic-concurrency work (PR #4 slice 9)

Handoff for continuing or completing EngineeredWood's Delta Lake concurrency support. Read this
first; it records the correct framing, what has landed, what remains, and the facts that were
established by *measurement* so they are not re-derived (several were wrong when reasoned from first
principles — see "Measure, don't assume" below).

## The correct framing

This is **optimistic-concurrency (OCC) correctness**, not multi-statement transactions. The PR's own
test naming (`BufferedTransactionTests`) over-emphasises fused multi-statement commits; that is one
*consumer* of the machinery, not its purpose. The heart is the classic Delta OptimisticTransaction: a
transaction records the version it read from, does its work, and at commit validates that nothing it
read was invalidated by a commit that landed in between — aborting only on a real conflict, otherwise
rebasing onto the newer version and committing.

### What master did before this work (verified)

- `DeleteAsync`/`WriteCoreAsync` compute actions against `CurrentSnapshot` and commit at
  `readVersion + 1`.
- `TransactionLog.WriteCommitAsync` uses an atomic write-temp-then-rename and throws
  `DeltaConflictException` on a name collision.
- **Nothing caught that exception.** No retry, no read-set validation, no rebase.

So master was **safe but fragile**: the atomic rename prevents lost updates, but any concurrent commit
failed an in-flight write even when there was no real conflict, and there was no notion of "did my
read stay valid". This work upgrades fragile-but-safe to robust-and-correct. It is *not* fixing active
data corruption.

## The three layers (and where "layer 1" really sits)

- **Layer 1 — OCC correctness** (the important part): read-version tracking + the ConflictChecker +
  bounded rebase-retry. This is what makes concurrent writers safe. **Steps 1 and 2 below are done.**
- **Layer 2 — full ConflictChecker parity**: the verdict rules. Landed as the checker in step 1.
- **Layer 3 — row-level concurrency** (Databricks extension): two deletes on the *same file* touching
  *disjoint rows* both land, instead of the second aborting. Needs DV re-union (`resolveAgainst:`) and
  remap-across-rewrite by stable row id. **Not started.** OSS Spark and delta-rs also conflict at file
  granularity here, so the current file-level abort is *correct*, just not maximally permissive.

The buffered multi-statement surface (`Compute*` schema ALTERs, identity chaining,
`ReconcileBatchToFields`, `ReadRowsByRowIds`) is **not needed for OCC correctness** and is deferred.

## What has landed

| Step | Commit | What |
|---|---|---|
| 1 | `705a4e2` | `ConflictChecker` (pure verdict function) + `IsolationLevel` + `ConflictCheckerTests` (9). |
| 2 | `b8e4fc6` | `DeltaTransaction` + `StartTransaction()` + the OCC commit loop + `DeltaTransactionTests` (4). |
| 3 | `7964d07` | Auto-committers routed through the OCC loop: `DeleteAsync` and the blind-append `WriteAsync` now rebase-retry instead of throwing on any collision. OCC loop generalised to `CommitOccAsync`; `AutoCommitConcurrencyTests` (6) + a rebased-commit tier-3 Spark test. |
| 4 | `3bfb9fb` | Appends + updates stageable on `DeltaTransaction` (`WriteAsync`/`UpdateAsync`), closing limitation 1. Extracted `ComputeWriteActionsAsync` (shared by `WriteCoreAsync` + txn append) and `ComputeUpdateActionsAsync` (shared by `UpdateAsync` + txn update); auto-committer `UpdateAsync` now also rebase-retries via `CommitOccAsync`. Operation-label tracking on the txn; `ValidateWritable(snapshot, isAppend)` shared gate. `DeltaTransactionTests` +5. |

Entry points:

- `src/EngineeredWood.DeltaLake.Table/Concurrency/ConflictChecker.cs` — pure, no I/O. Rules, in order:
  metadata change, protocol change, delete/delete, concurrentDeleteRead (dataChange=false compaction
  exempt), concurrentAppend (blind append exempt under WriteSerializable). Modeled on Spark's
  `ConflictChecker`.
- `src/EngineeredWood.DeltaLake.Table/IsolationLevel.cs` — public enum, `WriteSerializable` (default) /
  `Serializable`. The two differ only on whether a concurrent blind append matching read predicates
  conflicts.
- `src/EngineeredWood.DeltaLake.Table/DeltaTransaction.cs` — public; thin recorder of staged actions +
  read-set. `ReadVersion`, `IsolationLevel`, `WriteAsync`, `DeleteAsync`, `UpdateAsync`, `CommitAsync`.
  Accumulates `_dataActions` + `_removedPaths` + `_operations` (the last drives the commitInfo operation
  label: single-op → that op, mixed → "WRITE"). Several ops can be staged on one transaction.
- `src/EngineeredWood.DeltaLake.Table/DeltaTable.cs` — `StartTransaction()`, the internal
  `CommitTransactionAsync` (thin wrapper) → `CommitOccAsync` (the OCC loop), and the extracted compute
  halves each shared by an auto-committer and the transaction: `ComputeDeleteActionsAsync`,
  `ComputeWriteActionsAsync` (all write modes; the txn calls it with `Append` only), and
  `ComputeUpdateActionsAsync`. `WriteCoreAsync` = validate + compute + `CommitWriteAsync` (append →
  `CommitOccAsync`; overwrite family → single-attempt). `ValidateWritable(snapshot, isAppend)` is the
  shared write-precondition gate (protocol + writer features), validated against the txn's base snapshot.

Design facts worth keeping:

- A DELETE's read-set is exactly the files it rewrites, so the removed paths serve as **both** the
  concurrentDeleteRead read-set and the delete/delete planned-removes.
- On a no-conflict rebase the staged actions are re-committed **verbatim** — valid precisely because
  "no conflict" means nothing the transaction read or removed was touched. No action re-resolution is
  needed at file-level granularity (that is only for row-level, layer 3).
- The checker takes the concurrent commits as a parameter (stays pure/testable); the loop in
  `CommitTransactionAsync` owns reading `readVersion+1..latest` from the log.

## Deliberate limitations currently in place

1. ~~**DELETE-only on the transaction.**~~ **CLOSED (step 4).** `WriteAsync` (append) and `UpdateAsync`
   are now stageable alongside `DeleteAsync`. Overwrite modes remain auto-committer-only (their read-set
   is the whole active-file set / a partition predicate — not yet expressible for the checker; the txn's
   `WriteAsync` calls `ComputeWriteActionsAsync` with `Append` only). A transactional append is blind
   (reads empty → equivalent to `ReadSet.Blind`); an update reads the files it rewrites, exactly like a
   delete.
2. **Row-tracking tables abort on rebase.** A rebased add's `baseRowId` would need recomputing against
   the advanced high-water mark. They still commit on an uncontended first attempt; on any rebase they
   abort with a clear message. Fail-safe.
3. **concurrentAppend is inert for DELETE.** `DeleteAsync` takes a functional
   `Func<RecordBatch, BooleanArray>` predicate, not an analyzable `Expressions.Predicate`, so the
   transaction cannot feed a read predicate to `DeltaFilePruner`. `ReadSet.Predicates` is left empty
   (NOT faked with `TruePredicate`, which would wrongly conflict disjoint deletes). Under the default
   WriteSerializable this is moot. The checker already supports the predicate path (unit-tested); it
   lights up for free once DELETE gains an expression-predicate overload.

## Remaining work, in suggested order

1. ~~**Step 3 — route the auto-committers through the OCC loop.**~~ **DONE (`7964d07`).** Single-shot
   `DeleteAsync` now delegates to `StartTransaction()` + `DeleteAsync` + `CommitAsync`, and blind-append
   `WriteAsync` (mode `Append`, not dynamic-overwrite) commits through the shared `CommitOccAsync` — both
   rebase-retry instead of throwing on a non-conflicting collision, and abort with `DeltaConflictException`
   only on a real conflict. Covered by `AutoCommitConcurrencyTests`. **Deliberately NOT rebased:** the
   overwrite family (full / partition-scoped / dynamic) keeps the single-attempt commit — its remove-set is
   a read of the active-file set, so a verbatim rebase could silently drop a concurrent append; making it
   rebase-safe needs the partition-predicate plumbing of step 3-below (was limitation 3). Row-tracking
   writes also stay single-attempt (`rebaseSafe: false`) for the baseRowId reason (limitation 2).
2. ~~**Appends/updates on the transaction**~~ **DONE (`3bfb9fb`).** `DeltaTransaction` now
   has `WriteAsync` (blind append) and `UpdateAsync` in addition to `DeleteAsync`, via the extracted
   `ComputeWriteActionsAsync` / `ComputeUpdateActionsAsync` (each shared with its auto-committer). Two
   concurrent transactional appends both land; a transactional update aborts only if a concurrent commit
   removed a file it rewrote. Auto-committer `UpdateAsync` gained rebase-retry (routed through
   `CommitOccAsync`) as a side benefit. `DeltaTransactionTests` +5. Still NOT stageable: overwrite modes
   (see limitation 1).
3. **Expression-predicate DELETE/UPDATE overload** (closes limitation 3) — makes concurrentAppend
   precise, and is the prerequisite for rebasing partition/dynamic overwrite (partition read-set).
4. **Layer 3 — row-level concurrency.** The Databricks extension. Largest remaining piece; needs
   row-tracking-through-rewrite in EW's own rewrite path (confirm master's state first — the write path
   assigns baseRowId per file but the copy-on-write *rewrite* path likely does not preserve original
   ids). Corresponds to the 7 parked `RowLevelConcurrencyTests`.

The parked ledger is `test/EngineeredWood.DeltaLake.Table.Tests/PendingCoverageTests.cs`. The 7
`LogicalRebase` stubs are retired (now live in `ConflictCheckerTests` + `DeltaTransactionTests`); the
`RowLevelConcurrency`, `BufferedTxn`, `SetSchema`, and `CommitDataFiles` stubs remain.

## Measure, don't assume

This effort repeatedly found that reasoning about another implementation's behaviour was wrong, and
measuring corrected it. During slice 9 specifically: the VACUUM landing-notes plan said to protect
unexpired tombstones — Spark measurement showed it must not. Before starting layer 3, **verify against
Spark** (tier 3) rather than trust the PR notes or these notes. The interop tiers make measuring cheap;
use them.

**Step 3's cross-engine posture (measured).** Step 3 deliberately introduces **no new on-disk artifact**: a
rebased commit is byte-identical to a sequential one (same add/remove actions, a higher version number). The
only new *policy* is the blind-append conflict rule, which lives in the step-1 `ConflictChecker` (modeled on
Spark's own) and is unit-tested. The claim that a rebased commit reads correctly in a foreign engine was
**measured, not assumed**: `SparkInteropTests.EwConcurrentAppends_Rebased_SparkReadsAllRows` drives two racing
EW handles so the second commit rebases through `CommitOccAsync`, then has Spark 4.0.1 / delta-spark 4.0.0 read
the table — all rows present, versions consecutive. The full tier-3 suite (13) and delta-rs tier (9) also pass
against the rewired write path. Running it needs `JAVA_HOME` (JDK 17) + `HADOOP_HOME` (winutils) + `EW_REQUIRE_SPARK_INTEROP=1`.

## Running the tests

- Concurrency unit + integration tests are pure/local — no external toolchain:
  `dotnet test test/EngineeredWood.DeltaLake.Table.Tests -f net10.0 --filter "FullyQualifiedName~ConflictCheckerTests|FullyQualifiedName~DeltaTransactionTests"`
- Full validation uses the Delta interop tiers (delta-rs + PySpark). Setup and the
  `EW_REQUIRE_DELTA_INTEROP` / `EW_REQUIRE_SPARK_INTEROP` CI flags are in `doc/running-tests.md`. Tier 3
  needs `JAVA_HOME` (JDK 17+) and, on Windows, `HADOOP_HOME` with winutils on `PATH`.
- The Spark tier is slow (~25s); running the whole `DeltaLake.Table` suite twice across TFMs can exceed
  a 10-minute command budget. Filter out `SparkInteropTests` when iterating on non-interop code.

## Broader context

This slice 9 work is one thread of the larger PR #4 landing + Delta interop-hardening effort tracked in
`doc/upstream-landing-notes.md`. That doc also lists the unrelated remaining items (the ORC/Avro/Iceberg
sections of `doc/known-issues.md` were never verified against code; small interop divergences).
