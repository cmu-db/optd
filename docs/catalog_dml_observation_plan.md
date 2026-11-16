# Plan: preserve source DML and invalidate observed statistics

Status: implemented; the selected public boundary is a shared observed executor
for BOTH DML and DDL, used by the runner and available to other callers. Focused
post-fix tests, all 96 release connector library tests, 17 release feature SLTs,
and workspace Clippy passed. Source-specific transaction notifications and
cancellation reconciliation remain outside this implementation. See
`observed_execution.md` for the contract and verification record.

## Objective and boundaries

Allow each source provider's native writes while maintaining optd's statistics
cache when explicitly notified or when writes are observed through an optd-owned
SQL execution boundary. Optd is not the source-of-truth catalog, does not own source
transactions, and cannot promise atomicity between source writes and metadata.

Preserve:

- Existing basic definition/payload/column-statistics tables and their public API.
- The additive `optd_adv_stats` model, catalog namespaces, global writer protocol,
  and single metadata snapshot history.
- DDL write-through and compensation as they currently exist; schema CASCADE is
  an unrelated known limitation.
- Native provider capabilities and errors, including unsupported operations.

Do not implement automatic recollection, statistics deltas, stale-statistics
fallback, new freshness flags, source data snapshots, or optd-backed discovery
providers in this change. Do not require an atomic source/catalog commit or add a
background reconciliation service.

## Contract

1. Explicit invalidation and observed SQL writes enter the same catalog primitive.
2. A successful invalidation closes the active basic payload and all active advanced
   objects for each affected stable relation ID at one new metadata snapshot.
3. Basic definitions, historical column values, and relation/schema/column definitions
   remain unchanged. No catalog IDs are allocated; `schema_version` does not advance.
4. Recollection remains explicit. Basic recollection preserves its definition ID;
   advanced objects are recreated under the existing retirement policy.
5. Current planning falls back to defaults after invalidation. Reusing compatible
   historical statistics is a separate follow-up, not part of removing DML rejection.
6. Source writes are not rejected merely because persistence is enabled. Failure to
   invalidate is reported separately from the source write outcome; it must not imply
   that a successful source write was rolled back or should be replayed.
7. External writes and unobserved entry points require explicit notification. Metadata
   snapshot visibility is not proof of source-data freshness.

## Selected observation boundary

Prefer a common **observed execution helper**, used by `OptdRunner` and exposed to
callers that execute SQL outside the runner. Resolve targets from the logical plan
before execution, run through the original source providers, then report write
outcomes and invalidate through the catalog API. Do not parse SQL strings to infer
mutation targets or re-resolve names after execution.

An arbitrary `SessionContext::sql(...).collect()` outside that helper is not
transparently observed under this proposal, just as writes from external clients
are not observed. It still retains native write capabilities.

If automatic observation of arbitrary direct-session SQL is required, first prove
an appropriate DataFusion execution/transaction hook. A runner-only observer does
not meet that stronger contract, and a transparent table wrapper cannot assume
that returning an `ExecutionPlan` means a write committed. In the pinned DataFusion
53.1.0 implementation, MemTable INSERT returns a lazy sink plan, while UPDATE and
DELETE can mutate during their provider method. The implementation must account
for both without marking ordinary EXPLAIN/planning as a successful write.

This shared boundary was confirmed before implementation, including DDL. Actual
DDL callback receipts are collected in the same per-execution report without replaying
provider-owned persistence. Do not promise coverage of raw sessions, raw provider
calls, or external transactions.

## API direction

Keep `Catalog::invalidate_relation_statistics(expected, relation_id)` as a
convenience API. Add a batch form if the executor can report multiple affected
relations together:

```rust,ignore
catalog.invalidate_statistics(
    expected_snapshot,
    InvalidateStatisticsInfo {
        relation_ids: vec![orders_id, customers_id],
        cause: InvalidationCause::ObservedWrite,
        // Explicit for a caller-requested invalidation.
    },
).await?; // returns the new metadata snapshot
```

The cause belongs in a typed change summary, not in an extra statistics flag.
Preserve decoding of existing summaries; do not rewrite historical records.
Canonicalize/deduplicate IDs and validate all catalog ownership before applying
any closure. Reject an empty batch rather than creating meaningless history.

The explicit primitive retains expected-snapshot concurrency checks. An observer
may reread and explicitly retry **only invalidation**, with a bounded retry policy,
when its base becomes stale. It must never replay the source SQL. A retry may
conservatively invalidate statistics recollected during the observation delay;
this is preferable to silently losing a known invalidation and is not a strong
freshness guarantee. An uncertain metadata COMMIT outcome must be reported or
inspected, not represented as exactly-once delivery.

## Implementation sequence

### 1. Establish behavioral tests first

Replace the blanket-rejection regression with a narrow INSERT test showing that
enabling persistence must not remove native source write support. Confirm it fails
against the present wrapper. Add a small provider test double for deterministic
execution success, pre-write failure, partial failure, and lifecycle timing.

### 2. Finish the catalog notification surface

Relevant paths:

- `optd/catalog/src/catalog.rs`
- `optd/catalog/src/snapshot/{commit,mod}.rs`
- `optd/catalog/src/statistics/set_relation_statistics.rs`
- `optd/catalog/src/advanced_statistics.rs`

Reuse the existing payload/advanced closure helpers under the shared writer mutex.
Add batch validation and explicit/observed audit reasons without basic-schema
changes. Ensure one failing target or summary insertion rolls back every closure,
allocator/snapshot state and summary publication.

### 3. Remove rejection without changing source behavior

Relevant paths:

- `optd/connectors/datafusion/src/persistent_catalog.rs`
- `optd/connectors/datafusion/src/persistent_catalog/table.rs`

For the recommended observed-executor boundary, return the original source table
provider from schema lookup and delete the rejection-only wrapper. Keep catalog
and schema DDL write-through wrappers. Do not emulate unsupported provider DML.

If a transparent observer wrapper is instead chosen, it must delegate all relevant
capabilities and pass the lifecycle tests before replacing the current wrapper.

### 4. Wire SQL observation once, across runner branches

Relevant paths:

- `optd/connectors/datafusion/src/runner.rs`
- The shared observed-execution helper to be added after choosing its public boundary.

Capture stable target IDs and durable catalog bindings before side effects. Cover
INSERT/UPDATE/DELETE/TRUNCATE and supported overwrite/MERGE forms by resolved plan
targets, not spelling or raw SQL text. Unregistered/non-persistent targets have no
optd statistics to invalidate. Do not invalidate source tables merely read by a
write statement. CTAS creates a new relation through existing DDL mirroring and
must not invalidate its input relations.

Do not implement monitoring only in the optd-IR path: DML normally runs through the
DataFusion fallback, and optd-disabled execution must still be observed when the
caller explicitly uses the observed boundary. Prevent duplicate notifications
between runner and shared executor.

Outcome rules:

- Confirmed completed/autocommitted write: notify/invalidate.
- Confirmed no write or confirmed full rollback: no invalidation required.
- Partial or uncertain write: conservative invalidation; preserve the original
  source error and expose invalidation status separately.
- A provider-reported zero affected rows can skip invalidation only when that is
  reliable evidence of no mutation; otherwise conservative invalidation is valid.
- EXPLAIN and merely constructing a plan are not successful-write notifications.
- For a source transaction, statement success is not necessarily COMMIT. Buffer
  notifications until a real commit boundary if supported; do not claim transaction
  coverage without a trustworthy source callback. Unsupported source transaction
  lifecycles use explicit notification rather than inferred rollback guarantees.

Return/report source outcome separately from cache-maintenance status. A successful
source write plus metadata failure should yield an explicit diagnostic/replayable
invalidation request, not a generic error encouraging callers to retry the SQL.
No durable retry queue is promised in this scope.

### 5. Update documentation and review records

Update the fail-closed DML statements in:

- `docs/datafusion_catalog_integration.md`
- `docs/catalog_single_history.md`
- `docs/persistent_statistics_demo.md`
- `docs/advanced_statistics.md`
- `catalog_pr_review.md`

Mark earlier acceptance records as historical rather than presenting the old
rejection tests as verification of the new behavior. Document observation coverage,
manual notification requirements, failure diagnostics, and current fallback-to-
defaults behavior. Keep the provider's unsupported-operation limitations explicit.

## Verification matrix

- Native supported INSERT, UPDATE and DELETE through the observed runner/helper:
  rows really change, and both basic/advanced intervals close at one snapshot.
- Native unsupported overwrite/TRUNCATE/provider operations retain their errors.
- Basic definitions and physical layouts are untouched; snapshots allocate no new
  object IDs on invalidation and do not change `schema_version`.
- Pinned historical readers retain old basic values and every advanced object.
- Explicit and observed invalidation have the same cache effect and distinct audit
  origins. Multiple objects/relations close atomically and repeated notification is
  conservative. Mixed valid/invalid targets roll back the entire batch.
- Non-default catalogs, identical table names across catalogs, aliases and quoted
  identifiers resolve to the intended stable IDs; foreign/source-only reads do not
  invalidate another relation.
- Native writes through unobserved raw sessions remain possible; under the proposed
  boundary they require explicit notification and must not be presented as covered.
- Lazy INSERT plans, eager provider mutations, EXPLAIN, no-op writes, definite
  failure, partial writes and source transaction boundaries follow the declared
  observation contract.
- Metadata failure after source success does not rerun source SQL or report a
  fictional rollback. Stale observer bases, concurrent collection, bounded retry
  exhaustion and summary insertion failures have explicit outcomes.
- Recollection updates basic estimates after writes; advanced collection remains
  independent. No test assumes ended statistics are used by the current optimizer.
- DDL persistence, idempotent import, uninstall/reinstall, namespace isolation and
  non-finite bounds regressions remain passing.

Commands after implementation:

```sh
cargo test -p optd-catalog --locked
# Repeat with OPTD_TEST_POSTGRES_URL set to an isolated disposable LOCAL instance.
cargo test -p optd-datafusion --lib persistent_ --locked
# Include focused observed-executor tests if their names use a different filter.
cargo test --release -p optd-datafusion --test slt --locked -- features/
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo fmt --all --check
git diff --check
```

The exact SLT subset should follow the changed execution path and repository
fixtures. No production database or existing user container is needed.

## Commit and stop policy

No checkpoint commit is necessary merely to write/review this plan. Preserve all
existing staged/unstaged/untracked work; do not reset or stash the earlier catalog
changes. If implementation needs a checkpoint, review the complete diff and
explicitly separate existing catalog work from this behavior change before staging.
Do not use `git add .` to absorb unrelated work, and do not push.

During implementation, start with the narrow failing DML regression, then verify
the catalog primitive and execution lifecycle independently before widening tests.
If no reliable observation boundary exists for the requested direct-session/source
transaction coverage, stop with the hook investigation and request a narrower or
source-specific contract rather than retaining hidden write rejection or claiming
unobserved writes are covered.
