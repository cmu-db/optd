# Shared observed execution for DML and DDL

## Contract

Optd is a metadata/statistics cache, not the source transaction authority. Enabling
persistent metadata no longer replaces a source table with a DML-rejecting wrapper.
The original table provider decides which DML operations are supported. Existing
write-through DDL still rejects views and populated-schema CASCADE until durable
operations support them; CASCADE rejection happens before runtime removal. Source
writes and metadata maintenance do not share a transaction.

The opt-in common entry points are:

```rust,ignore
use optd_datafusion::observed_execution::execute_observed_sql;

let execution = execute_observed_sql(&session, "UPDATE numbers SET x = x + 1").await?;
// Inspect this even when the DataFusion result is successful.
let observation = execution.observation;
let source_result = execution.result;
```

`execute_observed_plan` accepts an already-resolved logical plan and returns the
same envelope. Both helpers execute through collection, not merely through plan
construction. The outer SQL result represents parse/bind errors before execution;
the envelope preserves the DataFusion/source execution result separately from
metadata observation. Do not replay SQL to repair an observation failure.

`OptdRunner` uses this path for DataFusion fallback execution, including DML, DDL,
optd-disabled execution, and runners using runtime statistics. Persistent bindings
are discovered from installed catalog providers rather than inferred from the
runner's statistics configuration. `last_observation()` exposes the latest report;
metadata failures also emit a diagnostic. Concurrent clients requiring per-call
reports should use the shared helper instead of the runner's last-report slot.

## DML

After expanding prepared statements and before collecting the DataFrame, the helper
captures the actual DML target, durable catalog/relation IDs, and binding identity.
It does not invalidate tables merely read by INSERT SELECT or CTAS. Captured source
provider identity must still match the current binding: a plan built before a
same-named table was dropped/recreated must not invalidate its replacement's stats.
Dynamic providers that cannot establish this identity need explicit notification.

The source executes with its original capabilities. The helper then invalidates
the active basic payload and all advanced objects for the captured relation in one
metadata transaction. Definitions and historical column values remain, object IDs
are not allocated, and `schema_version` does not advance. No new freshness flag or
basic statistics schema change is needed.

Source errors can represent partial/eager writes. Without source-specific evidence
of rollback/no effect, the helper conservatively invalidates on execution errors
and zero-row operations too. The original source error is retained. This is an
observation of a potentially mutating attempt, not a claim of source COMMIT.

Invalidation reports are:

- `Applied`: metadata snapshot plus captured request.
- `Pending`: source execution ran, but metadata maintenance failed; includes a
  request that can be retried without repeating source SQL.
- `Unavailable`: metadata/identity capture failed; the source still executed. No
  stable target ID is safely inferred afterward. Reconcile and notify explicitly.
- `Untracked`: no installed persistent binding or durable relation exists.
- `NotApplicable`: the executed statement is not a tracked DML attempt.

`retry_invalidation` is session-binding-specific. Uninstall/reinstall or replacement
invalidates that ephemeral binding token, even if its numeric catalog ID happens
to match. Apply explicit invalidation against the original metadata database after
reconciliation instead. Up to three fresh-base attempts retry only `StaleSnapshot`;
ambiguous database/COMMIT errors are returned, not automatically replayed. Retries
may conservatively expire newly recollected statistics; no exactly-once metadata
delivery or durable retry queue is promised.

## DDL uses the same execution boundary

DDL enters the same classifier/executor/report scope. Existing catalog/schema
write-through wrappers remain the sole owners of DDL persistence and compensation;
the observer does **not** publish a duplicate create/drop or invalidation snapshot.
A relation drop closes its basic payload and advanced objects inside the existing
DropRelation commit.

Task-scoped receipts record actual create/drop schema/table callbacks:

- `Confirmed`: persistence callback succeeded, including accepted idempotent imports.
- `Noop`: callback found no runtime object to remove.
- `Failed`: persistence failed, with any compensation errors reported separately.

An empty receipt list does not mean every DDL effect was mirrored. IF EXISTS/IF NOT
EXISTS can avoid callbacks, and catalog/function DDL or untracked namespaces do not
use these wrappers. Inspect statement kind, actual receipts and source result.

This distinction matters because DataFusion 53 may suppress provider errors for
`DROP TABLE IF EXISTS`, returning success after a failed metadata drop was
compensated. The receipt still reports failure. CREATE OR REPLACE may successfully
mirror a drop and then fail while computing its replacement; the report retains
that committed drop rather than pretending the whole DDL was rolled back.

Provider API calls outside the helper still perform existing DDL write-through,
but no per-execution receipt envelope is collected there.

## EXPLAIN, prepared statements, and limits

DataFusion 53 MemTable UPDATE/DELETE can mutate during physical planning, while
INSERT returns a lazy sink. Plain EXPLAIN of mutating plans therefore uses a
**logical-only indent explanation**, regardless of requested physical/TREE format,
without changing session-wide configuration. This prevents physical EXPLAIN from
invoking native mutation hooks. EXPLAIN ANALYZE executes its child and is observed
as a write. PREPARE alone does not invalidate; EXECUTE is inspected after expansion.

Raw `SessionContext::sql(...).collect()`, raw provider calls, external clients,
side effects hidden in user-defined functions/custom extension plans, and source
transaction commits outside this boundary are not automatically observed. The
source remains writable; callers must explicitly notify optd when needed. The
helper does not implement BEGIN/COMMIT/ROLLBACK tracking or infer that statement
success means source-transaction commit. Cancellation/dropped execution futures
are outside completion guarantees and require reconciliation/explicit notification.

No automatic recollection or compatible-historical-statistics fallback is added.
After successful invalidation, current planning uses default estimates until
`collect_stats` or explicit advanced publication. Stale fallback is a separate
optimizer-policy follow-up.

## Explicit API

The single-relation `invalidate_relation_statistics` API remains available. A new
batch form validates every catalog-scoped target before any closure, canonicalizes
and deduplicates IDs, rejects empty batches, and publishes one nonstructural snapshot:

```rust,ignore
catalog.invalidate_statistics(
    expected_snapshot,
    InvalidateStatisticsInfo {
        relation_ids: vec![orders_id, customers_id],
        cause: StatisticsInvalidationCause::Explicit,
    },
).await?;
```

The observer uses `ObservedSql` as its audit cause. Causes live in typed change
summaries, not new flags on statistics. Historical single-relation summary JSON
remains decodable. Atomicity is per metadata database/catalog request, never across
independent source systems.

## Verification surface

Tests are in `observed_execution/tests.rs`, persistent-catalog/runner unit tests,
and `optd/catalog/tests/statistics_invalidation.rs`. They check native row effects,
basic/advanced historical boundaries, actual DDL receipts, no duplicate publication,
prepared/stale plans, logical-only EXPLAIN, ANALYZE, namespace targeting, source and
metadata failures, metadata-only retry, batch rollback and old summary decoding.
A deterministic capture gate also verifies DROP/recreate/recollection between the
first provider check and metadata lookup cannot invalidate replacement statistics.

Run focused checks with:

```sh
cargo test -p optd-datafusion --lib observed_execution --locked
cargo test -p optd-catalog --test statistics_invalidation --locked
```
