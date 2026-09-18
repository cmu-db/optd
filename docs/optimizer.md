# Optimizer Architecture

## Scope and Sources of Truth

The framework lives in `optd/core/src/optimize/`. The DataFusion connector assembles the production
pipeline in `optd/connectors/datafusion/src/runner.rs::default_pass_manager`; use that function as
the authoritative pass list and order.

Related design documents:

- `docs/analysis_framework.md` — demand-driven analyses
- `docs/holistic_unnesting.md` — unnesting design
- `docs/join_ordering_design.md` — join enumeration and costing
- `docs/query_hypergraph.md` — join hypergraph representation

## Append-Only Invariant

Optimization treats existing `OperatorData`, `ExprData`, and `ColumnData` payloads as immutable.
A pass appends replacement nodes to `OptimizerContext::query` and records old-to-new operator
mappings in `OptimizerContext::rewrites`. Parents and the query root are then materialized with the
resolved inputs.

This preserves analysis results associated with old handles: their payloads never change. Use
`QueryContext::add_operator`, `add_expr`, and `add_column`, or the payload `.add(...)` helpers, rather
than mutating an existing node during a normal rewrite.

## Pass Interfaces

All passes implement `Pass`, which supplies a stable name. There are two implementation levels:

- `QueryPass` rewrites or analyzes a whole query and returns `PassResult::Changed` or
  `PassResult::Unchanged`.
- `OperatorRewrite` handles one operator and returns `Rewrite::Keep` or
  `Rewrite::Replace(new_operator)`. `OperatorRewriteAdaptor` turns it into a `QueryPass` and owns
  traversal, input resolution, parent rebuilding, and rewrite-map updates.

`OperatorRewrite` defaults to bottom-up traversal and may opt into top-down traversal through
`Direction`. Traversal visits each reachable operator handle once per pass invocation. Shared inputs
are visited once, and rewritten paths are materialized through the rewrite map.

A rule must return `Rewrite::Keep` when it made no change. A `QueryPass` must likewise return
`PassResult::Unchanged` once stable. Reporting a change unconditionally prevents convergence and,
when an iteration limit is configured, produces `OptimizeError::MaxIterationsReached`.

## Rewrite Map and Root Materialization

`RewriteMap::replace(old, new)` records a replacement, and `RewriteMap::resolve(op)` follows a chain
to its latest operator. The adaptor rebuilds reachable parents whose inputs resolve to replacements.
After a changed pass invocation, `PassManager` resolves and updates the query root.

Replacements may leave unreachable nodes in the arenas. This is intentional: deduplication and
compaction are not responsibilities of `QueryContext`.

## Pass Manager

`PassManager` runs passes in registration order. Each individual pass runs to fixpoint before the
next pass starts. `PassManager::new()` has no iteration limit;
`PassManager::with_max_iterations(n)` applies a per-pass safeguard.

Every invocation records a `PassProfile` with pass index, pass-local iteration, result, and duration.
`run_with_trace` additionally snapshots the query after each invocation for optimizer explain
output.

## DataFusion Pipeline

At the time of writing, `default_pass_manager` registers:

1. `SubqueryToJoin`
2. `ExprSimplify`
3. `HolisticUnnesting`
4. `MarkJoinToSemiJoin`
5. `PredicatePushdown`
6. `JoinTreeNormalize`
7. `ProjectionElimination`
8. `JoinOrdering`

Do not duplicate this list in operational instructions; it changes as the optimizer evolves. Update
this section when the architecture changes, while treating `default_pass_manager` as canonical.

## Adding or Changing a Pass

1. Implement the pass under `optd/core/src/optimize/` and add its module/re-export.
2. Implement `OperatorRewrite` for a local rule or `QueryPass` for a whole-query rewrite.
3. Re-export public APIs from `optd/core/src/lib.rs` when needed.
4. Register the pass in `default_pass_manager` at the semantically correct point.
5. Add narrow core tests; add SLT coverage when behavior is observable through DataFusion.
6. Verify convergence and run checks appropriate to the changed surface in `docs/development.md`.
7. Update this document when the pipeline or framework contract changes.

Ordering is semantic. For example, expression cleanup and unnesting must occur before passes that
rely on explicit join shapes. Add or update a pipeline-order test when introducing such a dependency.

## Profiling

Build and run the profiling binary with an optional run count (default `100`):

```sh
cargo build --release -p optd-datafusion --bin profile_passes
./target/release/profile_passes [runs]
```

Output is TSV with query, run, iteration, pass index, pass name, result, and duration. Measure before
encoding performance claims in documentation; do not preserve point-in-time timings as permanent
agent instructions.
