# Optimizer observation toolkit

Status: partially implemented. The initial in-memory collector is available in
`optd_core::analysis::observability`; its deliberately narrow detailed coverage is documented
below.

## Scope and decision

Replace ad hoc estimator stderr output with a small, opt-in toolkit for collecting typed
observations during one optimization run. This is an academic prototype: favor useful records,
simple ownership, and focused tests over a production telemetry framework.

This stack implements collection and inspection only. Dashboard integration, debugging interfaces,
EXPLAIN ANALYZE, durable artifact schemas, cross-run matching, exporters, and tracing infrastructure
are not deliverables. The records should be usable by later consumers without implementing those
consumers now.

Related contracts: [analysis framework](analysis_framework.md),
[statistics architecture](statistics_architecture.md), and [optimizer](optimizer.md).

## Ownership

Create one `ObservationSession` per observed optimization run. The session owns configuration,
append-only records, scope metadata, and soft-limit accounting. Estimator and optimizer code receive
a lightweight recorder referencing that session; do not put diagnostic fields in `Estimate`,
`CardinalityProfile`, logical facts, or catalog statistics.

Analysis-context forks participating in the same run share the session but have distinct context
identities. Separate runs have separate sessions. Avoid global mutable collectors and caller-supplied
observer callbacks. The current implementation is single-threaded; use ordinary shared ownership
and interior mutability consistent with the analysis framework, not speculative concurrency machinery.
A future parallel optimizer can revisit this boundary.

Suggested module: `optd/core/src/analysis/observability.rs`. The public API is conceptually:

```rust
pub enum ObservationMode {
    LastPlanSummary,
    DetailedSummary,
}

pub struct ObservationSession { /* shared, internally mutable state */ }

impl ObservationSession {
    pub fn new(mode: ObservationMode) -> Self;
    // Expose a borrowed read view, not mutable storage or an implicit full-buffer clone.
}
```

Exact attachment/read-view signatures should follow the repository's borrowing conventions.
Enable collection when constructing the optimization run; do not invalidate caches just to enable
logging mid-run. No runtime mode switches in the initial implementation. Observation is off unless
a caller explicitly creates and attaches a session.

## Two modes

### LastPlanSummary

After selection of the final root, walk its reachable operator DAG once and collect summary
statistics: operator and input identities, operator kind, row estimate, estimate source, available
cost, and column-profile summaries. Shared inputs appear once. No exploration history or detailed
estimator decisions are collected in this mode.

Use the selected plan's retained estimates where available, including cached estimates. If a missing
estimate is computed after selection, label it as a post-selection computation, not an estimate
that influenced selection. With no selected root, report that no final-plan summary is available.
Do not silently substitute the last candidate visited.

### DetailedSummary

Collect actual estimator decisions and summaries for considered subplans, including alternatives,
cache reuse, and selection/pruning information where the optimizer supplies it. Consideration is
recorded at optimizer entry points, not only on estimator cache misses. A reused estimate references
its original result; it does not duplicate the derivation. Never invent costs or estimates for a
candidate pruned before those quantities were computed.

The target includes every subplan considered by Cascades when that optimizer is integrated. The
current architecture documents heuristic passes and join enumeration, not an existing Cascades
memo. Instrument real current candidate/pass scopes; do not fabricate memo identities or claim
Cascades coverage now. Later Cascades scopes must distinguish memo group, expression, and required
properties because one expression may be costed in several contexts.

Detailed mode records the final selection/root as well, but does not perform a second final-plan
summary traversal. A future consumer can request additional presentation without changing capture
semantics.

## Append-only records and scopes

Use one small envelope with a typed payload:

```text
Record { id, scope_id, payload }
Scope  = Operator | Candidate | OptimizerPass
```

Scope metadata provides context identity, analysis generation, parent scope, and the relevant
operator/candidate/pass identity. Candidate scopes need not have a materialized operator handle.
IDs are local to the session; they are not stable between runs. Preserve scan and column-instance
identity: multiple aliases of one catalog table are different inputs.

Append records directly as decisions happen; do not buffer an entire operator computation for a
transactional commit. A scope starts as running and ends complete or failed. Failed computations
retain useful partial records but have no successful-result claim. Recursive child scopes remain
independently inspectable. Final session metadata contains the selected root when available and
run status (running/succeeded/failed). This avoids a parallel failure-event framework.

A cache hit does not generate duplicate derivation records, but candidate consideration may record
a reuse link. Analysis `clear()` advances the context's generation while retaining history; changed
configuration/sketches follow their existing cache-invalidation behavior. Reads do not clear caches
or drain records. Do not change estimator policy, arithmetic, cache keys, or optimization choices
to make observations easier to produce.

## Record content

Use typed payloads rather than enormous format strings or a universal struct full of null fields.
Reuse `EstimateSource` where applicable, with small model/reason enums where necessary. Capture
values already computed at the actual branch; do not rerun HLL estimates, frequent-value lookups,
or hypothetical alternatives to reconstruct an explanation afterward.

Three related kinds of information are useful:

1. Operator row-count derivation or a cardinality-preserving input link.
2. Column-profile inheritance, transformation, invalidation, and defaults.
3. Predicate/statistic decisions supplying intermediate estimates.

Start with these payloads, adding details only when they explain an actual implemented branch:

| Payload | Useful fields |
| --- | --- |
| Scan selection | Scan instance, resolved table, column, evaluated catalog/HLL NDV, selected source, raw value and applied cap |
| Grouping | Ordered key expressions, deduplicated equivalent keys, used NDVs/defaults, independence product, input-row cap and output groups |
| Aggregate output | Output column/function, frequency/nullability, bounds and NDV assumptions actually used |
| Equality edge | Original/oriented pairs, null semantics, domain model, selected pair/coverage models, applied/skipped reason |
| Join result | Join type, equality/residual selectivity, available coverage/fanout, actual output-row rule |
| Predicate | Expression, child decision links when useful, redundant/contradictory/evaluated status, selected model, selectivity and input/output rows |
| Column propagation | Input/output columns or expression, alias/default/transform behavior, invalidated statistics and caps |
| Operator summary | Input links, row estimate/source, available cost, relevant profile summaries, and normalization adjustments |
| Candidate decision | Candidate/input links, available estimates/costs, reuse and actual selection/pruning reason |

Do not report unsupported joint-statistics selection as `candidate_groups=[]`. Add candidate
statistics records when the selection algorithm exists, distinguishing not evaluated, unavailable,
rejected, and selected. Likewise, report the implemented outer-join row floor if that is the actual
model, not a fabricated unmatched-row calculation.

## Operator and predicate coverage

Keep a coverage checklist against the actual `OperatorData` variants. Use an exhaustive match
without a catch-all for operator summaries, so a new variant requires an observation decision.
Detailed branch coverage can grow incrementally; expose which areas are instrumented rather than
claiming every summary is a complete explanation.

| Operator | Coverage target |
| --- | --- |
| Scan / ConstScan / TableFunction | Catalog/sketch/default selection, exact constant-input profiles, explicit table-function fallback respectively |
| Selection | Predicate model, domain restriction versus thinning, resulting rows and affected profiles |
| Map | Direct-alias inheritance versus computed-expression fallback/actual transform |
| Aggregation | Scalar/grouped row rule, grouping keys, aggregate-output assumptions |
| Join | Inner, semi, anti, outer, mark and single output semantics, distinct from pair selectivity alone |
| CrossProduct | Actual multiplication and column propagation |
| Limit | Offset subtraction, floor at zero and optional fetch cap |
| Projection / Rename | Preserving row link and column retention/remapping |
| Sort / Output | Explicit preserving input link; no invented selectivity |

Capture the actual composition/fallback models for AND, OR, NOT, IN, LIKE, ranges, comparisons,
and null tests as they are instrumented. Preserve join pairs and non-equality residuals separately.
Do not flatten pairs to a column set or omit computed grouping expressions. A redundant predicate
must not claim a sketch lookup selected its result.

### HAVING and subqueries

HAVING uses the ordinary Selection/predicate payload, not a separate event hierarchy. Distinguish
pre-filter group count from surviving groups, and aggregate-output statistics from grouping-key
statistics. Base-column sketches do not describe the distribution of SUM/COUNT outputs. Record
the bound-based model or heuristic actually used instead of implying observed aggregate statistics.

Scalar aggregation emits one row on empty input, but a following HAVING predicate can remove it.
Explain predicates at their actual location after rewriting, including grouping-key predicates
pushed below aggregation. Do not infer original WHERE/HAVING/ON labels from adjacency; SQL-source
provenance is outside this stack. Explain lowered subqueries through their actual semi/anti/mark/
single and other operators, without adding a second fictional subquery selectivity.

## Soft limits, not budgets

There is no `Budget`, hard cap, eviction, rolling window, automatic dump, or disk spill. Collection
continues after a soft limit is crossed; records are not discarded. This deliberately accepts that
large optimizer searches can consume substantial memory or exhaust the process. The toolkit does
not provide a bounded-memory guarantee or out-of-memory recovery.

Keep internal default soft thresholds, initially 10,000 records and 4 MiB estimated diagnostic memory.
They are not arguments threaded through helpers or part of a configuration framework. Account
incrementally for retained records, scope tables, and owned variable-size payloads/capacity; avoid
rescanning the buffer on every append. The estimate excludes existing IR, estimator caches, and
caller-owned copies, and is not a claim about process RSS. Avoid cloning entire profiles or sketches
into records when scalar summaries and handles suffice.

Log once per session per threshold when record count or estimated memory crosses its threshold.
Include session identity, mode, count, estimated bytes, and threshold. The message explicitly states
that collection continues; it must not claim truncation or a dump. Use a standard logging facade
with application-owned initialization, not `eprintln!` or a custom callback system. If this adds a
core dependency, keep it minimal (for example `log`); no subscriber/exporter setup belongs in core.
An application with logging disabled can still inspect threshold-crossed flags in session metadata.
No per-record logs are emitted. Disabled observation performs no record allocation or formatting.

## Scope boundaries and future consumers

This stack exposes in-memory inspection and records final run/root metadata. It does not implement
versioned JSON, persistence, dashboard views, a debugging UI, or EXPLAIN ANALYZE. Avoid promising a
stable public wire format before a consumer needs one. Keep core's optional serialization feature
boundaries intact and do not copy SQL/literal text into records just to make them readable.

Use existing pass traces as correlation context; do not duplicate plan snapshots in this collector.
Future EXPLAIN ANALYZE needs separate runtime observations plus logical-to-physical provenance,
not estimates overwritten by actuals or matching by node display names. This is a future integration
constraint, not a requirement to build physical-node mappings in this stack.

## Implementation sequence

1. Add a run-owned session, modes, append-only scopes/records, read view, and soft-limit warnings.
   Test shared ownership across forks and independent sessions across runs.
2. Add LastPlanSummary finalization over the selected reachable DAG, using existing estimates and
   clearly identifying any post-selection computations. Cover every operator summary variant.
3. Add DetailedSummary hooks for current optimizer candidate/pass scopes and branch-local scan,
   filter, grouping, and join decisions. Migrate existing stderr sites through the toolkit.
4. Extend detailed column/predicate/operator coverage with narrow tests; keep a checklist of what
   is implemented. Add Cascades consideration/cost/pruning hooks when that optimizer is integrated,
   without blocking the current toolkit on a hypothetical memo implementation.

No dashboard, exporter, runtime instrumentation, production tracing, or metrics work is included.

## Current implementation

Create an `ObservationSession`, attach it with `AnalysisContext::observe_with`, and call
`AnalysisContext::finalize_observations(&query, selected_root)` once selection succeeds.
`LastPlanSummary` records the selected reachable DAG (and marks a cache miss computed during
finalization as post-selection). `DetailedSummary` records cardinality derivations, cache reuse,
scan NDV choices, DPhyp candidate retain/discard decisions, and the selected root. It does not yet
record predicate, grouping, or join model branch details; those remain incremental coverage work.

Forked analysis contexts receive distinct context IDs while sharing the same session. `clear()`
advances that context generation. Soft-limit metadata and one-time `log` facade warnings are
available through the session; capture continues after either threshold.

## Focused verification

- Last-plan mode contains only the final reachable DAG, visits shared inputs once, handles warm
  caches, and reports an unavailable root instead of substituting a candidate on failure.
- Detailed records agree with actual computed estimates and models, including early returns,
  normalization, scan NDV caps, grouping, join types, residuals and predicate fallbacks.
- HAVING/scalar aggregation, aliases, repeated tables, expression keys, and redundant/contradictory
  predicates have focused cases as their hooks land. Use TPC-H/JOB fixtures to exercise real shapes,
  not as a substitute for branch-specific assertions.
- Observation off/on produces the same estimates and selected plans. Cache reuse does not duplicate
  derivations; generation changes, shared-session forks, and failure status preserve coherent links.
- Small internal test thresholds trigger each warning once, retain all subsequent records, and leave
  optimization unchanged. Metadata reports counts, estimated memory, and crossed thresholds.
- Run focused tests, formatting, relevant feature combinations (including no-default-features core),
  and the repository-required Clippy command when code lands. Measure capture overhead rather than
  claiming it is zero. No workspace-wide instrumentation framework is needed.

This change is documentation only; no runtime checks or memory-safety guarantees are implied.
