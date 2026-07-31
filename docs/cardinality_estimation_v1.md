# CardinalityEstimationV1

## Summary

`CardinalityEstimationV1` estimates row counts and column profiles for each
operator. It should use `HypergraphOf` for join groups so cardinality
estimation, join-tree normalization, and join ordering share predicate splitting
and relation classification.

The analysis should return both a point estimate and conservative bounds. The
point estimate feeds costing; bounds preserve derivation facts such as
`distinct <= frequency <= rows`.

## Profile Model

Each operator gets a `CardinalityProfile`:

- `rows`: estimated output row count.
- `columns`: per-column profiles.
- `equivalence_classes`: only nontrivial (two-or-more-column) equality classes.

The cache stores profiles behind `Arc` so recursive estimation and costing can share immutable
results. The public `AnalysisContext::get::<CardinalityEstimationV1>` contract still returns an
owned profile; internal read-only consumers use the shared lookup.

Each estimate stores:

- `value`: current best estimate.
- `lower` / `upper`: known or derived range.
- `source`: exact, catalog, derived, mock, or default.

Each column profile stores:

- `lower_bound` (`l_A`): minimum known value.
- `upper_bound` (`u_A`): maximum known value.
- `frequency` (`f_A`): tuples with a value in the known bounds.
- `distinct` (`d_A`): distinct values in the known bounds.

V1 should keep the model explicit even when many fields are unknown. This avoids
pretending rough estimates are exact.

## Base Statistics

For `Scan`, use statistics in this order:

1. Catalog-provided table and column statistics.
2. Deterministic mock statistics for TPCH and JOB tables.
3. Stable default estimates.

The core analysis should not execute SQL to collect statistics. Query-based
collection belongs in the DataFusion connector, which can run local aggregate
queries and load the results into the catalog before optimization.

## Operator Propagation

`Projection` and `Output` preserve selected column profiles.

`Rename` copies profiles from original columns to renamed columns.

`Map` preserves input columns. New computed columns are initially conservative,
but this is a clear improvement area: simple expressions can transform profiles.
For example, `new_value := x + 1` can shift `min/max` by `+1`, preserve
`distinct`, and preserve frequency/null behavior.

`Selection` splits conjuncts and applies predicate selectivity one predicate at a
time. Equality predicates use NDV; range predicates use known bounds when
available; unsupported predicates fall back to defaults.

`Aggregation` estimates group count from grouping-key distinct counts, capped by
input rows. Grouping columns preserve adjusted profiles; aggregate output columns
start conservative except for simple count-like bounds.

`Sort` preserves profiles.

`Limit` caps rows, frequency, and distinct values by the fetch count.

## Join Estimation

Join estimation should be shared between `CardinalityEstimationV1` and
`JoinOrdering`.

`HypergraphOf` provides the join group. The estimator can infer connecting edges
from `left_nodes`, `right_nodes`, and the hypergraph; `edge_indices` should not
be part of the public statistics API.

Equivalent-column information is important. Equality predicates create
equivalence classes, and under a join-containment assumption the estimator can
use one NDV for the whole class instead of multiplying independent selectivities.
This avoids over-penalizing chains such as `A.x = B.x` and `B.x = C.x`.

Transient or redundant equality edges should be treated specially for costing.
They may need to remain in the IR for execution or backend behavior, but CE
should not multiply selectivity for every redundant edge. V1 should classify
join predicates into equivalence-class edges and residual predicates, then apply
at most one equality selectivity per new equivalence-class connection.

## JoinOrdering Integration

`JoinOrdering` asks its `CostModel` to cost each DP candidate. The default model obtains cached
`CardinalityProfile` values through the same analysis used by the rest of the optimizer. Join
predicates already split into hypergraph edges can call the pre-flattened conjunct entry point,
avoiding repeated expression-tree traversal.

Outer joins deliberately do not carry equality knowledge across their null-supplying boundary:
left outer joins retain only left-input classes, right outer joins retain only right-input classes,
and full outer joins retain none. Row estimates and bounds are raised together to the preserved-side
minimum, maintaining `lower <= value <= upper`.

## Implementation Progress

- [x] Add profile structs: `Estimate`, `ColumnProfile`, `CardinalityProfile`.
- [x] Add base table statistics source for catalog stats plus TPCH/JOB mock stats.
- [x] Implement `CardinalityEstimationV1` as a cached operator analysis.
- [x] Implement filter selectivity for equality, ranges, `AND`, `OR`, and fallback predicates.
- [ ] Add first-class literal-list `IN` selectivity if the IR gains an `IN`-list expression.
- [x] Implement operator propagation for projection, rename, map, selection, aggregation, sort, limit, joins, and fallback operators.
- [x] Add computed-column improvement notes for expressions like `new_value := x + 1`.
- [x] Add join equivalence-class handling for equality predicates.
- [x] Treat transient/redundant equality edges specially so selectivity is not double-counted.
- [x] Share cached profiles internally with `Arc` while retaining the owned public API.
- [x] Store only sparse, nontrivial equivalence classes and use iterative path compression.
- [x] Flatten join conjuncts once and expose a pre-flattened internal entry point.
- [x] Restrict outer-join equality propagation to sound input classes and preserve row bounds.
- [x] Filter newly-owned equality classes in place and merge input column maps structurally.
- [x] Add unit tests for scan, filter, map transformation, aggregation, join NDV, join ordering, redundant equality edges, and connector stats extraction.
- [x] Add a DataFusion connector helper/API for SQL-based stats extraction into catalog statistics.
