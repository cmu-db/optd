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

## JoinOrdering Statistics

`JoinOrdering` currently needs a cost for each DP state. V1 should make the DP
state carry a `CardinalityProfile`, while cost continues to use
`profile.rows.value`.

The statistics trait should provide base profiles and join selectivity. Full
output profile construction should live in shared propagation helpers so the
analysis and join ordering use the same formulas.

The public trait should stay small:

```rust
pub trait Statistics: Send + Sync {
    fn base_profile(&self, node_idx: usize, hg: &QueryHypergraph) -> CardinalityProfile;

    fn join_selectivity(
        &self,
        left_nodes: NodeSet,
        right_nodes: NodeSet,
        hg: &QueryHypergraph,
        inputs: JoinInputProfiles<'_>,
    ) -> Estimate;
}
```

`JoinOrdering` can infer connecting edges internally, compute selectivity, then
call shared join-profile propagation.

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
- [x] Refactor `JoinOrdering` DP state to carry `CardinalityProfile`.
- [x] Keep `UniformStatistics` as the fallback stats implementation.
- [x] Add unit tests for scan, filter, map transformation, aggregation, join NDV, join ordering, redundant equality edges, and connector stats extraction.
- [x] Add a DataFusion connector helper/API for SQL-based stats extraction into catalog statistics.
