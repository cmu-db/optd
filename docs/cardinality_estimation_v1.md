# CardinalityEstimationV1

## Summary

`CardinalityEstimationV1` estimates row counts and column profiles for each
operator. It uses lazily cached `LogicalFactsAnalysis` results for value lineage, accumulated
constraints, and equality classes. Query-local HyperLogLog and SpaceSaving sketches can refine NDV,
equality-filter, and skewed equijoin estimates. See
[`statistics_architecture.md`](statistics_architecture.md) for the separation between logical facts
and estimator policy.

It should use `HypergraphOf` for join groups so cardinality estimation, join-tree normalization, and
join ordering share predicate splitting and relation classification.

The analysis should return both a point estimate and conservative bounds. The
point estimate feeds costing; bounds preserve derivation facts such as
`distinct <= frequency <= rows`.

## Profile Model

Each operator gets a `CardinalityProfile`:

- `rows`: estimated output row count.
- `columns`: per-column profiles.
- `equivalence_classes`: only nontrivial (two-or-more-column) equality classes.

The cache stores profiles behind `Arc` so recursive estimation can share immutable results. The
public `AnalysisContext::get::<CardinalityEstimationV1>` contract still returns an owned profile;
costing currently uses that owned API, while internal recursive estimation uses the shared lookup.

Each estimate stores:

- `value`: current best estimate.
- `lower` / `upper`: known or derived range.
- `source`: exact, catalog, sketch-derived, operator-derived, or default.

Each column profile stores:

- `lower_bound` (`l_A`): minimum known value.
- `upper_bound` (`u_A`): maximum known value.
- `frequency` (`f_A`): tuples with a value in the known bounds.
- `distinct` (`d_A`): distinct values in the known bounds.
- `value`: estimator-independent base or derived value identity.
- `sketches`: compatible base-population sketches, retained only while the population is unchanged.

V1 should keep the model explicit even when many fields are unknown. This avoids
pretending rough estimates are exact.

## Base Statistics

For `Scan`, use statistics in this order:

1. Catalog-provided table and column statistics.
2. Query-local HLL for NDV when catalog NDV is absent.
3. Stable default estimates.

The core analysis should not execute SQL to collect statistics. Query-based
collection belongs in the DataFusion connector, which can run local aggregate
queries and load the results into the catalog before optimization.

## Operator Propagation

`Projection` and `Output` preserve selected column profiles.

`Rename` copies profiles from original columns to renamed columns.

`Map` preserves input columns. Direct aliases preserve the source profile. Other computed columns
remain opaque until a generic statistics-transform provider can prove bounds, NDV, and null/sketch
behavior; expression-specific lineage variants are intentionally avoided.

`Selection` splits conjuncts and applies predicate selectivity one predicate at a time. Equality
predicates use NDV; range predicates use known bounds when available; unsupported predicates fall
back to defaults. Literal equality yields at most one NDV, inequality removes one modeled value, and
ordered comparisons proportionally restrict NDV while tightening bounds (including strict discrete
predecessor/successor bounds). Other columns use a uniform occupancy estimate for surviving NDV
instead of only capping the old NDV by output rows.

`Aggregation` estimates group count from grouping-key distinct counts, capped by
input rows. Grouping columns preserve adjusted profiles; aggregate output columns
start conservative except for simple count-like bounds.

`Sort` preserves profiles.

`Limit` subtracts the offset before applying the fetch cap, then caps column frequency and distinct
values by the resulting row estimate.

## Join Estimation

Join estimation should be shared between `CardinalityEstimationV1` and
`JoinOrdering`.

`HypergraphOf` provides the join group. The estimator can infer connecting edges
from `left_nodes`, `right_nodes`, and the hypergraph; `edge_indices` should not
be part of the public statistics API.

Equivalent-column information is important. Equality predicates create equivalence classes, and
under a join-containment assumption the estimator can use one NDV for the whole class instead of
multiplying transitively redundant selectivities. This avoids over-penalizing chains such as
`A.x = B.x` and `B.x = C.x`.

Inner-join pair selectivity and semi/anti existence probability are separate estimates. For a left
semi/anti equality, directional coverage is estimated as:

```text
left_non_null_fraction * intersection_ndv / left_ndv
```

The general fallback uses containment (`intersection_ndv = min(left_ndv, right_ndv)`). Compatible
ordered ranges detect disjoint domains and estimate numeric/date overlap under uniformity.
SpaceSaving common values refine known matched left-row mass without letting duplicate right rows
inflate coverage; uncertain counters cannot lower the uniform baseline. Semi output key profiles are
capped by the intersection domain, and pure single-key anti joins propagate complementary unmatched
frequency and NDV. Single-column catalog unique/FK assertions provide stronger estimates only while
the required base population is complete. Histograms, samples, and sampled multi-column NDV remain
future work.

Transient or redundant equality edges should be treated specially for costing.
They may need to remain in the IR for execution or backend behavior, but CE
should not multiply selectivity for every redundant edge. V1 should classify
join predicates into equivalence-class edges and residual predicates, then apply
at most one equality selectivity per new equivalence-class connection.

## JoinOrdering Integration

`JoinOrdering` asks its `CostModel` to cost each DP candidate. The default model obtains an owned
`CardinalityProfile` through the same cached analysis used by the rest of the optimizer. Join
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
- [x] Keep arbitrary computed columns opaque pending a generic statistics-transform provider.
- [x] Add join equivalence-class handling for equality predicates.
- [x] Treat transient/redundant equality edges specially so selectivity is not double-counted.
- [x] Share cached profiles internally with `Arc` while retaining the owned public API.
- [x] Store only sparse, nontrivial equivalence classes and use iterative path compression.
- [x] Flatten join conjuncts once and expose a pre-flattened internal entry point.
- [x] Restrict outer-join equality propagation to sound input classes and preserve row bounds.
- [x] Filter newly-owned equality classes in place and merge input column maps structurally.
- [x] Add unit tests for scan, filter, map transformation, aggregation, join NDV, join ordering, redundant equality edges, and connector stats extraction.
- [x] Add a DataFusion connector helper/API for SQL-based stats extraction into catalog statistics.
- [x] Add demand-driven logical value lineage and accumulated constraints.
- [x] Keep derived expressions opaque instead of adding expression-specific lineage transforms.
- [x] Add query-local HLL and SpaceSaving storage with downstream filter/join consumers.
- [x] Separate directional equality coverage from tuple-pair selectivity for semi/anti joins.
- [x] Add ordered-range domain overlap and disjointness checks.
- [x] Add occupancy-based filtered NDV and proportional literal-domain restriction.
- [x] Consume population-safe single-column unique/FK catalog assertions.
- [ ] Replace uniform range overlap with histograms/samples.
- [ ] Add sampled multi-column NDV after sampling infrastructure is available.
- [ ] Persist compatible sketches through the catalog statistics contract.
