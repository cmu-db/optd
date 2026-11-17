# CardinalityEstimationV1

## Summary

`CardinalityEstimationV1` estimates row counts and column profiles for each
operator. It uses lazily cached `LogicalFactsAnalysis` results for value lineage, accumulated
constraints, and equality classes. Query-local HyperLogLog and SpaceSaving sketches can refine NDV,
equality-filter, and skewed equijoin estimates. Population-compatible column-group HLLs additionally
refine selected grouping and direct composite-inner-join estimates. See
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
- `joint_ndvs`: compatible immutable column-group estimates keyed by logical value identity.

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

1. Catalog-provided table, exact column, and semantic constraint statistics.
2. A compatible query-local HLL when exact cardinality is unavailable.
3. Stable default estimates.

Validated column-group statistics are installed as joint NDVs only when their population matches the
complete scan population. Their set lookup is independent of grouping or predicate order.

The core analysis should not execute SQL to collect statistics. Query-based
collection belongs in the DataFusion connector, which can run local aggregate
queries and load the results into the catalog before optimization.

## Operator Propagation

`Projection` and `Output` preserve selected column profiles.

`Rename` copies profiles from original columns to renamed columns.

`Map` preserves input columns and joint metadata over untouched input values. Direct aliases
preserve the source profile. The centralized statistics-transform layer reads opaque definitions
from `LogicalFacts` and currently recognizes lossless widening casts, bounded integer affine
expressions, and bounded year extraction. These rules explicitly prove injectivity or an output-domain
cap, monotonicity, and NULL preservation; unsupported or potentially overflowing expressions retain
the stable unknown-column fallback. Computed outputs never inherit base sketches, and
expression-specific lineage variants remain intentionally avoided.

`Selection` first attempts whole-predicate sampling over the complete scan-filter pipeline
(scan/projection/sort/output, including preceding selections). All predicates must be evaluable;
unsupported predicates, maps, renames, joins, and limits refuse this path. Matching sample rows
provide the lower bound, the population-scaled count provides the point estimate, and the input
upper bound is retained. By design, past samples remain reusable population evidence. A full
population sample has equal model-relative endpoints but retains `Sample` provenance: historical
coverage is not a logical proof about the current population. Zero matches in a partial sample
do not prove emptiness. Logical contradictions take precedence and yield exact zero. Column NDVs remain on the ordinary
profile-propagation path, using conditional sample fractions rather than independent marginals.

Without a fully evaluable sample pipeline, `Selection` splits conjuncts and applies predicate
selectivity one predicate at a time, without treating heuristic selectivity as a row bound. Equality
predicates use NDV; range predicates use known bounds when available; unsupported predicates fall
back to defaults. Literal equality yields at most one NDV, inequality removes one modeled value, and
ordered comparisons proportionally restrict NDV while tightening bounds (including strict discrete
predecessor/successor bounds). Other columns use a uniform occupancy estimate for surviving NDV
instead of only capping the old NDV by output rows.

`Aggregation` first reduces estimation keys by proven functional dependencies, while retaining
all requested output columns. Provider-asserted scan keys and aggregation grouping tuples provide
dependencies; equality closure propagates them through inner joins and aliases. Observed NDV alone
is not a proof, self-join instances remain separate, and null-supplying outer-join sides lose their
dependencies. This prevents dependent attributes from inflating grouping estimates after filters
or row-duplicating joins. Only independent keys participate in joint-NDV/product/SCBC estimation;
SCBC requires at least two remaining keys. Group count remains capped by input rows.
Semantic uniqueness wins first. Otherwise, the estimator uses the largest compatible joint NDV once
and multiplies single-column NDVs only for uncovered keys. Without a fully covering joint NDV,
multi-key grouping first tries sketch-corrected bound-corrected (SCBC) NDV estimation using tuple
and per-column frequencies from a population sample.
Partial joint NDVs constrain the SCBC result from below. The sample walker progresses supported
filters over scan/projection/sort/output pipelines; stale sample population metadata, joins,
limits, renames, maps, unknown predicates, missing columns, and unsupported sample types fall back
to the product path. Empty or undersized samples also fall back. Per-key distinct prefers a live HLL
sketch when present; filters/joins invalidate base sketches, so filtered SCBC uses adjusted profile
NDVs, never base HLLs. NULL is a grouping value in SCBC, including population-known NULLs
that miss a sparse sample. Grouping columns preserve adjusted
profiles; aggregate output columns start conservative except for simple count-like bounds.

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

Semi/anti joins use PostgreSQL 17's `eqjoinsel_semi` policy rather than a separate custom
match-probability model. For reliable NDVs without paired MCV lists, equality selectivity is
`left_non_null_fraction * min(1, right_ndv / left_ndv)`; an unknown/default NDV uses half the
non-null left population. For direct base values, this policy reads the original variable NDV from
catalog statistics (including catalog HLLs and provider-asserted uniqueness), not the NDV already
thinned by input filters. Otherwise a selective left filter can falsely imply complete right-side
coverage. Only the right variable NDV is capped by the immediate right input's row count, and that
cap makes its estimate non-default. Values without recoverable base statistics use their current
profile estimate. This lookup is local to semi/anti estimation: filtered column profiles, inner joins,
and grouping continue to use population-adjusted NDVs. With current-population MCV lists on both
sides, known matched left MCV frequencies are counted once and only the uncertain remainder uses
the NDV ratio (after subtracting matched distinct values). Right MCVs are frequency-ranked and
limited to the capped right NDV. Base-population distributions retained as proxies after joins
are not treated as current-population MCVs.

As in PostgreSQL's `eqjoinsel`, each equality's semi selectivity is capped by
`right_rows * inner_pair_selectivity`. Conjunct selectivities, including residual predicates, are
multiplied directly; there is no fanout exponent, SpaceSaving coverage override, sampled key-presence
estimate, or inferred anti-join domain subtraction. Semi rows are `left_rows * selectivity`; anti
rows are `left_rows * (1 - selectivity)`. Empty inputs and ordinary equality against an all-NULL
right key produce no semi rows. Null-safe equality additionally admits the left NULL fraction when
the right key has NULLs, subject to the same inner-pair cap. Population-safe FK assertions retain
their stronger ordinary-equality estimate. Column profiles use ordinary row-thinning/capping;
only proven ordinary-equality keys become non-null on semi output.

This follows [PostgreSQL 17 `selfuncs.c`](https://github.com/postgres/postgres/blob/REL_17_STABLE/src/backend/utils/adt/selfuncs.c)
(`eqjoinsel_semi` and its caller), adapted to our profile statistics, NULL-safe equality, and existing
inner-pair estimator. Clause independence and containment remain estimation assumptions, not proofs
of composite-key overlap. Recovering a catalog variable NDV also recovers its non-default status;
otherwise the default flag comes from the current profile's `EstimateSource::Default`. Computed
values do not borrow statistics from an unrelated base column. Leaf samples estimate filter selectivity only.

For inner-pair estimation, compatible ordered ranges detect disjoint domains and estimate numeric/date overlap under
uniformity. Before the range-fraction fallback, compatible live HLLs use **joint Poisson maximum
likelihood** (Ertl) for domain intersection, never HLL inclusion–exclusion. Ertl rejects unidentified, failed, or low-Jaccard fits (`ERTL_MIN_JACCARD = 0.05`)
and falls back to range/NDV containment. Disjoint ranges still take precedence. Compatibility
requires equal precision, hash seed, and value-encoding version. Only surviving base-population
sketches qualify; filtered/joined profiles do not invent replacement HLLs.
SpaceSaving common values refine inner-join pair counts, not semi/anti coverage. Catalog unique/FK
assertions provide stronger estimates only while the required base population is complete. Direct-column composite inner equalities may
consume one compatible joint NDV from either input, with normalized edge orientation and per-edge
fallback only for uncovered equalities. Multi-column histograms, joint NDVs beyond schema-prefix
HLLs remain future work.

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
- [x] Keep unsupported computed columns opaque behind a centralized statistics-transform layer.
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
- [x] Retain opaque derived definitions instead of adding expression-specific lineage variants.
- [x] Derive profiles for safe casts, bounded integer affine transforms, and year extraction.
- [x] Add query-local HLL and SpaceSaving storage with downstream filter/join consumers.
- [x] Use PostgreSQL-style semi/anti equality selectivity separately from inner-join pair selectivity.
- [x] Add ordered-range domain overlap and disjointness checks.
- [x] Add occupancy-based filtered NDV and proportional literal-domain restriction.
- [x] Consume population-safe single-column unique/FK catalog assertions.
- [x] Collect query-local HLLs for leading logical-schema prefixes through length five.
- [x] Consume population-safe joint NDV for grouping and direct composite inner equalities.
- [x] Replace uniform range overlap with histograms/samples (leaf samples + MCV/hist; progressive sample ANDs).
- [x] Use leaf-filter sampling for filters only; semi/anti joins use PostgreSQL NDV/MCV selectivity and RHS row clamping.
- [x] Prefer live HLL sketch NDV for aggregation grouping keys when sketches survive on the input.
- [x] Estimate compatible live HLL domain intersections by Ertl joint MLE with quality gating.
- [x] Use sketch-corrected bound-corrected NDV estimation (SCBC) for multi-column grouping without fully covering joint NDVs, when a population sample is available.
- [ ] Add catalog **joint NDVs** beyond leading schema-prefix HLLs (correlated filters/joins/groupby).
- [ ] Add **multi-column histograms** (or sampled column groups) for residual/correlated predicates.
- [ ] Persist and refresh compatible sketches through a production catalog statistics provider.
