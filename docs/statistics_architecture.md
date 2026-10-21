# Statistics and Value-Lineage Architecture

## Scope

Optimizer statistics are derived lazily for one query and cached in `AnalysisContext`. Persistent
caching and collection scheduling belong at the catalog boundary and are future work.

The implementation borrows behavioral ideas from the `ac/test-card-est` exploration without
sharing its IR property system.

## Separation of concerns

`LogicalFactsAnalysis` derives facts that are true independently of any cardinality model:

- each output column's logical value identity;
- direct base-column provenance through projections, renames, and aliases;
- opaque definitions for computed values;
- equality classes;
- literal and numeric range constraints;
- contradictions and residual predicates.

`CardinalityEstimationV1` consumes those facts alongside catalog statistics and query-local
sketches. It owns estimator policy, fallback assumptions, and operator-specific row-count math.
An expression such as `x + 1` is recorded as an opaque derivation by lineage; lineage does not gain
an `AddOne`, `ExtractYear`, or function-specific variant. The centralized `statistics_transform`
module recognizes selected expressions from `LogicalFacts::definitions` and derives profiles only
when it can prove the required value-domain properties.

This distinction also separates two meanings of provenance:

- **value lineage** answers which base or derived logical value an output carries;
- **estimate source** answers whether a number is exact, catalog-provided, sketch-derived,
  operator-derived, or a fallback.

## Demand-driven caching

Both logical facts and cardinality profiles are operator analyses. Existing operator handles are
immutable under the optimizer's append-only invariant, so bottom-up results remain valid until the
analysis context is cleared. Query-local base-column sketches are carried in each catalog `ColumnStatistics` payload.
`TableStatistics::column_groups` separately stores cardinality statistics for canonical sets of
logical-schema ordinals. `PlannedQuery` retains that catalog so post-optimization analysis uses the
same statistical inputs as optimization.

## Sketches

The `optd-sketches` crate provides IR-independent, serializable implementations of:

- HyperLogLog for approximate distinct counts;
- SpaceSaving for bounded frequent-item summaries.

Both consume the canonical scalar byte encoding defined by `optd-core`. Compatibility therefore
does not depend on display formatting or catalog JSON. The complete `ColumnSketches` payload records
the encoding version; HyperLogLog additionally records precision and hash seed. Deserialization and
installation validate sketch shape, counter bounds, population, and encoding compatibility.

A scan prefers exact catalog cardinality over an installed HLL; HLL is used when no exact
cardinality is available. SpaceSaving improves equality filters, skewed equality-join pair counts,
and directional semi/anti key coverage. For an inner join, tracked values
contribute their frequency cross-products; for left-side coverage, a tracked left frequency
contributes once when the value is known to occur on the right, independent of right-side duplicate
multiplicity. The untracked remainder uses the NDV-overlap model. Classic HLL is forbidden for set-intersection estimation: inclusion-exclusion subtracts noisy
cardinality estimates and is practically unstable, especially for small overlaps. It currently
improves the join domain estimate only through per-column NDV. Use an intersection-capable
Theta/KMV sketch or sampled overlap when available.

DataFusion does not supply HLL or SpaceSaving payloads automatically. The connector's current
runtime collector obtains exact aggregate row count, non-null count, NDV, minimum, and maximum for
referenced columns. The explicit full-scan collector used by fixture tests and the regression
harness stores HLL and SpaceSaving in the same `ColumnStatistics` payload. It also builds HLLs for
the leading logical-schema prefixes of lengths two through five. Tuple hashes use framed canonical
scalar encodings in schema order, and rows with any NULL component are omitted. Unsupported columns
suppress only groups containing that column.

Column-group lookup is set-based even though collection and tuple encoding use logical schema order.
Each group records its source, total population, all-components-non-null population, optional exact
cardinality, and optional HLL. Catalog installation validates group shape, schema ordinals,
populations, and cardinality bounds. The runtime cache includes table/provider identity and an
ordered schema signature; retaining provider ownership prevents address-reuse cache collisions.
Production collection still requires an asynchronous or bounded collector, persistence, refresh,
and staleness policy.

Base sketches and immutable joint-NDV metadata are propagated only while their population remains
unchanged:

- projection, output, rename, sorting, and direct aliases may retain compatible metadata;
- filtering, limiting, joins, cross products, aggregation, and opaque computations invalidate it;
- `Map` retains joint metadata over untouched input values because it preserves their population,
  while a computed output receives no base sketch;
- a join may consume immediate-input joint NDV but does not attach the base-population statistic to
  its output;
- a proven transformation may produce a sound derived profile but not masquerade as the original
  base-population sketch.

## Expression-statistics transformations

`statistics_transform` describes recognized expressions with three independent properties:
injectivity, monotonicity, and NULL behavior. The first rules cover direct aliases, lossless integer
and same-scale decimal widening casts, bounded integer affine transformations, and bounded
`date_part('year', date)`/`extract` forms. Integer transformations require known endpoints and
checked arithmetic, so possible overflow returns to the ordinary unknown-expression fallback.
Year extraction is many-to-one: its NDV is capped by the inclusive year span proven by the input
bounds. Recognized rules preserve non-null frequency because every current rule is NULL-preserving.
Only direct aliases retain value identity and compatible sketches; all other results use the derived
identity already recorded by `LogicalFacts`.

Estimate endpoints are propagated as semantic bounds rather than confidence intervals. For a
multi-key grouping estimate, the upper endpoint multiplies component-domain upper bounds while the
lower endpoint takes their maximum: correlated columns make multiplying NDV lower bounds unsound.
A row-count cap uses the row estimate's upper endpoint instead of treating its point estimate as a
certainty. Probabilistic HLL uncertainty remains out of scope.

## Join and outer-join facts

Inner joins combine input facts and add equality facts from the join condition. Semi, anti, and mark
joins retain facts from the preserved input. Outer joins do not claim that their `ON` equalities hold
for null-extended rows: left joins retain only left constraints, right joins retain only right
constraints, and full outer joins retain neither side's constraints.

Cardinality profiles materialize nontrivial column equivalence classes from logical value identity.
This lets join estimation avoid repeatedly charging transitive or redundant equality edges while
keeping equality reasoning independent of estimator internals.

Equality estimation separates two quantities:

- pair selectivity estimates matching tuple pairs for inner joins;
- directional coverage estimates the probability that a preserved-side row has at least one
  partner for semi/anti joins.

For left coverage, the uniform fallback is
`left_non_null_fraction * intersection_ndv / left_ndv`. The containment fallback sets
`intersection_ndv = min(left_ndv, right_ndv)`. When compatible ordered bounds are available,
disjoint ranges prove a zero intersection and numeric/date ranges scale each side's NDV by the
fraction of its range in the overlap. This remains a uniform approximation; histograms and samples are the planned replacement. For a
direct-column composite inner equality, the estimator can cover multiple edges with one compatible
joint NDV from either input, applying semantic uniqueness first and single-column fallback only to
uncovered edges. It does not infer HLL intersections or carry the consumed statistic through the
join. Other join forms continue to use per-edge estimates; sampled overlap or an
intersection-capable sketch is still required for correlated domain overlap.

Provider-asserted single-column unique keys make non-null frequency an exact NDV while the profile
still represents the complete base population. A later join may duplicate those values, so the
uniqueness shortcut is not reused once row provenance becomes derived. A single-column FK to a
complete, unfiltered referenced population proves that every surviving non-null local row has a
partner. Filtering the referenced side invalidates that coverage guarantee.

Semi-join output profiles apply occupancy thinning to unrelated columns and cap equality-key NDV by
the estimated intersection domain. Ordinary equality makes the surviving key non-null. For a pure
single-key anti join, the key's unmatched non-null frequency and NDV are the directional complements
of matched coverage; with multiple keys or residual predicates, the conservative occupancy fallback
is retained because a row may fail for another reason.

## Filtered NDV propagation

Selection distinguishes value-domain restriction from row thinning:

- direct equality yields at most one surviving value, inequality removes the modeled literal from
  the domain, and ordered ranges proportionally reduce NDV after removing the input null fraction
  from predicate selectivity; discrete strict comparisons also tighten to the predecessor/successor;
- other columns use the uniform occupancy model
  `D' = D * (1 - (1 - s)^(F / D))`, where `F / D` is average non-null multiplicity and `s` is row
  survival probability.

The occupancy model is more useful than merely capping NDV by output rows, but it still assumes
independent thinning and uniform multiplicity. Conditional statistics and sampling are needed for
correlated filters.

## Extension points

Planned extensions should preserve these boundaries:

- persist and refresh base and column-group sketches through a production catalog provider;
- add histogram and sampling consumers to cardinality estimation;
- select bounded workload-, constraint-, or index-derived column groups beyond the leading schema
  prefixes;
- add tuple-frequency and intersection-capable statistics where NDV alone is insufficient;
- extend the centralized transformation rules or add a provider registry without adding variants to
  `ValueId`;
- expose derivation traces for explain output without making them part of logical identity.

## Evidence base

The design treats deployed-system behavior and established database venues as primary evidence.
[PostgreSQL's multivariate statistics](https://www.postgresql.org/docs/current/multivariate-statistics-examples.html)
provide the closest production precedent for combining `ndistinct`, MCV, and dependency synopses.
Peer-reviewed foundations include:

- *Bifocal Sampling for Skew-Resistant Join Size Estimation* (SIGMOD 1996,
  [DOI 10.1145/233269.233340](https://doi.org/10.1145/233269.233340));
- *Processing Data-Stream Join Aggregates Using Skimmed Sketches* (EDBT 2004,
  [DOI 10.1007/978-3-540-24741-8_33](https://doi.org/10.1007/978-3-540-24741-8_33));
- *Sketches for Size of Join Estimation* (ACM TODS 2008,
  [DOI 10.1145/1386118.1386121](https://doi.org/10.1145/1386118.1386121));
- *Join Size Estimation Subject to Filter Conditions* (PVLDB 2015,
  [DOI 10.14778/2824032.2824051](https://doi.org/10.14778/2824032.2824051));
- *Two-Level Sampling for Join Size Estimation* (SIGMOD 2017,
  [DOI 10.1145/3035918.3035921](https://doi.org/10.1145/3035918.3035921));
- *COMPASS* (SIGMOD 2021,
  [DOI 10.1145/3448016.3452840](https://doi.org/10.1145/3448016.3452840)).

ArXiv-only HLL-intersection proposals are not implementation authority; classic HLL remains an NDV
synopsis only.
