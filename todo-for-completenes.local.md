# Cardinality Estimation Completeness TODO

This is the local implementation backlog for closing the remaining estimation and planning gaps. It is intentionally separate from the durable architecture documents until the individual designs stabilize.

## Statistics collection and storage

- [ ] Persist catalog statistics and query-local sketch payloads through the production catalog provider.
- [ ] Add a collector for HLL and SpaceSaving data. DataFusion exposes ordinary `Statistics` (row counts, null counts, min/max, and sometimes distinct counts depending on the provider), but it does not provide HLL or SpaceSaving sketches automatically.
- [ ] Decide between a full-scan collector and a bounded sampling collector; record provenance, population size, collection time, and freshness.
- [ ] After sampling is available, collect and persist multi-column distinct counts for composite joins, grouping keys, and correlated predicates.
- [ ] Add sample- or set-sketch-based distinct-domain overlap; do not use classic HLL inclusion/exclusion as a precise intersection estimate.
- [ ] Add refresh/invalidation policy for stale, appended, and replaced tables.
- [ ] Add partial and predicate-conditioned statistics where a backend can provide them.

## Join estimation

- [x] Separate equality-domain coverage used by semi/anti joins from pair multiplicity used by inner joins.
- [x] Use directional NDV coverage with outer non-null fraction for equality semi/anti joins.
- [x] Use ordered min/max ranges to detect disjoint domains and estimate numeric/date overlap under uniformity.
- [x] Use single-column unique/FK catalog assertions when their population preconditions are safe.
- [ ] Replace uniform ordered-range overlap with histogram intersection.
- [ ] Refine overlap with persisted samples or an intersection-capable sketch.
- [ ] Estimate both left and right match probabilities and use them for general full-outer unmatched-row estimation.
- [ ] Model residual join predicates as `P(at least one candidate survives)` using fanout and correlation, instead of multiplying match coverage by residual pair selectivity.
- [ ] Use sampled multi-column NDV/overlap for composite equality keys rather than multiplying independent single-column coverage estimates.
- [ ] Add multi-column FK/unique-key inference for composite joins.
- [ ] Carry confidence/provenance for pair selectivity, match coverage, and fanout separately.

## Filter and expression estimation

- [x] Use an occupancy model for NDV after row-independent filtering.
- [x] Use proportional domain reduction for direct literal comparisons and tighten ordered bounds.
- [ ] Use histograms for range predicates, including inclusive/exclusive endpoints and non-uniform buckets.
- [ ] Use MCV plus residual decomposition for equality, `IN`, and `NOT IN` predicates.
- [ ] Add first-class `IN`/`NOT IN` and SQL NULL-semantics estimation.
- [ ] Improve OR estimation with overlap/correlation information.
- [ ] Add LIKE/prefix/text statistics where supported.
- [ ] Add a generic expression-statistics transform provider for monotonicity, null behavior, NDV mapping, bounds, and sketch compatibility; keep specialized transforms out of `ValueId`.
- [ ] Add conditional statistics for filtered populations.

## Catalog constraints

- [x] Consume provider-asserted single-column unique keys and foreign keys conservatively.
- [ ] Import PK/unique/FK metadata automatically from the DataFusion-facing catalog provider when the underlying source exposes it.
- [ ] Track enforcement, nullable-key semantics, and dialect match mode explicitly rather than relying only on the provider contract.
- [ ] Add check-constraint domain inference.
- [ ] Add functional-dependency metadata beyond equality classes.

## Aggregation

- [ ] Use sampled multi-column NDV for correlated grouping keys.
- [ ] Use functional dependencies to remove redundant grouping dimensions.
- [ ] Improve distinct-aggregate estimates and statistics for aggregate outputs.
- [ ] Propagate useful statistics through grouping sets when supported.

## Outer, mark, and single joins

- [ ] Estimate unmatched rows directionally for left, right, and full outer joins.
- [ ] Model nullable mark-column truth distributions (`true`/`false`/`unknown`) from match and NULL probabilities.
- [ ] Use uniqueness/cardinality-violation semantics to improve single-join estimates.

## Costing and plan quality

- [ ] Replace column-count width with type-aware and variable-length row-size estimates.
- [ ] Model hash build/probe asymmetry, memory overhead, spilling, and runtime filters.
- [ ] Model existing ordering, external sorting, indexes/lookup joins, repartitioning, network cost, and parallelism.
- [ ] Add confidence-aware or robust plan comparison when competing plans depend on low-confidence estimates.
- [ ] Consider adaptive reoptimization/runtime feedback after the static estimator is stable.

## Validation and regressions

- [ ] Re-run the TPC-H and JOB estimate/actual baseline after these changes.
- [ ] Check in a compact q-error baseline and a curated routine regression subset.
- [ ] Track median, p90, and maximum q-error by operator class, plus plan-shape and timeout changes.
- [ ] Add property tests: right-duplicate invariance for semi joins, anti/semi complementarity, monotonicity when new distinct inner keys are added, disjoint-domain zero, and FK/unique exact cases.
- [ ] Benchmark sketch collection and estimator overhead.
