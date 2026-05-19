# Cardinality Estimation Improvement Plan

The `EXPLAIN ANALYZE` feedback loop now reaches optd plans, so the next work is
to reduce estimator error rather than fix fallback behavior.

## Current Read

The estimator is structurally useful:

- `CardinalityProfile` carries row and column profiles.
- Equality classes are carried through joins.
- Transient equality edges are not multiplied as independent filters.
- `EXPLAIN ANALYZE` uses the optd-produced child plan shape.

The remaining errors are still large:

- TPC-H median max-join estimate / actual max rows: `3.1e2`.
- JOB median max-join estimate / actual max rows: `1.45e3`.
- Worst completed JOB miss: `6f`, `4.6e4`.

This points first at weak base statistics and filter selectivity, not a need to
replace the whole estimator.

## Direction

Improve in layers:

1. Better base table statistics.
   - Use real row count, non-null frequency, NDV, min, and max.
   - Prefer collected stats over TPCH/JOB mocks.
   - Keep mocks only as fallback for local development without data.
2. Better filter selectivity.
   - Numeric/date ranges should use min/max and NDV.
   - Equality and `IN` should use NDV and cap by non-null frequency.
   - String `LIKE` needs column-aware defaults or lightweight value-frequency
     stats because JOB has selective string predicates.
3. Better FK-domain propagation.
   - PK/FK joins should preserve filtered-domain information.
   - Joining filtered `title.id` to `movie_info.movie_id` should reduce the
     movie-info domain instead of treating the next join as independent.
4. Operator-local feedback.
   - Find the first operator where estimate and actual rows diverge.
   - Classify misses as scan/filter/join/aggregation before changing math.

## Existing Hook

`connectors/optd-datafusion/src/statistics.rs` already collects:

- `COUNT(*)`
- `COUNT(column)`
- `COUNT(DISTINCT column)`
- `MIN(column)`
- `MAX(column)`

and can write the result into an optd catalog. Core
`CardinalityEstimationV1` already prefers catalog statistics over mock table
statistics.

The missing implementation is making those stats easy to collect, persist, and
load in the benchmark/CLI path.

## Implementation Steps

- [x] Add `EXPLAIN ANALYZE` support for optd child plans.
- [x] Record current TPC-H/JOB CE baseline.
- [x] Add a reusable CE comparison script.
- [ ] Add a stats export command or script that writes TPCH/JOB table stats.
- [ ] Add a stats load path for `optd-cli`.
- [ ] Re-run the CE comparison with collected stats enabled.
- [ ] Add operator-level first-bad-estimate reporting.
- [ ] Improve string filter selectivity for JOB.
- [ ] Improve FK-domain propagation after filtered PK-side joins.

## Decision

Do not replace `CardinalityEstimationV1` yet. First feed it better statistics
and use the feedback loop to identify which operator class causes each large
miss. A different estimator without better stats would still be guessing.
