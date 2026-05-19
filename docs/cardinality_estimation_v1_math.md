# CardinalityEstimationV1 Math Notes

## Current Behavior

`CardinalityEstimationV1` now has row and column profiles, but the first math
version is intentionally rough:

- `Scan` uses catalog statistics, then TPCH/JOB mocks, then defaults.
- The TPCH/JOB mocks initially had table row counts but little column NDV data.
- Unknown columns default to low NDV, which makes large PK/FK joins look too
  weakly selective.
- Equality join selectivity is `1 / max(left_ndv, right_ndv)`.
- Transient/redundant equalities are only deduped within a single selectivity
  call, not carried across derived join profiles.

## Sample-Pass Evidence

The generated pass JSON under
`connectors/optd-datafusion/data/sample-passes/job_optimizer_json/` shows this
clearly.

JOB `1a` has the cluster:

- `title.id = movie_companies.movie_id`
- `title.id = movie_info_idx.movie_id`
- `movie_companies.movie_id = movie_info_idx.movie_id`

JOB `3c`, `14c`, and `33c` contain similar clusters with several `movie_id`
columns and `title.id`. After `JoinOrdering`, some `estimated_rows` values are
huge because the profile loses enough PK/FK containment information while
building intermediate DP states.

## Problems

- Row-only mocks make FK columns look like generic low-NDV columns.
- Equality-class state needs to survive in `CardinalityProfile`.
- Transient edges should remain in the IR when needed, but should not multiply
  selectivity after the equality class is already connected.
- Multiple equalities connecting the same classes should pick the strongest
  containment domain, not multiply all predicates.
- Semi/anti joins should use match probability, not inner-join output rows.
- Outer joins need preserved-side lower bounds without losing the expected
  inner-match estimate.

## Design Direction

Treat equality joins as PK/FK-like containment joins. For a new equality between
two equivalence classes, choose the largest known NDV as the containment domain
and apply one selectivity multiplier: `1 / chosen_ndv`.

For transient/redundant equalities:

- If both columns are already in the same carried equivalence class, multiply no
  extra selectivity.
- If multiple predicates connect the same pair of classes in one join, apply
  only the predicate with the largest chosen NDV.
- After the join, merge equivalence classes and carry the largest known NDV.

## Action Items

- [x] Document current CE math issues and JOB examples.
- [x] Carry equivalence classes in `CardinalityProfile`.
- [x] Add deterministic TPCH/JOB key-column NDV mocks.
- [x] Use PK/FK containment for equality selectivity.
- [x] Pick the largest NDV for transient/multiple equality edges.
- [x] Fix semi/anti join row math to use match probability.
- [x] Preserve outer-join lower bounds while keeping expected match estimates.
- [x] Add focused CE tests for PK/FK, transient edges, semi/anti, and outer joins.
