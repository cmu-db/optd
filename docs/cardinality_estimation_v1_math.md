# CardinalityEstimationV1 Math Notes

## Current Behavior

`CardinalityEstimationV1` has row and column profiles with sparse equality state:

- `Scan` uses catalog statistics, then TPCH/JOB mocks, then defaults.
- The TPCH/JOB mocks initially had table row counts but little column NDV data.
- Unknown columns default to low NDV, which makes large PK/FK joins look too
  weakly selective.
- Equality join selectivity is `1 / max(left_ndv, right_ndv)`.
- Transient/redundant equalities are deduplicated against classes carried by derived inner-join
  profiles.

## Sample-Pass Evidence

The generated pass JSON under
`optd/connectors/datafusion/data/sample-passes/job_optimizer_json/` shows this
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
- Equality-class state must remain sparse: singleton columns already carry their own NDV.
- Transient edges should remain in the IR when needed, but should not multiply
  selectivity after the equality class is already connected.
- Multiple equalities connecting the same classes should pick the strongest
  containment domain, not multiply all predicates.
- Semi/anti joins use PostgreSQL 17 `eqjoinsel_semi`: count matched left MCV mass, apply
  `min(1, right_ndv / left_ndv)` to the uncertain non-null remainder, and use 0.5 when an NDV
  is default. Clamp only the right NDV to right rows, then cap each equality's semi selectivity
  by `right_rows * inner_pair_selectivity`. Conjuncts multiply directly; remove the custom
  residual-fanout exponent, SpaceSaving coverage floor, and anti-domain subtraction. This is a
  containment/independence estimator, not a proof of composite-key coverage. See the current
  [behavior and source reference](cardinality_estimation_v1.md#join-estimation).
- Use leaf samples for filter selectivity, not independent-sample join key presence.
  Our compatible live-HLL
  intersection estimator is Ertl joint Poisson MLE; reject low-Jaccard (<0.05), unidentified,
  incompatible, or failed fits and retain range/NDV fallback. Never use HLL inclusion–exclusion.
- Aggregation grouping prefers live HLL NDV when sketches remain on the input profile.
  Without a fully covering joint NDV, multi-key grouping uses sketch-corrected bound-corrected
  (SCBC) NDV estimation when a population sample is available: `sqrt(L_SCBC * U_SCBC) + sum_{i>=2} f_i` (CIDR 2019, equations 16–18),
  capped by observed, marginal, product, and input-row sanity bounds. Tuple/per-column sample
  frequencies tighten singleton bounds. Reservoir sampling uses the without-replacement lower
  bound from §3.2; filtered samples use adjusted profile NDVs, never stale base HLLs.
- Outer joins need preserved-side lower bounds without losing the expected inner-match estimate,
  and must not propagate ON-condition equality across null-extended rows.

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
- [x] Use PostgreSQL-style semi/anti selectivity and remove the custom match-probability model.
- [x] Preserve outer-join lower bounds while keeping expected match estimates.
- [x] Preserve only sound equivalence classes for left, right, and full outer joins.
- [x] Keep point estimates and upper/lower row bounds mutually consistent after null extension.
- [x] Use iterative compact equality state for wide classes and reverse-oriented chains.
- [x] Add focused CE tests for PK/FK, transient edges, semi/anti, and outer joins.
