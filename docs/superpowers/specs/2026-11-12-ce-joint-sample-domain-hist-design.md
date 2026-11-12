# CE: Joint Sample AND, Join Domains, Eq-Class Histograms, Bounds

**Date:** 2026-11-12  
**Status:** Approved (user: “Run.” / slice B)  
**Branch:** `ac/histogram-ce` (stacked on leaf-sample + histogram)

## Problem

After fixing leaf-sample NDV wipe, TPC-H joins track classic V1, but:

1. Leaf sample conjuncts are still charged **independently** against the full sample (product of marginals).
2. Join output key NDVs / eq-class distincts are not always the **intersection domain**, so the next join overestimates domain size.
3. `derive_join_side_columns` clears `distribution`, so residual filters cannot use MCV/hist via equality classes.
4. Bounds exist on samples but join uppers are weak product×selectivity only.

Out of scope: semi/anti match-probability redesign, adaptive QE, string/LIKE stats, agg HLL.

## Design

### 1. Progressive leaf-sample conjunct selectivity

In `apply_selection_profile`, keep `working_sample: Option<SampleRelation>` starting from the leaf sample.

For each new, non-redundant conjunct:

- If sample-translatable: `sel = match_fraction(working, conjunct)`, then `working = filter_relation(working, conjunct)`; charge `sel` (conditional, not marginal).
- Else: classic `filter_selectivity` on the current profile.

Facts / contradiction / redundant short-circuits unchanged. `rows.source = Sample` and LpBound-style `lower=0`, `upper=input_upper` when any sample conjunct was used.

### 2. Filtered join-key domain propagation

Equality edges already compute `LeftEqualityCoverage.intersection_distinct`. Today that is consumed mainly for semi/anti.

For **inner** joins (and other paths that emit merged eq-classes with coverages):

- After building the join profile, set each covered join-key column’s `distinct` to `min(current, intersection_distinct)`.
- Set the owning `ColumnEquivalenceClass.distinct` to that intersection (not the pre-join max/chosen NDV alone).

Downstream equi-joins then read the filtered domain from the class / column profiles.

Do **not** change semi/anti match-probability formulas.

### 3. Eq-class histogram / MCV residual selectivity

- **Retain** `distribution` through `derive_join_side_columns` (still clear `sketches` when population is unsafe). Document: post-join hist is a **base-population proxy**.
- In filter helpers (`frequent_value_selectivity`, `histogram_range_selectivity`, equality path), resolve the column through its eq-class to a peer that still has `distribution` (prefer richer MCV/hist). Apply selectivity once.

Row-thinning filters (`scale_profile`) continue to clear `distribution` / sketches.

### 4. Join row bounds

When both inputs have `rows.upper`, set inner-join `rows.upper` to  
`min(L_upper * R_upper * sel, L_upper * R_upper)` and keep `lower = 0` (or preserved-side mins for outers as today).  
Sample-pair `lpbound_equijoin_upper` remains on the sampling crate join API; classic CE does not require samples at join time for this bound.

### 5. TODOs (document only)

In `docs/cardinality_estimation_v1.md` open items and a code comment at joint-NDV consume / group sites:

- Catalog **joint NDVs** beyond leading schema prefixes.
- **Multi-column histograms** (or sampled column groups) for correlated filters/joins.

## Tests

1. Two correlated leaf predicates: progressive sample sel ≠ product of marginals; rows match joint fraction × scale.
2. Filter then equi-join: output key / class NDV ≈ intersection, not base max.
3. Join then residual filter on peer column: uses partner MCV/hist via eq-class.
4. Sample selection + join: uppers finite and ≤ product of input uppers.

## Non-goals

Match probability redesign; adaptive CE; LIKE/string; post-agg HLL (future).
