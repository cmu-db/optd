# CE: Leaf Filter Sampling + Joint HLL Intersection + Sketch-Corrected Grouping NDV

**Date:** 2026-11-13  
**Status:** Implemented (workspace formatting blocked by pre-existing unrelated files; see plan verification record)  
**Branch context:** `ac/histogram-ce` (or stacked follow-on). Current HEAD may include match-prob sample key presence (`29edcb4` era).

## Goal

Use **leaf samples for filter selectivity and NDVs for join estimates**, replace naive domain
overlap with **joint HLL maximum likelihood** when compatible sketches exist, and add
**sketch-corrected bound-corrected (SCBC) grouping NDV estimation** when catalog `joint_ndvs`
are missing.

## Non-goals

- Online concurrent reservoir maintenance (DaMoN).
- Index-based join sampling (Leis CIDR’17).
- Sample⨝sample join CE.
- Default Rusu **sample key presence** for semi/anti (explicitly removed / demoted).
- Claiming an undocumented database-specific HLL intersection formula; we implement **Ertl (arXiv:1702.01284 §5)** as the named technique.
- Full sketch-corrected estimator suite (SCGEE/SCAE optional later); ship **SCBC** first (`SBSC` in discussion = **SCBC**).

## Literature anchors

| Piece | Source | Use |
| --- | --- | --- |
| Leaf sample σ; joins not sample-joined | Leaf-sample optimizer design, CIDR’20 §3.2 | Policy |
| HLL + sample for multi-col NDV | *Every Row Counts*, CIDR’19 | SCBC |
| Joint HLL ∩ / complements | Ertl, *New cardinality estimation algorithms for HyperLogLog sketches*, §5 | Join domain ∩ |
| Semi/anti inclusion baseline | Postgres / Leis-style NDV coverage (already in tree) | After removing sample presence |

## Current optd baseline (implementer must read)

- Leaf progressive sample AND: `apply_selection_profile` + `sample_cardinality.rs`.
- Semi/anti: `match_probability` from `equality_edge_estimate`; **catalog sample key presence currently overrides inclusion** (`sample_key_match_probability` / `key_presence_match_fraction`).
- Domain ∩ today: range disjoint + uniform range fraction + else `min(dL,dR)`; **explicitly refuses HLL inclusion–exclusion** in `estimated_distinct_intersection`.
- Aggregation: unique key → best `joint_ndv` → product of per-key distincts; `grouping_key_distinct` prefers live single-col HLL when sketches survive.
- Sketches cleared on filter/join population change (`scale_profile`, join derive).
- HLL API: `optd/sketches/src/hyperloglog.rs` — `registers()`, `precision()`, `is_compatible()`, default `p=12`, MurmurHash64A seed.

## Empirically known regression pressure

From TPC-H matchprob vs joint (`cardinality-regression-tpch-matchprob` vs `…-tpch-joint`):

- **q4** LeftSemi (EXISTS): sample presence **regressed** join q-error ~1.84 → ~139.
- **q22** anti: sample presence **improved** join q-error ~641 → ~4.76.

The leaf-filter sampling policy is expected to restore q4; q22 may worsen unless Ertl∩ (or later sketch-corrected NDV / constraints) finds small overlap. Do **not** keep sample presence as default to protect q22.

---

## Design

### A. Leaf-filter sampling with NDV-based joins

1. Leaf selections: unchanged (sample conjuncts when translatable).
2. Inner / semi / anti equality edges: **never** prefer independent table-sample key-set overlap as the primary `P(match)`.
3. Remove or demote `sample_key_match_probability` from the hot path in `equality_edge_estimate`.
4. Keep: NDV clamp to frequency/rows; inclusion `nn_L * intersection / dL`; SpaceSaving sketch match as optional **raise** of inclusion (existing); residual `residual_match_probability`.

### B. Ertl joint MLE domain intersection

When both column profiles still carry **compatible** `ColumnSketches.distinct_values` HLLs:

1. Run Ertl §5 joint MLE → rates \((\hat\lambda_a, \hat\lambda_b, \hat\lambda_x)\) for only-left, only-right, intersection.
2. Use \(\hat\lambda_x\) as `intersection.distinct`, clamped to \([0, \min(d_L, d_R)]\).
3. **Refuse** (fall back) when:
   - sketches incompatible (precision/seed/encoding);
   - optimizer reports unidentified / flat \(\lambda_x\) (Ertl notes all-equal-register pathology);
   - implied Jaccard \(\hat\lambda_x / (\hat\lambda_a+\hat\lambda_b+\hat\lambda_x)\) below a configured floor (start at `0.05`, document constant);
   - numerical failure.
4. Fallback order: range-disjoint → Ertl (if ok) → existing range-fraction / hist path → `min(dL,dR)`.
5. Population honesty: only apply when sketches are still attached (base scan / map retention). Do **not** invent filtered HLLs. After join, keep clearing sketches.

### C. Sketch-corrected bound-corrected grouping NDV (SCBC)

When aggregation (or composite equality needing joint NDV) lacks a usable catalog/schema-prefix `joint_ndv`:

1. Obtain a **row sample of the population being estimated**:
   - Prefer progressed leaf `SampleRelation` if the agg input is still a scan pipeline with samples;
   - Else catalog `table_samples` for the base table of the grouping columns when all keys share one base table and population is still complete;
   - Else skip sketch-corrected grouping NDV estimation (keep product-of-distincts).
2. Project grouping key columns; compute frequency multiset → \(f_i\) (count of distinct tuples occurring \(i\) times). *Every Row Counts* §4 recursive frequency algorithm is preferred; a correct HashMap prototype is OK for v1 if tested.
3. For each grouping column \(j\), read single-col HLL \(\hat D_j\) (or profile distinct if no HLL). Estimate \(\hat F_{1,j} = \max(0, \hat D_j - \sum_{i\ge 2} f_{i,j})\) using **per-column** sample frequencies when correcting bounds (as in the source paper); if per-column sample freqs are awkward in v1, use inequality (11) sanity bounds with \(\hat D_j\) only and document the simplification.
4. Apply **SCBC** (*Every Row Counts* §3.3.3): tighten BC singleton bounds with sketch inequalities (10)–(11), then  
   \(\hat D = \sqrt{\hat L_{\mathrm{SCBC}} \hat U_{\mathrm{SCBC}}} + \sum_{i\ge 2} f_i\) (paper formulas 16–18).
5. Cap by input rows; source `EstimateSource::Sketch` or a derived tagged source; use as group-count / joint distinct.

**SBSC naming:** treat as synonym for **SCBC** in this handoff. Optional follow-up: SCGEE as simpler sibling.

## Implementation choices

- Ertl uses dependency-free bounded coordinate maximization (golden-section searches with explicit
  zero-rate boundaries) of the joint Poisson likelihood, rather than BFGS. Per-rank
  `C< / C= / C>` sufficient statistics keep each likelihood evaluation O(q). Fits that exhaust the
  finite search or convergence budget return `NumericalFailure`. Identical sketches have an
  identified shared estimate; strict register dominance without positive equal registers is the
  unidentified case.
- SCBC uses HashMap tuple/per-column frequencies, the without-replacement lower bound from
  *Every Row Counts* §3.2, and equations 6–18 (including the unmodified product bound in equation 17).
  Population NDVs include a NULL grouping value when profile frequency indicates NULLs,
  even if NULL is absent from the reservoir. A fully covering joint NDV wins; partial joint NDVs
  constrain the full-tuple SCBC estimate from below.
- Grouping samples progress supported selections on scan/projection/output/sort pipelines.
  Sample/catalog population-size mismatches, unknown predicates, joins, limits, renames, maps,
  missing keys, and unsupported value types refuse SCBC and preserve product/joint fallback.
  Filtered grouping uses population-adjusted profile NDVs because base HLLs are cleared.
- No optional config knobs were added: compatible live HLLs enable Ertl with the documented
  `ERTL_MIN_JACCARD = 0.05` constant. SCBC currently corrects grouping, not composite join NDVs.

## Out of scope interactions

- Do not re-enable sample key presence behind sketch-corrected NDV / joint HLL estimation.
- Do not use HLL inclusion–exclusion (`|A|+|B|-|A∪B|`) anywhere.
- LIKE / adaptive QE / multi-col histograms remain separate TODOs.
