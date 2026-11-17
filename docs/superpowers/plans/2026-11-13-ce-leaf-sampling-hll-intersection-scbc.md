# Leaf Filter Sampling + Joint HLL Intersection + Sketch-Corrected Grouping NDV — Implementation Handoff

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Implement task-by-task with TDD. Checkboxes track progress.  
> **Design:** [`docs/superpowers/specs/2026-11-13-ce-leaf-sampling-hll-intersection-scbc-design.md`](../specs/2026-11-13-ce-leaf-sampling-hll-intersection-scbc-design.md)  
> **Repo guidance:** `AGENTS.md`, `docs/development.md`, `docs/optimizer.md` (append-only IR).  
> **Note:** `SBSC` in the requesting chat means **SCBC** (sketch-corrected bound-corrected).

**Goal:** (1) Leaf-filter sampling with NDV-based joins: remove the sample key-presence override
on semi/anti joins. (2) Joint HLL maximum likelihood for domain intersection when compatible
sketches exist. (3) Sketch-corrected bound-corrected (SCBC) grouping NDV estimation when
`joint_ndvs` are missing.

**Tech stack:** Rust 2024 workspace; `optd-sketches`, `optd-sampling`, `optd-core`; unit tests under `#[cfg(test)]`; TPC-H regress via `scripts/run_cardinality_regression.sh --suite tpch` on remote if available.

**Success criteria:**
- Focused unit tests green; `cargo clippy -p optd-sketches -p optd-sampling -p optd-core --all-targets --locked -- -D warnings`.
- q4-style LeftSemi no longer crushed by sample presence (regression test + ideally TPC-H q4 join q-error back near pre-matchprob).
- Ertl used on at least one synthetic compatible-HLL join where ∩ < min(dL,dR) and IE would be noisy.
- SCBC used on a multi-key aggregation without catalog joint_ndv where product-of-distincts would overestimate.

---

## Context the implementer must not re-discover

### Files to touch

| Area | Path |
| --- | --- |
| HLL + Ertl | `optd/sketches/src/hyperloglog.rs`, `optd/sketches/src/lib.rs` (new module OK: `ertl_joint.rs`) |
| Tuple frequencies / SCBC | Prefer `optd/sketches` or `optd/sampling` pure helpers; call from `optd/core` |
| Join / semi CE | `optd/core/src/analysis.rs` — `equality_edge_estimate`, `estimated_distinct_intersection`, `sample_key_match_probability`, `aggregation_profile`, `grouping_key_distinct` |
| Leaf samples | `optd/core/src/analysis/sample_cardinality.rs` (may need “sample for scan pipeline” reuse at agg) |
| Sampling presence API | `optd/sampling/src/evaluate.rs` — keep `key_presence_match_fraction` as library util; stop calling from CE hot path |
| Docs | `docs/cardinality_estimation_v1.md`, design spec status → Implemented |

### Critical existing behaviors

```text
equality_edge_estimate (today, violates leaf-filter sampling policy):
  inclusion = nn_L * intersection / dL
  sample_match = key presence on raw catalog samples   ← REMOVE from default
  non_null_match = sample_match.or(sketch.max(inclusion)).unwrap_or(inclusion)

estimated_distinct_intersection (today):
  range disjoint / range fraction / else min(dL,dR)
  comment forbids HLL inclusion-exclusion ← keep forbid; add Ertl instead

aggregation_profile (today):
  unique → joint_ndv → product(grouping_key_distinct)
  grouping_key_distinct prefers live HLL if sketches present
```

### Disk / build notes

Local disk may be tight; use `CARGO_TARGET_DIR=/tmp/optd-cargo-target` if needed. Prefer focused tests over full workspace.

---

### Task 0: Branch + baseline

- [x] Confirm clean working tree / create branch from current CE branch (e.g. `ac/histogram-ce` or `ac/leaf-sampling-hll-intersection-scbc`).
- [x] Skim design spec; skim `equality_edge_estimate` + `anti_join_uses_sample_key_presence_over_inclusion_ndv`.
- [x] Record baseline: `cargo test -p optd-core --lib anti_join_uses_sample` and `aggregation_prefers_hll` (expect presence test exists).

---

Baseline / branch evidence: the working branch is `ac/leaf-sampling-hll-intersection-scbc`, based on `ac/histogram-ce`; only the supplied design/plan were untracked. Both baseline tests passed (one test each). The replacement sparse-sample regression failed before the fix (anti rows 4 instead of 0), then passed after removal of the CE override; the focused semi/anti and sampling-presence utility tests also passed.

### Task 1: Leaf-filter sampling with NDV-based joins (demote sample presence)

**Files:** `optd/core/src/analysis.rs`; tests in same file; optionally `docs/superpowers/specs/2026-11-12-ce-match-prob-groupby-design.md` note superseded.

**TDD:**
- [x] Rewrite or replace `anti_join_uses_sample_key_presence_over_inclusion_ndv`:
  - Either delete and add `semi_anti_do_not_use_independent_sample_key_presence` asserting inclusion wins when samples would claim tiny overlap but NDVs nest; **or**
  - Keep presence helper tested only in `optd-sampling`, not CE.
- [x] Add `left_semi_uses_inclusion_when_catalog_samples_would_underestimate` mirroring q4: dL≈dR large, samples sparse → semi rows stay near inclusion, not ~0.

**Implement:**
- [x] In `equality_edge_estimate`, stop using `sample_key_match_probability` for `non_null_match_probability`.
- [x] Leave `sample_key_match_probability` / sampling crate API in place but unused (or `#[allow(dead_code)]` / private) until a future gated experiment — do **not** call it from the hot path.
- [x] Update `docs/cardinality_estimation_v1.md` join section: remove “Rusu-style key-presence overrides inclusion” as default; point to this plan.

**Verify:**
- [x] `cargo test -p optd-core --lib left_semi_uses_inclusion`
- [x] `cargo test -p optd-core --lib match_probability_for_semi`
- [x] `cargo test -p optd-sampling --lib key_presence` (library util still ok)

---

### Task 2: Ertl joint MLE in `optd-sketches`

**Files:** new `optd/sketches/src/ertl_joint.rs` (recommended); export from `lib.rs`.

**API sketch:**

```rust
pub struct JointCardinalityEstimate {
    pub only_left: f64,
    pub only_right: f64,
    pub intersection: f64,
    pub union: f64,
}

pub enum JointEstimateQuality {
    Ok,
    UnidentifiedIntersection,
    LowJaccard { jaccard: f64 },
    NumericalFailure,
}

pub fn estimate_joint(
    left: &HyperLogLog,
    right: &HyperLogLog,
) -> Result<(JointCardinalityEstimate, JointEstimateQuality), HyperLogLogError>;
```

**Algorithm (Ertl arXiv:1702.01284 §5):**
- [x] Require `left.is_compatible(right)`.
- [x] Build register-pair tallies / multiplicity stats \(C^{<}, C^{=}, C^{>}\) (paper notation).
- [x] Optimize \((\lambda_a, \lambda_b, \lambda_x)\) under Poisson joint likelihood (bounded coordinate maximization; see implementation record). Prefer a small dependency-free implementation; if adding a crate, justify in PR.
- [x] Map rates → cardinalities; `union = only_left + only_right + intersection`.
- [x] Quality: `UnidentifiedIntersection` when likelihood flat in \(\lambda_x\); `LowJaccard` if \(J < 0.05\) (constant `ERTL_MIN_JACCARD`); clamp negatives to 0.

**TDD:**
- [x] Identical sketches → intersection ≈ single estimate; only_* ≈ 0 (or Unidentified — assert documented behavior).
- [x] Disjoint synthetic registers / known construction → intersection ≈ 0.
- [x] Partial overlap construction → Ertl closer to truth than inclusion–exclusion on the same pair (assert with fixed seeds).
- [x] Incompatible precision/seed → `Err(Incompatible)`.

**Verify:**
- [x] `cargo test -p optd-sketches --lib`
- [x] `cargo clippy -p optd-sketches --all-targets -- -D warnings`

---

### Task 3: Wire Ertl into join domain intersection

**Files:** `optd/core/src/analysis.rs` — `estimated_distinct_intersection` and/or `equality_edge_estimate`.

**TDD:**
- [x] Two base columns with compatible HLLs whose domains partially overlap (construct sketches by observing known value sets) → intersection uses Ertl, not blind `min`.
- [x] After a filter that clears sketches, path falls back to min/range (no panic).
- [x] Low-Jaccard / Unidentified → fallback; never NaN.

**Implement:**
- [x] If both profiles have `sketches.distinct_values` and `estimate_joint` returns `Ok` + `Quality::Ok`, set `intersection.distinct = ertl.intersection.clamp(0.0, min(dL,dR))`.
- [x] Keep existing range-disjoint short-circuit **before** Ertl.
- [x] Update the comment that forbade IE: still forbid IE; Ertl is the allowed sketch ∩.
- [x] Config decision: no optional knobs added; default on with live compatible sketches and constant `ERTL_MIN_JACCARD = 0.05`.

**Verify:**
- [x] `cargo test -p optd-core --lib` filtered to join/intersection/HLL tests
- [x] Full `analysis::tests` if cheap enough

---

### Task 4: Sketch-corrected bound-corrected grouping NDV (SCBC)

**Files:** new helper module (e.g. `optd/sampling/src/sketch_corrected_ndv.rs` or `optd/sketches/src/sketch_corrected_ndv.rs`); `aggregation_profile` in `analysis.rs`; possibly expose sample access for scan-pipeline agg inputs.

**Formulas (*Every Row Counts*, CIDR’19 §3.3.3 SCBC):**
- From sample of \(n\) rows over the grouping tuple: frequencies \(f_i\), \(f_1\) singletons, \(D_{\mathrm{sample}}\) etc. as in paper.
- Single-col HLL estimates \(\hat D_j\).
- Bounds (paper 10–11, 16–18): tighten BC singleton bounds with \(\max_j \hat F_{1,j}\) and \(\prod_j \hat D_j\);  
  \(\hat D_{\mathrm{SCBC}} = \sqrt{\hat L \hat U} + \sum_{i\ge 2} f_i\).
- Cap by \(N\) (input rows).

**TDD:**
- [x] Two correlated grouping columns: product of distincts ≫ true joint; SCBC closer to true joint on a synthetic sample+HLL fixture.
- [x] When catalog `joint_ndv` exists, still prefer joint_ndv (SCBC not used).
- [x] When no sample available, fall back to today’s product path (no panic).
- [x] Single grouping key: SCBC not required; existing `grouping_key_distinct` HLL path remains.

**Implement:**
- [x] Pure function: `(sample_batches or SampleRelation, key column names, &[f64] /* per-col HLL NDVs */, population_rows) -> Estimate`.
- [x] In `aggregation_profile`, when `!unique_grouping` and no sufficiently covering `joint_ndv`, try SCBC before folding product-of-distincts.
- [x] Sample acquisition: reuse `try_leaf_sample_relation`-style scan-pipeline walk on agg input; if selections exist, either use progressed filters (best) or base sample with documented limitation for v1 — **prefer progressed leaf sample** if implementable with existing `sample_conjunct_selectivity_progressing` helpers.
- [x] Do not apply base HLL as filtered population without sample from the filtered population.

**Verify:**
- [x] `cargo test -p optd-core --lib aggregation`
- [x] `cargo test -p optd-sketches --lib` / sampling SCBC tests

---

### Task 5: Docs + completeness TODOs

- [x] Mark design spec **Status: Implemented** when all tasks done.
- [x] Update `docs/cardinality_estimation_v1.md`:
  - Join: leaf-filter sampling policy; Ertl∩ when sketches compatible; no sample-presence override.
  - Aggregation: SCBC when joint_ndv missing.
  - Progress checkboxes for Ertl + SCBC.
- [x] Short note in `docs/cardinality_estimation_v1_math.md` if present.
- [x] Identify joint HLL maximum likelihood as our intersection estimator; do not attribute it to another database system.

---

### Task 6: Verification + optional TPC-H

**Local:**
- [x] `cargo test -p optd-sketches -p optd-sampling -p optd-core --lib` (reused existing target cache; no extra `/tmp` cache needed).
- [x] `cargo clippy -p optd-sketches -p optd-sampling -p optd-core --all-targets --locked -- -D warnings`
- [ ] `cargo fmt --all --check`

**Remote (if asked / same workflow as prior CE slices):**
- [ ] Commit + push the implementation (remote testing used a verified uncommitted snapshot instead; shared-history message rewrites were pushed separately).
- [x] On `achopra2@dev13.pdl`: test the source snapshot in an isolated worktree;  
  `REGRESSION_ROOT=target/cardinality-regression-tpch-leaf-sampling-hll-intersection-scbc \`  
  `scripts/run_cardinality_regression.sh --suite tpch --skip-download --skip-load`  
  log `/tmp/optd-tpch-leaf-sampling-hll-intersection-scbc.log`.
- [x] Compare report vs `cardinality-regression-tpch-joint` and `…-matchprob` especially **q4** and **q22** join/root q-errors.

**Handoff report must include:** changed files, tests run, TPC-H deltas if run, known limitations (base-HLL-only Ertl; SCBC sample population caveats; q22 expectation).

---

## Implementation / verification record

- Ertl fits the published joint Poisson likelihood with bounded coordinate maximization and explicit zero-rate boundaries instead of BFGS/Newton. No dependencies were added. The per-rank sufficient statistics bound objective work to O(q); exhausted search/convergence budgets return `NumericalFailure`.
- Default library suites passed: **277 core, 13 sampling, 11 sketches**. Core `--no-default-features --lib` passed: **265 tests**. Affected-package strict Clippy and `cargo clippy --workspace --all-targets --locked -- -D warnings` passed. Final-source verification also passed all of those suites and full-workspace Clippy after the strengthened million-NDV semi/anti, partial-joint lower-bound, stale-sample, and rare-NULL regressions.
- Initial integration builds caught a test-only `Arc<ColumnSketches>` mismatch; fixed before the passing suites. The sparse-sample semi/anti regression was confirmed failing before the policy fix and subsequently strengthened to equal million-NDV populations with four-row independent samples.
- Independent read-only reviews checked Ertl math and SCBC population safety. Follow-up SCBC review accepted corrections for partial joints, stale sample row counts, and population-known NULLs. Its proposed equation-17 subtraction was withdrawn after checking the published equation; a mixed-frequency test preserves the paper's formula.
- `cargo fmt --all --check` was run and **fails on pre-existing formatting** in `optd/connectors/datafusion/src/{distribution_stats,runtime_statistics}.rs`, `optd/core/src/lib.rs`, and `optd/sampling/src/evaluate.rs`. Those files were left unchanged. Explicit rustfmt checks for all six changed Rust files and `git diff --check` passed.
- LSP could not confirm diagnostics (core analysis exceeds its size limit; other changed files had no primary server response). Cargo tests/Clippy supply compiler validation instead.
- Remote verification on `dev13.pdl` passed **301 release library tests + 265 no-default-features core tests**, then all **22 TPC-H queries** for both engines with **zero failures** (354 optd / 238 PostgreSQL measurements). Relative to matchprob, q4 LeftSemi q-error recovered **139.072853 → 1.840762**, while q22 LeftAnti regressed **4.762390 → 641**. q22 root q-error improved **14.285714 → 7**. All 22 root actual row counts match both baselines. All-node geometric-mean q-error changed **3.0498 → 3.1643**; no overall CE-quality improvement is claimed. See the [remote verification report](../../investigations/2026-11-13-leaf-sampling-hll-intersection-scbc-tpch.md) for commands, source provenance, aggregate metrics, limitations, and artifact paths. The implementation remains uncommitted; remote tests used a SHA-256-verified snapshot over base `8b3b241`.
- Remaining scope limits: Ertl requires compatible live/base-population HLLs and rejects Jaccard below 0.05; SCBC uses HashMap frequencies, adjusted profile NDVs after filters, and only supported scan pipelines. Unknown filters, maps, renames, joins, limits, unsupported values, stale sample metadata, and inadequate samples retain fallback. SCBC is wired for grouping, not composite joins.

## Implementation order (do not reorder casually)

1. Task 1 leaf-filter sampling policy (unblocks q4-class failures; simplifies mental model).  
2. Task 2 Ertl in sketches (pure, testable).  
3. Task 3 Wire Ertl.  
4. Task 4 sketch-corrected bound-corrected NDV estimation (SCBC).  
5. Task 5 Docs.  
6. Task 6 Verify / regress.

## Pitfalls

- **Hash seed / encoding mismatch** across tables → Ertl never fires; ensure regression collector uses one HLL config.
- **Filtered populations + base HLL** → wrong ∩; refuse when sketches cleared.
- **Reimplementing IE “temporarily”** → forbidden.
- **Keeping sample presence “just for anti”** without an explicit gated design + tests → out of scope for this handoff.
- **SCBC on joined inputs** without a sample of the join output → skip sketch-corrected grouping NDV estimation; product/joint_ndv only.
- Disk full on local builds → use `/tmp/optd-cargo-target`.

## Done definition

All Task 1–5 checkboxes complete; Task 6 local verify green; design spec status updated; PR/handoff summary lists before/after q4/q22 expectations even if remote regress is pending.
