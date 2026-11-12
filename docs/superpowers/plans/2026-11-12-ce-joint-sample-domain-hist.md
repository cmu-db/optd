# Joint Sample / Domain / Hist CE Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Progressive leaf-sample AND selectivity, inner-join intersection NDV carry, eq-class MCV/hist residuals, tighter join uppers, plus TODOs for joint NDV / multi-col histograms.

**Architecture:** Adjust `apply_selection_profile` (working sample), post-inner-join domain caps from `LeftEqualityCoverage`, retain `distribution` across join column derivation and resolve filters via eq-class, clamp join `rows.upper`.

**Tech Stack:** Rust `optd-core` / `optd-sampling`, existing unit tests in `analysis.rs`.

---

### Task 1: Progressive sample conjuncts

**Files:** `optd/core/src/analysis.rs` (`apply_selection_profile`), `optd/core/src/analysis/sample_cardinality.rs` (export translate if needed)

- [x] Failing test: correlated AND on leaf sample ≠ independent product
- [x] Implement working-sample loop with `match_fraction` + `filter_relation`
- [x] Confirm existing sample unit tests still pass

### Task 2: Join-key intersection domain

**Files:** `optd/core/src/analysis.rs` (`join_profile_with_selectivity_and_classes` / helper)

- [x] Failing test: filtered PK ⋈ FK sets class/key NDV to intersection
- [x] Apply coverages to inner-join column + class distincts

### Task 3: Eq-class histogram selectivity

**Files:** `optd/core/src/analysis.rs` (`derive_join_side_columns`, filter helpers)

- [x] Failing test: residual eq on peer uses MCV via class after join
- [x] Retain `distribution` on join derive; resolve distribution peer in filter path

### Task 4: Join uppers + TODOs

**Files:** `analysis.rs` inner row estimate; `docs/cardinality_estimation_v1.md`

- [x] Clamp join upper by product of input uppers × selectivity
- [x] Add TODOs for joint NDV / multi-col hist

### Task 5: Verify

- [x] Focused `cargo test -p optd-core` + `clippy -p optd-core --all-targets -- -D warnings`
