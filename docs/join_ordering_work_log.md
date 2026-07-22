# Join Ordering Improvement Work Log

Times are recorded in UTC and elapsed/token totals are copied from the active Codex goal at
milestones. Token counts are cumulative estimates reported by the task runtime, not source-code
token counts.

| Started (UTC) | Finished (UTC) | Task | Outcome | Goal tokens / elapsed |
|---|---|---|---|---|
| 2026-07-16 02:02 | 2026-07-16 02:04 | Baseline repository and literature audit | Located the monolithic `u64` DPhyp implementation, hypergraph/cost dependencies, existing design notes, and reference algorithms. The 18 focused tests pass; the existing 65-relation test confirms the hard failure. | 8,654 at goal inspection / 7 s |
| 2026-07-16 02:04 | 2026-07-16 02:10 | Modular relation-set and graph design | Added canonical inline/dynamic `RelationSet`; migrated hypergraph, CD-E connectivity, analysis, normalization, and DPhyp; removed the 64-relation assertion. | milestone folded into 317,388 / 1,326 s total below |
| 2026-07-16 02:10 | 2026-07-16 02:16 | Adaptive enumerators and correctness | Added bounded connected-subgraph counting, configurable policy, interval DP, GOO plus exact maximal-subtree improvement, non-commutative orientation preservation, and opt-in physical orientation search. Added closed-form, exhaustive-oracle, and 65-relation tests. | milestone folded into 317,388 / 1,326 s total below |
| 2026-07-16 02:16 | 2026-07-16 02:20 | Benchmarks and feature coverage | Added dependency-free `cargo bench`, end-to-end exact/linearized SQL cases, and direct dynamic-DPhyp/GOO coverage. Refined the symmetric-cost hot path after the first measurement. | milestone folded into 317,388 / 1,326 s total below |
| 2026-07-16 02:20 | 2026-07-16 02:24 | Focused validation | Core tests and all-target clippy passed with warnings denied. Updated design and evidence log. | 317,388 / 1,326 s |
| 2026-07-16 02:24 | 2026-07-16 02:32 | Baseline comparison and hot-path audit | Compared the release pass profiler against untouched `54c9bdf`. Removed release-mode witness-tree cloning; final 64-relation delta is +0.59%. Added forced-exact 128-chain control, showing adaptive GOO/DP is 7.0× faster. | milestone folded into final usage below |
| 2026-07-16 02:32 | 2026-07-16 02:58 | Fixture-backed workspace validation | Provisioned pinned TPC-H/JOB fixtures, isolated pre-existing JOB expectation drift against the untouched parent and DuckDB, replaced impractical wide SQL execution with direct optimizer tests, and completed all non-JOB, no-default, lint, format, and workflow gates. | 685,188 / 3,392 s final goal usage |
| Not recorded | 2026-07-16 16:14 | Join-ordering module documentation and layout | Moved the pass into `join_ordering/mod.rs`; extracted exact DP, candidate handling, group discovery, and tests into focused modules; expanded module and design documentation. Committed as `14ee750`. | Post-goal; tracker unavailable |
| 2026-07-16 16:14 | 2026-07-16 16:22 | Explicit pass scheduling | Added `PassMode::{Once, ToFixpoint}`, migrated `JoinOrdering` and `JoinTreeNormalize`, removed pointer/run-id lifecycle state, and added manager-reuse and one-shot scheduling tests. | Post-goal; tracker unavailable |
| 2026-07-16 16:43 | 2026-07-16 16:55 | Paper-style scalability artifacts | Added a reproducible random-tree/topology/RelationSet benchmark, collected 391 release measurements, and generated five inspected PNG/SVG figures plus raw and summary CSV/Markdown tables under `artifacts/join_ordering_scalability/`. | Post-goal; tracker unavailable |
| 2026-07-21 20:26 | 2026-07-21 20:36 | Shared cardinality profiles | Switched the internal cardinality cache and cost-model lookups to `Arc<CardinalityProfile>` while preserving the public owned API; added cache-identity and invalidation tests. JOB 15c `JoinOrdering` median fell from 46.95 ms to 31.62 ms across five release runs. | 98,855 / 618 s |
| 2026-07-21 20:53 | 2026-07-21 20:57 | Sparse equivalence metadata | Removed redundant singleton equality classes, documented the sparse invariant, and added constructor, projection, rename, inner-join, and transitive-selectivity tests. JOB 15c fell further to 22.57 ms median. | 135,534 / 1,890 s |
| 2026-07-21 20:59 | 2026-07-21 21:03 | Single-pass join conjuncts | Split cardinality estimation into expression and pre-flattened-conjunct entry points, removed repeated nested-`AND` traversal, and added semantic-parity coverage. JOB 15c measured 22.08 ms median. | 180,185 / 2,234 s |
| 2026-07-21 21:03 | 2026-07-21 21:11 | Dynamic `RelationSet` mutation and construction | Implemented allocation-free owned union when capacity suffices and one-pass `FromIterator`; added representation/hash/boundary tests and direct old-formulation benchmark comparisons. At 256 relations in-place `|=` is 4.1x faster; at 1,024 relations one-pass collection is 53.8x faster. | 212,463 / 2,697 s |
| 2026-07-21 23:01 | 2026-07-21 23:10 | Outer-join equality correctness | Prevented null-producing join sides and `ON` equalities from becoming globally valid equivalence classes; added all-join-type and chained-join tests demonstrating that downstream equalities are not incorrectly treated as redundant. | 306,292 / 3,252 s |
| 2026-07-21 23:10 | 2026-07-21 23:16 | Candidate evaluation and plan-recipe scaffold | Routed DPhyp, linearized DP, and GOO through shared `JoinSearch`, evaluator, and accepted-plan arena abstractions. Added compatibility tests for custom cost composition and deferred recipe reconstruction. Across 103 deterministic benchmark rows, algorithm choice and candidate counts were identical; timing ratio was 0.993. JOB 15c remained neutral at 22.06 ms median over five runs. | 406,300 / 3,626 s |
| 2026-07-21 23:16 | 2026-07-22 02:58 | Checkpoint approval and tool wait | The scaffold was already reviewed, tested, and staged at 23:16; commit `b8dc8d7` completed after the approval/tool call returned. This interval is recorded separately from implementation time. | 489,920 / 16,952 s |
| 2026-07-22 02:58 | 2026-07-22 02:59 | Outer-join cardinality bounds | Raised a finite row-count upper bound alongside the null-extension minimum, preserving `lower <= value <= upper`; added exact-profile coverage for left, right, and full outer joins. | milestone included in the next usage snapshot |

## Evidence Baseline

- Command: `cargo test -p optd-core optimize::join_ordering -- --nocapture`
- Result: 18 passed, 0 failed, 160 filtered out.
- Existing limitation reproduced by `dphyp_solve_rejects_65_node_groups`.
- Existing design notes predated the current catalog-aware cost model and reported much lower
  absolute timings. Therefore the meaningful regression baseline is the untouched parent commit
  built and measured on the same machine and target profile.

## New Evidence

- `cargo test -p optd-core`: 189 tests passed, plus doc tests.
- `cargo nextest run --release -p optd-datafusion --test slt -- adaptive_join_ordering`:
  1 passed in 1.135 s (287 skipped by the filter); it executes exact, linearized, dynamic-bitset,
  and GOO result cases through the complete SQL optimization pipeline.
- `cargo nextest run --release --workspace -E 'not test(/tests\\/slt\\/job/)' --no-fail-fast`:
  306 passed in 5.108 s (225 JOB tests excluded).
- `cargo test -p optd-core --no-default-features`: 182 passed, plus doc tests.
- `cargo test -p optd-datafusion --lib`: 52 passed.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: passed.
- `cargo fmt --all --check`, `git diff --check`, and `actionlint`: passed.
- `cargo bench -p optd-core --bench join_ordering -- 3`, post-refinement means:
  DPhyp clique-10 105.47 ms; linearized clique-18 14.78 ms; dynamic DPhyp chain-65
  50.24 ms; GOO/DP chain-128 135.27 ms; forced DPhyp chain-128 948.68 ms.
- `profile_passes` (two runs): unchanged parent 64-relation max 4,868.10 ms; new tree
  4,896.65 ms (+0.59%).
- JOB 15c pass-only timing after shared cardinality profiles: 31.62 ms median across five release
  runs (30.71--53.70 ms), versus the 46.95 ms pre-change median, a 32.7% reduction.
- JOB 15c after sparse equivalence metadata: 22.57 ms median across five release runs
  (21.93--25.13 ms), 28.6% below the Arc-only median and 51.9% below the original baseline.
- JOB 15c after single-pass conjunct handling: 22.08 ms median across five release runs
  (21.99--23.29 ms), 53.0% below the original baseline.
- JOB 15c after introducing shared evaluator/recipe infrastructure: 22.06 ms median across five
  release runs (21.87--22.67 ms); 103 deterministic scalability rows retained identical algorithm
  decisions and candidate counts, with a 0.993 median timing ratio.
- Dynamic `RelationSet` direct comparisons: in-place `|=` is 4.1x faster than allocate-and-replace
  at 256 relations; one-pass `FromIterator` is 53.8x faster than repeated singleton union at
  1,024 relations. Raw distributions and the revised boundary chart are under
  `artifacts/join_ordering_scalability/`.

The unfiltered workspace run is not currently a clean regression signal: the checked-in JOB
expectations disagree with the repository's pinned parquet revision. Two representative failures
(`results/10c` and `explain_flat/10a`) reproduce byte-for-byte on untouched `54c9bdf`, and DuckDB
returns the same result row as optd for `results/10c`. Those fixtures were not rewritten as part
of this join-ordering change.

## Logging convention

Each implementation milestone receives a row with wall-clock timestamps, validation performed,
and the cumulative goal usage visible at that milestone. Short compile/fix iterations are folded
into the enclosing task so the log remains useful rather than becoming a shell transcript.
