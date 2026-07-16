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
  1 passed in 0.696 s (287 skipped by the filter); it executes exact and linearized result cases.
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

The unfiltered workspace run is not currently a clean regression signal: the checked-in JOB
expectations disagree with the repository's pinned parquet revision. Two representative failures
(`results/10c` and `explain_flat/10a`) reproduce byte-for-byte on untouched `54c9bdf`, and DuckDB
returns the same result row as optd for `results/10c`. Those fixtures were not rewritten as part
of this join-ordering change.

## Logging convention

Each implementation milestone receives a row with wall-clock timestamps, validation performed,
and the cumulative goal usage visible at that milestone. Short compile/fix iterations are folded
into the enclosing task so the log remains useful rather than becoming a shell transcript.
