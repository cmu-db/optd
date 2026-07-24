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
| 2026-07-22 02:58 | 2026-07-22 03:02 | Outer-join cardinality bounds | Raised a finite row-count upper bound alongside the null-extension minimum, preserving `lower <= value <= upper`; added exact-profile coverage for left, right, and full outer joins. Commit approval/tool wait accounts for most of this interval. | milestone included in the next usage snapshot |
| 2026-07-22 03:02 | 2026-07-22 03:18 | Deferred default-cost evaluation | Added `PlanProperties` with `Arc<CardinalityProfile>`, profile-only join costing for `DefaultCostModel`, explicit compatibility selection for custom models, and bit-exact differential coverage for all join types and all three enumerators. Deferred search appends exactly `n-1` winning joins. JOB 15c measured 22.26 ms median, statistically flat versus 22.06 ms; Samply confirms profile construction is now 89.5% inclusive. | 593,554 / 18,100 s |
| 2026-07-22 03:18 | 2026-07-22 03:27 | Deferred-evaluator commit approval | The implementation and Stage-2 validation were complete at 03:18; the repository commit call returned as `e48711c` at 03:26. The wait is separated from implementation time. | milestone included below |
| 2026-07-22 03:27 | 2026-07-22 03:49 | Equality-participation-only cardinality DSU | Replaced full-column hash state with a sorted compact DSU over inherited class members and equality endpoints, using logarithmic lookup and iterative path compression. Added residual-wide, inherited/overlapping-class, NDV precedence, deterministic-order, 512-column, and adversarial 16,384-deep-chain tests. JOB 15c fell to 8.05 ms median; phase-3 Samply contains 214 matching samples versus 544 in phase 2. | 843,025 / 19,932 s |
| 2026-07-22 03:49 | 2026-07-22 03:58 | Durable benchmarks, snapshots, and final validation | Added a fair six-run deferred/materializing DPhyp benchmark with raw TSV, corrected its dynamic-set case to 65 relations, preserved the profiling SQL, refreshed 14 explain snapshots after proving every change is a bijective operator-ID renumbering, and completed all validation gates. Commits: `f09d420`, `d109c8f`. | 920,638 / 20,537 s |
| 2026-07-22 03:58 | 2026-07-22 04:15 | Artifact integrity and commit approval | Force-added the requested charts and raw/profile evidence despite the repository-wide artifact ignore, normalized generated CSV line endings, verified both SHA-256 manifests and all gzip/JSON payloads, and committed the evidence as `dc8b387`. Most elapsed time was repository approval/tool wait. | 981,289 / 21,523 s |
| 2026-07-24 21:24 | 2026-07-24 21:36 | Paper ASI and linearization | Implemented the associative `C_out` summary algebra, selectivity-minimum spanning tree, IKKBZ compound normalization, all-root comparison, and interval DP. Added ASI context-independence, algebra, exhaustive small-tree, and every-labeled-five-node-tree oracles. | Post-goal; tracker unavailable |
| 2026-07-24 21:36 | 2026-07-24 21:48 | Canonical GOO and Figure-7 repair | Replaced plan-cost greedy ordering with minimum-output-cardinality GOO; added indexed component discovery, most-expensive maximal repair scheduling, opaque contraction, and one global budget charged by actual inner-DP states. Added pair-scan differential, scheduling, contraction, and budget tests. | Post-goal; tracker unavailable |
| 2026-07-24 21:48 | 2026-07-24 21:56 | Adaptive policy, relation sets, and telemetry | Matched the paper's regular-graph/hypergraph solver pairing and relation thresholds; exposed per-group state/repair counts; completed canonical `Inline64`/`Inline128`/dense/sparse storage with indexed graph operations and sparse-safe construction. | Post-goal; tracker unavailable |
| 2026-07-24 21:56 | 2026-07-24 22:10 | Independent correctness audit and stack safety | Found and corrected an overcount in the first genuine-hypergraph CSG counter using an independent DPhyp-state oracle. Exhaustively checked all 1,024 five-node graphs and all 64 three-node hypergraphs, sampled 256 five-node hypergraphs, matched a directed multi-node LEFT ANTI repair to an exhaustive bushy oracle, and made group discovery, optimizer traversal, hypergraph construction, and recipe materialization iterative. Committed as `edb398e`. | Post-goal; tracker unavailable |
| 2026-07-24 22:10 | 2026-07-24 22:26 | Lazy custom-cost properties and release gates | Made compatibility evaluation build cardinality profiles only for linearized DP and GOO; exact DPhyp now remains independent of unused catalog statistics. Added empty-catalog negative-canary and forced linearized/GOO routing tests. An independent code audit found no blocker; 72 focused tests, 273 default-feature tests, 266 no-default tests, the unskipped adaptive SLT, 391 non-JOB workspace tests, and all-target clippy passed. | Post-goal; tracker unavailable |

## Evidence Baseline

- Command: `cargo test -p optd-core optimize::join_ordering -- --nocapture`
- Result: 18 passed, 0 failed, 160 filtered out.
- Existing limitation reproduced by `dphyp_solve_rejects_65_node_groups`.
- Existing design notes predated the current catalog-aware cost model and reported much lower
  absolute timings. Therefore the meaningful regression baseline is the untouched parent commit
  built and measured on the same machine and target profile.

## Historical Evidence (through the 2026-07-22 milestone)

The results below predate the 2026-07-24 paper-faithful IKKBZ and global-budget GOO/DP work. They
remain the regression baseline; current-algorithm results belong in a later milestone after its
validation completes.

- `cargo test -p optd-core`: 224 tests passed, plus doc tests.
- `cargo nextest run --release -p optd-datafusion --test slt -- adaptive_join_ordering`:
  1 passed in 0.464 s (287 skipped by the filter); it executes exact, linearized, dynamic-bitset,
  and GOO result cases through the complete SQL optimization pipeline.
- `cargo nextest run --release --workspace -E 'not test(/tests\\/slt\\/job/)' --no-fail-fast`:
  342 passed in 6.018 s (225 JOB tests excluded).
- `cargo test -p optd-core --no-default-features`: 217 passed, plus doc tests.
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
- JOB 15c with deferred profile-only candidate evaluation: 22.26 ms median across five release
  runs (21.27--23.49 ms), a noise-level +0.9% versus the scaffold. The default evaluator now
  materializes only the winning `n-1` joins; the compatibility evaluator remains available for
  arbitrary models. A 10 kHz Samply capture reported 544 matching samples, with
  `join_profile_from_conjuncts` at 89.5% inclusive and allocator/hash work dominating self time.
- JOB 15c with the compact equality DSU: 8.05 ms median across five release runs
  (8.03--9.35 ms), 63.8% below deferred phase 2 and 82.8% below the original baseline. The
  equivalent Samply capture fell to 214 matching samples; SipHash self samples dropped from 61 to
  2 and DSU lookup self samples from 26 to 3. `combine_join_columns`/B-tree insertion is now the
  largest profile-construction bucket.
- `cargo bench -p optd-core --bench join_ordering_candidate_evaluation -- 6`: deferred evaluation
  was 2.4%--22.9% faster over five exact-DPhyp chain/clique cases. It appended only `n-1` joins,
  versus 286 candidates for a 12-chain, 45,760 for a 65-chain, and 24,604 for a 9-clique. The
  benchmark uses symmetric untimed warmups, alternating order, fresh contexts, and identical
  catalog statistics; raw TSV and medians are preserved with the artifacts.
- The 14 explain snapshots affected by deferred materialization were regenerated. A separate audit
  verified a bijective old-to-new operator-ID mapping for every node and reference; masking
  `id`/`input`/`outer`/`inner` integers makes every plan byte-identical.
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
