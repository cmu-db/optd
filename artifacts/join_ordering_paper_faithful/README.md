# Adaptive Join Ordering Performance Artifacts

Generated on 2026-07-24 from commit `e604ff54448a1b01a4319ef98b8ef4be63fbaeac`.

## Headline results

- Adaptive median planning time is **0.274 ms at 10 relations**,
  **18.919 ms at 100**, **40.373 ms at 256**, and
  **2.038 s at 5,000**.
- The 5,000-relation adaptive runs selected **goo_linearized_dp=9**, built a median
  **10057 inner-DP states**, and contracted a median
  **66 subproblems**.
- At 256 relations, forced whole-query linearized DP takes **86.015 ms**; the adaptive
  to forced-linearized timing ratio is **0.47x**.
- Against the catalog-corrected pre-paper baseline at 256 relations, adaptive planning is **55.1x faster** (2223.087 ms -> 40.373 ms).
- Crossing from the inline 64-bit `RelationSet` tier to `Inline128` changes borrowed-union time
  by **1.07x** at 64 -> 65 relations.
- At 256 relations, in-place `|=` is **3.2x faster** than the previous
  allocate-and-replace formulation.
- At 1,024 relations, bulk `FromIterator` is **48.7x faster** than repeated
  singleton insertion and union.
- The sparse 16,385-slot allocate-and-replace/in-place timing ratio is **1.01x**.

## Relationship to Neumann and Radke (SIGMOD 2018)

Reference: [Adaptive Optimization of Very Large Join Queries](https://db.in.tum.de/~radke/papers/hugejoins.pdf).

The experiment mirrors the paper's optimization-time plots: deterministic random-tree join
graphs, increasing relation counts through 5,000, multiple algorithms, and
min/quantile/median/max summaries. The implementation follows Figure 8: DPhyp when the bounded
connected-subgraph count fits, IKKBZ-linearized interval DP for ordinary graphs through 100
relations, and GOO with a global 10,000-state linearized-DP repair budget above that. Hypergraphs
instead use bounded DPhyp repair.

The comparison is directional, not hardware-normalized. The paper uses 100 queries per size and
`C_out` as its full cost model. This local run uses 3 queries per ordinary size,
3 per mega-query size, and a constant-time execution-cost model so it isolates
search, cardinality ranking, hypergraph construction, and data-structure overhead.

## Methodology

- Hardware/platform: arm; `macOS-15.6.1-arm64-arm-64bit-Mach-O`.
- Toolchain: `rustc 1.96.0 (ac68faa20 2026-05-25)`.
- Workload: 3 deterministic random recursive trees per ordinary size and
  3 per mega-query size; 3 timed repetitions per query.
- Timed region: `JoinOrdering::run`, excluding query construction, cloning, and CSV output.
- Sizes: 10, 20, 30, 40, 70, 100, 128, 192, 256, 512, 1,024, 2,000, 5,000 relations.
- Forced DPhyp is limited to 10-18 relations to avoid unbounded exponential runs.
- No timeout samples or extrapolated values are included.

Reproduce from the repository root:

```bash
cargo bench -p optd-core --bench join_ordering_scalability -- \
  "$PWD/artifacts/join_ordering_paper_faithful/raw_measurements.csv" 3 3
python3 optd/core/benches/render_join_ordering_scalability.py \
  artifacts/join_ordering_paper_faithful/raw_measurements.csv \
  artifacts/join_ordering_paper_faithful \
  artifacts/join_ordering_paper_faithful/baseline_7a98797_catalog_fixed.csv
```

## Files

- `raw_measurements.csv`: every measurement.
- `adaptive_summary.csv` / `.md`: paper-style adaptive distribution table.
- `algorithm_summary.csv`: all algorithm distributions.
- `baseline_7a98797_catalog_fixed.csv`, `baseline_catalog_fixture.patch`, and
  `baseline_comparison.*`: fair pre-paper comparison and the complete fixture-only baseline diff.
- `relation_set_summary.csv` / `.md`: four-tier RelationSet operation and construction distributions.
- `figure_1_algorithm_scaling.*`: paper-style optimization-time curves.
- `figure_2_adaptive_policy.*`: policy choices and transition costs.
- `figure_3_work_scaling.*`: elapsed time versus materialized candidates.
- `figure_4_relation_set_boundary.*`: Inline64/Inline128/dense representation boundaries.
- `figure_5_topology_sensitivity.*`: chain, star, and clique behavior.
- `manifest.json`: SHA-256 inventory.

## Interpretation cautions

- These are optimizer-kernel timings, not SQL parsing, execution, or end-to-end query latency.
- A constant-time cost model makes algorithmic/data-structure effects visible but understates the
  production cardinality-costing overhead.
- The pre-paper comparison changes only its previously empty benchmark catalog fixture; it does
  not backport any optimizer change.
- `baseline_catalog_fixture.patch` is zero-context output so it carries no whitespace-only context;
  verify or apply it at `7a98797` with `git apply --unidiff-zero`.
- The random-graph quantiles are a workload distribution, as in the paper. Repetitions improve
  timing stability but are not independent query shapes.
- Candidate counts are appended IR operators for the compatibility evaluator. DP-state and repair
  columns are the algorithm's direct execution telemetry and are a better cross-representation
  work measure.
- The scheduler stops launching repairs once its 10,000-state budget is spent. One already-started
  repair can finish slightly above that number because its actual state count is known only after
  enumeration.
