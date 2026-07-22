# Adaptive Join Ordering Performance Artifacts

Generated on 2026-07-21 from commit `ac1d171562fd0f9843504c040c4e65541c2ca149`.

## Headline results

- Adaptive median planning time grows from **0.329 ms at 10 relations** to
  **13.487 ms at 100** and **2.339 s at 256**.
- At 256 relations, forced linearized DP takes **36.684 ms**, or **63.8x less
  time** than the current adaptive choice (`GooDp`) on these sparse random trees.
- The policy chooses DPhyp for every 10-relation query, is mixed at 20 relations, uses linearized
  DP from 30 through 100, and switches to GOO/DP at 128 relations.
- Crossing from the inline 64-bit `RelationSet` representation to the dynamic representation
  increases the mixed set-operation microbenchmark by **9.7x** at 64 -> 65 relations.
- At 256 relations, in-place `|=` is **4.1x faster** than the previous
  allocate-and-replace formulation.
- At 1,024 relations, one-pass `FromIterator` is **53.8x faster** than repeated
  singleton insertion and union.

## Relationship to Neumann and Radke (SIGMOD 2018)

Reference: [Adaptive Optimization of Very Large Join Queries](https://db.in.tum.de/~radke/papers/hugejoins.pdf).

The experiment mirrors the paper's median optimization-time plots and appendix distributions:
deterministic random tree join graphs, increasing relation counts, multiple algorithms, and
min/quantile/median/max summaries. The paper uses 100 queries per size and its `Cout` cost model;
this local run uses 10 queries per size and a constant-time enumeration cost so it
isolates search and data-structure overhead.

The comparison is directional, not a hardware-normalized reproduction. The paper's adaptive
system uses GOO/linearized-DP for very large joins and reports roughly 10-70 ms around 100
relations, about 500 ms around 700 relations, and less than 20 seconds for 5,000 relations. optd's
current large-query path is GOO with bounded exact DPhyp repair (`GooDp`), not GOO/linearized-DP.
The measured 2.3-second median at only 256 relations identifies the
large-query path as the main remaining scalability gap.

## Methodology

- Hardware/platform: arm; `macOS-15.6.1-arm64-arm-64bit-Mach-O`.
- Toolchain: `rustc 1.96.0 (ac68faa20 2026-05-25)`.
- Workload: 10 deterministic random recursive trees per size; one timed pass per query.
- Timed region: `JoinOrdering::run`, excluding query construction, cloning, and CSV output.
- Sizes: 10, 20, 30, 40, 70, 100, 128, 192, and 256 relations.
- Forced DPhyp is limited to 10-18 relations to avoid unbounded exponential runs.
- No timeout samples or extrapolated values are included.

Reproduce from the repository root:

```bash
cargo bench -p optd-core --bench join_ordering_scalability -- \
  "$PWD/artifacts/join_ordering_scalability/raw_measurements.csv" 10 1
python3 optd/core/benches/render_join_ordering_scalability.py \
  artifacts/join_ordering_scalability/raw_measurements.csv \
  artifacts/join_ordering_scalability
```

## Files

- `raw_measurements.csv`: every measurement.
- `adaptive_summary.csv` / `.md`: paper-style adaptive distribution table.
- `algorithm_summary.csv`: all algorithm distributions.
- `relation_set_summary.csv` / `.md`: dynamic-set operation and construction distributions.
- `figure_1_algorithm_scaling.*`: paper-style optimization-time curves.
- `figure_2_adaptive_policy.*`: policy choices and transition costs.
- `figure_3_work_scaling.*`: elapsed time versus materialized candidates.
- `figure_4_relation_set_boundary.*`: inline/dynamic representation boundary.
- `figure_5_topology_sensitivity.*`: chain, star, and clique behavior.
- `manifest.json`: SHA-256 inventory.

## Interpretation cautions

- These are optimizer-kernel timings, not SQL parsing, execution, or end-to-end query latency.
- A constant-time cost model makes algorithmic/data-structure effects visible but understates the
  production cardinality-costing overhead.
- One timing per random graph gives a workload distribution, as in the paper, rather than repeated
  microbenchmark confidence intervals for an identical graph.
- Candidate counts are appended IR operators, not the number of pair-connectivity checks. GOO's
  pair search therefore consumes much more time than its materialized-candidate count suggests.
