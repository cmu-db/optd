# JOB 15c optimizer progression

All values are five-run medians of the `JoinOrdering` pass in a release build on the same Apple M2
Max development machine. Samply captures are not used for elapsed-time comparison because the 10
kHz sampler adds measurable overhead.

| Implementation phase | Median | Improvement from original |
|:---|---:|---:|
| Original materializing evaluator | 46.95 ms | — |
| Shared `Arc<CardinalityProfile>` cache | 31.62 ms | 32.7% |
| Sparse equivalence metadata | 22.57 ms | 51.9% |
| Pre-flattened join conjuncts | 22.08 ms | 53.0% |
| Compact plan-recipe scaffold | 22.06 ms | 53.0% |
| Deferred default-cost evaluation | 22.26 ms | 52.6% |
| Equality-participation-only compact DSU | 8.05 ms | 82.8% |

The phase-3 runs were 8.052459, 8.034459, 8.030083, 8.454333, and 9.354334 ms. The first three
optimizations reduce copied metadata. Deferred evaluation then proves that materializing rejected
IR nodes is no longer necessary, while the final DSU change attacks the dominant profile hotspot.
