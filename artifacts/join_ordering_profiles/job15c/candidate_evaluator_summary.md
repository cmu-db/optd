# Deferred candidate-evaluation benchmark

Command:

```bash
cargo bench -p optd-core --bench join_ordering_candidate_evaluation -- 6
```

Each path receives an untimed warmup. Six measured repetitions use fresh cloned queries and pass
instances, identical catalog statistics and adaptive configuration, and alternating evaluator
order. Timings cover only `JoinOrdering::run`. This run was recorded at 2026-07-21 23:47 EDT on an
Apple M2 Max from base `e48711c` plus the sparse-DSU milestone working tree; the finalized source
commit is `d109c8f`. Raw observations are in `candidate_evaluator_raw.tsv`.

| Shape | Width | Deferred median | Materializing median | Improvement | Appended operators (deferred / materializing) |
|:---|---:|---:|---:|---:|---:|
| 12-chain | 16 | 2.883 ms | 2.955 ms | 2.4% | 11 / 286 |
| 12-chain | 64 | 9.248 ms | 9.868 ms | 6.3% | 11 / 286 |
| 65-chain | 1 | 314.383 ms | 367.334 ms | 14.4% | 64 / 45,760 |
| 9-clique | 16 | 262.806 ms | 340.013 ms | 22.7% | 8 / 24,604 |
| 9-clique | 64 | 739.595 ms | 959.887 ms | 22.9% | 8 / 24,604 |

Both evaluators selected exact DPhyp in every case and differential unit tests establish bit-exact
tree and cost parity. The appended-operator column demonstrates the intended mechanism: deferred
evaluation appends only the winning `n - 1` joins, independent of the number of rejected candidates.
This benchmark therefore quantifies the DPhyp evaluator path only; separate semantic differential
tests cover linearized DP and GOO/DP, and the scalability benchmark measures their elapsed time.
