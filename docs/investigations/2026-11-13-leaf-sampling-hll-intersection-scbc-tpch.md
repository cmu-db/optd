# Leaf-filter sampling, joint HLL intersection, and SCBC: dev13 TPC-H verification

## Tested source and environment

- Host: `achopra2@dev13.pdl` (Linux x86_64).
- Completed: `2026-11-13T22:37:15Z`.
- Local branch: `ac/leaf-sampling-hll-intersection-scbc`.
- Base commit: `8b3b2412d4c301fae2f462b5bc14d39ae554443b` **plus an uncommitted source snapshot**.
  The commit alone does not contain the implementation. `source-snapshot.json` records SHA-256
  hashes of all 13 transferred files; those hashes were verified before execution.
- Isolated remote worktree: `/home/achopra2/optd-ce-leaf-sampling-hll-intersection-scbc`.
- Existing remote checkout `/home/achopra2/optd` stayed on `ac/histogram-ce` at `29edcb4`,
  with no tracked-file or index changes. Its idle release build cache was reused.
- Rust/Cargo: pinned `1.96.0`; `CARGO_BUILD_JOBS=4`.
- Existing TPC-H SF 0.1 Parquet fixtures and PostgreSQL `optd-postgres-18` were reused.
  No data download or table reload was performed. Both engines were measured afresh.

## Commands and outcomes

```sh
export PATH="$HOME/bin:$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
export CARGO_TARGET_DIR=/home/achopra2/optd/target
export CARGO_BUILD_JOBS=4

cargo test --release -p optd-sketches -p optd-sampling -p optd-core --lib --locked
cargo test --release -p optd-core --no-default-features --lib --locked

REGRESSION_ROOT=target/cardinality-regression-tpch-leaf-sampling-hll-intersection-scbc \
REGRESSION_RUN_ID=leaf-sampling-hll-intersection-scbc-01a1d354 \
scripts/run_cardinality_regression.sh --suite tpch --skip-download --skip-load
```

- Release library tests: **277 core + 13 sampling + 11 sketches = 301 passed**, zero failures.
- Core without default features: **265 passed**, zero failures.
- optd collector: **22 queries, 354 subtree measurements, zero failures**.
- PostgreSQL collector: **22 queries, 238 plan-node measurements, zero failures**.
- SQLite, plots, CSV summaries, checkpoints, and run metadata were generated successfully.
- All 22 SQL-root actual row counts match both prior optd baselines. This harness measures
  cardinalities, not complete result values; no SQLLogicTest suite was run in this remote task.

## Comparison with prior optd runs

The baselines are `target/cardinality-regression-tpch-joint` (before sample-presence override)
and `target/cardinality-regression-tpch-matchprob` (with sample-presence override).
Q-error floors actual and estimated row counts at one.

| Query/node | Actual rows | Prior joint estimate | Prior joint q-error | Prior matchprob estimate | Prior matchprob q-error | Current estimate | Current q-error |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| q4 LeftSemi | 5,093 | 9,375 | 1.840762 | 36.621094 | 139.072853 | 9,375 | 1.840762 |
| q22 LeftAnti | 641 | 0.216629 | 641 | 3,052.692264 | 4.762390 | 0.216629 | 641 |
| q4 root | 5 | 5 | 1 | 4.996750 | 1.000650 | 5 | 1 |
| q22 root | 7 | 0.216629 | 7 | 100 | 14.285714 | 0.216629 | 7 |

The q4 semi-join regression is restored to the prior joint baseline. The q22 anti-join estimate
regresses to that baseline too, as expected when independent-sample key presence is removed.
Its root group-count estimate nevertheless has a smaller q-error than the matchprob run.

| Scope | Nodes | Joint geometric-mean q-error | Matchprob geometric-mean q-error | Current geometric-mean q-error |
| --- | ---: | ---: | ---: | ---: |
| All operator subtrees | 354 | 3.1627 | 3.0498 | 3.1643 |
| Join nodes | 72 | 3.2732 | 3.2478 | 3.2731 |
| SQL roots | 22 | 3.9805 | 4.1118 | 3.9805 |
| Aggregation nodes | 31 | 3.5135 | 3.6018 | 3.5198 |

These aggregate metrics describe distributions over each run's plans. The current and joint
reports also have matching operator names, parameters, and paths for all 354 nodes; 16 estimates
differ. Examples include q18's grouping NDV changing from 150,000 to 153,972.084 (actual 150,000),
with downstream selection q-error increasing from 7,500 to 7,698.604. q1's root remains 6 estimated
versus 4 actual (q-error 1.5). There is no claim of an overall CE-quality improvement: all-node
geometric-mean q-error is slightly worse than joint and worse than matchprob, while root error is
better than matchprob.

Synthetic compatible-HLL join and correlated-grouping SCBC regressions passed remotely. The
TPC-H reports do not tag each estimator invocation, so no per-query Ertl/SCBC usage or SCBC gain
is inferred solely from these report differences. Existing population-safety limitations remain:
filtered HLLs are unavailable, and grouping samples skip unsupported maps, renames, joins, limits,
and predicates. q22 remains an unresolved estimation-quality limitation, not a collector failure.

## Artifacts on dev13

Root: `/home/achopra2/optd/target/cardinality-regression-tpch-leaf-sampling-hll-intersection-scbc/`

- `source-snapshot.json`: exact transferred source hashes and base revision.
- `run-metadata.json`: environment, query counts, failure counts, and uncommitted-snapshot marker.
- `verification.log` and `reproduce-remote.sh`: full release tests and regression execution.
- `optd/report.json`, `postgres/report.json`, and `cardinality.sqlite`: raw measurements.
- `comparison/`: plots, CSVs, and `baseline-comparison-summary.{json,md}`.
- `tested-cardinality-regression`: retained hard link to the tested executable.
- `baseline-cardinality-regression`: retained pre-build executable.

The live execution log is also at `/tmp/optd-tpch-leaf-sampling-hll-intersection-scbc.log`.
The isolated worktree and final artifacts are retained for inspection/reproduction.
