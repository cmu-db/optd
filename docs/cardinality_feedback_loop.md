# Cardinality Feedback Loop

This documents the first `EXPLAIN ANALYZE` comparison run after
`EXPLAIN`/`ANALYZE` wrappers started routing their child plans through optd.

## Current Result

Command shape:

```sh
cargo build --release -p optd-datafusion --bin optd-cli
target/release/optd-cli --tpch -c "SET optd.log_explain_steps = false; EXPLAIN ANALYZE ..."
target/release/optd-cli --job -c "SET optd.log_explain_steps = false; EXPLAIN ANALYZE ..."
```

Raw comparison output:

```text
/tmp/optd_plan_compare2.tsv
```

Summary:

| Dataset | Completed | Timed out | Shape mismatches | Median max-join estimate / actual max rows | Worst completed |
| --- | ---: | ---: | ---: | ---: | --- |
| TPC-H | 22 / 22 | 0 | 0 | `3.1e2` | `q11`, `8.1e3` |
| JOB | 109 / 113 | 4 | 0 | `1.45e3` | `6f`, `4.6e4` |

Timed out JOB queries:

- `16a`
- `16c`
- `16d`
- `19d`

Slow completed JOB queries:

- `29c`: 12.7s
- `29a`: 9.5s
- `29b`: 9.2s
- `16b`: 4.5s

The join and scan counts matched for every completed query. That means the
`EXPLAIN ANALYZE` path is now using the optd-produced plan shape closely enough
for CE feedback. Remaining error is mainly cardinality-estimation quality, not
fallback to native DataFusion planning.

## Comparison Method

For each query:

- Get final optd estimates with `explain_optimizer_json(sql)`.
- Run `EXPLAIN ANALYZE sql` through `optd-cli`.
- Parse physical-plan metrics, especially `output_rows=...`.
- Compare:
  - final optd root estimate vs physical root rows,
  - max optd join estimate vs max observed physical rows,
  - logical scan/join counts vs physical source/join counts.

The max-join comparison is intentionally rough. Physical plans can add
projection, aggregate, repartition, and coalesce nodes, and DataFusion may add
runtime filters. The useful signal is whether CE is off by stable orders of
magnitude on the same logical shape.

## Reusable Procedure

Inputs for a dataset:

- a flag or setup mode for `optd-cli`, such as `--tpch` or `--job`;
- a directory of SLT result files, or a list of SQL files;
- a per-query timeout;
- a dataset label.

Procedure:

```sh
cargo build --release -p optd-datafusion --bin optd-cli

# For each SQL query:
# 1. SELECT explain_optimizer_json('<sql>');
# 2. EXPLAIN ANALYZE <sql>;
# 3. write one TSV row with estimates, metrics, plan-shape counts, duration,
#    and timeout/error status.
```

Suggested output columns:

```text
dataset
query
status
optimizer_seconds
analyze_seconds
optd_root_estimate
optd_max_estimate
optd_max_join_estimate
actual_root_rows
actual_max_rows
metric_count
optd_scan_count
physical_source_count
optd_join_count
physical_join_count
optd_aggregate_count
physical_aggregate_count
optd_operator_counts
physical_operator_counts
error
```

## Script

A reusable script lives at:

```text
optd/connectors/datafusion/scripts/compare_cardinality.py
```

Example CLI:

```sh
python3 optd/connectors/datafusion/scripts/compare_cardinality.py \
  --dataset tpch \
  --cli target/release/optd-cli \
  --setup-flag=--tpch \
  --queries optd/connectors/datafusion/tests/slt/tpch/results \
  --timeout 10 \
  --output /tmp/optd_tpch_ce.tsv

python3 optd/connectors/datafusion/scripts/compare_cardinality.py \
  --dataset job \
  --cli target/release/optd-cli \
  --setup-flag=--job \
  --queries optd/connectors/datafusion/tests/slt/job/results \
  --timeout 15 \
  --timeout-override '^16=8' \
  --timeout-override '^19d$=8' \
  --output /tmp/optd_job_ce.tsv
```

Special handling:

- Allow per-query timeout overrides, initially for JOB `16*`, `19d`, and `29*`.
- Parse SLT `query` blocks and stop at `----`.
- Disable pass logging before each command with
  `SET optd.log_explain_steps = false`.
- Emit TSV even for timeout/error rows.
- Print a short summary with status counts, slowest queries, shape mismatches,
  and top CE overestimates.

## Feedback Loop

Use this as the CardinalityEstimationV1 regression loop:

- [x] Support `EXPLAIN`/`EXPLAIN ANALYZE` wrappers without native fallback.
- [x] Run TPC-H and JOB once and record current baseline.
- [x] Add the reusable comparison script.
- [ ] Check in a small baseline summary file, not the full TSV.
- [ ] Add a focused CI/manual command for TPC-H and small JOB subsets.
- [ ] Track top CE misses by query and operator class after each estimator
  change.
- [ ] Promote stable improvements into unit tests where the expected math is
  local and deterministic.

The loop should stay lightweight: use full JOB for investigation, but use a
small curated subset for routine checks so long queries do not dominate local
development.
