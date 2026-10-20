# Local TPC-H / JOB Benchmark Emitter

The local performance collector emits planning/execution measurements for
[`github-action-benchmark`](https://github.com/benchmark-action/github-action-benchmark)'s
`customSmallerIsBetter` input. It does not publish results, create GitHub comments, deploy Pages,
or replace SQL correctness tests. See [the continuous benchmarking design](continuous_benchmarking.md)
for repository ownership and later passes.

## Run locally

Run from the repository root with the pinned Rust toolchain, `protoc`, Python 3.11+, and curl installed.
Build in release mode; do not compare debug and release binaries.

```sh
cargo build --release --locked -p optd-datafusion --bin query-benchmark
bash scripts/download_tpch_hf.sh
bash scripts/download_job_hf.sh

# A small pilot; --limit is applied independently to each suite.
python3 optd/connectors/datafusion/scripts/run_query_benchmarks.py \
  --suite all --limit 1 --samples 3 --warmups 1 --target-partitions 1 \
  --query-timeout 900 --output target/query-benchmarks/pilot-001

# Full selected workloads, after auditing the pilot and supported coverage.
python3 optd/connectors/datafusion/scripts/run_query_benchmarks.py \
  --suite all --output target/query-benchmarks/nightly-001
```

The output directory must be new or empty. A run never resumes or combines results from an older
source revision. Use `--suite tpch` or `--suite job` to choose one dataset. With one suite, `--queries`
can select a single `.sql`/`.slt` file or an explicit directory of supported query fixtures. A reduced
manifest is a different comparison cohort, not silently a full-suite result.

Query files are naturally sorted. The default manifests are the existing
`tests/slt/tpch/results` and `tests/slt/job/results` directories under the DataFusion connector. The
worker parses SLT using the existing SLT library and requires exactly one unconditional successful
query, without setup statements or control/include/connection directives. It rejects ambiguity rather
than selecting only the first query. Expected SLT output is not compared by this performance emitter.

## What is measured

Each selected query runs in an isolated `query-benchmark` child process. The child registers the
suite's existing local Parquet providers and uses a reusable session with aggregate runtime-statistics
caching. It runs at least one warm-up, then the requested samples. Every sample constructs fresh
optimizer state and a fresh physical execution plan. It uses the production pass manager/root
resolution and the same direct physical conversion/physical optimization helper as `OptdRunner`.
There is no logical-conversion or native-DataFusion fallback: unsupported direct paths are explicit
failures and cannot contaminate the direct-execution series.

Raw samples record monotonic wall-clock milliseconds:

| Field | Boundary |
| --- | --- |
| `sql_planning_ms` | DataFusion SQL logical-plan creation |
| `statistics_import_ms` | Type coercion, provider checks, runtime statistics/cache lookup, and optd import |
| `optimizer_ms` | Canonical pass-manager run and final root resolution |
| `physical_planning_ms` | Direct optd-to-DataFusion conversion and DataFusion physical optimization |
| `planning_ms` | Entire planning path above, including statistics/import and small inter-phase overhead |
| `execution_ms` | Execute and fully collect the fresh physical plan, including result materialization |

Table registration, dataset hashing, warm-ups, row-count diagnostics, physical-plan formatting, and
JSON/checkpoint writes are outside recorded timings. Pass invocation timings/outcomes and the final
physical plan are retained as diagnostics, not extra headline charts. Sum-of-pass time is not total
planning time. Runtime statistics use normal bounded aggregate collection, not full-scan sketch mode.

These are **warm-cache** measurements. Dataset identity collection reads every Parquet file to compute
SHA-256 hashes, and warm-ups execute the query; neither a fresh child nor a fresh physical plan implies
a cold OS cache. Default parallelism is one DataFusion target partition and is recorded explicitly.
The collector does not control CPU frequency, OS scheduling, or hardware isolation.

## Structured Python orchestration

`optd/connectors/datafusion/scripts/query_benchmark_models.py` defines dataclasses for CLI options,
configuration/provenance, dataset/query/cohort identities, worker reports and samples, subprocess
results, query outcomes, manifests, and action metrics. Status values use string enums. Immutable
configuration/sample records prevent accidental field assignment; mutable manifests retain explicit
checkpoint state. The runner uses attribute access rather than nested untyped dictionaries.

Dataclass annotations do not perform runtime validation. `WorkerReport.from_json`,
`WorkerSample.from_json`, and `WorkerFailure.from_json` explicitly validate the worker JSON boundary;
`validate_worker` then checks execution policy, dataset path, repetition counts, and row consistency.
The `json_value` serialization boundary preserves version-1 JSON keys, optional fields, and cohort
identity encoding. Full Rust pass/plan diagnostics stay in the raw reports, not the aggregation models.

## Output and failure behavior

```text
<output>/
  manifest.json               # selection, provenance, configuration, coverage and statuses
  tpch.json                   # action input, ONLY if every selected TPC-H query succeeded
  job.json                    # action input, ONLY if every selected JOB query succeeded
  raw/<suite>/<query>/
    report.json               # warm-up progress, successful raw samples, pass timings and plans
    worker.log                # child diagnostics
```

Each action entry contains `name`, `unit: "ms"`, median `value`, descriptive min/max `range`, and
context in `extra`. Export only planning and execution medians initially. The reported range is not
a confidence interval and three samples do not establish statistical significance.

Metric names include a versioned cohort hash covering query content/selection, data-file identities,
partition count, statistics policy, and recorded environment/toolchain identity. They exclude source
SHA so history can compare code revisions. The manifest separately records source SHA/dirty status,
binary hash, collector-checkout SHA/dirty status/lockfile/toolchain, platform, runner metadata, and
workflow run/attempt when present. The worker reports its compiled-in dataset path; the parent rejects
success if that path differs from the data it hashed, preventing cross-worktree misattribution.

**Binary build provenance is explicitly `unknown`.** Collector-checkout metadata does not prove the
source revision, compiler, or profile used to build a stale/custom `--binary`. Build the worker with the
release command above in this checkout immediately before collection. A future publishing workflow
must bind the executable to its trusted source/build job before attaching source commit comments or
accepting baselines. Local dirty runs are allowed for investigation, not automatic accepted baselines.

A per-query deadline covers registration, statistics, warm-ups, and all measured samples, not just
execution. The orchestrator kills the query's process group on timeout. Successful sample checkpoints
survive failures. Manifest statuses distinguish `success`, `unsupported`, `error`, `crash`, and
`timeout`; the final run exits nonzero if any selected query failed. Other queries still run so one
failure does not discard coverage evidence. No failed measurement becomes zero or a fabricated timing.
A failed suite has no publishable scalar file; a fully successful other suite may still have one.
Read `manifest.json` before publishing and do not blindly glob every JSON file.

**Correctness is explicitly `not_checked`.** Stable output row counts across repetitions are a useful
sanity check, not a semantic oracle. Existing SLT correctness CI must remain in place. Full result
comparison, richer machine/hardware cohorts, memory limits, suite-wide budgets, and statistically
calibrated regression decisions are later work. Results are buffered like the production runner, so
large result sets can exhaust memory; process isolation/checkpointing preserves other queries but does
not impose a memory limit.

## Verification

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover \
  -s optd/connectors/datafusion/scripts -p 'test_run_query_benchmarks.py' -v
cargo test --release --locked -p optd-datafusion --lib benchmark::
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
```

The Rust tests exercise the real direct path on small in-memory data, reject ambiguous SLT, and load
all current TPC-H/JOB result fixtures without requiring dataset downloads. Python tests cover median
aggregation, invalid/incomplete samples, duplicate names, isolated failure statuses, retained successful
reports, no partial-suite export, stale-output refusal, cross-worktree dataset rejection, and explicit
unknown executable build provenance. Real release-mode dataset pilots are still
needed to establish query coverage and resource requirements before enabling nightly collection.

Initial verification completed: three focused Rust tests, 23 runner regression tests, 12 Python
emitter tests, Rust formatting, and workspace all-target Clippy passed. A release pilot using one
warm-up and three samples succeeded for TPC-H q1 and JOB 1a, producing complete action inputs and
raw checkpoints in `target/query-benchmarks/pilot-002/` (local ignored output, not committed results).
This is a two-query integration smoke test, not full-suite coverage or semantic correctness evidence.
