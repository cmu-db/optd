# Cardinality Regression Harness

The DataFusion connector includes a harness that measures cardinality error for every relational
operator subtree in each input plan. It is intended for estimator evaluation, not as a correctness
oracle for query results.

## Measurement

Every operator reachable through relational input edges is treated as the root of a subtree:

- **Join count** is the number of joins in that subtree, matching the grouping used by Leis et al. in
  “How Good Are Query Optimizers, Really?”. Both predicate joins and cross products count because
  each combines two relational inputs.
- **Estimated rows** come from `CardinalityEstimationV1` on the final optimized optd plan.
- **Actual rows** come from streaming the DataFusion physical plan constructed directly for that
  subtree and summing its output batch row counts.
- **Q-error** is the larger of estimated/actual and actual/estimated after flooring both counts at
  one row. Thus zero versus zero has q-error 1, while zero estimated versus ten actual rows has
  q-error 10.

Shared operator handles are reported once per tree occurrence but exact counts are cached by handle
within a query. Operators embedded only inside scalar subquery expressions are not separate
measurements. A relational subtree with free outer columns has no context-independent cardinality,
so the harness rejects it rather than reporting a misleading q-error. Exact subtree execution can be
expensive because each distinct closed subtree is executed independently.

## Running

The root `justfile` provides the supported build, test, baseline-update, and regression entry
points. Run `just` for the complete list. Regression recipes select both TPC-H and JOB by default;
set `DATASET=tpch` or `DATASET=job` to run one suite. A typical two-suite workflow is:

```sh
just build-binaries
just load-postgres-all
just regression-optd-smoke
just regression-postgres-smoke
PLOT_PYTHON="conda run -n c0bench python" just regression-compare-smoke
```

For a TPC-H-only smoke workflow, prefix the collector recipes with `DATASET=tpch` and load only
`just load-postgres-tpch`. `--limit N` is applied independently to each selected suite.

### Portable end-to-end workflow

`scripts/run_cardinality_regression.sh` is the end-to-end entry point for either a local or remote
benchmark machine. It downloads the selected Parquet suite when needed, creates or starts a pinned
`postgres:18.6-bookworm` container with persistent host storage, waits for readiness, loads and
`ANALYZE`s the tables, runs the isolated optd collector and PostgreSQL `EXPLAIN ANALYZE` collector,
then generates the suite matrix, dashboard, logs, and run metadata. JOB is the script default:

```sh
PLOT_PYTHON="conda run -n c0bench python" \
  scripts/run_cardinality_regression.sh --suite job
```

Use `--suite all` for TPC-H plus JOB or `--suite tpch` for TPC-H only. The machine needs Docker,
DuckDB, curl, Git, the Rust toolchain, and a Python environment containing Matplotlib, pandas, and
Seaborn. Important overrides include `POSTGRES_DATA_DIR`, `REGRESSION_ROOT`,
`OPTD_QUERY_TIMEOUT`, and `POSTGRES_STATEMENT_TIMEOUT`. `--skip-download` and `--skip-load` reuse
existing data; `--skip-optd` or `--skip-postgres` reuse an existing corresponding report. Outputs
are written under `target/cardinality-regression` by default, including `dashboard.html`,
`run-metadata.json`, per-engine `report.json`/`errors.json`, checkpoint reports, plots, CSVs, and a
UTC-stamped execution log.

The same workflow is available as `DATASET=job just regression-run`.

`regression-compare` validates that both reports can be grouped by join count and then writes the
comparison plots and summary. It defaults to every normalization level; use
`regression-compare-smoke` when a limited run may contain no joins. Set `PLOT_PYTHON` when plotting
dependencies live in a separate environment. The comparison is distributional: optd rows are
logical operator subtrees, while PostgreSQL rows are nodes in its chosen physical plan. They are
not paired as if they represented identical subplans.

Download the benchmark data first, then point the command at one `.sql`/`.slt` file or a directory.
For SLT files, the first `query` block is measured.

```sh
./scripts/download_tpch_hf.sh
DATASET=tpch \
  TPCH_QUERIES=optd/connectors/datafusion/tests/slt/tpch/results \
  OPTD_OUTPUT=target/cardinality-regression/tpch \
  just regression-optd
```

Set `OPTD_EXTRA_ARGS="--limit N --target-partitions N"` for a smoke run or to control DataFusion
execution parallelism. JOB uses `DATASET=job`, `JOB_QUERIES`, and requires
`./scripts/download_job_hf.sh`. With `DATASET=all` (the default), the combined report adds a `suite`
field to every measurement so identically named queries remain distinguishable. The `just` workflow
uses `optd_cardinality_regression.py` to execute each query in an isolated child process, checkpoint
`report.json` after every success, and resume completed `(suite, query)` pairs. This prevents one
memory-intensive JOB query from discarding the rest of a long run. Failed or timed-out children are
recorded in `errors.json`; set `OPTD_QUERY_TIMEOUT` to change the default 900-second per-query bound.

Pass `OPTD_EXTRA_ARGS="--full-scan-sketches"` to make the optd harness materialize each referenced
base-table column and install query-local HLL and SpaceSaving sketches. This is deliberately
opt-in because it adds unbounded table scans. The harness logs every full-scan collection and every
measured subtree's sketch-backed column count, so a run can verify that the estimator did not fall
back to an empty sketch payload.

### PostgreSQL chosen-plan measurements

The PostgreSQL collector runs `EXPLAIN (ANALYZE, FORMAT JSON)` and records every node in the chosen
physical plan. `Plan Rows` is compared with the per-loop `Actual Rows`; `Actual Loops` is retained in
the report. Parallel query and JIT are disabled in each measurement transaction so row accounting is
stable. InitPlan and SubPlan joins are measured in their own subtrees but do not contribute to their
parent query block's join count.

For the local PostgreSQL 18 container, load the same Parquet data used by optd. The container must
already exist and be running, and the configured user and database must already exist. The repository
does not provision the container. The loaders recreate their benchmark tables, stream Parquet
through DuckDB's CSV output, create primary-key/benchmark lookup indexes, and run `ANALYZE`:

```sh
just load-postgres-all       # TPC-H and JOB
just load-postgres-tpch      # TPC-H only
just load-postgres-job       # JOB only
```

Then collect both suites (the default), or select one with `DATASET`:

```sh
POSTGRES_OUTPUT=target/cardinality-regression/postgres just regression-postgres
DATASET=job POSTGRES_OUTPUT=target/cardinality-regression/postgres-job \
  just regression-postgres
```

The Python collector has the equivalent `--suite all|tpch|job` option. `--tpch-queries` and
`--job-queries` override each suite's query path; the legacy `--queries` override is accepted only
with a single-suite selection.

The collector defaults to both suites, container `optd-postgres-18`, database `optd_bench`, and user `optd`.
Override them with `--container`, `--database`, and `--user`. Use `--limit N` for a smoke run and
`--statement-timeout SECONDS` to bound each query. The report is checkpointed after every successful
query; `--resume` skips queries already present in it, while `--continue-on-error` records failures
in `errors.json` and proceeds. `EXPLAIN ANALYZE` executes the SQL, so the collector is intended only
for trusted, read-only benchmark queries.

The commands write raw subtree measurements, including each subtree's join count, to `report.json`.
With Matplotlib, pandas, and Seaborn available in the active Python environment, generate box and
violin plots grouped by join count with:

```sh
python3 optd/connectors/datafusion/scripts/plot_cardinality_regression.py \
  target/cardinality-regression/tpch/report.json
```

The script writes `qerror-boxplot.svg` and `qerror-violin.svg` beside the report by default. Pass
`--output DIR` to choose another directory, or `--summary` to also write
`summary-by-join-count.csv` with counts, geometric means, quartiles, and box-plot whiskers. Both
plots use a logarithmic q-error axis.

Use `--normalization LEVEL` to control how aggressively structurally duplicated measurements are
removed:

- `none` (the default) keeps every measured operator.
- `wrappers` removes only `Output` and `Sort`. It retains projections, renames, maps and function
  computations, selections, aggregations, limits, sources, and joins.
- `row-preserving` additionally removes `Projection`, `Rename`, and `Map`, while retaining operators
  that may determine cardinality, including selections, aggregations, limits, table functions, and
  joins.
- `joins` keeps only joins and cross products.

Non-default levels add the level to generated filenames so several views can coexist in one output
directory. Pass `--normalization all` to generate every view in one invocation. Reports that contain
`suite` fields can be filtered with repeatable `--suite SUITE` arguments. `--suite-matrix` generates
one artifact set per suite plus an `all` aggregate, which is the format consumed by the dashboard.

Once a PostgreSQL run has produced the same raw report schema, pass it with `--compare-report` to
write side-by-side box plots and a comparison CSV, and print a Markdown table containing sample
counts, geometric means, medians, 95th percentiles, and the percentage of measurements with q-error
at least 10:

```sh
python3 optd/connectors/datafusion/scripts/plot_cardinality_regression.py \
  target/cardinality-regression/tpch/report.json \
  --compare-report target/cardinality-regression/postgres/report.json \
  --normalization all --summary
```

This writes `comparison-by-join-count-all.csv` and one
`qerror-comparison-boxplot-LEVEL.svg` per normalization level. Comparison requires raw per-node
PostgreSQL measurements; aggregate percentages from a paper are insufficient to reconstruct the
distributions.

## Feature dashboard

Build a self-contained HTML snapshot of estimator implementation status, JOB matched-probe readiness,
and per-query/per-join-count q-error metrics from the current reports:

```sh
PLOT_PYTHON="conda run -n c0bench python" just regression-dashboard
```

The default output is `target/cardinality-regression/dashboard.html`. An area-proportional Euler
impact map uses venn.js and D3 to group missing or partial estimator features by their current
high-q-error query memberships. Individual optimization clusters can be enabled or disabled, and a
focused query is marked in its exact active intersection. The default scope shows queries with at
least one 10× q-error outlier; an `Include all measured queries` switch expands the diagram and query
selector to the complete measured suite. Selecting a query in either the impact map or metrics table
keeps both views synchronized. The map supports hover details, pan, and zoom. D3 and venn.js are
loaded from pinned jsDelivr URLs when the HTML is opened. The q-error plot section embeds the canonical
SVG artifacts produced by `just regression-all` in a 2×2 grid containing raw, wrapper-normalized,
row-preserving, and join-only views. Suite checkboxes select TPC-H, JOB, or their aggregate. Metrics,
feature memberships, query choices, and plot links all follow that selection. `regression-compare`
uses `--suite-matrix` to generate canonical artifacts for each suite and the all-suite aggregate;
suites are combined only when the user selects more than one. The SVGs aggregate all queries within
the selected suite set and remain suite-wide when an individual query is selected; query selection
filters the impact map and metrics table. A shared plot-type
selector switches all four panels among comparison box, optd box, and optd violin plots without
rendering a second approximation. Full-size SVG and per-view CSV links remain available below each
embedded plot.
The table reports sample count, minimum, geometric mean, median, maximum, and
q-error-at-least-10 percentage for each query and recursive join-count bucket. It also shows the worst
measured node and a code-backed diagnostic hypothesis with relevant improvement features. The impact
map and cause descriptions are hypotheses derived from operator shape, query syntax, and documented
estimator limitations; they are not causal proof or a guarantee that one feature fixes a query.

The dashboard also inventories a generated JOB logical-probe manifest when present. Probe generation
alone is not a paired benchmark: a valid JOB comparison still requires manifest-aware root collectors,
shared actual cardinalities, and strict matching by probe ID.
