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

The root `Makefile` provides the supported build, test, baseline-update, and regression entry
points. Run `make help` for the complete list. A typical TPC-H workflow is:

```sh
make build-binaries
make test-tpch
make regression-optd-smoke
make load-postgres-tpch
make regression-postgres-smoke
make regression-compare-smoke PLOT_PYTHON="conda run -n c0bench python"
```

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
make regression-optd \
  DATASET=tpch \
  QUERIES=optd/connectors/datafusion/tests/slt/tpch/results \
  OPTD_OUTPUT=target/cardinality-regression/tpch
```

Set `OPTD_EXTRA_ARGS="--limit N --target-partitions N"` for a smoke run or to control DataFusion
execution parallelism. JOB uses `DATASET=job` and requires `./scripts/download_job_hf.sh`.

### PostgreSQL chosen-plan measurements

The PostgreSQL collector runs `EXPLAIN (ANALYZE, FORMAT JSON)` and records every node in the chosen
physical plan. `Plan Rows` is compared with the per-loop `Actual Rows`; `Actual Loops` is retained in
the report. Parallel query and JIT are disabled in each measurement transaction so row accounting is
stable. InitPlan and SubPlan joins are measured in their own subtrees but do not contribute to their
parent query block's join count.

For the local PostgreSQL 18 container, load the same TPC-H Parquet data used by optd. The container
must already exist and be running, and the configured user and database must already exist. The
repository does not provision the container. This command recreates the eight benchmark tables in
`optd_bench`, streams them through DuckDB's CSV output, creates primary-key and benchmark lookup
indexes, and runs `ANALYZE`:

```sh
make load-postgres-tpch
```

Then collect the selected PostgreSQL plans:

```sh
make regression-postgres \
  QUERIES=optd/connectors/datafusion/tests/slt/tpch/results \
  POSTGRES_OUTPUT=target/cardinality-regression/postgres
```

The collector defaults to container `optd-postgres-18`, database `optd_bench`, and user `optd`.
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
directory. Pass `--normalization all` to generate every view in one invocation.

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
