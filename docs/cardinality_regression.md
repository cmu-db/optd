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

Download the benchmark data first, then point the command at one `.sql`/`.slt` file or a directory.
For SLT files, the first `query` block is measured.

```sh
./scripts/download_tpch_hf.sh
cargo run --release -p optd-datafusion --bin cardinality-regression -- \
  --dataset tpch \
  --queries optd/connectors/datafusion/tests/slt/tpch/results \
  --output target/cardinality-regression/tpch
```

Use `--limit N` for a smoke run and `--target-partitions N` to control DataFusion execution
parallelism. JOB uses `--dataset job` and requires `./scripts/download_job_hf.sh`.

The command writes raw subtree measurements, including each subtree's join count, to `report.json`.
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
