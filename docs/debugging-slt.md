# Debugging SQLLogicTest Failures

Use this guide for failures under `optd/connectors/datafusion/tests/slt/`. For general command and
verification selection, see `docs/development.md`.

## Reproduce Narrowly

Run SLT in release mode because debug execution is slow. Start with the narrowest file or filter that
reproduces the behavior:

```sh
cargo nextest run --release -p optd-datafusion --test slt <filter>
```

The harness uses optd physical planning by default. Add `-- --logical` only for a deliberate
comparison with the older logical-conversion path. A passing test on the wrong path is not regression
coverage.

JOB `19d` is disabled because it remains long-running under physical planning. Do not re-enable it
for unrelated verification.

## Locate the Boundary

Determine where the observed plan or result first becomes wrong:

1. DataFusion logical-plan import
2. optd optimization
3. direct physical planning or logical-plan export
4. DataFusion execution
5. expected-output comparison

For optimizer failures, inspect the canonical pass order in
`optd/connectors/datafusion/src/runner.rs::default_pass_manager`. Use optimizer explain or trace
output when pass-by-pass state matters.

Consult more specific investigation notes only when the symptom matches:

- `docs/try_via_ir_failures.md` — IR conversion and execution fallback failures
- `docs/job_result_failures.md` — Join Order Benchmark result discrepancies

## Add a Regression

For SQL-visible behavior, add or update the narrowest SLT case. When practical:

1. Add the case before changing the implementation.
2. Confirm that it fails for the expected reason.
3. Confirm that it reaches the physical or logical path under investigation.
4. Implement the fix and rerun the focused case.
5. Widen to connector or workspace coverage according to impact.

If a new test passes before the fix, inspect its execution path before treating it as coverage.

## Regenerate Expected Output

Do not overwrite expected output until the difference is understood and intended. Regenerate with
optd, unmodified DataFusion, or DuckDB as appropriate:

```sh
cargo nextest run --release -p optd-datafusion --test slt -- --override <filter>
cargo nextest run --release -p optd-datafusion --test slt -- --override --engine datafusion <filter>
cargo nextest run --release -p optd-datafusion --features duckdb --test slt -- --override --engine duckdb <filter>
```

Review generated changes before accepting them. A changed expected result is not itself evidence that
the implementation is correct.
