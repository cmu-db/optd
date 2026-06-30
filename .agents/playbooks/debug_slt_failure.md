# Debug SLT Failure

1. Reproduce the focused file or suite with `cargo nextest run --release -p optd-datafusion --test slt <filter>`.
2. SLT uses physical planning by default; add `-- --logical` only when comparing against the old logical DataFusion conversion path.
3. Check whether the failing behavior is import, optimization, export, or DataFusion execution.
4. For optimizer bugs, inspect the pass order in `optd/connectors/datafusion/src/runner.rs`.
5. Add or update the narrowest SLT regression when behavior is SQL-visible.
6. Finish with the focused SLT command, `cargo fmt --all --check`,
   `cargo clippy --workspace --all-targets --locked -- -D warnings`,
   `actionlint`, and `cargo nextest run --release --workspace`.

Related docs:

- `docs/try_via_ir_failures.md`
- `docs/job_result_failures.md`
