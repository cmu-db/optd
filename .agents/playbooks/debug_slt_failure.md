# Debug SLT Failure

1. Reproduce the focused file or suite with `cargo nextest run --release -p optd-datafusion --test slt <filter>`.
2. Check whether the failing behavior is import, optimization, export, or DataFusion execution.
3. For optimizer bugs, inspect the pass order in `optd/connectors/datafusion/src/runner.rs`.
4. Add or update the narrowest SLT regression when behavior is SQL-visible.
5. Finish with the focused SLT command and `cargo nextest run --release --workspace`.

Related docs:

- `docs/try_via_ir_failures.md`
- `docs/job_result_failures.md`
