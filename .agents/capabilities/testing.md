# Testing

Core behavior belongs in `optd/core/src/` unit tests or `optd/core/tests/` integration
tests. DataFusion SQL behavior belongs in connector-local SLT coverage under
`optd/connectors/datafusion/tests/slt/`.

For optimizer or SQL semantics changes, prefer a focused SLT regression when
the behavior is observable through the DataFusion connector.

Useful commands:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
actionlint
cargo nextest run -p optd-core --no-default-features --locked
cargo nextest run --release -p optd-datafusion --test slt
cargo nextest run --release --workspace
```

Regenerate SLT expected output with optd, unmodified DataFusion, or DuckDB:

```sh
cargo nextest run --release -p optd-datafusion --test slt -- --override <filter>
cargo nextest run --release -p optd-datafusion --test slt -- --override --engine datafusion <filter>
cargo nextest run --release -p optd-datafusion --features duckdb --test slt -- --override --engine duckdb <filter>
```

Known long-running JOB result cases can be excluded during broad refactor
verification:

```sh
cargo nextest run --release -p optd-datafusion --test slt -E 'not test(/job\/results\/(16a|16c|16d|19d)\.slt/)'
```
