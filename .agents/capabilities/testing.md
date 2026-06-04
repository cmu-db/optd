# Testing

Core behavior belongs in `optd/core/src/` unit tests or `optd/core/tests/` integration
tests. DataFusion SQL behavior belongs in connector-local SLT coverage under
`optd/connectors/datafusion/tests/slt/`.

For optimizer or SQL semantics changes, prefer a focused SLT regression when
the behavior is observable through the DataFusion connector.

Useful commands:

```sh
cargo test -p optd-core
cargo test -p optd-core --no-default-features
cargo nextest run --release -p optd-datafusion --test slt
cargo nextest run --release --workspace
```

Known long-running JOB result cases can be excluded during broad refactor
verification:

```sh
cargo nextest run --release -p optd-datafusion --test slt -E 'not test(/job\/results\/(16a|16c|16d|19d)\.slt/)'
```
