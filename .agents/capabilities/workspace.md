# Workspace

This repository is a Rust workspace.

- `optd/core/` is package `optd-core` and imports as `optd_core`.
- `optd/connectors/datafusion/` is package `optd-datafusion`.
- `optd/crates/` is reserved for future authored support crates.
- `docs/` holds durable design notes and investigations.

Common checks:

```sh
cargo metadata --no-deps
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
actionlint
cargo nextest run --workspace --locked -E 'not binary(slt)'
```
