# Development Guide

This document contains contributor details that are useful for implementation work but too specific
to load as repository-wide agent instructions.

## Workspace

The workspace uses Rust 2024 with the pinned toolchain in `rust-toolchain.toml`.

- `optd/core/` is package `optd-core` (crate name `optd_core`). Its `src/lib.rs` defines and re-exports the core IR, analyses, catalog, display support, Substrait conversion, and optimizer APIs.
- `optd/core/src/optimize/` contains optimizer passes.
- `optd/core/examples/basic.rs` builds and displays sample plans.
- `optd/core/examples/rename.rs` demonstrates the rename operator.
- `optd/connectors/datafusion/` is package `optd-datafusion`. It contains the DataFusion bridge, direct physical planner, runtime statistics, profiling tools, and SQLLogicTest harness.
- `optd/crates/` is reserved for future authored support crates.
- `docs/` holds durable architecture, development, debugging, and investigation material.
- `.agents/skills/` is reserved for specialized agent workflows that justify a discoverable skill package; ordinary project documentation belongs in `docs/`.

Inspect current source rather than copying volatile lists into new guidance. In particular,
`optd/connectors/datafusion/src/runner.rs::default_pass_manager` is the canonical optimizer pipeline.

## Common Commands

Build and examples:

```sh
cargo build --workspace
cargo run -p optd-core --example basic
cargo run -p optd-core --example rename
```

Formatting and linting:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
actionlint # only when GitHub Actions files change
```

Core tests:

```sh
cargo test -p optd-core
cargo test -p optd-core --no-default-features
```

DataFusion and workspace tests:

```sh
cargo nextest run --release -p optd-datafusion --test slt
cargo nextest run --release --workspace
```

Use release mode for SLT because debug runs are slow. Prefer a test filter or the narrowest affected
SLT file while iterating, then widen coverage according to risk. See `docs/debugging-slt.md` for
execution-path comparison, expected-output regeneration, and failure triage.

## Selecting Verification

Verification should be proportional to the changed surface:

1. Run the narrow test that covers the behavior while iterating.
2. Run formatting for Rust edits.
3. Run Clippy for substantive Rust changes.
4. Run the package or connector suite when shared behavior changed.
5. Run the release workspace suite for optimizer or SQL-semantics changes with broad impact, or before a handoff that requires full regression evidence.

Do not run `actionlint` for source-only changes. For SQL-visible regressions, follow
`docs/debugging-slt.md` and verify that the test reaches the intended execution path.

## Pull Requests and Commits

Use a short imperative commit title and keep commits focused. A pull request should summarize the
behavioral change, important design decisions, and commands actually run. Include plan output only
when display or planning output changed materially.
