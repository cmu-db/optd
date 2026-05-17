# Repository Guidelines

## Project Structure & Module Organization

This is a Rust 2024 crate for relational query IR experiments.

- `src/lib.rs` defines the core IR handles, operator payloads, expressions, `QueryContext`, and public re-exports.
- `src/analysis.rs` contains demand-driven analyses such as available, used, free, created columns, and nullability.
- `src/catalog.rs` contains table references, in-memory catalog metadata, and schema resolution.
- `src/display.rs` contains generic display nodes and box/JSON rendering.
- `src/substrait.rs` imports and exports Substrait plans.
- `src/main.rs` is an executable example that builds and prints sample plans.
- `crates/simple-graph-datafusion/` contains the DataFusion bridge and its sqllogictest coverage.

## Build, Test, and Development Commands

- `cargo build` compiles the library and example binary.
- `cargo run` runs `src/main.rs` and prints example query plans.
- `cargo test` runs unit tests, integration tests, and doc tests.
- `cargo fmt` formats Rust code using `rustfmt`; run this before committing.
- `cargo clippy --workspace` checks for lints; fix all warnings before committing.

The default feature set includes `serde`. Use `cargo test --no-default-features` when checking code that should not depend on serialization.

## Coding Style & Naming Conventions

Use standard Rust formatting with 4-space indentation. Keep APIs explicit and small. Handles such as `Operator`, `Expr`, and `Column` are opaque arena references; payloads live in `OperatorData`, `ExprData`, and `ColumnData`.

Prefer descriptive struct names for operator payloads, for example `Scan`, `Projection`, `Aggregation`, `Sort`, and `Limit`. Use `snake_case` for functions, fields, and test names. Keep doc comments on public types and methods.

## Testing Guidelines

Tests use Rust’s built-in test framework plus async `tokio` tests for DataFusion interop. Put narrow module tests beside implementation code under `#[cfg(test)]`; DataFusion bridge behavior lives under `crates/simple-graph-datafusion/tests/`.

Name tests by behavior, such as `imports_sort_and_fetch` or `datafusion_consumes_substrait_plan_produced_by_simple_graph`. Add tests for new IR operators, analyses, formatter behavior, and Substrait conversion paths.

For optimizer or SQL semantics changes, prefer an SLT regression test when the behavior is observable through the DataFusion bridge. Add the narrow SLT case first, run the focused SLT command, and confirm it fails on the current branch while the bug is still present. Then implement the fix, re-run the focused SLT, and finish with `cargo test --workspace`. In the handoff, explicitly note the before/after result: the new SLT failed before the fix and passed after.

If a new SLT passes before the fix, check whether the relevant optimizer pass or conversion path is actually enabled in the tested execution path before treating the test as coverage.

## Commit & Pull Request Guidelines

Recent commits use short imperative titles, for example `Add sort and limit operators` and `Export simple plans to Substrait`. Keep commits focused and include tests with behavior changes.

Pull requests should include a concise summary, important design notes, and the test commands run. Link related issues when available. Include rendered plan snippets only when display output changes materially.

## Agent-Specific Instructions

Avoid rewriting unrelated files or generated lockfile sections unless dependency changes require it. Preserve existing API style, run `cargo fmt`, and verify with `cargo test` before handing off.
