# Repository Guidance

`optd` is a Rust 2024 workspace for relational query IR and query-optimizer experiments.
Keep this file concise because it is loaded for every task. Read only the references
that apply to the work at hand.

## Working Agreement

- Inspect the relevant code and nearby tests before editing; do not require a full-repository tour for a local change.
- Continue through implementation, focused verification, and fixes caused by the change. The local test suites use repository fixtures and have no production access, so routine build, lint, and test commands do not require separate approval.
- Preserve unrelated work. Do not rewrite unrelated files or generated lockfile sections unless a dependency change requires it.
- Prefer the smallest coherent change. Keep public APIs explicit and document public behavior.
- If requirements are genuinely ambiguous or an action could affect external systems, credentials, or user data, stop and ask. Otherwise make the local, reversible choice and proceed.
- In the handoff, summarize changed files, checks actually run, and any remaining limitations. Do not claim checks that were not run.

## Where to Look

- `optd/core/`: `optd-core`, including IR handles and payloads, analyses, catalog, display, Substrait conversion, and optimizer passes.
- `optd/connectors/datafusion/`: DataFusion import/export, physical planning, runtime statistics, and SQLLogicTest coverage.
- `docs/`: durable architecture, development, debugging, and investigation notes.
- `.agents/skills/`: optional Codex skills. Add one only for a specialized recurring workflow that benefits from its own instructions, references, or scripts.

Use these references contextually:

- Workspace layout and command selection: `docs/development.md`
- Optimizer framework or pass changes: `docs/optimizer.md`
- Join ordering: `docs/join_ordering_design.md` and `docs/query_hypergraph.md`
- Analysis changes: `docs/analysis_framework.md`
- Substrait conversion: `docs/substrait_integration.md`
- Unnesting: `docs/holistic_unnesting.md` and `docs/unnesting_before_direct_physical_execution.md`
- SLT failures: `docs/debugging-slt.md`; consult its linked failure notes only when relevant

Treat implementation as authoritative when a design note describes planned or historical work. Update the relevant document when behavior or architecture changes.

## Code Conventions

Use standard Rust formatting with four-space indentation and `snake_case` names. Handles such as
`Operator`, `Expr`, and `Column` are opaque arena references; payloads live in `OperatorData`,
`ExprData`, and `ColumnData`. Follow the append-only optimizer invariant in `docs/optimizer.md`.

Put narrow unit tests beside implementation code under `#[cfg(test)]`. Put DataFusion bridge and
SQL-visible behavior coverage under `optd/connectors/datafusion/tests/`.

## Verification

Choose checks based on the changed surface rather than running every command for every edit:

- Rust formatting changes: `cargo fmt --all --check`
- Core code: focused `cargo test -p optd-core ...`; include `--no-default-features` when checking code that must not require serialization
- DataFusion SQL behavior: focused release-mode SLT, then broader coverage when warranted
- Rust changes before handoff: `cargo clippy --workspace --all-targets --locked -- -D warnings`
- GitHub Actions changes: `actionlint`

For an optimizer or SQL-semantics regression, add the narrow failing test first when practical,
confirm it exercises the affected execution path, implement the fix, and report the before/after
result. See `docs/development.md` for exact commands and escalation to workspace-wide testing.
