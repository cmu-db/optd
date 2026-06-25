# GitHub config and workflows

This directory contains CI workflow and dependency-update configuration adapted
from <https://github.com/jonhoo/rust-ci-conf/>.

The upstream project documents a `git merge --allow-unrelated-histories` import
strategy so future CI template updates can be merged from the original
repository. This repository intentionally uses manually adapted files instead:
the workflows keep the same broad split (`check`, `test`, `safety`, and
`scheduled`) while tailoring commands for this Cargo workspace, `cargo-nextest`,
and the generated TPC-H/JOB SQLLogicTest data.

The workflows also apply Rust CI speedups such as locked Cargo commands,
Rust-aware caching, disabled incremental compilation, reduced test debug info,
and nextest-based test execution.

## Workflow overview

| Workflow | Runs on | Purpose |
| --- | --- | --- |
| `check.yml` | Pull requests and pushes to `main` | Formatting, clippy on stable/beta, docs, and `cargo-hack` feature checks for `optd-core`. |
| `test.yml` | Pull requests and pushes to `main` | Nextest workspace tests, `optd-core` no-default-features, light SLT, and TPC-H SLT with cached/generated data. |
| `safety.yml` | Pull requests and pushes to `main` | Sanitizer and Miri checks scoped to `optd-core`. |
| `scheduled.yml` | Manual dispatch and nightly schedule | Nightly tests, updated-dependency tests, and full SLT with cached/generated TPC-H and JOB data. |
