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
| `check.yml` | Pull requests and pushes to `main` | Formatting, clippy, docs, and `cargo-hack` feature checks on the pinned Rust toolchain. |
| `test.yml` | Pull requests and pushes to `main` | Nextest workspace tests, `optd-core` no-default-features, feature SLT, TPC-H SLT, and coverage on the pinned Rust toolchain. TPC-H data is cached after download from Hugging Face. |
| `safety.yml` | Manual dispatch | Optional sanitizer and Miri checks scoped to `optd-core`; kept out of the required PR path because the repo has no first-party `unsafe` code today. |
| `scheduled.yml` | Manual dispatch, nightly schedule, and PRs labeled `ci:full-slt` | Nightly tests, updated-dependency tests, and full SLT using the default physical-planning path with cached TPC-H and JOB data downloaded from Hugging Face. |

Beta and nightly compatibility checks are kept in scheduled/manual workflows so
normal PR checks use one required Rust toolchain.
