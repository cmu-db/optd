# Catalog PR merge-readiness audit

Status: review complete; no remaining catalog/bridge source blockers identified.
The proposed working tree is locally merge-ready within the reviewed catalog scope:
398 release workspace tests, real PostgreSQL coverage, feature isolation, required SQL
groups and final static checks passed. Hosted CI still needs to run after publication.
The full JOB benchmark remains a disclosed verification limit, not a green gate.
The final demo filename-alias finding was reproduced, fixed and regression-tested.

## Objective and scope

Review the actual proposed tree for correctness, concise idiomatic Rust, and merge
readiness; resolve findings with regression coverage; finish with a PR title and
description for the owner to review.

The comparison base is **`origin/main` = `429db6b`**, confirmed against the remote
with `git ls-remote`. Local `main` is stale. The proposed tree is branch
`yuchen/catalog` at `5259c84` plus its tracked and untracked catalog changes. There
is no open GitHub PR. No commit, push, reset, stash application, or existing database
modification was performed during this audit.

The committed delta against the real base is catalog/workspace/bridge/demo work,
not the earlier estimator/sketch PRs already present on origin/main. The parent
inspected that committed delta and the extra working-tree files. Independent
read-only reviews inspected the complete current catalog and named bridge sources;
their narrower diff tooling did not independently certify the committed range.

## Findings resolved in this audit

| Finding | Narrow fix | Concrete regression evidence |
| --- | --- | --- |
| Conflicting basic statistics for one column were accepted | Reject duplicate IDs before persistence; reject duplicate persisted values on read, without changing table layout | `duplicate_column_statistics_do_not_replace_the_previous_payload`; `duplicate_persisted_column_statistics_fail_closed_on_read` |
| Object/schema counters could panic or wrap | Fallible allocation and checked snapshot/schema arithmetic with structured errors | `exhausted_catalog_counters_fail_without_publication_or_partial_objects`; `nonstructural_refresh_does_not_need_an_available_generation_or_object_id` |
| Statistics UDFs used installation-time defaults after SET/RESET | Small `state_for_execution` helper replaces only configuration from invocation options, preserving registry/runtime | Both `persistent_statistics_follow_execution_time_*_defaults` tests |
| Unsupported populated-schema CASCADE removed runtime state before inevitable rejection | Explicit rejection before the inner catalog callback; no cascade implementation added | `persistent_populated_cascade_is_rejected_before_runtime_removal` checks zero source removals, stable provider identity, metadata and basic/advanced values |
| Demo deleted an existing user-supplied file | Atomic `create_new`, fresh default path, open-only connection options | Real-binary existing-file and repeat-invocation regressions in `tests/catalog_cli.rs` |
| Filename URL decoding could redirect demo writes to a neighboring file | Canonical absolute `SqliteConnectOptions::filename`, no URL interpolation or new dependency | New process-level regression reproduced the neighbor write before the fix; percent escapes, `file:` names and query/fragment characters covered |

The duplicate-write, counter, both UDF-default, CASCADE, and demo regressions failed
against the old paths before their fixes. The later read-side duplicate and
nonstructural boundary tests add adversarial coverage; they are not claimed as
separate pre-fix red runs.

## Prompt-to-artifact completion checklist

| Requirement / surface | Artifacts inspected | Evidence / status |
| --- | --- | --- |
| Correct temporal catalog and migration behavior | Catalog APIs and snapshot/source/schema/relation/index modules under `optd/catalog/src/`; migrations and frozen fixtures | Source re-review found no remaining blockers; SQLite and real PostgreSQL upgrade, mutex, concurrent publication and rollback tests passed |
| Basic and advanced statistics remain distinct | Basic entity/migration files; `advanced_statistics.rs`; invalidation helpers/tests | Basic entity/statistics-migration diffs remain empty relative to HEAD; typed advanced values and atomic basic/advanced closure covered |
| Native DML preserved; shared observation covers DML and DDL | `observed_execution.rs`, tests, provider and runner code | Prepared/stale plans, gated replacement race, EXPLAIN/ANALYZE, actual DDL receipts, failure separation and metadata-only retry covered by tests |
| Namespace and current-default correctness | Catalog scoping; UDF resolution/configuration; multi-catalog tests | Scoped mutations, identical names, foreign rejection, dynamic SET/RESET and estimator inputs exercised |
| Code conciseness and Rust conventions | Two independent current-source audits plus parent review of dependencies, CLI, scripts and full base-relative path list | Reviewers recommend no broad refactor; fixes reuse existing helpers and use checked Result propagation, standard formatting and no warning suppressions |
| Workspace regression coverage | Release nextest command below | 398 tests passed in the final refresh; four PostgreSQL cases excluded and exercised separately |
| Actual backend coverage, not skipped tests | Isolated PostgreSQL 18.6 and `cargo test -p optd-catalog --locked` with URL supplied | 12 unit + 29 integration + 3 doctests passed; all four PostgreSQL tests actually ran, including new batch-summary rollback |
| Native CLI safety | `catalog-stats-demo.rs`, `tests/catalog_cli.rs` | All three process-level tests passed in the final release workspace run; literal percent escapes, `file:` names and query/fragment characters preserve the neighboring file |
| Rust lint and formatting | Workspace/all-targets Clippy; rustfmt | Passed with warnings denied |
| Feature isolation | `cargo test -p optd-core --no-default-features --locked` | 237 tests passed (`b4298e698`), without serialization/default-feature assumptions |
| CI changes | `.github/workflows/{test,scheduled}.yml` | Dedicated health-checked PostgreSQL 17 job added; service-free jobs retain explicit exclusion; actionlint passed. Hosted CI has not run because no PR was opened |
| SQL regression assessment | 287 enabled SLTs; required feature/TPC-H groups; isolated baseline worktree | All 17 feature and 45 TPC-H cases passed. Full run timed out with JOB explain mismatches. Focused JOB 1a failed with a byte-identical diagnostic on this tree and origin/main; no goldens were changed. Other JOB failures and the timeout are not individually certified as baseline-only |
| Documentation matches actual limits | Integration, statistics, migration and observed-execution notes | No source-of-truth/data-time-travel/atomic-source-write claims; no implicit demo reset; unsupported CASCADE explicit |
| Final PR title and description | `docs/catalog_pr.md` | Complete draft for the owner to review; not posted to GitHub |

## Checks actually run

- `cargo nextest run --release --workspace --locked -E 'not binary(slt) and not (package(optd-catalog) and test(/^postgres_/))'` — **398 passed, 4 PostgreSQL cases skipped**, final service-free refresh (`baadf73ed`); supersedes the earlier 397-test run (`bc774115e`).
- `cargo test --workspace --doc --locked` — **3 compile-fail doctests passed**.
- `cargo test -p optd-core --no-default-features --locked` — **237 passed** (`b4298e698`).
- `OPTD_TEST_POSTGRES_URL=<isolated loopback test server> cargo test -p optd-catalog --locked` — **44 passed**, including four actual PostgreSQL cases (`b9468e3ba`). The server was stopped and its private extracted runtime/cluster removed afterward; no existing service was started or modified.
- `cargo clippy --workspace --all-targets --locked -- -D warnings` — passed after the final demo fix and regression (`baadf73ed`).
- `cargo fmt --all --check`, `actionlint`, `git diff --check` — passed in the final direct command. The combined `baadf73ed` task had stopped at an array-layout formatting diff after its tests and Clippy passed; only whitespace was corrected before rerunning these static checks.
- Final demo alias red/green: `bd51bde6e` reproduced the neighboring-file write, then all three `catalog_cli` tests passed both directly and in the 398-test workspace refresh (`baadf73ed`). Absolute filename options bypass URL decoding and SQLite `file:` interpretation without adding a dependency.
- Focused pre/post-fix tests — failures reproduced in `be648a5d7` / `b9690271b`; fixed duplicate/counter/UDF/demo cases passed in `bdc749439`, and CASCADE passed in the release workspace run.
- `cargo test --release -p optd-datafusion --test slt --locked -- --test-threads=2` — **not passing**: timed out after 3600 s with JOB explain mismatches (`bc6e4171c`). All 17 feature cases passed before that timeout; no expected files were regenerated.
- `cargo test --release -p optd-datafusion --test slt --locked -- tpch/ --test-threads=2` — **45 passed** (`b7027578a`).
- Focused JOB `tests/slt/job/explain_flat/1a.slt --exact` — fails on both this tree and isolated origin/main (`b34df185b`, `bf21fc7c4`). The complete mismatch diagnostic is byte-identical (SHA-256 `6006ad8b876989d50a055ac80f5b5014966b0ba4c3230ecafa797868ed8a1c92`). `git diff --exit-code origin/main -- optd/core optd/sketches optd/connectors/datafusion/src/explain_udfs.rs` is empty. The baseline worktree was removed afterward.

The full JOB run remains a verification limit, not passing evidence. Only the focused
1a mismatch is proven to reproduce on the base; other JOB failures and the timeout
were not individually baseline-compared. No optimizer source or expected output was
modified to conceal them. If a fully green JOB benchmark run is a release requirement,
separate benchmark triage is still needed. The catalog-specific, normal feature/TPC-H
and workspace merge gates passed locally.

The full harness already excludes JOB result query `19d`; that exclusion predates
this PR and is not new coverage. Passing counts are evidence for their named
surfaces, not proof of source correctness or a substitute for the source audit.

## Intentional limits retained

Snapshots version planning metadata, not external data. Raw-session DML, external
writes, custom hidden side effects, source transactions and cancellation outside
completed observed execution require explicit notification/reconciliation. There
is no automatic recollection, stale-statistics fallback, provider hydration,
view implementation, or populated-schema cascade. Source writes and metadata
maintenance are not a distributed transaction.
