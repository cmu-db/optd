# Proposed PR

## Title

Add a snapshot-aware metadata catalog and observed SQL statistics

## Description

### Summary

- Add `optd-catalog`, a SQLite/PostgreSQL-backed metadata and statistics catalog
  inspired by DuckLake's temporal schema. Use one global snapshot history,
  `begin_snapshot`/`end_snapshot`, structural generations, and atomic typed changes.
- Persist source registrations, schemas, typed relations/columns, and relational
  index metadata with catalog-scoped reads and mutations.
- Preserve the basic table/column statistics layout and add independent advanced
  statistics objects for caller-collected joint NDV and scalar histograms.
- Integrate persistent statistics with `collect_stats`, `cardest`, the runner, and
  `optd-cli --catalog-db`, including explicit idempotent startup imports.
- Preserve source-provider DML capabilities. A shared, opt-in executor observes
  DML and actual DDL callbacks, atomically invalidates basic and advanced statistics,
  and reports source outcomes separately from metadata maintenance. Metadata retries
  never replay source SQL or duplicate DDL publication.
- Provide non-destructive forward migrations, namespace/validation safeguards,
  rollback/concurrency regressions, safe literal demo-file handling, and a dedicated
  service-backed PostgreSQL CI job.

### Validation

- 398 release workspace tests passed in the final service-free run; PostgreSQL was
  exercised separately rather than treating unset-URL tests as backend coverage.
- All 44 catalog tests/doctests passed with an isolated PostgreSQL 18.6 server,
  including four actual PostgreSQL cases and batch-invalidation rollback.
- All 17 feature SLTs and 45 TPC-H SLTs passed in release mode.
- 237 core tests passed with `--no-default-features`; workspace doctests,
  all-targets Clippy with `-D warnings`, rustfmt, actionlint, and whitespace checks
  passed.
- Independent source review and red/green regressions addressed duplicate column
  statistics, counter overflow, UDF SET/RESET defaults, early CASCADE rejection,
  demo-file existence/filename-alias safety, and observation lifecycle/identity races.

**Benchmark caveat:** the full enabled SQL run reported JOB explain mismatches and
hit its one-hour timeout. The focused JOB `1a` mismatch is byte-identical on the
proposed tree and `origin/main` (`429db6b`); optimizer/explain sources are unchanged.
No expected outputs were regenerated, and the full JOB suite is not claimed green.

### Scope and deployment

Optd is a metadata/statistics cache, not the source-of-truth catalog or storage
engine. Metadata snapshots do not imply external-data time travel or atomic source
writes. Raw-session/external DML, source transaction commits and interrupted
execution require explicit notification or reconciliation. Automatic recollection,
stale-statistics fallback, and provider hydration remain follow-ups. Persistent
views and populated-schema CASCADE are explicitly unsupported.

Back up existing metadata databases and stop writers before upgrading. The legacy
upgrade requires a single `main` branch at the latest stored snapshot; ambiguous
histories are rejected rather than discarded. Downgrade requires restoring a backup.

See `docs/catalog_single_history.md`, `docs/advanced_statistics.md`, and
`docs/observed_execution.md` for the current contracts, and `catalog_pr_review.md`
for the final source/gate audit.
