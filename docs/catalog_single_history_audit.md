# Single-history goal: requirement-to-evidence audit

> Historical acceptance record, predating the additive advanced-statistics extension.
> Its source fingerprints and test results do not verify that extension. The basic
> statistics layout is retained; the new fourth migration is described in
> `catalog_single_history.md` and `advanced_statistics.md`.

## Outcome and completion standard

The deliverable is a metadata-only DuckLake adaptation in Rust/SeaORM: one global snapshot
history without runtime branches; structural versions and summaries; atomic publication;
external catalog namespaces; preserved temporal metadata/statistics and persistent DML rejection;
non-destructive upgrades; and evidence-backed documentation. It is not a managed lake storage
engine, a DuckLake interoperability implementation, or an external-data time-travel system.

Final current-code verification is task `b139bc73b`. It passed all required checks and reported
`CODE_SNAPSHOT_DIFFERENCES: []` and `CODE_SNAPSHOT_STABLE: 373 files`. The subsequent final
ref/diff/fingerprint check after documentation reconciliation also passed: all 373 package
files remained unchanged, HEAD and stash matched the starting references, formatting and diff
checks passed, and every changed/untracked file was within the allowed scope. No further
implementation edits were made during documentation reconciliation.

Earlier source-stability attempts detected concurrent edits and were **not** accepted as final
evidence. One earlier PostgreSQL legacy fixture failed before migration because its timestamp
normalizer also changed `CURRENT_TIMESTAMP`; that fixture was corrected and rerun. Earlier
public mutation helper bypasses were closed. The cross-catalog uninstall regression failed
before the binding-aware removal fix and passed afterward. These failed attempts are not hidden
behind the final green results.

## Deliverables and semantic requirements

Paths below are relative to the repository; implementation paths abbreviated as `catalog/src/`
mean `optd/catalog/src/`.

| Requirement | Artifact / inspected evidence | Result |
| --- | --- | --- |
| Inspect DuckLake implementation and specification | `docs/catalog_single_history.md` records upstream `ed31fddd089b038a800fa35e155f30e3c3abc8f0`, specification URLs, source locations and the inspected commit/conflict/backend protocol | Complete |
| Inspect stashed schema without applying or dropping it | Read `stash@{0}:docs/optd-catalog.dbml`; retained stash object `a8a588013f98132d8a1056a95cea252c781042f1` | Inspected; ref retained |
| One global snapshot ID history | `catalog/src/snapshot/get_current_snapshot_info.rs`, explicit successor allocation in `snapshot/commit.rs`; fresh-schema regression asserts absence of `optd_branch` | Implemented and tested |
| Remove runtime branch APIs and branch-head table | No runtime branch entity, head publication helper, branch constructor, or branch errors; frozen legacy migration models exist only to upgrade the old schema | Inspected and tested |
| Structural `schema_version` | Snapshot entity, `SnapshotChange::structural`, migration boundary backfill; statistics-only snapshots retain their structural version | Implemented and tested |
| Snapshot change summaries | `entity/snapshot_change.rs` and typed `SnapshotChange`; legacy provenance explicitly unknown; new summary inserted with the snapshot | Typed summary tests and injected failure rollback pass |
| Shared atomic commit layer | All typed Catalog mutation methods call `snapshot::commit`; helper mutations are crate-private | Inspected; 3 compile-fail API tests pass |
| PostgreSQL concurrency | Top-level READ COMMITTED transaction, row mutex before visibility reads; independent pools, held-lock waiting in both directions, stale-writer rejection with a non-default session isolation default | Real local PostgreSQL tests pass |
| SQLite concurrency | Writer reservation before reads, expected snapshot validation; explicit held mutex and overlapping writers in DELETE and WAL journal modes | Both modes pass |
| External catalog registration and schema ownership | `source.rs`, `entity/source_catalog.rs`, schema `catalog_id`, scoped `Catalog::on_catalog`, scoped DataFusion DDL/statistics lookup | Core and connector namespace tests pass |
| Preserve temporal relation/column/index/statistics behavior | Original tests retained in `catalog_tests.rs`; independent legacy fixture; historical readers and relation-scoped column IDs retained | All 9 catalog unit tests pass |
| Preserve historical visibility and stable IDs | Legacy SQLite/PostgreSQL fixture checks IDs, timestamps, allocators, historical statistics/indexes, structural boundaries and snapshot gaps; first post-upgrade commit uses the preserved allocator | Upgrade tests pass |
| Statistics refresh/invalidation | Existing payload lifecycle retained; refresh/invalidation summaries and unchanged schema versions asserted | Pass |
| Catalog-qualified statistics isolation | Bound-catalog regression for default and non-default installations; two persistent catalogs share a database without same-name aliasing or scoped-drop leakage | Pass |
| Preserve fail-closed persistent DML | Read-through provider rejects INSERT/overwrite, UPDATE, DELETE and TRUNCATE before mutation; direct SQL and runner paths covered, data/payload/head unchanged | Pass |
| Multi-catalog uninstall lifecycle | Binding-aware `unregister_statistics_udfs` preserves another catalog's active UDFs and removes helpers when their owner is uninstalled | Red-before-fix regression now passes |
| Constraints and views extension points | Concrete temporal structures, durable references, validity/enforcement distinctions, authoritative SQL, and commit integration documented in `catalog_single_history.md` | Documented; runtime operations deliberately deferred |
| Metadata versus data/freshness boundary | Source-owned data, metadata-only snapshots, no automatic external-write detection, no distributed atomicity claim | Explicit in architecture/integration/demo docs |
| Deferred storage scope | No file manifests, delete files, file-ID counters, compaction, GC, physical data time travel, or new storage-provider integration added | Diff scope inspected |
| Migration plan chosen before schema changes | Initial strategy recorded before wiring runtime model changes and third migration; final offline deployment procedure retained in architecture document | Complete |
| Non-destructive schema upgrade | Third migration forces a transaction on both backends, validates the sole main/latest legacy history before DDL, preserves rows and intervals, marks unknown audit provenance, removes legacy head only last | SQLite/PostgreSQL upgrade and injected rollback/retry tests pass |
| Ambiguous legacy histories rejected | Extra branch, absent main, empty branch table, older head and dangling head fixtures | All reject without partial DDL or migration records |
| Rollback without partial publication | Invalid operation and injected summary insertion failures roll back new rows, interval closure, snapshot and allocator; later successful commit confirms a failed closure did not leak | Pass |

## Commands and actual verification

All PostgreSQL commands used the local disposable `postgres:17-alpine` container on loopback
port 32768, with fresh per-test schemas. No external source database was accessed. The URL has
no production credentials. Test schemas/container contents have not been discarded.

| Gate | Exact command / evidence | Result |
| --- | --- | --- |
| Catalog tests with real PostgreSQL | `OPTD_TEST_POSTGRES_URL=postgres://postgres@127.0.0.1:32768/postgres cargo test -p optd-catalog --locked`, task `b139bc73b` | 9 unit + 10 integration + 3 compile-fail tests passed |
| Focused DataFusion regression | `cargo test -p optd-datafusion --lib persistent_ --locked`, same task | 9 passed |
| Workspace Clippy | `cargo clippy --workspace --all-targets --locked -- -D warnings`, same task | Passed |
| Rust formatting | `cargo fmt --all --check`, same task | Passed |
| Diff whitespace | `git diff --check`, same task | Passed; checked again after documentation changes |
| Broader release library regression | `cargo test --release -p optd-datafusion --lib --locked`, task `b1342c9b4` | 79 passed |
| Broader release SQL semantics | `cargo test --release -p optd-datafusion --test slt --locked -- features/`, same task | 17 passed; 270 non-feature SLTs filtered out |
| Stable code | SHA-256 manifest `/tmp/optd-final-code-baseline.json`, checked after the complete current-code command chain | All 373 package files unchanged during verification |
| Independent review | Read-only source reviewers inspected upstream and migration/publication logic; public helper bypass and cross-catalog UDF removal findings were resolved and parent-verified against code/tests | Addressed findings; unrelated cascade/DML/file work not silently included |
| Review report | `catalog_pr_review.md` preserves completed issues 1/2; issue 4 is marked completed only after duplicate-name rejection and unchanged snapshot tests passed | Issue 3 remains open; issue 5 was subsequently resolved in `catalog_pr_review.md` |
| No commit/push or stash mutation | Starting HEAD `5259c8460e073f2d9d9fa1bab796f6dbe1adabdc`; stash hash above; final ref/scope check | Existing work retained; no commit/push/apply/drop performed |

The final passing run used the current code, not merely an earlier implementation. Documentation
reconciliation followed that run and source fingerprints were checked again successfully before
handoff. The release suite is broader regression evidence, not a claim that all workspace, JOB or
TPC-H tests ran.

## Migration instructions and intentional deviations

See `docs/catalog_single_history.md#non-destructive-migration-and-deployment` for the complete
procedure: back up and test restoration; stop old/new writers; upgrade a copy first; require a
single main/latest legacy history; apply the transactional third migration; restart only the
new binary. Downgrades require restoring the pre-upgrade backup, not running destructive down
migrations or deleting databases to bypass errors. The installer runs pending migrations, so
preparation must happen before application startup.

Intentional upstream deviations:
- Conservative global serialization and whole-history stale rejection replace DuckLake's
  operation-aware optimistic conflict/rebase/retry protocol.
- Optd's snapshot domain spans its registered sources, while DuckLake orders one lake's own
  metadata. It does not atomically snapshot those sources' data.
- Optd stores source registrations, relational indexes and temporal optimizer payloads; it
  does not copy DuckLake's file lifecycle, file counters, inlined data or storage engine.
- Typed JSON summaries are inspection/audit records, not redo logs or CDC. Legacy intent and
  authorship cannot be recovered and are not fabricated.

## Remaining limitations

- Persistent-provider DML remains rejected. Coordinated source writes/statistics invalidation
  and external-write freshness detection are future work.
- Constraints/views are specified extension points, not newly implemented operations. Source
  registration currently supports creation; refresh/rename/replacement/drop require future
  typed operations.
- Catalog 0 named `datafusion` is an explicit migration convention, not inferred provenance
  for an otherwise unidentified old source.
- Global serialization may limit metadata write throughput. There is no transparent semantic
  rebase, automatic retry of stale mutations, or exactly-once guarantee for uncertain network
  COMMIT outcomes.
- Providers and physical table data are not restored from metadata at process restart.
- Cascade schema drops remain an open PR issue. Non-finite statistics serialization was
  subsequently resolved; see `catalog_pr_review.md`.
