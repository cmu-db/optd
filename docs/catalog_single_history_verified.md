# Single-history catalog: final verification and requirement audit

> Historical acceptance record, predating the additive advanced-statistics extension.
> The basic statistics layout is retained; these checks and fingerprints do not verify
> the extension. See `catalog_single_history.md` and `advanced_statistics.md`.

## Outcome

The metadata-only DuckLake adaptation is implemented and verified. The catalog has one global
snapshot history, structural schema generations, typed atomic change summaries, scoped external
catalog registrations, and a shared SeaORM commit path. Metadata history is not source-data time
travel. This note records this session's independent acceptance run. The companion
`catalog_single_history_audit.md` records the parallel session's audit and is preserved without
overwriting it. Earlier blocked checkpoints were not accepted as final verification evidence.

## Evidence examined

- Upstream DuckLake: `ed31fddd089b038a800fa35e155f30e3c3abc8f0`. The implementation/specification
  comparison and exact source references are in `catalog_single_history.md`. The parent inspected
  the publication loop and snapshot/change insertion code, alongside bounded read-only upstream
  investigation and migration/code reviews.
- Stashed design: `stash@{0}:docs/optd-catalog.dbml`, inspected without applying the stash.
  Stash object remains `a8a588013f98132d8a1056a95cea252c781042f1`.
- HEAD remains `5259c8460e073f2d9d9fa1bab796f6dbe1adabdc`. No commit or push was performed.
- Final acceptance task: `bf75542ff` (`Verify final unchanged source`), exit 0. Log:
  `.pi/tasks/01a15971-a2e8-7500-9bb7-055d6e5fbe45-84359/bf75542ff.output`.
- The acceptance command fingerprinted 69 Rust files under `optd/catalog` and connector `src`
  before running the checks and verified those same bytes afterward. Subsequent read-only checks
  matched. Manifest: `/tmp/optd-final-source.sha256`; manifest SHA-256:
  `620a6aad73871984c8d8d4a8b6efdceb3217a1415b0155e53a28634103a5fdd0`.
  The fingerprint is supplementary evidence, not a substitute for inspecting test coverage.
- Real PostgreSQL verification used a local disposable PostgreSQL 17.9 container,
  `optd-catalog-goal-pg`, bound only to loopback port 32768. Tests created separate per-run schemas.
  No external source database was accessed or existing user database contents discarded.
- The initial executable regression failed because the legacy schema still contained
  `optd_branch`. Two earlier test-compilation mistakes were corrected before that runtime failure;
  they are not counted as evidence of the semantic regression.
- Independent closure review found no remaining findings in the previously identified public
  mutation bypass, cross-catalog UDF uninstall, and contradictory integration documentation.
  Its read-only verdict is advisory; the parent-owned checks below provide execution evidence.

## Prompt-to-artifact checklist

| Explicit requirement | Implementation/documentation | Concrete verification |
| --- | --- | --- |
| Inspect DuckLake source/specification and record revision | `catalog_single_history.md`, upstream references above | Actual publication loop and SQL inspected; conservative deviations documented |
| Inspect stashed DBML without applying/dropping stash | Stashed global-history/source-registration model; unchanged stash object above | Read-only `git show`; final stash ref checked |
| One global snapshot ID, no runtime branches/head table | `snapshot/get_current_snapshot_info.rs`, `snapshot/commit.rs`; old public branch entity/APIs removed | `publication_uses_global_history_not_branch_heads`; runtime source audit found no branch API/head fields; branch model retained only for frozen upgrade code |
| Structural `schema_version` | Snapshot model, `SnapshotChange::structural`, migration backfill | History assertions; structural begin/end boundaries and gaps in legacy fixture; statistics refresh/invalidation remain at generation 2 |
| Snapshot change summaries | `entity/snapshot_change.rs`, typed `SnapshotChange`, insertion in shared transaction | Exact typed summary assertions; matching snapshot/summary counts; injected summary failure |
| Shared atomic commit | All public mutations route through `snapshot/commit.rs` | Failure tests preserve metadata, interval closures, snapshot and allocator; successful retry publishes B+1 |
| Justified PostgreSQL/SQLite concurrency | Owned top-level transaction; PostgreSQL READ COMMITTED; mutex UPDATE before visibility reads | Held-lock waiting and overlapping independent connections on SQLite DELETE/WAL and PostgreSQL; PostgreSQL non-default isolation test |
| No supported typed mutation bypass | Schema/relation mutation helpers and exports are crate-private | Three compile-fail doctests: transaction-backed mutation and both formerly public drop helpers |
| External catalog registration and schema ownership | `source.rs`, `entity/source_catalog.rs`, schema `catalog_id`, scoped `Catalog::on_catalog` | Distinct registrations with identical schema names; cross-namespace mutation rejected |
| Proper DataFusion namespaces | Installation resolves durable catalog ID; scoped DDL/statistics handles | Two persistent catalogs share one database without relation/statistics aliasing; scoped drops preserve the other catalog |
| Preserve relations, columns, indexes, statistics and stable IDs | Original catalog tests retained in `catalog_tests.rs`; independent legacy SQL fixture | All nine catalog unit tests; legacy object/column/index/statistics reads, ID/time/counter preservation and post-upgrade writes |
| Historical metadata visibility | Pinned readers and half-open intervals | Historical relation/index/statistics reads survive later changes; old payloads survive refresh and invalidation |
| Catalog-qualified statistics isolation | Bound catalog checks plus scoped durable lookup | Original cross-catalog overwrite regression, bare-reference/default binding checks, foreign-catalog fallback and unchanged rejected-write snapshots |
| Preserve fail-closed persistent DML | Existing read-through table wrapper retained | Direct SQL and runner reject insert/overwrite/update/delete/truncate without changing rows, payload or snapshot; non-persistent/uninstalled writes still work |
| Statistics refresh/invalidation | Temporal payload split retained; shared commit variants | Refresh/invalidate typed summaries, stable definition identity, historical payload bounds and unchanged structural version |
| Constraints/views extension points without unrelated implementation | Concrete temporal structures, reference validation and commit integration described in `catalog_single_history.md` | Documentation inspected; no new runtime view/constraint subsystem introduced |
| Do not claim data snapshots or automatic external-write detection | Scope/freshness sections and DataFusion integration/demo notes | Documentation explicitly separates planning metadata visibility from source freshness |
| Defer files/delete files/counters/compaction/GC/data time travel/providers | Scope and extension sections | Diff audit: no managed-file subsystem, `next_file_id`, new provider integration, or dependency change |
| Non-destructive strategy documented before schema work | Initial `catalog_single_history.md` strategy, final deployment procedure, migration README | Frozen legacy models; transactional third migration; independent old-schema fixture on both backends |
| Migration safety, rollback, ambiguity and retry | `migration/single_history.rs` with forced transactions | Fresh installs; extra/missing/dangling/stale main rejection; injected SQLite/PostgreSQL DDL/backfill failure rolls back and retries; forward-only downgrade refusal |
| Preserve existing work; no stash apply/drop, commit/push | Existing namespace/DML fixes, SQLite test helper and root report retained | Unchanged HEAD/stash refs; scope/diff audit; no changes to Cargo.lock, core or sketches |
| Update review report only for tested resolutions | `catalog_pr_review.md` issues 1/2 retained completed; issue 4 completed with duplicate-name tests | Backend duplicate-schema regression and namespace/compile-fail coverage; issues 3 and 5 remain open |
| Iterate using failing evidence, not green proxies | Regression, backend fixture correction, independent review fixes, repeated affected checks | Final acceptance run on unchanged Rust sources; source/test/migration inspection in addition to command exit status |
| Finish with evidence, deviations, deployment and limitations | This note plus `catalog_single_history.md` | Final artifact audit, command results below and explicit limitations |

## Final command results

All commands ran successfully in final task `bf75542ff`:

```sh
OPTD_TEST_POSTGRES_URL=postgres://postgres@127.0.0.1:32768/postgres cargo test -p optd-catalog --locked
# 9 unit + 10 integration tests passed, including actual PostgreSQL; 3 compile-fail doctests passed
cargo test -p optd-datafusion --lib persistent_ --locked
# 9 passed
cargo clippy --workspace --all-targets --locked -- -D warnings
# passed
cargo test --release -p optd-datafusion --lib --locked
# 79 passed
cargo test --release -p optd-datafusion --test slt --locked -- features
# 17 passed, 270 filtered out
cargo fmt --all --check
# passed
git diff --check
# passed
shasum -c /tmp/optd-final-source.sha256
# all fingerprinted Rust files unchanged
```

This does not claim that the full 287-file SLT suite, every optimizer benchmark, or upstream
DuckLake's test suite ran. The broader release check covered the connector library and feature SLTs.

## Upstream deviations and deployment

Optd deliberately uses conservative global serialization and whole-history stale rejection,
not DuckLake's operation-aware optimistic rebase/retry protocol. It retains its richer temporal
statistics payload split and adds source namespaces and index metadata. It is not a DuckLake
metadata-format-compatible database or a port of the storage engine.

Upgrade procedure: back up the metadata database, stop all writers, test upgrading a copy, then
run the new application/migrator. Only a sole legacy `main` at the largest snapshot is accepted.
The transaction preserves IDs, timestamps, allocator state and intervals; old schemas become
catalog 0 (`datafusion`), and unknown original summaries become `legacy_import`. Automatic
downgrade is refused. Restore the pre-upgrade backup into the old binary if rollback is needed.
See `catalog_single_history.md` and `optd/catalog/migrations/README.md` for details.

## Remaining limitations

- External row mutations and freshness are not automatically observed. Planning-metadata
  snapshots do not pin external data.
- Persistent DML remains rejected until source writes and statistics invalidation can be
  coordinated. Retained raw providers and writes while persistence is disabled are untracked.
- Constraint/view runtime operations, source refresh/rename reconciliation, and registration
  replacement/drop remain future typed extensions. File ownership and physical time travel
  remain deferred.
- Global serialization is intentionally conservative. Busy/connection/uncertain-COMMIT errors
  are not transparently replayed; inspect history before retrying an uncertain outcome.
- Offline migration is required. Legacy provenance cannot be reconstructed, and unusual branch
  states require explicit resolution rather than discarded history.
- PR issues 3 (schema cascade) and 5 (non-finite bounds) remain open; they were not silently
  included in or claimed fixed by this metadata adaptation.
- Prior concurrent edits caused failed verification attempts. The final run succeeded with
  stable Rust bytes; no processes were terminated and no other session's audit was overwritten.
