# Optional advanced statistics

## Boundary

Advanced statistics are **additional** metadata, not a replacement for basic
statistics. `optd_statistics`, `optd_statistics_payload`, and
`optd_column_statistics` keep their existing schema and read/write API. Their
original definition/payload/column-version references are unchanged. DataFusion's
`collect_stats`, `cardest`, and persistent runner still use that basic path.
These are relation-level summaries, not a preserved copy of Parquet file/row-group
footers; source/physical metadata remains a separate concern.

The fourth migration adds only:

```text
optd_adv_stats(
  row_id PK, statistics_id, relation_id, statistics_name, statistics_kind,
  column_ids JSON, begin_snapshot, end_snapshot?,
  value_encoding_version, contents TEXT, collected_at
)
```

`statistics_id` comes from the existing global `next_catalog_id` allocator.
`row_id` is private persistence identity. All visibility intervals use the same
global snapshot history; there is no advanced-statistics snapshot domain.
`contents` is typed JSON encoded as TEXT to preserve large Decimal128 bounds.
Unique indexes protect object/version pairs and relation-local active names.
The migration does not rewrite basic tables or allocate snapshots/object IDs.

## Contents supported by the storage API

- **Joint NDV:** number of distinct tuples, excluding rows with any NULL target
  value. Requires at least two distinct, visible durable column IDs. Target order
  is irrelevant and is canonicalized.
- **Scalar histogram:** exactly one target column and one or more buckets.
  The first interval is `(-infinity, upper_0]`; later intervals are
  `(upper_previous, upper_current]`. Each frequency counts observed non-null
  values in that interval. All observed non-null values must be covered; these
  frequencies describe the caller's collected population, not necessarily a full
  table scan. Upper bounds must strictly increase, be finite and match the column
  type. Boolean, Int32, Int64, Float64, UTF-8, Date32 and Decimal128 bounds are
  supported. Frequency totals must fit `u64`.

These APIs persist caller-collected estimates. They do not implement a histogram
or multicolumn collection algorithm, automatic freshness checks, or optimizer
consumption. Dependencies, sketches, multidimensional histograms, sampling
provenance and future contents need explicit typed variants/contracts rather than
unversioned arbitrary JSON. These contracts adapt DuckLake-style temporal ideas;
they do not claim DuckLake on-disk statistics format compatibility.

## API and lifecycle

```rust,ignore
use optd_catalog::advanced_statistics::{
    AdvancedStatisticsContents, CreateAdvancedStatisticsInfo,
    UpdateAdvancedStatisticsInfo, DropAdvancedStatisticsInfo,
};

let id = catalog.create_advanced_statistics(
    reader.snapshot_id(),
    CreateAdvancedStatisticsInfo {
        relation_id,
        statistics_name: "orders_customer_product_ndv".into(),
        column_ids: vec![customer_column_id, product_column_id],
        contents: AdvancedStatisticsContents::JointNdv { distinct_count: 500 },
    },
).await?;
```

- `CatalogReader::get_advanced_statistics(id)` returns a visible object or `None`.
- `get_relation_advanced_statistics(relation_id)` lists objects in stable-ID order.
- `update_advanced_statistics(expected, UpdateAdvancedStatisticsInfo)` replaces
  contents while keeping ID, relation, name, targets, and kind immutable.
- `drop_advanced_statistics(expected, DropAdvancedStatisticsInfo)` retires the
  active object. It cannot subsequently be updated/resurrected. Names can be reused
  by newly allocated objects after retirement.
- `invalidate_relation_statistics` closes the active **basic payload and every
  advanced object** in one transaction. Repeated invalidation is allowed. Basic
  recollection retains its definition ID as before; advanced recollection creates
  new objects/IDs. This deliberately avoids a second identity/payload lifecycle
  or a hidden distinction between dropped and invalidated advanced rows.
- Basic recollection alone leaves advanced objects unchanged; it is not evidence
  that their distribution/correlation estimates were recollected.
- Relation drops also close advanced objects. Historical readers keep old versions.

Every mutation owns a top-level transaction through `snapshot/commit.rs`, including
writer locking, stale-base rejection, validation, ID allocation, interval closure,
snapshot and typed summary publication. Advanced-only mutations do not advance
`schema_version` or modify basic rows. Namespace checks apply to IDs as well as
names. Writes and reads validate typed contents, targets, kind and encoding.
Metadata visibility does not establish external-data freshness; raw/external
writes still need explicit invalidation or recollection. The shared SQL execution
boundary now observes DML and actual DDL callbacks; see
[observed_execution.md](observed_execution.md). This preserves source write
capabilities instead of installing the earlier DML-rejecting table wrapper.

## Deployment and verification

Back up the catalog, stop writers, and upgrade a copy before deployment, as described
in [catalog_single_history.md](catalog_single_history.md). The supported input is
the existing basic-statistics layout, with pending single-history migration allowed.
The withdrawn experimental flattening migration is not an upgrade input; restore a
pre-refactor backup rather than editing migration records or deleting data.

Coverage lives in `optd/catalog/tests/advanced_statistics.rs`, the retained basic
statistics test in `optd/catalog/src/catalog_tests.rs`, and
`persistent_advanced_statistics_do_not_replace_basic_statistics` in the DataFusion
runner tests. It covers historical versions, immutable identity/metadata,
invalidation, namespace isolation, semantic validation, precision, rollback,
additive migration preservation, and basic-only estimator behavior.

Run:

```sh
cargo test -p optd-catalog --locked
cargo test -p optd-datafusion --lib persistent_ --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo fmt --all --check
git diff --check
```

PostgreSQL tests require `OPTD_TEST_POSTGRES_URL` pointing to a disposable local
instance; unset-URL tests return early and are not PostgreSQL verification.

Historical verification before shared write observation (`be103e6dc`) passed:

- Catalog: 12 unit tests, 19 integration tests, and 3 compile-fail doctests.
  Integration coverage includes three tests against an actual disposable local
  PostgreSQL 18 instance: legacy migration, concurrent publication, and advanced
  object publication/invalidation with injected summary failure.
- DataFusion: all 11 focused `persistent_` library tests, including the new
  basic-only estimator regression.
- Workspace/all-targets Clippy with `-D warnings`, formatting, and diff checks.

An independent read-only reviewer found the Decimal128 decoding defect, also
reproduced by the new test; external-tagged contents fixed it and the regression
passed in the final run. A formatting-only failure in an earlier command chain was
also corrected before the final gate. The follow-up review found no remaining
concrete blockers. The newly created loopback-only test container and its anonymous
volumes were removed after successful verification; the existing container was not
used or modified. Full release SLT/workspace test suites were not run for this change.
