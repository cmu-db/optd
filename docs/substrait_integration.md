# Substrait Integration

This document is a handoff for the current Substrait work in `optd`.

## Current State

Main implementation lives in [`optd/core/src/substrait.rs`](../optd/core/src/substrait.rs). Public entry points:

- `substrait::from_plan(&Plan) -> Result<QueryContext, SubstraitError>`
- `substrait::from_plan_with_catalog(&Plan, Arc<dyn Catalog>) -> Result<QueryContext, SubstraitError>`
- `substrait::from_rel(&Rel) -> Result<QueryContext, SubstraitError>`
- `substrait::from_rel_with_catalog(&Rel, Arc<dyn Catalog>) -> Result<QueryContext, SubstraitError>`
- `substrait::to_plan(&QueryContext) -> Result<Plan, SubstraitError>`

The importer is substantially ahead of the exporter. It supports enough Substrait to consume a real DataFusion plan for:

```sql
SELECT id FROM users WHERE age >= 18 ORDER BY id DESC LIMIT 5
```

The exporter now handles:

- `Output`
- `Projection`
- `Selection`
- `Sort`
- `Limit`
- `Join`
- `CrossProduct`
- `Aggregation`
- `Map`
- `TableFunction` (local-files read forms where representable)
- named-table `Scan`
- root output names
- scalar expression export (`ColumnRef`, `Literal`, `BinaryOp`, `UnaryOp`, `NaryOp`, `ScalarFunction`) with inferred output types for common cases
- scalar function anchors via `Plan.extensions`
- basic Arrow types: `Boolean`, `Int32`, `Int64`, `Float64`, `Utf8`

## Tested Interop

Core-crate Substrait/DataFusion integration tests have been removed. Current automated coverage is
through unit tests in `optd-core` and sqllogictest coverage in
`optd/connectors/datafusion/tests/`.

Run:

```bash
cargo test
```

Important detail: DataFusion and this crate may use different `substrait` crate versions. Interop
checks should cross that boundary through protobuf bytes, not direct Rust type sharing.

## Importer Coverage

Importer-supported relation families include:

- `ReadRel` named tables, local files, filters, simple projections, and `emit`
- `FilterRel` -> `Selection`
- `ProjectRel` -> `Map`, plus `emit` -> `Projection`
- `JoinRel`, `HashJoinRel`, `MergeJoinRel`, `NestedLoopJoinRel`
- `CrossRel` -> `CrossProduct`
- `SortRel` -> `Sort`
- `FetchRel` -> `Limit`
- simple `AggregateRel` -> `Aggregation`

Expression import supports literals, column references, scalar functions, casts, common binary ops, `AND`/`OR`, and basic unary ops. Unsupported expression forms return explicit `SubstraitError` variants.

## Known Limitations

Exporter gaps remain:

- some table-function extension/file formats are intentionally conservative
- expression coverage is still conservative compared to full Substrait expression space
- `ConstScan` export is currently unsupported in Substrait (`SubstraitError::UnsupportedRel("ConstScan")`)

Importer limitations to watch:

- aggregation grouping allows expression keys, but available-column analysis rejects expression aggregation keys for now
- `ColumnData` no longer stores nullability; nullability is analysis/catalog-derived
- Substrait type handling is still a conservative subset
- local file import maps file formats to `TableFunctionDef`, but exporter does not emit local files

Note: the DataFusion bridge (`optd/connectors/datafusion`) now handles
`EmptyRelation`/`Values` via `ConstScan`, but that is separate from Substrait conversion.

## Recommended Next Steps

1. Broaden table-function export coverage.
   Add more extension/local-file format mappings as needed by downstream consumers.

2. Broaden expression coverage.
   Add explicit export support for additional Substrait expression families when the IR grows to represent them.

## Design Notes

Keep the importer and exporter byte-compatible with DataFusion rather than type-compatible with DataFusion’s `substrait` crate version. Prefer adding narrow tests that roundtrip through DataFusion’s public Substrait APIs.

The existing converter builds `QueryContext` append-only and wraps imported roots in an explicit `Output` operator. The exporter expects callers to have set a root; if the root is `Output`, it exports the input relation and uses the output columns as `RelRoot.names`.
