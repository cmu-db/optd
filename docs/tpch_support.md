# TPC-H Support

This document tracks the remaining work to make Substrait import/export in `simple-graph` practical for TPC-H query plans.

## Current Baseline

Implemented exporter coverage:

- `Scan` (named table), `Selection`, `Projection`, `Map`
- `Join`, `CrossProduct`
- `Sort`, `Limit`
- `Aggregation`
- `TableFunction` for representable local-file reads
- scalar expressions (`Literal`, `ColumnRef`, `Unary`, `Binary`, `Nary`, `Cast`, `CaseWhen`, `ScalarFunction`, `Exists`, `InSubquery`, `ScalarSubquery`) with conservative output typing

Root-crate integration tests for these operators have been removed. The DataFusion bridge keeps its
own sqllogictest coverage under `crates/simple-graph-datafusion/tests/`.

## Local TPC-H Data Setup

The DataFusion sqllogictest TPC-H suite reads local Parquet files instead of generating tables at
test time. Generate the files once with `tpchgen-cli`:

```sh
tpchgen-cli -s 0.1 -f parquet -o crates/simple-graph-datafusion/data/tpch/sf-0.1
```

The generated `crates/simple-graph-datafusion/data/` contents are ignored by git. Keep only
`crates/simple-graph-datafusion/data/.gitignore` tracked.

Run the TPC-H SLT suite with either:

```sh
cargo test -p simple-graph-datafusion --test slt tpch
cargo nextest run -p simple-graph-datafusion --test slt tpch
```

The Rust TPC-H matrix now has one direct IR builder function per query (`tpch_q1` through `tpch_q22`). Each builder manually constructs a `QueryContext` using simple-graph IR operators. The IR can now represent SQL subquery expressions directly with `Exists`, `InSubquery`, and `ScalarSubquery`; later passes should lower those forms to semi/anti/single/mark joins before Substrait export. Q6 remains the first export+consume green query while the other builders track the relational and expression shapes that still need lowering plus Substrait/DataFusion compatibility work.

## Gaps Most Likely to Block TPC-H

1. **Type coverage**
   - Decimal types used by `l_extendedprice`, `l_discount`, `l_tax`
   - Date and timestamp types used in TPC-H predicates and expressions
   - Full nullability/type-variation fidelity where consumers depend on it

2. **Expression coverage**
   - Proper `CAST` export
   - `CASE WHEN`-style conditional expressions
   - richer function set used by TPC-H style plans (date extraction, string ops, arithmetic variants)
   - consistent function extension naming/anchors compatible with DataFusion consumer behavior

3. **Join/subquery semantics**
   - represent SQL `EXISTS`, `[NOT] IN`, and scalar subqueries in `ExprData`
   - support for semi/anti join semantics where plans use `EXISTS`/`IN` forms
   - ensure mark/single join behavior is either supported or intentionally lowered pre-export

4. **Table read fidelity**
   - broader table-function and local-file format options as needed by benchmark setup
   - catalog/schema fidelity for benchmark schemas

## Proposed Execution Plan

1. **Type first**
   - extend Arrow <-> Substrait type mapping for decimal/date/timestamp family
   - add narrow unit tests for every newly supported type

2. **Expression expansion**
   - add explicit export handling for cast/conditional expressions
   - add tests that validate both produced Substrait shape and DataFusion consumption

3. **Join semantics**
   - add/validate semi/anti-compatible lowering or direct mapping strategy
   - add query-shape tests for subquery-like plans
   - add a lowering/decorrelation pass from subquery expressions to joins before export
   - current TPC-H sqllogictest blocker: DataFusion Substrait producer rejects scalar subquery expressions (`Cannot convert <subquery> to Substrait`) starting at Q2

4. **TPC-H query coverage harness**
   - add or restore an integration matrix for TPC-H queries (Q1..Q22)
   - track per-query status:
     - exports successfully
     - DataFusion consumes exported plan
     - output schema compatibility

## Tracking Template

- [x] Decimal/date/timestamp type coverage complete
- [x] Cast/conditional expression coverage complete
- [x] Semi/anti/subquery expression semantics represented
- [ ] Subquery-to-join lowering pass added
- [ ] TPC-H integration harness added
- [ ] Q1–Q22 export+consume matrix green
