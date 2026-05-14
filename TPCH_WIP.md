# TPC-H Support WIP

This document tracks the remaining work to make Substrait import/export in `simple-graph` practical for TPC-H query plans.

## Current Baseline

Implemented exporter coverage:

- `Scan` (named table), `Selection`, `Projection`, `Map`
- `Join`, `CrossProduct`
- `Sort`, `Limit`
- `Aggregation`
- `TableFunction` for representable local-file reads
- scalar expressions (`Literal`, `ColumnRef`, `Unary`, `Binary`, `Nary`, `Cast`, `CaseWhen`, `ScalarFunction`, `Exists`, `InSubquery`, `ScalarSubquery`) with conservative output typing

Integration tests currently validate DataFusion consumer interoperability for these operators. A sqllogictest harness validates DataFusion SQL plans after a Substrait export/import round trip with logical optimizer rules disabled. Plan-only sqllogictests carry all TPC-H Q1-Q22 SQL shapes over empty benchmark schemas as separate files under `tests/slt/tpch/`; the full TPC-H run is gated behind `INCLUDE_TPCH=true cargo test --test sqllogictest_harness` until remaining Substrait gaps are closed.

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
   - add an integration matrix for TPC-H queries (Q1..Q22)
   - track per-query status:
     - exports successfully
     - DataFusion consumes exported plan
     - output schema compatibility

## Current TPC-H sqllogictest Status

Command: `INCLUDE_TPCH=true cargo test --test sqllogictest_harness`

Last checked: May 14, 2026.

| Query | File | Status | Current failure |
| --- | --- | --- | --- |
| Q01 | `tests/slt/tpch/q01.slt` | Pass | None |
| Q02 | `tests/slt/tpch/q02.slt` | Fail | DataFusion Substrait producer: `Cannot convert <subquery> to Substrait` |
| Q03 | `tests/slt/tpch/q03.slt` | Pass | None |
| Q04 | `tests/slt/tpch/q04.slt` | Fail | DataFusion Substrait producer: `Cannot convert Exists { subquery: <subquery>, negated: false } to Substrait` |
| Q05 | `tests/slt/tpch/q05.slt` | Pass | None |
| Q06 | `tests/slt/tpch/q06.slt` | Pass | None |
| Q07 | `tests/slt/tpch/q07.slt` | Pass | None |
| Q08 | `tests/slt/tpch/q08.slt` | Pass | None |
| Q09 | `tests/slt/tpch/q09.slt` | Pass | None |
| Q10 | `tests/slt/tpch/q10.slt` | Pass | None |
| Q11 | `tests/slt/tpch/q11.slt` | Fail | DataFusion Substrait producer: `Cannot convert <subquery> to Substrait` |
| Q12 | `tests/slt/tpch/q12.slt` | Pass | None |
| Q13 | `tests/slt/tpch/q13.slt` | Pass | None |
| Q14 | `tests/slt/tpch/q14.slt` | Pass | None |
| Q15 | `tests/slt/tpch/q15.slt` | Fail | DataFusion Substrait producer: `Cannot convert <subquery> to Substrait` |
| Q16 | `tests/slt/tpch/q16.slt` | Fail | Physical planner: `Physical plan does not support logical expression InSubquery(...)` |
| Q17 | `tests/slt/tpch/q17.slt` | Fail | DataFusion Substrait producer: `Cannot convert <subquery> to Substrait` |
| Q18 | `tests/slt/tpch/q18.slt` | Fail | Physical planner: `Physical plan does not support logical expression InSubquery(...)` |
| Q19 | `tests/slt/tpch/q19.slt` | Pass | None |
| Q20 | `tests/slt/tpch/q20.slt` | Fail | DataFusion Substrait producer: `Cannot convert <subquery> to Substrait` |
| Q21 | `tests/slt/tpch/q21.slt` | Fail | DataFusion Substrait producer: `Cannot convert Exists { subquery: <subquery>, negated: false } to Substrait` |
| Q22 | `tests/slt/tpch/q22.slt` | Fail | DataFusion Substrait producer: `Cannot convert <subquery> to Substrait` |

## Tracking Template

- [x] Decimal/date/timestamp type coverage complete
- [x] Cast/conditional expression coverage complete
- [x] Semi/anti/subquery expression semantics represented
- [ ] Subquery-to-join lowering pass added
- [x] TPC-H integration harness added
- [ ] Q1–Q22 export+consume matrix green
