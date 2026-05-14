# TPC-H Support WIP

This document tracks the remaining work to make Substrait import/export in `simple-graph` practical for TPC-H query plans.

## Current Baseline

Implemented exporter coverage:

- `Scan` (named table), `Selection`, `Projection`, `Map`
- `Join`, `CrossProduct`
- `Sort`, `Limit`
- `Aggregation`
- `TableFunction` for representable local-file reads
- scalar expressions (`Literal`, `ColumnRef`, `Unary`, `Binary`, `Nary`, `ScalarFunction`) with conservative output typing

Integration tests currently validate DataFusion consumer interoperability for these operators.

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

4. **TPC-H query coverage harness**
   - add an integration matrix for TPC-H queries (Q1..Q22)
   - track per-query status:
     - exports successfully
     - DataFusion consumes exported plan
     - output schema compatibility

## Tracking Template

- [x] Decimal/date/timestamp type coverage complete
- [x] Cast/conditional expression coverage complete
- [ ] Semi/anti/subquery-lowered semantics covered
- [ ] TPC-H integration harness added
- [ ] Q1–Q22 export+consume matrix green
