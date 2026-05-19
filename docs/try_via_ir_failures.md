# `try_via_ir` Failure Inventory

This captures the SLT failures visible after `try_via_ir` stopped silently
falling back to DataFusion for IR conversion/execution failures.

Observed command:

```sh
cargo test -p optd-datafusion --test slt
```

Initial observed result:

```text
47 passed; 8 failed
```

## Failure Summary

| Status | Case | Location | Error class | Likely owner |
| --- | --- | --- | --- | --- |
| [x] | `features/correlated_subquery.slt` | line 15 | unresolved outer reference | correlated subquery import/export |
| [x] | `features/explain_udfs.slt` | line 109 | `explain_steps()` routed through IR | runner unsupported-plan detection |
| [x] | `tpch/results/q2.slt` | line 19 | unresolved outer reference | correlated scalar subquery |
| [x] | `tpch/results/q4.slt` | line 19 | unresolved outer reference | correlated `EXISTS` |
| [x] | `tpch/results/q17.slt` | line 19 | unresolved outer reference | correlated scalar subquery |
| [x] | `tpch/results/q20.slt` | line 19 | unsupported `LeftMark` join | `connectors/optd-datafusion/src/to_df.rs` |
| [x] | `tpch/results/q21.slt` | line 19 | unsupported `LeftMark` join | `connectors/optd-datafusion/src/to_df.rs` |
| [x] | `tpch/results/q22.slt` | line 19 | unresolved outer reference | correlated scalar / `NOT EXISTS` |

## Failure Classes

### Unresolved Outer References

These cases fail while DataFusion type coercion resolves a join/subquery
predicate produced from optd IR. The generated DataFusion expression still
refers to an outer table column while being planned against the inner input's
schema.

Representative error:

```text
DataFusion error: Execution error: type_coercion
caused by
Schema error: No field named customers.c_id. Valid fields are orders.o_id,
orders.o_custkey, orders.o_total.
```

Known failing cases:

- [x] `connectors/optd-datafusion/tests/slt/features/correlated_subquery.slt:15`
  - Query: `EXISTS (SELECT 1 FROM orders WHERE o_custkey = c_id)`
  - Missing field: `customers.c_id`
  - Valid schema: `orders.*`
  - Fixed by exporting correlated optd `LeftSemi` joins as DataFusion
    `EXISTS` filters with explicit outer-reference metadata.
- [x] `connectors/optd-datafusion/tests/slt/tpch/results/q2.slt:19`
  - Query shape: scalar `min(ps_supplycost)` subquery correlated on
    `p_partkey = ps_partkey`
  - Missing field: `part.p_partkey`
  - Valid schema: `partsupp`, `supplier`, `nation`, `region`
  - Fixed by reconstructing `Selection(SingleJoin)` predicates as DataFusion
    scalar subquery expressions with explicit outer-reference metadata.
- [x] `connectors/optd-datafusion/tests/slt/tpch/results/q4.slt:19`
  - Query shape: correlated `EXISTS` over `lineitem`
  - Missing field: `orders.o_orderkey`
  - Valid schema: `lineitem.*`
  - Fixed by exporting correlated optd `LeftSemi` joins as DataFusion
    `EXISTS` filters with explicit outer-reference metadata.
- [x] `connectors/optd-datafusion/tests/slt/tpch/results/q17.slt:19`
  - Query shape: scalar `avg(l_quantity)` subquery correlated on
    `l_partkey = p_partkey`
  - Missing field: `part.p_partkey`
  - Valid schema: `lineitem.*`
  - Fixed by reconstructing `Selection(SingleJoin)` predicates as DataFusion
    scalar subquery expressions with explicit outer-reference metadata.
- [x] `connectors/optd-datafusion/tests/slt/tpch/results/q22.slt:19`
  - Query shape: derived table with scalar `avg(c_acctbal)` and correlated
    `NOT EXISTS` over `orders`
  - Missing field: `customer.c_custkey`
  - Valid schema: `orders.*`
  - Fixed by reconstructing correlated scalar joins as DataFusion scalar
    subquery expressions and correlated optd `LeftAnti` joins as negated
    `EXISTS` filters.

### Unsupported `LeftMark` Join Export

These cases reach `to_logical_plan` with `JoinType::LeftMark`, but
`connectors/optd-datafusion/src/to_df.rs` does not convert that optd join type
to a DataFusion logical plan.

Representative error:

```text
DataFusion error: Execution error: unsupported IR node: join type LeftMark(Column(44))
```

Known failing cases:

- [x] `connectors/optd-datafusion/tests/slt/tpch/results/q20.slt:19`
  - Query shape: nested `IN` plus correlated scalar quantity subquery
  - Error: `unsupported IR node: join type LeftMark(Column(44))`
  - Fixed by exporting marker-consuming `Selection(LeftMarkJoin)` patterns as
    semi/anti joins and by reconstructing `Selection(SingleJoin)` predicates as
    DataFusion scalar subquery expressions.
- [x] `connectors/optd-datafusion/tests/slt/tpch/results/q21.slt:19`
  - Query shape: `EXISTS` and `NOT EXISTS` over aliased `lineitem`
  - Error: `unsupported IR node: join type LeftMark(Column(117))`
  - Fixed by exporting marker-consuming `Selection(LeftMarkJoin)` patterns as
    semi/anti joins.

This is the known `LeftMark` join conversion gap in
`connectors/optd-datafusion/src/to_df.rs`.

### `explain_steps()` Table Function Routed Through IR

`explain_steps()` is registered by `OptdRunner::new`, but the query is attempted
through `try_via_ir`. The IR path cannot preserve or resolve the registered
table function, so the test now fails instead of silently falling back.

Failing case:

- [x] `connectors/optd-datafusion/tests/slt/features/explain_udfs.slt:109`
  - Query: `SELECT step, pass, result FROM explain_steps(..., 'flat')`
  - Error: `table not found: explain_steps()`
  - Fixed by rejecting non-catalog DataFusion `TableScan` nodes before optd IR
    import, causing table-function results to use direct DataFusion execution.

Implemented fix direction: classify table-function scans that optd cannot
import or export as `TryViaIrError::Unsupported`, so they still use direct
DataFusion execution, while preserving hard failures for supported IR paths.
