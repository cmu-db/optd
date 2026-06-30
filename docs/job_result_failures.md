# JOB Result Failure Inventory

This tracks known execution failures and long-running cases in the JOB result
SLT suite. The enabled JOB plan-shape suite under
`optd/connectors/datafusion/tests/slt/job/explain_flat/` is green. The result suite
is enabled, but JOB `19d` remains a long-running benchmark case and is disabled
in the SLT harness.

Observed command:

```sh
cargo nextest run --release -p optd-datafusion --test slt job/results
```

Earlier nextest runs on the old path timed out on several cases:

```text
109 passed; 4 timed out
```

With physical planning enabled by default, `16a`, `16c`, and `16d` have been
observed to complete locally. Keep `19d` disabled until its runtime is fixed.
Use this command to run the enabled JOB result suite:

```sh
cargo nextest run --release -p optd-datafusion --test slt -E 'test(/job\/results/)'
```

## Failure Summary

| Status | Case | Location | Error class | Likely owner |
| --- | --- | --- | --- | --- |
| [x] | `job/results/17a.slt.disabled` | line 1 | duplicate projection expression name | DataFusion import/export alias preservation |
| [x] | `job/results/17b.slt.disabled` | line 1 | duplicate projection expression name | DataFusion import/export alias preservation |
| [x] | `job/results/17c.slt.disabled` | line 1 | duplicate projection expression name | DataFusion import/export alias preservation |
| [x] | `job/results/16a.slt` | line 1 | completed under physical planning | JOB benchmark execution/runtime |
| [x] | `job/results/16c.slt` | line 1 | completed under physical planning | JOB benchmark execution/runtime |
| [x] | `job/results/16d.slt` | line 1 | completed under physical planning | JOB benchmark execution/runtime |
| [ ] | `job/results/19d.slt` | line 1 | exceeds 240s nextest timeout | JOB benchmark execution/runtime |

## Failure Classes

### Duplicate Aggregate Projection Names

JOB Q17 variants project the same aggregate expression twice with different SQL
aliases:

```sql
SELECT MIN(n.name) AS member_in_charnamed_movie, MIN(n.name) AS a1 ...
```

DataFusion can initially plan the SQL, but the optd IR round trip loses the two
distinct projection aliases. The imported IR has one aggregation output column
named like `min(n.name)` and a projection that emits that same column twice:

```text
γ Aggregation
  aggregates:
    min(n.name)(#92) := min(name(#60))
π Projection
  columns:
    min(n.name)(#92)
    min(n.name)(#92)
```

When `optd/connectors/datafusion/src/to_df.rs` exports that projection back to a
DataFusion logical plan, both projected expressions have the same display name.
DataFusion rejects the plan:

```text
DataFusion error: Execution error: plan build error: Error during planning:
Projections require unique expression names but the expression "min(n.name)" at
position 0 and "min(n.name)" at position 1 have the same name. Consider aliasing
("AS") one of them.
```

Known failing cases:

- [x] `optd/connectors/datafusion/tests/slt/job/results/17a.slt.disabled:1`
  - Query projects `MIN(n.name)` as `member_in_charnamed_american_movie` and
    again as `a1`.
  - Fixed by materializing repeated projected columns as `Map` computations,
    preserving the distinct output aliases.
- [x] `optd/connectors/datafusion/tests/slt/job/results/17b.slt.disabled:1`
  - Query projects `MIN(n.name)` as `member_in_charnamed_movie` and again as
    `a1`.
  - Fixed by materializing repeated projected columns as `Map` computations,
    preserving the distinct output aliases.
- [x] `optd/connectors/datafusion/tests/slt/job/results/17c.slt.disabled:1`
  - Query projects `MIN(n.name)` as `member_in_charnamed_movie` and again as
    `a1`.
  - Fixed by materializing repeated projected columns as `Map` computations,
    preserving the distinct output aliases.

Implemented fix: when a DataFusion projection emits the same input column more
than once, `from_df` now materializes each repeated projection output as a
`Map` computation with the projection field name. This mirrors DataFusion's
aggregate deduplication while preserving the distinct user-visible aliases that
the final projection needs.

### Long-Running JOB Result Query

The following case still runs too long under the default physical-planning path
and is disabled in the SLT harness:

- `optd/connectors/datafusion/tests/slt/job/results/19d.slt`

This is treated as a benchmark/runtime long runner rather than a refactor
regression.
