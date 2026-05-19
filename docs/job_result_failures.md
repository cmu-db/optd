# JOB Result Failure Inventory

This tracks known execution failures in the disabled JOB result SLT suite. The
enabled JOB plan-shape suite under `connectors/optd-datafusion/tests/slt/job/explain_flat/`
is green; the result suite is kept as `.slt.disabled` files so normal test
discovery does not run the long JOB execution workload by default.

Observed command, after temporarily renaming
`connectors/optd-datafusion/tests/slt/job/results/*.slt.disabled` to `*.slt`:

```sh
cargo test --release -p optd-datafusion --test slt -- job/results
```

Observed result:

```text
113 passed; 0 failed
```

The suite passes because the three currently unsupported result cases are
recorded as `query error` expectations. They should be flipped back to normal
`query ...` records as each issue is fixed.

## Failure Summary

| Status | Case | Location | Error class | Likely owner |
| --- | --- | --- | --- | --- |
| [ ] | `job/results/17a.slt.disabled` | line 1 | duplicate projection expression name | DataFusion import/export alias preservation |
| [ ] | `job/results/17b.slt.disabled` | line 1 | duplicate projection expression name | DataFusion import/export alias preservation |
| [ ] | `job/results/17c.slt.disabled` | line 1 | duplicate projection expression name | DataFusion import/export alias preservation |

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

When `connectors/optd-datafusion/src/to_df.rs` exports that projection back to a
DataFusion logical plan, both projected expressions have the same display name.
DataFusion rejects the plan:

```text
DataFusion error: Execution error: plan build error: Error during planning:
Projections require unique expression names but the expression "min(n.name)" at
position 0 and "min(n.name)" at position 1 have the same name. Consider aliasing
("AS") one of them.
```

Known failing cases:

- [ ] `connectors/optd-datafusion/tests/slt/job/results/17a.slt.disabled:1`
  - Query projects `MIN(n.name)` as `member_in_charnamed_american_movie` and
    again as `a1`.
  - Current expected record is `query error ... duplicate expression name`.
- [ ] `connectors/optd-datafusion/tests/slt/job/results/17b.slt.disabled:1`
  - Query projects `MIN(n.name)` as `member_in_charnamed_movie` and again as
    `a1`.
  - Current expected record is `query error ... duplicate expression name`.
- [ ] `connectors/optd-datafusion/tests/slt/job/results/17c.slt.disabled:1`
  - Query projects `MIN(n.name)` as `member_in_charnamed_movie` and again as
    `a1`.
  - Current expected record is `query error ... duplicate expression name`.

Likely fix direction: preserve projection output aliases when importing
DataFusion projections, or synthesize unique aliases when exporting optd
projections that repeat the same underlying expression/column. After fixing,
regenerate these disabled result files and change the corresponding checklist
entries to `[x]`.
