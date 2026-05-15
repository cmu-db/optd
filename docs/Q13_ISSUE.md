# Q13 Converter Issue: DataFusion Join Schema Qualified/Unqualified Ambiguity

## Status

Q13 is the only TPC-H query that fails the IR round-trip (22/23 passing).

## Root Cause

DataFusion's `LogicalPlanBuilder::join_on` produces a join output schema that contains
**both** qualified and unqualified versions of each column. For example, after
`customer LEFT JOIN orders`, the schema has:

- `customer.c_custkey` (qualified)
- `c_custkey` (unqualified alias, added by DataFusion's schema normalization)

Any subsequent operator that calls `aggregate()` or `project()` with a qualified
column reference like `customer.c_custkey` triggers DataFusion's validation error:

```
Schema error: Schema contains qualified field name customer.c_custkey
and unqualified field name c_custkey which would be ambiguous
```

## Why Q13 Specifically

Q13 has the pattern:

```sql
SELECT c_count, count(*) FROM (
  SELECT c_custkey, count(o_orderkey)
  FROM customer LEFT OUTER JOIN orders ON ...
  GROUP BY c_custkey        -- ← inner GROUP BY on join output
) AS c_orders (c_custkey, c_count)
GROUP BY c_count
```

The inner `GROUP BY c_custkey` sits directly above the join. Our `to_df.rs`
emits `customer.c_custkey` (qualified, from `ColumnData.qualifier`) as the
group key. DataFusion's `aggregate()` validates this against the join schema
which has both `customer.c_custkey` and `c_custkey` — ambiguous.

## What Was Tried

1. **Always unqualified refs** — breaks Q7/Q8 where `n1.n_nationkey` and
   `n2.n_nationkey` are genuinely ambiguous (two `nation` aliases).

2. **`fields_with_unqualified_name` count check** — returns 2 for `c_custkey`
   because DataFusion's join schema has both the qualified field and its
   unqualified alias, so the check incorrectly keeps the qualified form.

3. **`qualified_fields_with_unqualified_name` count check** — returns 1 for
   `c_custkey` (only one qualified field), so `normalize_col_ref` strips the
   qualifier. But `aggregate()` still fails because the join schema itself
   has the ambiguity regardless of what we pass as group keys.

4. **Normalization projection after join** — wrapping the join in a `project()`
   to strip qualifiers fails because `project()` itself validates against the
   ambiguous join schema.

## Correct Fix

The fix requires bypassing DataFusion's schema validation for the normalization
step. Options:

**Option A**: Use `project_with_validation(exprs, false)` for a normalization
projection immediately after the join. The `false` flag skips expression
normalization/validation, allowing us to re-project all columns with unqualified
names before any subsequent operator sees the ambiguous schema.

**Option B**: Use `LogicalPlan::Projection` directly (bypassing the builder)
with a manually constructed output schema that has only unqualified fields.

**Option C**: Teach `to_df.rs` to detect when it's building an operator above
a join and always emit unqualified column refs for columns that are unambiguous
(count of qualified fields = 1). This requires threading the input schema
through `convert_expr`.

Option A is the most targeted fix. The blocker is that `project_with_validation`
with `false` skips normalization entirely, which breaks column resolution for
other operators. A more surgical approach would be to use `false` only for the
normalization projection, not for user-facing projections.
