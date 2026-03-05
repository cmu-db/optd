use super::SimplificationPass;
use crate::ir::{
    Column, DataType,
    builder::*,
    catalog::DataSourceId,
    operator::{LogicalGet, LogicalJoin, LogicalProject, LogicalSelect, join::JoinType},
};
use arrow_schema::{DataType as ArrowDataType, Field, Schema};

// Input plan tree:
// LogicalSelect [c3 = 20 AND true]
//   LogicalSelect [c1 = 10]
//     LogicalJoin [Inner, cond = true]
//       MockScan [c1, c2]
//       MockScan [c3, c4]
//
// Output plan tree:
// LogicalJoin [Inner, cond = true]
//   LogicalSelect [c1 = 10]
//     MockScan [c1, c2]
//   LogicalSelect [c3 = 20]
//     MockScan [c3, c4]
#[test]
fn pushes_predicates_to_join_inputs_and_merges_selects() {
    let ctx = crate::ir::IRContext::with_empty_magic();
    let left = ctx.mock_scan(1, vec![1, 2], 100.);
    let right = ctx.mock_scan(2, vec![3, 4], 100.);

    let plan = left
        .logical_join(right, boolean(true), JoinType::Inner)
        .logical_select(column_ref(Column(1)).eq(int32(10)))
        .logical_select(column_ref(Column(3)).eq(int32(20)).and(boolean(true)));

    let simplified = SimplificationPass::new().apply(plan, &ctx);
    let join = simplified.try_borrow::<LogicalJoin>().unwrap();
    assert!(join.join_cond().is_true_scalar());

    let left_filter = join.outer().try_borrow::<LogicalSelect>().unwrap();
    assert!(
        left_filter
            .predicate()
            .used_columns()
            .is_subset(&left_filter.input().output_columns(&ctx))
    );
    let right_filter = join.inner().try_borrow::<LogicalSelect>().unwrap();
    assert!(
        right_filter
            .predicate()
            .used_columns()
            .is_subset(&right_filter.input().output_columns(&ctx))
    );
}

// Input plan tree:
// LogicalSelect [out > 5]
//   LogicalProject [out := alias_left + 1, alias_right]
//     LogicalProject [alias_left := c1, alias_right := c2]
//       MockScan [c1, c2]
//
// Output plan tree:
// LogicalProject [out := c1 + 1, alias_right := c2]
//   LogicalSelect [c1 + 1 > 5]
//     MockScan [c1, c2]
#[test]
fn pushes_filter_through_project_and_merges_projects() {
    let ctx = crate::ir::IRContext::with_empty_magic();
    let scan = ctx.mock_scan(1, vec![1, 2], 100.);
    let alias_left = ctx.define_column(DataType::Int32, None);
    let alias_right = ctx.define_column(DataType::Int32, None);
    let out = ctx.define_column(DataType::Int32, None);

    let plan = scan
        .logical_project([
            column_assign(alias_left, column_ref(Column(1))),
            column_assign(alias_right, column_ref(Column(2))),
        ])
        .logical_project([
            column_assign(out, column_ref(alias_left).plus(int32(1))),
            column_ref(alias_right),
        ])
        .logical_select(column_ref(out).gt(int32(5)));

    let simplified = SimplificationPass::new().apply(plan, &ctx);
    let project = simplified.try_borrow::<LogicalProject>().unwrap();
    let filter = project.input().try_borrow::<LogicalSelect>().unwrap();
    assert!(
        filter
            .input()
            .try_borrow::<crate::ir::operator::MockScan>()
            .is_ok()
    );
    assert!(filter.predicate().used_columns().contains(&Column(1)));
    assert!(!filter.predicate().used_columns().contains(&alias_left));
}

// Input plan tree:
// LogicalProject [c0]
//   LogicalProject [c0, c2]
//     LogicalGet [projections = [0, 1, 2]]
//
// Output plan tree:
// LogicalProject [c0 := c0]
//   LogicalGet [projections = [0]]
#[test]
fn prunes_get_projections_from_required_columns() {
    let ctx = crate::ir::IRContext::with_empty_magic();
    let schema = Schema::new(vec![
        Field::new("a", ArrowDataType::Int32, true),
        Field::new("b", ArrowDataType::Int32, true),
        Field::new("c", ArrowDataType::Int32, true),
    ]);
    let get = ctx.logical_get(DataSourceId(1), &schema, None);
    let mut cols = get.output_columns(&ctx).iter().copied().collect::<Vec<_>>();
    cols.sort_by_key(|col| col.0);

    let plan = get
        .logical_project([column_ref(cols[0]), column_ref(cols[2])])
        .logical_project([column_ref(cols[0])]);

    let simplified = SimplificationPass::new().apply(plan, &ctx);
    let project = simplified.try_borrow::<LogicalProject>().unwrap();
    let get = project.input().try_borrow::<LogicalGet>().unwrap();
    assert_eq!(get.projections().as_ref(), &[0]);
}
