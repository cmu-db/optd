use super::{SimplificationPass, scalar::simplify_scalar_recursively};
use crate::ir::{
    Column, DataType, ScalarValue,
    builder::*,
    convert::IntoOperator,
    operator::{Get, Join, Limit, Project, Select, join::JoinType},
    table_ref::TableRef,
    test_utils::test_ctx_with_tables,
};
use arrow::datatypes::IntervalMonthDayNano;

// Input plan tree:
// LogicalSelect [c3 = 20 AND true]
//   LogicalSelect [c1 = 10]
//     LogicalJoin [Inner, cond = true]
//       LogicalGet [t1.c0, t1.c1]
//       LogicalGet [t2.c0, t2.c1]
//
// Output plan tree:
// LogicalJoin [Inner, cond = true]
//   LogicalSelect [c1 = 10]
//     LogicalGet [t1.c0, t1.c1]
//   LogicalSelect [c3 = 20]
//     LogicalGet [t2.c0, t2.c1]
#[test]
fn pushes_predicates_to_join_inputs_and_merges_selects() {
    let ctx = test_ctx_with_tables(&[("t1", 2), ("t2", 2)]).unwrap();
    let left = ctx.logical_get(TableRef::bare("t1"), None).unwrap().build();
    let right = ctx.logical_get(TableRef::bare("t2"), None).unwrap().build();
    let t1_c0 = ctx.col(Some(&TableRef::bare("t1")), "c0").unwrap();
    let t2_c0 = ctx.col(Some(&TableRef::bare("t2")), "c0").unwrap();

    let plan = left
        .with_ctx(&ctx)
        .logical_join(right, boolean(true), JoinType::Inner)
        .select(column_ref(t1_c0).eq(int32(10)))
        .select(column_ref(t2_c0).eq(int32(20)).and(boolean(true)))
        .build();

    let simplified = SimplificationPass::new().apply(plan, &ctx).unwrap();
    let join = simplified.try_borrow::<Join>().unwrap();
    assert!(join.join_cond().is_true_scalar());

    let left_filter = join.outer().try_borrow::<Select>().unwrap();
    assert!(
        left_filter
            .predicate()
            .used_columns()
            .is_subset(left_filter.input().output_columns(&ctx).unwrap().as_ref())
    );
    let right_filter = join.inner().try_borrow::<Select>().unwrap();
    assert!(
        right_filter
            .predicate()
            .used_columns()
            .is_subset(right_filter.input().output_columns(&ctx).unwrap().as_ref())
    );
}

fn assert_pushes_filter_to_outer_for_semi_or_anti(join_type: JoinType) {
    let ctx = test_ctx_with_tables(&[("t1", 2), ("t2", 2)]).unwrap();
    let left = ctx.logical_get(TableRef::bare("t1"), None).unwrap().build();
    let right = ctx.logical_get(TableRef::bare("t2"), None).unwrap().build();
    let t1_c0 = ctx.col(Some(&TableRef::bare("t1")), "c0").unwrap();

    let plan = left
        .with_ctx(&ctx)
        .logical_join(right, boolean(true), join_type)
        .select(column_ref(t1_c0).eq(int32(10)))
        .build();

    let simplified = SimplificationPass::new().apply(plan, &ctx).unwrap();
    let join = simplified.try_borrow::<Join>().unwrap();
    assert_eq!(join.join_type(), &join_type);
    assert!(join.join_cond().is_true_scalar());
    assert!(join.inner().try_borrow::<Select>().is_err());

    let outer_filter = join.outer().try_borrow::<Select>().unwrap();
    assert!(
        outer_filter
            .predicate()
            .used_columns()
            .is_subset(outer_filter.input().output_columns(&ctx).unwrap().as_ref())
    );
}

#[test]
fn pushes_predicates_to_outer_input_for_left_semi_join() {
    assert_pushes_filter_to_outer_for_semi_or_anti(JoinType::LeftSemi);
}

#[test]
fn pushes_predicates_to_outer_input_for_left_anti_join() {
    assert_pushes_filter_to_outer_for_semi_or_anti(JoinType::LeftAnti);
}

// Input plan tree:
// LogicalSelect [out > 5]
//   LogicalProject [out := alias_left + 1, alias_right]
//     LogicalProject [alias_left := c1, alias_right := c2]
//       LogicalGet [t1.c0, t1.c1]
//
// Output plan tree:
// LogicalProject [out := c1 + 1, alias_right := c2]
//   LogicalSelect [c1 + 1 > 5]
//     LogicalGet [t1.c0, t1.c1]
#[test]
fn pushes_filter_through_project_and_merges_projects() {
    let ctx = test_ctx_with_tables(&[("t1", 2)]).unwrap();
    let get = ctx.logical_get(TableRef::bare("t1"), None).unwrap().build();
    let get_table_index = *get.borrow::<Get>().table_index();

    let first_project = ctx
        .project(
            get,
            [
                column_ref(Column(get_table_index, 0)),
                column_ref(Column(get_table_index, 1)),
            ],
        )
        .unwrap()
        .build();
    let first_table_index = *first_project.borrow::<Project>().table_index();
    let alias_left = Column(first_table_index, 0);
    let alias_right = Column(first_table_index, 1);

    let second_project = ctx
        .project(
            first_project,
            [
                column_ref(alias_left).plus(int32(1)),
                column_ref(alias_right),
            ],
        )
        .unwrap()
        .build();
    let second_table_index = *second_project.borrow::<Project>().table_index();
    let out = Column(second_table_index, 0);

    let plan = second_project
        .with_ctx(&ctx)
        .select(column_ref(out).gt(int32(5)))
        .build();

    let simplified = SimplificationPass::new().apply(plan, &ctx).unwrap();
    let project = simplified.try_borrow::<Project>().unwrap();
    let filter = project.input().try_borrow::<Select>().unwrap();
    let get = filter.input().try_borrow::<Get>().unwrap();
    assert!(
        filter
            .predicate()
            .used_columns()
            .contains(&Column(*get.table_index(), 0))
    );
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
    let ctx = test_ctx_with_tables(&[("t1", 3)]).unwrap();
    let get = ctx.logical_get(TableRef::bare("t1"), None).unwrap().build();
    let get_table_index = *get.borrow::<Get>().table_index();

    let first_project = ctx
        .project(
            get,
            [
                column_ref(Column(get_table_index, 0)),
                column_ref(Column(get_table_index, 2)),
            ],
        )
        .unwrap()
        .build();
    let first_project_index = *first_project.borrow::<Project>().table_index();

    let plan = ctx
        .project(first_project, [column_ref(Column(first_project_index, 0))])
        .unwrap()
        .build();

    let simplified = SimplificationPass::new().apply(plan, &ctx).unwrap();
    let project = simplified.try_borrow::<Project>().unwrap();
    let get = project.input().try_borrow::<Get>().unwrap();
    assert_eq!(get.projections().as_ref(), &[0]);
}

#[test]
fn folds_literal_casts() {
    let expected =
        literal(ScalarValue::try_from_string("1993-07-01".to_string(), &DataType::Date32).unwrap());
    let scalar = cast(utf8(Some("1993-07-01")), DataType::Date32);

    assert_eq!(simplify_scalar_recursively(scalar), expected);
}

#[test]
fn collapses_nested_casts_with_same_target_type() {
    let scalar = cast(cast(int64(Some(7)), DataType::Int64), DataType::Int64);

    assert_eq!(simplify_scalar_recursively(scalar), int64(Some(7)));
}

#[test]
fn folds_date32_plus_interval_month_day_nano_literals() {
    let scalar = cast(utf8(Some("1993-07-01")), DataType::Date32).plus(literal(
        ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNano::new(3, 0, 0))),
    ));
    let expected =
        literal(ScalarValue::try_from_string("1993-10-01".to_string(), &DataType::Date32).unwrap());

    assert_eq!(simplify_scalar_recursively(scalar), expected);
}

#[test]
fn pushes_limit_through_project() {
    let ctx = test_ctx_with_tables(&[("t1", 2)]).unwrap();
    let get = ctx.logical_get(TableRef::bare("t1"), None).unwrap().build();
    let get_table_index = *get.borrow::<Get>().table_index();

    let project = ctx
        .project(get, [column_ref(Column(get_table_index, 0))])
        .unwrap()
        .build();
    let project_table_index = *project.borrow::<Project>().table_index();

    let plan = Limit::new(project, int64(1), int64(2)).into_operator();
    let simplified = SimplificationPass::new().apply(plan, &ctx).unwrap();

    let project = simplified.try_borrow::<Project>().unwrap();
    assert_eq!(project.table_index(), &project_table_index);
    let limit = project.input().try_borrow::<Limit>().unwrap();
    assert_eq!(limit.skip(), &int64(1));
    assert_eq!(limit.fetch(), &int64(2));
    assert!(limit.input().try_borrow::<Get>().is_ok());
}

#[test]
fn pushes_inner_join_condition_conjuncts_to_inputs() {
    let ctx = test_ctx_with_tables(&[("t1", 2), ("t2", 2)]).unwrap();
    let left = ctx.logical_get(TableRef::bare("t1"), None).unwrap().build();
    let right = ctx.logical_get(TableRef::bare("t2"), None).unwrap().build();
    let t1_c0 = ctx.col(Some(&TableRef::bare("t1")), "c0").unwrap();
    let t1_c1 = ctx.col(Some(&TableRef::bare("t1")), "c1").unwrap();
    let t2_c0 = ctx.col(Some(&TableRef::bare("t2")), "c0").unwrap();
    let t2_c1 = ctx.col(Some(&TableRef::bare("t2")), "c1").unwrap();

    let join_cond = column_ref(t1_c0)
        .eq(column_ref(t2_c0))
        .and(column_ref(t1_c1).gt(int32(5)))
        .and(column_ref(t2_c1).gt(int32(10)));

    let plan = left
        .with_ctx(&ctx)
        .logical_join(right, join_cond, JoinType::Inner)
        .build();

    let simplified = SimplificationPass::new().apply(plan, &ctx).unwrap();
    let join = simplified.try_borrow::<Join>().unwrap();
    let outer_filter = join.outer().try_borrow::<Select>().unwrap();
    let inner_filter = join.inner().try_borrow::<Select>().unwrap();

    assert!(outer_filter.predicate().used_columns().contains(&t1_c1));
    assert!(inner_filter.predicate().used_columns().contains(&t2_c1));
    assert!(join.join_cond().used_columns().contains(&t1_c0));
    assert!(join.join_cond().used_columns().contains(&t2_c0));
    assert!(!join.join_cond().used_columns().contains(&t1_c1));
    assert!(!join.join_cond().used_columns().contains(&t2_c1));
}

#[test]
fn factors_or_join_condition_to_enable_pushdown() {
    let ctx = test_ctx_with_tables(&[("t1", 2), ("t2", 2)]).unwrap();
    let left = ctx.logical_get(TableRef::bare("t1"), None).unwrap().build();
    let right = ctx.logical_get(TableRef::bare("t2"), None).unwrap().build();
    let t1_c0 = ctx.col(Some(&TableRef::bare("t1")), "c0").unwrap();
    let t1_c1 = ctx.col(Some(&TableRef::bare("t1")), "c1").unwrap();
    let t2_c0 = ctx.col(Some(&TableRef::bare("t2")), "c0").unwrap();
    let t2_c1 = ctx.col(Some(&TableRef::bare("t2")), "c1").unwrap();

    let common = column_ref(t1_c0)
        .eq(column_ref(t2_c0))
        .and(column_ref(t1_c1).gt(int32(5)))
        .and(column_ref(t2_c1).lt(int32(30)));
    let join_cond = common
        .clone()
        .and(column_ref(t2_c1).gt(int32(10)))
        .or(common.and(column_ref(t2_c1).gt(int32(20))));

    let plan = left
        .with_ctx(&ctx)
        .logical_join(right, join_cond, JoinType::Inner)
        .build();

    let simplified = SimplificationPass::new().apply(plan, &ctx).unwrap();
    let join = simplified.try_borrow::<Join>().unwrap();
    let outer_filter = join.outer().try_borrow::<Select>().unwrap();
    let inner_filter = join.inner().try_borrow::<Select>().unwrap();

    assert!(outer_filter.predicate().used_columns().contains(&t1_c1));
    assert!(inner_filter.predicate().used_columns().contains(&t2_c1));
    assert!(join.join_cond().used_columns().contains(&t1_c0));
    assert!(join.join_cond().used_columns().contains(&t2_c0));
    assert!(!join.join_cond().used_columns().contains(&t1_c1));
}
