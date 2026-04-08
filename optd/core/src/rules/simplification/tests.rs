use super::SimplificationPass;
use crate::ir::{
    Column,
    builder::*,
    operator::{Get, Join, Project, Select, join::JoinType},
    table_ref::TableRef,
    test_utils::test_ctx_with_tables,
};

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
