use std::ops::ControlFlow;

use itertools::Itertools;
use optd_core::{
    cascades::{CascadesOptimizer, RelNodeContext},
    cost::Cost,
};
use serde::{de::DeserializeOwned, Serialize};
use union_find::{disjoint_sets::DisjointSets, union_find::UnionFind};

use crate::{
    cost::base_cost::{
        stats::{Distribution, MostCommonValues},
        DEFAULT_NUM_DISTINCT,
    },
    plan_nodes::{
        BinOpType, ColumnRefExpr, Expr, ExprList, JoinType, LogOpExpr, LogOpType, OptRelNode,
        OptRelNodeRef, OptRelNodeTyp,
    },
    properties::column_ref::{
        BaseTableColumnRef, ColumnRef, ColumnRefPropertyBuilder, EqBaseTableColumnSets,
        EqPredicate, GroupColumnRefs, SemanticCorrelation,
    },
};

use super::{OptCostModel, DEFAULT_UNK_SEL};

impl<
        M: MostCommonValues + Serialize + DeserializeOwned,
        D: Distribution + Serialize + DeserializeOwned,
    > OptCostModel<M, D>
{
    pub(super) fn get_nlj_cost(
        &self,
        join_typ: JoinType,
        children: &[Cost],
        context: Option<RelNodeContext>,
        optimizer: Option<&CascadesOptimizer<OptRelNodeTyp>>,
    ) -> Cost {
        let (row_cnt_1, _, _) = Self::cost_tuple(&children[0]);
        let (row_cnt_2, _, _) = Self::cost_tuple(&children[1]);
        let (_, compute_cost, _) = Self::cost_tuple(&children[2]);
        let selectivity = if let (Some(context), Some(optimizer)) = (context, optimizer) {
            let column_refs =
                optimizer.get_property_by_group::<ColumnRefPropertyBuilder>(context.group_id, 1);
            let expr_group_id = context.children_group_ids[2];
            let expr_trees = optimizer.get_all_group_bindings(expr_group_id, false);
            // there may be more than one expression tree in a group.
            // see comment in OptRelNodeTyp::PhysicalFilter(_) for more information
            let expr_tree = expr_trees.first().expect("expression missing");
            self.get_join_selectivity_from_expr_tree(
                join_typ,
                expr_tree.clone(),
                &column_refs,
                row_cnt_1,
                row_cnt_2,
            )
        } else {
            DEFAULT_UNK_SEL
        };
        Self::cost(
            (row_cnt_1 * row_cnt_2 * selectivity).max(1.0),
            row_cnt_1 * row_cnt_2 * compute_cost + row_cnt_1,
            0.0,
        )
    }

    pub(super) fn get_hash_join_cost(
        &self,
        join_typ: JoinType,
        children: &[Cost],
        context: Option<RelNodeContext>,
        optimizer: Option<&CascadesOptimizer<OptRelNodeTyp>>,
    ) -> Cost {
        let (row_cnt_1, _, _) = Self::cost_tuple(&children[0]);
        let (row_cnt_2, _, _) = Self::cost_tuple(&children[1]);
        let selectivity = if let (Some(context), Some(optimizer)) = (context, optimizer) {
            let column_refs =
                optimizer.get_property_by_group::<ColumnRefPropertyBuilder>(context.group_id, 1);
            let left_keys_group_id = context.children_group_ids[2];
            let right_keys_group_id = context.children_group_ids[3];
            let left_col_cnt = optimizer
                .get_property_by_group::<ColumnRefPropertyBuilder>(context.children_group_ids[0], 1)
                .column_refs()
                .len();
            let left_keys_list = optimizer.get_all_group_bindings(left_keys_group_id, false);
            let right_keys_list = optimizer.get_all_group_bindings(right_keys_group_id, false);
            // there may be more than one expression tree in a group.
            // see comment in OptRelNodeTyp::PhysicalFilter(_) for more information
            let left_keys = left_keys_list.first().expect("left keys missing");
            let right_keys = right_keys_list.first().expect("right keys missing");
            self.get_join_selectivity_from_keys(
                join_typ,
                ExprList::from_rel_node(left_keys.clone())
                    .expect("left_keys should be an ExprList"),
                ExprList::from_rel_node(right_keys.clone())
                    .expect("right_keys should be an ExprList"),
                &column_refs,
                row_cnt_1,
                row_cnt_2,
                left_col_cnt,
            )
        } else {
            DEFAULT_UNK_SEL
        };
        Self::cost(
            (row_cnt_1 * row_cnt_2 * selectivity).max(1.0),
            row_cnt_1 * 2.0 + row_cnt_2,
            0.0,
        )
    }

    /// A wrapper to convert the join keys to the format expected by get_join_selectivity_core()
    #[allow(clippy::too_many_arguments)]
    fn get_join_selectivity_from_keys(
        &self,
        join_typ: JoinType,
        left_keys: ExprList,
        right_keys: ExprList,
        column_refs: &GroupColumnRefs,
        left_row_cnt: f64,
        right_row_cnt: f64,
        left_col_cnt: usize,
    ) -> f64 {
        assert!(left_keys.len() == right_keys.len());
        // I assume that the keys are already in the right order
        // s.t. the ith key of left_keys corresponds with the ith key of right_keys
        let on_col_ref_pairs = left_keys
            .to_vec()
            .into_iter()
            .zip(right_keys.to_vec())
            .map(|(left_key, right_key)| {
                (
                    ColumnRefExpr::from_rel_node(left_key.into_rel_node())
                        .expect("keys should be ColumnRefExprs"),
                    ColumnRefExpr::from_rel_node(right_key.into_rel_node())
                        .expect("keys should be ColumnRefExprs"),
                )
            })
            .collect_vec();
        self.get_join_selectivity_core(
            join_typ,
            on_col_ref_pairs,
            None,
            column_refs,
            left_row_cnt,
            right_row_cnt,
            left_col_cnt,
        )
    }

    /// The core logic of join selectivity which assumes we've already separated the expression
    /// into the on conditions and the filters.
    ///
    /// Hash join and NLJ reference right table columns differently, hence the
    /// `right_col_ref_offset` parameter.
    ///
    /// For hash join, the right table columns indices are with respect to the right table,
    /// which means #0 is the first column of the right table.
    ///
    /// For NLJ, the right table columns indices are with respect to the output of the join.
    /// For example, if the left table has 3 columns, the first column of the right table
    /// is #3 instead of #0.
    #[allow(clippy::too_many_arguments)]
    fn get_join_selectivity_core(
        &self,
        join_typ: JoinType,
        on_col_ref_pairs: Vec<(ColumnRefExpr, ColumnRefExpr)>,
        filter_expr_tree: Option<OptRelNodeRef>,
        column_refs: &GroupColumnRefs,
        left_row_cnt: f64,
        right_row_cnt: f64,
        right_col_ref_offset: usize,
    ) -> f64 {
        let join_on_selectivity =
            self.get_join_on_selectivity(&on_col_ref_pairs, column_refs, right_col_ref_offset);
        // Currently, there is no difference in how we handle a join filter and a select filter,
        // so we use the same function.
        //
        // One difference (that we *don't* care about right now) is that join filters can contain expressions from multiple
        // different tables. Currently, this doesn't affect the get_filter_selectivity() function, but this may change in
        // the future.
        let join_filter_selectivity = match filter_expr_tree {
            Some(filter_expr_tree) => self.get_filter_selectivity(filter_expr_tree, column_refs),
            None => 1.0,
        };
        let inner_join_selectivity = join_on_selectivity * join_filter_selectivity;
        match join_typ {
            JoinType::Inner => inner_join_selectivity,
            JoinType::LeftOuter => f64::max(inner_join_selectivity, 1.0 / right_row_cnt),
            JoinType::RightOuter => f64::max(inner_join_selectivity, 1.0 / left_row_cnt),
            JoinType::Cross => {
                assert!(
                    on_col_ref_pairs.is_empty(),
                    "Cross joins should not have on columns"
                );
                join_filter_selectivity
            }
            _ => unimplemented!("join_typ={} is not implemented", join_typ),
        }
    }

    /// The expr_tree input must be a "mixed expression tree", just like with `get_filter_selectivity`.
    ///
    /// This is a "wrapper" to separate the equality conditions from the filter conditions before
    /// calling the "main" `get_join_selectivity_core` function.
    fn get_join_selectivity_from_expr_tree(
        &self,
        join_typ: JoinType,
        expr_tree: OptRelNodeRef,
        column_refs: &GroupColumnRefs,
        left_row_cnt: f64,
        right_row_cnt: f64,
    ) -> f64 {
        assert!(expr_tree.typ.is_expression());
        if expr_tree.typ == OptRelNodeTyp::LogOp(LogOpType::And) {
            let mut on_col_ref_pairs = vec![];
            let mut filter_expr_trees = vec![];
            for child_expr_tree in &expr_tree.children {
                if let Some(on_col_ref_pair) =
                    Self::get_on_col_ref_pair(child_expr_tree.clone(), column_refs)
                {
                    on_col_ref_pairs.push(on_col_ref_pair)
                } else {
                    let child_expr = Expr::from_rel_node(child_expr_tree.clone()).expect(
                        "everything that is a direct child of an And node must be an expression",
                    );
                    filter_expr_trees.push(child_expr);
                }
            }
            assert!(on_col_ref_pairs.len() + filter_expr_trees.len() == expr_tree.children.len());
            let filter_expr_tree = if filter_expr_trees.is_empty() {
                None
            } else {
                Some(
                    LogOpExpr::new(LogOpType::And, ExprList::new(filter_expr_trees))
                        .into_rel_node(),
                )
            };
            self.get_join_selectivity_core(
                join_typ,
                on_col_ref_pairs,
                filter_expr_tree,
                column_refs,
                left_row_cnt,
                right_row_cnt,
                0,
            )
        } else {
            #[allow(clippy::collapsible_else_if)]
            if let Some(on_col_ref_pair) = Self::get_on_col_ref_pair(expr_tree.clone(), column_refs)
            {
                self.get_join_selectivity_core(
                    join_typ,
                    vec![on_col_ref_pair],
                    None,
                    column_refs,
                    left_row_cnt,
                    right_row_cnt,
                    0,
                )
            } else {
                self.get_join_selectivity_core(
                    join_typ,
                    vec![],
                    Some(expr_tree),
                    column_refs,
                    left_row_cnt,
                    right_row_cnt,
                    0,
                )
            }
        }
    }

    /// Check if an expr_tree is a join condition, returning the join on col ref pair if it is.
    /// The reason the check and the info are in the same function is because their code is almost identical.
    /// It only picks out equality conditions between two column refs on different tables
    fn get_on_col_ref_pair(
        expr_tree: OptRelNodeRef,
        column_refs: &GroupColumnRefs,
    ) -> Option<(ColumnRefExpr, ColumnRefExpr)> {
        // 1. Check that it's equality
        if expr_tree.typ == OptRelNodeTyp::BinOp(BinOpType::Eq) {
            let left_child = expr_tree.child(0);
            let right_child = expr_tree.child(1);
            // 2. Check that both sides are column refs
            if left_child.typ == OptRelNodeTyp::ColumnRef
                && right_child.typ == OptRelNodeTyp::ColumnRef
            {
                // 3. Check that both sides don't belong to the same table (if we don't know, that means they don't belong)
                let left_col_ref_expr = ColumnRefExpr::from_rel_node(left_child)
                    .expect("we already checked that the type is ColumnRef");
                let right_col_ref_expr = ColumnRefExpr::from_rel_node(right_child)
                    .expect("we already checked that the type is ColumnRef");
                let left_col_ref = &column_refs[left_col_ref_expr.index()];
                let right_col_ref = &column_refs[right_col_ref_expr.index()];
                let is_same_table = if let (
                    ColumnRef::BaseTableColumnRef(BaseTableColumnRef {
                        table: left_table, ..
                    }),
                    ColumnRef::BaseTableColumnRef(BaseTableColumnRef {
                        table: right_table, ..
                    }),
                ) = (left_col_ref, right_col_ref)
                {
                    left_table == right_table
                } else {
                    false
                };
                if !is_same_table {
                    Some((left_col_ref_expr, right_col_ref_expr))
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Get the selectivity of one column eq predicate, e.g. colA = colB.
    fn get_join_selectivity_from_on_col_ref_pair(
        &self,
        left: &ColumnRef,
        right: &ColumnRef,
    ) -> f64 {
        // the formula for each pair is min(1 / ndistinct1, 1 / ndistinct2)
        // (see https://postgrespro.com/blog/pgsql/5969618)
        let ndistincts = vec![left, right].into_iter().map(|col_ref| {
            match self.get_single_column_stats_from_col_ref(col_ref) {
                Some(per_col_stats) => per_col_stats.ndistinct,
                None => DEFAULT_NUM_DISTINCT,
            }
        });
        // using reduce(f64::min) is the idiomatic workaround to min() because
        // f64 does not implement Ord due to NaN
        let selectivity = ndistincts.map(|ndistinct| 1.0 / ndistinct as f64).reduce(f64::min).expect("reduce() only returns None if the iterator is empty, which is impossible since col_ref_exprs.len() == 2");
        assert!(
            !selectivity.is_nan(),
            "it should be impossible for selectivity to be NaN since n-distinct is never 0"
        );
        selectivity
    }

    /// Given a set of equality predicates P that define N equal columns, find the selectivity of
    /// the most selective N - 1 predicates that "touches" all the columns.
    ///
    /// We solve the problem using MST (Minimum Spanning Tree), where the columns are nodes and the
    /// predicates are undirected edges. Since all the columns are equal, the graph is connected.
    fn get_join_selecitivity_from_most_selective_predicates(
        &self,
        predicates: Vec<EqPredicate>,
        num_cols: usize,
    ) -> f64 {
        let mut acc_sel = 1.0;
        let mut num_picked_predicates = 0;
        let mut disjoint_sets = DisjointSets::new();

        // Use Kruskal to compute MST.
        // Step 1: sort predicates by selectivity in ascending order.
        let mut sorted_predicates = predicates
            .into_iter()
            .map(|p| {
                let sel: f64 = self.get_join_selectivity_from_on_col_ref_pair(
                    &p.left.clone().into(),
                    &p.right.clone().into(),
                );
                (p, sel)
            })
            .sorted_by(|(_, sel1), (_, sel2)| sel1.partial_cmp(sel2).unwrap());

        // Step 2: pick predicates until all columns are "connected" by the predicates.
        sorted_predicates.try_for_each(|(p, sel)| {
            if !disjoint_sets.contains(&p.left) {
                disjoint_sets.make_set(p.left.clone()).unwrap();
            }
            if !disjoint_sets.contains(&p.right) {
                disjoint_sets.make_set(p.right.clone()).unwrap();
            }
            if !disjoint_sets.same_set(&p.left, &p.right).unwrap() {
                acc_sel *= sel;
                num_picked_predicates += 1;
                disjoint_sets.union(&p.left, &p.right).unwrap();
            }
            if num_picked_predicates == num_cols - 1 {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        });
        debug_assert_eq!(
            num_picked_predicates,
            num_cols - 1,
            "we should have picked N - 1 predicates"
        );
        debug_assert_eq!(
            disjoint_sets.num_sets(),
            1,
            "all columns should be connected by the predicates"
        );
        debug_assert_eq!(
            disjoint_sets.num_items(),
            num_cols,
            "all columns should be connected by the predicates"
        );
        acc_sel
    }

    /// A predicate set contains "redundant" predicates if some of them can be expressed with the rest.
    /// E.g. In { A = B, B = C, A = C }, one of the predicates is redundant.
    /// In this case, we want to pick the most selective predicates that touch all the columns
    /// that this set of predicates touches.
    ///
    /// If we have N columns that are equal, and the set of equality predicates P that defines the
    /// equalities (|P| >= N - 1), we pick the N - 1 most selective predicates (denoted P') that
    /// define the equalities by computing the MST of the graph where the columns are nodes and the
    /// predicates are edges (see `get_join_selecitivity_from_most_selective_predicates` for
    /// implementation).
    ///
    /// But since child has already picked some predicates which might not be the most selective
    /// (because it has not seen the most selective ones), when we encounter a potentially more
    /// selective `predicate` (in the parameter) and a set of previously seen predicates
    /// `past_eq_columns`, `predicate` produces a selectivity adjustment factor, which is the
    /// multiplied selectivity of the most selective N - 1 predicate among `past_eq_columns` union
    /// `predicate` divided by the selectivity of the `past_eq_columns`.
    ///
    /// NOTE: This function modifies `past_eq_columns` by adding `predicate` to it.
    fn get_join_selectivity_adjustment_from_redundant_predicates(
        &self,
        predicate: EqPredicate,
        past_eq_columns: &mut EqBaseTableColumnSets,
    ) -> f64 {
        let left = predicate.left.clone();
        // Compute the selectivity of the most selective N - 1 predicates.
        let children_pred_sel = {
            let predicates = past_eq_columns.find_predicates_for_eq_column_set(&left);
            self.get_join_selecitivity_from_most_selective_predicates(
                predicates,
                past_eq_columns.num_eq_columns(&left),
            )
        };
        // Add predicate to past_eq_columns.
        past_eq_columns.add_predicate(predicate);
        // Repeat the same process with the new predicate.
        let new_pred_sel = {
            let predicates = past_eq_columns.find_predicates_for_eq_column_set(&left);
            self.get_join_selecitivity_from_most_selective_predicates(
                predicates,
                past_eq_columns.num_eq_columns(&left),
            )
        };

        // Compute division of MSTs as the selectivity.
        new_pred_sel / children_pred_sel
    }

    /// Get the selectivity of the on conditions.
    ///
    /// Note that the selectivity of the on conditions does not depend on join type.
    /// Join type is accounted for separately in get_join_selectivity_core().
    ///
    /// We also check if each predicate is correlated with any of the previous predicates.
    ///
    /// More specifically, we are checking if the predicate can be expressed with other existing predicates.
    /// E.g. if we have a predicate like A = B and B = C is equivalent to A = C.
    //
    /// However, we don't just throw away A = C, because we want to pick the most selective predicates.
    /// For details on how we do this, see `get_join_selectivity_from_redundant_predicates`.
    fn get_join_on_selectivity(
        &self,
        on_col_ref_pairs: &[(ColumnRefExpr, ColumnRefExpr)],
        column_refs: &GroupColumnRefs,
        right_col_ref_offset: usize,
    ) -> f64 {
        let mut past_eq_columns = column_refs
            .input_correlation()
            .map(SemanticCorrelation::eq_base_table_columns)
            .cloned()
            .unwrap_or_default();

        // multiply the selectivities of all individual conditions together
        on_col_ref_pairs
            .iter()
            .map(|on_col_ref_pair| {
                let left_col_ref = &column_refs[on_col_ref_pair.0.index()];
                let right_col_ref = &column_refs[on_col_ref_pair.1.index() + right_col_ref_offset];

                if let (ColumnRef::BaseTableColumnRef(left), ColumnRef::BaseTableColumnRef(right)) =
                    (left_col_ref, right_col_ref)
                {
                    let predicate = EqPredicate::new(left.clone(), right.clone());
                    if past_eq_columns.is_eq(left, right) {
                        return self.get_join_selectivity_adjustment_from_redundant_predicates(
                            predicate,
                            &mut past_eq_columns,
                        );
                    } else {
                        past_eq_columns.add_predicate(predicate);
                    }
                }

                self.get_join_selectivity_from_on_col_ref_pair(left_col_ref, right_col_ref)
            })
            .product()
    }
}

#[cfg(test)]
mod tests {
    use optd_core::rel_node::Value;

    use crate::{
        cost::base_cost::{tests::*, DEFAULT_EQ_SEL},
        plan_nodes::{BinOpType, JoinType, LogOpType, OptRelNodeRef},
        properties::column_ref::{
            BaseTableColumnRef, ColumnRef, EqBaseTableColumnSets, EqPredicate, GroupColumnRefs,
            SemanticCorrelation,
        },
    };

    /// A wrapper around get_join_selectivity_from_expr_tree that extracts the
    /// table row counts from the cost model.
    fn test_get_join_selectivity(
        cost_model: &TestOptCostModel,
        reverse_tables: bool,
        join_typ: JoinType,
        expr_tree: OptRelNodeRef,
        column_refs: &GroupColumnRefs,
    ) -> f64 {
        let table1_row_cnt = cost_model.per_table_stats_map[TABLE1_NAME].row_cnt as f64;
        let table2_row_cnt = cost_model.per_table_stats_map[TABLE2_NAME].row_cnt as f64;
        if !reverse_tables {
            cost_model.get_join_selectivity_from_expr_tree(
                join_typ,
                expr_tree,
                column_refs,
                table1_row_cnt,
                table2_row_cnt,
            )
        } else {
            cost_model.get_join_selectivity_from_expr_tree(
                join_typ,
                expr_tree,
                column_refs,
                table2_row_cnt,
                table1_row_cnt,
            )
        }
    }

    #[test]
    fn test_inner_const() {
        let cost_model = create_one_column_cost_model(get_empty_per_col_stats());
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_join_selectivity_from_expr_tree(
                JoinType::Inner,
                cnst(Value::Bool(true)),
                &GroupColumnRefs::new_test(vec![], None),
                f64::NAN,
                f64::NAN
            ),
            1.0
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_join_selectivity_from_expr_tree(
                JoinType::Inner,
                cnst(Value::Bool(false)),
                &GroupColumnRefs::new_test(vec![], None),
                f64::NAN,
                f64::NAN
            ),
            0.0
        );
    }

    #[test]
    fn test_inner_oncond() {
        let cost_model = create_two_table_cost_model(
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                5,
                0.0,
                Some(TestDistribution::empty()),
            ),
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                4,
                0.0,
                Some(TestDistribution::empty()),
            ),
        );
        let expr_tree = bin_op(BinOpType::Eq, col_ref(0), col_ref(1));
        let expr_tree_rev = bin_op(BinOpType::Eq, col_ref(1), col_ref(0));
        let column_refs = GroupColumnRefs::new_test(
            vec![
                ColumnRef::base_table_column_ref(String::from(TABLE1_NAME), 0),
                ColumnRef::base_table_column_ref(String::from(TABLE2_NAME), 0),
            ],
            None,
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(&cost_model, false, JoinType::Inner, expr_tree, &column_refs),
            0.2
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                &cost_model,
                false,
                JoinType::Inner,
                expr_tree_rev,
                &column_refs,
            ),
            0.2
        );
    }

    #[test]
    fn test_inner_and_of_onconds() {
        let cost_model = create_two_table_cost_model(
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                5,
                0.0,
                Some(TestDistribution::empty()),
            ),
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                4,
                0.0,
                Some(TestDistribution::empty()),
            ),
        );
        let eq0and1 = bin_op(BinOpType::Eq, col_ref(0), col_ref(1));
        let eq1and0 = bin_op(BinOpType::Eq, col_ref(1), col_ref(0));
        let expr_tree = log_op(LogOpType::And, vec![eq0and1.clone(), eq1and0.clone()]);
        let expr_tree_rev = log_op(LogOpType::And, vec![eq1and0.clone(), eq0and1.clone()]);
        let column_refs = GroupColumnRefs::new_test(
            vec![
                ColumnRef::base_table_column_ref(String::from(TABLE1_NAME), 0),
                ColumnRef::base_table_column_ref(String::from(TABLE2_NAME), 0),
            ],
            None,
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(&cost_model, false, JoinType::Inner, expr_tree, &column_refs),
            0.2
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                &cost_model,
                false,
                JoinType::Inner,
                expr_tree_rev,
                &column_refs
            ),
            0.2
        );
    }

    #[test]
    fn test_inner_and_of_oncond_and_filter() {
        let cost_model = create_two_table_cost_model(
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                5,
                0.0,
                Some(TestDistribution::empty()),
            ),
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                4,
                0.0,
                Some(TestDistribution::empty()),
            ),
        );
        let eq0and1 = bin_op(BinOpType::Eq, col_ref(0), col_ref(1));
        let eq100 = bin_op(BinOpType::Eq, col_ref(1), cnst(Value::Int32(100)));
        let expr_tree = log_op(LogOpType::And, vec![eq0and1.clone(), eq100.clone()]);
        let expr_tree_rev = log_op(LogOpType::And, vec![eq100.clone(), eq0and1.clone()]);
        let column_refs = GroupColumnRefs::new_test(
            vec![
                ColumnRef::base_table_column_ref(String::from(TABLE1_NAME), 0),
                ColumnRef::base_table_column_ref(String::from(TABLE2_NAME), 0),
            ],
            None,
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(&cost_model, false, JoinType::Inner, expr_tree, &column_refs),
            0.05
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                &cost_model,
                false,
                JoinType::Inner,
                expr_tree_rev,
                &column_refs
            ),
            0.05
        );
    }

    #[test]
    fn test_inner_and_of_filters() {
        let cost_model = create_two_table_cost_model(
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                5,
                0.0,
                Some(TestDistribution::empty()),
            ),
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                4,
                0.0,
                Some(TestDistribution::empty()),
            ),
        );
        let neq12 = bin_op(BinOpType::Neq, col_ref(0), cnst(Value::Int32(12)));
        let eq100 = bin_op(BinOpType::Eq, col_ref(1), cnst(Value::Int32(100)));
        let expr_tree = log_op(LogOpType::And, vec![neq12.clone(), eq100.clone()]);
        let expr_tree_rev = log_op(LogOpType::And, vec![eq100.clone(), neq12.clone()]);
        let column_refs = GroupColumnRefs::new_test(
            vec![
                ColumnRef::base_table_column_ref(String::from(TABLE1_NAME), 0),
                ColumnRef::base_table_column_ref(String::from(TABLE2_NAME), 0),
            ],
            None,
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(&cost_model, false, JoinType::Inner, expr_tree, &column_refs),
            0.2
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                &cost_model,
                false,
                JoinType::Inner,
                expr_tree_rev,
                &column_refs
            ),
            0.2
        );
    }

    #[test]
    fn test_inner_colref_eq_colref_same_table_is_not_oncond() {
        let cost_model = create_two_table_cost_model(
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                5,
                0.0,
                Some(TestDistribution::empty()),
            ),
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                4,
                0.0,
                Some(TestDistribution::empty()),
            ),
        );
        let expr_tree = bin_op(BinOpType::Eq, col_ref(0), col_ref(0));
        let column_refs = GroupColumnRefs::new_test(
            vec![
                ColumnRef::base_table_column_ref(String::from(TABLE1_NAME), 0),
                ColumnRef::base_table_column_ref(String::from(TABLE2_NAME), 0),
            ],
            None,
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(&cost_model, false, JoinType::Inner, expr_tree, &column_refs),
            DEFAULT_EQ_SEL
        );
    }

    // We don't test joinsel or with oncond because if there is an oncond (on condition), the top-level operator must be an AND

    /// I made this helper function to avoid copying all eight lines over and over
    fn assert_outer_selectivities(
        cost_model: &TestOptCostModel,
        expr_tree: OptRelNodeRef,
        expr_tree_rev: OptRelNodeRef,
        column_refs: &GroupColumnRefs,
        expected_table1_outer_sel: f64,
        expected_table2_outer_sel: f64,
    ) {
        // all table 1 outer combinations
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                cost_model,
                false,
                JoinType::LeftOuter,
                expr_tree.clone(),
                column_refs
            ),
            expected_table1_outer_sel
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                cost_model,
                false,
                JoinType::LeftOuter,
                expr_tree_rev.clone(),
                column_refs
            ),
            expected_table1_outer_sel
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                cost_model,
                true,
                JoinType::RightOuter,
                expr_tree.clone(),
                column_refs
            ),
            expected_table1_outer_sel
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                cost_model,
                true,
                JoinType::RightOuter,
                expr_tree_rev.clone(),
                column_refs
            ),
            expected_table1_outer_sel
        );
        // all table 2 outer combinations
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                cost_model,
                true,
                JoinType::LeftOuter,
                expr_tree.clone(),
                column_refs
            ),
            expected_table2_outer_sel
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                cost_model,
                true,
                JoinType::LeftOuter,
                expr_tree_rev.clone(),
                column_refs
            ),
            expected_table2_outer_sel
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                cost_model,
                false,
                JoinType::RightOuter,
                expr_tree.clone(),
                column_refs
            ),
            expected_table2_outer_sel
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                cost_model,
                false,
                JoinType::RightOuter,
                expr_tree_rev.clone(),
                column_refs
            ),
            expected_table2_outer_sel
        );
    }

    /// Unique oncond means an oncondition on columns which are unique in both tables
    /// There's only one case if both columns are unique and have different row counts: the inner will be < 1 / row count
    ///   of one table and = 1 / row count of another
    #[test]
    fn test_outer_unique_oncond() {
        let cost_model = create_two_table_cost_model_custom_row_cnts(
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                5,
                0.0,
                Some(TestDistribution::empty()),
            ),
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                4,
                0.0,
                Some(TestDistribution::empty()),
            ),
            5,
            4,
        );
        // the left/right of the join refers to the tables, not the order of columns in the predicate
        let expr_tree = bin_op(BinOpType::Eq, col_ref(0), col_ref(1));
        let expr_tree_rev = bin_op(BinOpType::Eq, col_ref(1), col_ref(0));
        let column_refs = GroupColumnRefs::new_test(
            vec![
                ColumnRef::base_table_column_ref(String::from(TABLE1_NAME), 0),
                ColumnRef::base_table_column_ref(String::from(TABLE2_NAME), 0),
            ],
            None,
        );
        // sanity check the expected inner sel
        let expected_inner_sel = 0.2;
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                &cost_model,
                false,
                JoinType::Inner,
                expr_tree.clone(),
                &column_refs
            ),
            expected_inner_sel
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                &cost_model,
                false,
                JoinType::Inner,
                expr_tree_rev.clone(),
                &column_refs
            ),
            expected_inner_sel
        );
        // check the outer sels
        assert_outer_selectivities(
            &cost_model,
            expr_tree,
            expr_tree_rev,
            &column_refs,
            0.25,
            0.2,
        );
    }

    /// Non-unique oncond means the column is not unique in either table
    /// Inner always >= row count means that the inner join result is >= 1 / the row count of both tables
    #[test]
    fn test_outer_nonunique_oncond_inner_always_geq_rowcnt() {
        let cost_model = create_two_table_cost_model_custom_row_cnts(
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                5,
                0.0,
                Some(TestDistribution::empty()),
            ),
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                4,
                0.0,
                Some(TestDistribution::empty()),
            ),
            10,
            8,
        );
        // the left/right of the join refers to the tables, not the order of columns in the predicate
        let expr_tree = bin_op(BinOpType::Eq, col_ref(0), col_ref(1));
        let expr_tree_rev = bin_op(BinOpType::Eq, col_ref(1), col_ref(0));
        let column_refs = GroupColumnRefs::new_test(
            vec![
                ColumnRef::base_table_column_ref(String::from(TABLE1_NAME), 0),
                ColumnRef::base_table_column_ref(String::from(TABLE2_NAME), 0),
            ],
            None,
        );
        // sanity check the expected inner sel
        let expected_inner_sel = 0.2;
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                &cost_model,
                false,
                JoinType::Inner,
                expr_tree.clone(),
                &column_refs
            ),
            expected_inner_sel
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                &cost_model,
                false,
                JoinType::Inner,
                expr_tree_rev.clone(),
                &column_refs
            ),
            expected_inner_sel
        );
        // check the outer sels
        assert_outer_selectivities(
            &cost_model,
            expr_tree,
            expr_tree_rev,
            &column_refs,
            0.2,
            0.2,
        );
    }

    /// Non-unique oncond means the column is not unique in either table
    /// Inner sometimes < row count means that the inner join result < 1 / the row count of exactly one table.
    ///   Note that without a join filter, it's impossible to be less than the row count of both tables
    #[test]
    fn test_outer_nonunique_oncond_inner_sometimes_lt_rowcnt() {
        let cost_model = create_two_table_cost_model_custom_row_cnts(
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                10,
                0.0,
                Some(TestDistribution::empty()),
            ),
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                2,
                0.0,
                Some(TestDistribution::empty()),
            ),
            20,
            4,
        );
        // the left/right of the join refers to the tables, not the order of columns in the predicate
        let expr_tree = bin_op(BinOpType::Eq, col_ref(0), col_ref(1));
        let expr_tree_rev = bin_op(BinOpType::Eq, col_ref(1), col_ref(0));
        let column_refs = GroupColumnRefs::new_test(
            vec![
                ColumnRef::base_table_column_ref(String::from(TABLE1_NAME), 0),
                ColumnRef::base_table_column_ref(String::from(TABLE2_NAME), 0),
            ],
            None,
        );
        // sanity check the expected inner sel
        let expected_inner_sel = 0.1;
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                &cost_model,
                false,
                JoinType::Inner,
                expr_tree.clone(),
                &column_refs
            ),
            expected_inner_sel
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                &cost_model,
                false,
                JoinType::Inner,
                expr_tree_rev.clone(),
                &column_refs
            ),
            expected_inner_sel
        );
        // check the outer sels
        assert_outer_selectivities(
            &cost_model,
            expr_tree,
            expr_tree_rev,
            &column_refs,
            0.25,
            0.1,
        );
    }

    /// Unique oncond means an oncondition on columns which are unique in both tables
    /// Filter means we're adding a join filter
    /// There's only one case if both columns are unique and there's a filter:
    /// the inner will be < 1 / row count of both tables
    #[test]
    fn test_outer_unique_oncond_filter() {
        let cost_model = create_two_table_cost_model_custom_row_cnts(
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                50,
                0.0,
                Some(TestDistribution::new(vec![(Value::Int32(128), 0.4)])),
            ),
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                4,
                0.0,
                Some(TestDistribution::empty()),
            ),
            50,
            4,
        );
        // the left/right of the join refers to the tables, not the order of columns in the predicate
        let eq0and1 = bin_op(BinOpType::Eq, col_ref(0), col_ref(1));
        let eq1and0 = bin_op(BinOpType::Eq, col_ref(1), col_ref(0));
        let filter = bin_op(BinOpType::Leq, col_ref(0), cnst(Value::Int32(128)));
        let expr_tree = log_op(LogOpType::And, vec![eq0and1, filter.clone()]);
        // inner rev means its the inner expr (the eq op) whose children are being reversed, as opposed to the and op
        let expr_tree_inner_rev = log_op(LogOpType::And, vec![eq1and0, filter.clone()]);
        let column_refs = GroupColumnRefs::new_test(
            vec![
                ColumnRef::base_table_column_ref(String::from(TABLE1_NAME), 0),
                ColumnRef::base_table_column_ref(String::from(TABLE2_NAME), 0),
            ],
            None,
        );
        // sanity check the expected inner sel
        let expected_inner_sel = 0.008;
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                &cost_model,
                false,
                JoinType::Inner,
                expr_tree.clone(),
                &column_refs
            ),
            expected_inner_sel
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                &cost_model,
                false,
                JoinType::Inner,
                expr_tree_inner_rev.clone(),
                &column_refs
            ),
            expected_inner_sel
        );
        // check the outer sels
        assert_outer_selectivities(
            &cost_model,
            expr_tree,
            expr_tree_inner_rev,
            &column_refs,
            0.25,
            0.02,
        );
    }

    // Ensure that in `select t1, t2, t3 where t1.a = t2.a and t2.a = t3.a and t1.a = t3.a`,
    // even if the first join picks the most selective predicate (which should have been discarded,
    // since we need to ensure the most selective N - 1 predicates are picked), the selectivity is
    // adjusted in the second join so that the final selectivity is the product of the
    // selectivities of the 2 most selective redicates.
    #[test]
    fn test_inner_redundant_predicate() {
        let cost_model = create_three_table_cost_model(
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                2,
                0.0,
                Some(TestDistribution::empty()),
            ),
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                4,
                0.0,
                Some(TestDistribution::empty()),
            ),
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                5,
                0.0,
                Some(TestDistribution::empty()),
            ),
        );
        let col01_sel = 0.25;
        let col02_sel = 0.2;
        let col12_sel = 0.2;
        let col0_base_ref = BaseTableColumnRef {
            table: String::from(TABLE1_NAME),
            col_idx: 0,
        };
        let col1_base_ref = BaseTableColumnRef {
            table: String::from(TABLE2_NAME),
            col_idx: 0,
        };
        let col2_base_ref = BaseTableColumnRef {
            table: String::from(TABLE3_NAME),
            col_idx: 0,
        };
        let col0_ref: ColumnRef = col0_base_ref.clone().into();
        let col1_ref: ColumnRef = col1_base_ref.clone().into();
        let col2_ref: ColumnRef = col2_base_ref.clone().into();

        let mut eq_columns = EqBaseTableColumnSets::new();
        eq_columns.add_predicate(EqPredicate::new(col0_base_ref, col1_base_ref));
        let semantic_correlation = SemanticCorrelation::new(eq_columns);
        let column_refs = GroupColumnRefs::new_test(
            vec![col0_ref.clone(), col1_ref.clone(), col2_ref.clone()],
            Some(semantic_correlation),
        );

        let eq0and2 = bin_op(BinOpType::Eq, col_ref(0), col_ref(2));
        let eq1and2 = bin_op(BinOpType::Eq, col_ref(1), col_ref(2));
        let expr_tree = log_op(LogOpType::And, vec![eq0and2, eq1and2]);
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(&cost_model, false, JoinType::Inner, expr_tree, &column_refs),
            col02_sel * (col02_sel * col12_sel) / (col01_sel * col12_sel)
        );
    }
}
