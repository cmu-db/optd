use itertools::Itertools;
use optd_core::{
    cascades::{CascadesOptimizer, RelNodeContext},
    cost::Cost,
};

use crate::{
    cost::{
        base_cost::DEFAULT_NUM_DISTINCT,
        stats::{Distribution, MostCommonValues},
    },
    plan_nodes::{
        ColumnRefExpr, Expr, ExprList, JoinType, LogOpExpr, LogOpType, OptRelNode, OptRelNodeRef,
        OptRelNodeTyp,
    },
    properties::column_ref::{ColumnRefPropertyBuilder, GroupColumnRefs},
};

use super::{OptCostModel, DEFAULT_UNK_SEL};

impl<M: MostCommonValues, D: Distribution> OptCostModel<M, D> {
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
            // there may be more than one expression tree in a group. see comment in OptRelNodeTyp::PhysicalFilter(_) for more information
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
            let left_keys_list = optimizer.get_all_group_bindings(left_keys_group_id, false);
            let right_keys_list = optimizer.get_all_group_bindings(right_keys_group_id, false);
            // there may be more than one expression tree in a group. see comment in OptRelNodeTyp::PhysicalFilter(_) for more information
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
    fn get_join_selectivity_from_keys(
        &self,
        join_typ: JoinType,
        left_keys: ExprList,
        right_keys: ExprList,
        column_refs: &GroupColumnRefs,
        left_row_cnt: f64,
        right_row_cnt: f64,
    ) -> f64 {
        assert!(left_keys.len() == right_keys.len());
        // I assume that the keys are already in the right order s.t. the ith key of left_keys corresponds with the ith key of right_keys
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
        )
    }

    /// The core logic of join selectivity which assumes we've already separated the expression into the on conditions and the filters
    fn get_join_selectivity_core(
        &self,
        join_typ: JoinType,
        on_col_ref_pairs: Vec<(ColumnRefExpr, ColumnRefExpr)>,
        filter_expr_tree: Option<OptRelNodeRef>,
        column_refs: &GroupColumnRefs,
        left_row_cnt: f64,
        right_row_cnt: f64,
    ) -> f64 {
        let join_on_selectivity = self.get_join_on_selectivity(&on_col_ref_pairs, column_refs);
        // Currently, there is no difference in how we handle a join filter and a select filter, so we use the same function
        // One difference (that we *don't* care about right now) is that join filters can contain expressions from multiple
        //   different tables. Currently, this doesn't affect the get_filter_selectivity() function, but this may change in
        //   the future
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

    /// The expr_tree input must be a "mixed expression tree", just like with get_filter_selectivity()
    /// This is a "wrapper" to separate the equality conditions from the filter conditions before calling
    ///   the "main" get_join_selectivity_core() function.
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
                )
            } else {
                self.get_join_selectivity_core(
                    join_typ,
                    vec![],
                    Some(expr_tree),
                    column_refs,
                    left_row_cnt,
                    right_row_cnt,
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
                    ColumnRef::BaseTableColumnRef {
                        table: left_table, ..
                    },
                    ColumnRef::BaseTableColumnRef {
                        table: right_table, ..
                    },
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

    /// Get the selectivity of the on conditions.
    ///
    /// Note that the selectivity of the on conditions does not depend on join type. Join type is accounted for separately in get_join_selectivity_core().
    fn get_join_on_selectivity(
        &self,
        on_col_ref_pairs: &[(ColumnRefExpr, ColumnRefExpr)],
        column_refs: &GroupColumnRefs,
    ) -> f64 {
        // multiply the selectivities of all individual conditions together
        on_col_ref_pairs.iter().map(|on_col_ref_pair| {
            // the formula for each pair is min(1 / ndistinct1, 1 / ndistinct2) (see https://postgrespro.com/blog/pgsql/5969618)
            let ndistincts = vec![&on_col_ref_pair.0, &on_col_ref_pair.1].into_iter().map(|on_col_ref_expr| {
                match self.get_per_column_stats_from_col_ref(&column_refs[on_col_ref_expr.index()]) {
                    Some(per_col_stats) => per_col_stats.ndistinct,
                    None => DEFAULT_NUM_DISTINCT,
                }
            });
            // using reduce(f64::min) is the idiomatic workaround to min() because f64 does not implement Ord due to NaN
            let selectivity = ndistincts.map(|ndistinct| 1.0 / ndistinct as f64).reduce(f64::min).expect("reduce() only returns None if the iterator is empty, which is impossible since col_ref_exprs.len() == 2");
            assert!(!selectivity.is_nan(), "it should be impossible for selectivity to be NaN since n-distinct is never 0");
            selectivity
        }).product()
    }
}
