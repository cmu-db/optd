use std::collections::HashSet;

use itertools::Itertools;
use optd_core::{
    cascades::{CascadesOptimizer, RelNodeContext},
    cost::Cost,
};
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    cost::base_cost::{
        stats::{Distribution, MostCommonValues},
        DEFAULT_NUM_DISTINCT,
    },
    plan_nodes::{
        BinOpType, ColumnRefExpr, Expr, ExprList, JoinType, LogOpExpr, LogOpType, OptRelNode,
        OptRelNodeRef, OptRelNodeTyp,
    },
    properties::{
        column_ref::{
            BaseTableColumnRef, BaseTableColumnRefs, ColumnRef, ColumnRefPropertyBuilder,
            EqBaseTableColumnSets, EqPredicate, SemanticCorrelation,
        },
        schema::{Schema, SchemaPropertyBuilder},
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
            let schema =
                optimizer.get_property_by_group::<SchemaPropertyBuilder>(context.group_id, 0);
            let column_refs =
                optimizer.get_property_by_group::<ColumnRefPropertyBuilder>(context.group_id, 1);
            let column_refs = column_refs.base_table_column_refs();
            let expr_group_id = context.children_group_ids[2];
            let expr_tree = optimizer
                .get_predicate_binding(expr_group_id)
                .expect("no expressions??");
            let input_correlation = self.get_input_correlation(&context, optimizer);
            self.get_join_selectivity_from_expr_tree(
                join_typ,
                expr_tree,
                &schema,
                column_refs,
                input_correlation,
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
            let schema =
                optimizer.get_property_by_group::<SchemaPropertyBuilder>(context.group_id, 0);
            let column_refs =
                optimizer.get_property_by_group::<ColumnRefPropertyBuilder>(context.group_id, 1);
            let column_refs = column_refs.base_table_column_refs();
            let left_keys_group_id = context.children_group_ids[2];
            let right_keys_group_id = context.children_group_ids[3];
            let left_col_cnt = optimizer
                .get_property_by_group::<ColumnRefPropertyBuilder>(context.children_group_ids[0], 1)
                .base_table_column_refs()
                .len();
            let left_keys = optimizer
                .get_predicate_binding(left_keys_group_id)
                .expect("no expression found?");
            let right_keys = optimizer
                .get_predicate_binding(right_keys_group_id)
                .expect("no expression found?");
            // there may be more than one expression tree in a group.
            // see comment in OptRelNodeTyp::PhysicalFilter(_) for more information
            let input_correlation = self.get_input_correlation(&context, optimizer);
            self.get_join_selectivity_from_keys(
                join_typ,
                ExprList::from_rel_node(left_keys.clone())
                    .expect("left_keys should be an ExprList"),
                ExprList::from_rel_node(right_keys.clone())
                    .expect("right_keys should be an ExprList"),
                &schema,
                column_refs,
                input_correlation,
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

    fn get_input_correlation(
        &self,
        context: &RelNodeContext,
        optimizer: &CascadesOptimizer<OptRelNodeTyp>,
    ) -> Option<SemanticCorrelation> {
        let left_group_id = context.children_group_ids[0];
        let right_group_id = context.children_group_ids[1];
        let left_correlation = optimizer
            .get_property_by_group::<ColumnRefPropertyBuilder>(left_group_id, 1)
            .output_correlation()
            .cloned();
        let right_correlation = optimizer
            .get_property_by_group::<ColumnRefPropertyBuilder>(right_group_id, 1)
            .output_correlation()
            .cloned();
        SemanticCorrelation::merge(left_correlation, right_correlation)
    }

    /// A wrapper to convert the join keys to the format expected by get_join_selectivity_core()
    #[allow(clippy::too_many_arguments)]
    fn get_join_selectivity_from_keys(
        &self,
        join_typ: JoinType,
        left_keys: ExprList,
        right_keys: ExprList,
        schema: &Schema,
        column_refs: &BaseTableColumnRefs,
        input_correlation: Option<SemanticCorrelation>,
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
            schema,
            column_refs,
            input_correlation,
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
        schema: &Schema,
        column_refs: &BaseTableColumnRefs,
        input_correlation: Option<SemanticCorrelation>,
        left_row_cnt: f64,
        right_row_cnt: f64,
        right_col_ref_offset: usize,
    ) -> f64 {
        let join_on_selectivity = self.get_join_on_selectivity(
            &on_col_ref_pairs,
            column_refs,
            input_correlation,
            right_col_ref_offset,
        );
        // Currently, there is no difference in how we handle a join filter and a select filter,
        // so we use the same function.
        //
        // One difference (that we *don't* care about right now) is that join filters can contain expressions from multiple
        // different tables. Currently, this doesn't affect the get_filter_selectivity() function, but this may change in
        // the future.
        let join_filter_selectivity = match filter_expr_tree {
            Some(filter_expr_tree) => {
                self.get_filter_selectivity(filter_expr_tree, schema, column_refs)
            }
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
    #[allow(clippy::too_many_arguments)]
    fn get_join_selectivity_from_expr_tree(
        &self,
        join_typ: JoinType,
        expr_tree: OptRelNodeRef,
        schema: &Schema,
        column_refs: &BaseTableColumnRefs,
        input_correlation: Option<SemanticCorrelation>,
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
                schema,
                column_refs,
                input_correlation,
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
                    schema,
                    column_refs,
                    input_correlation,
                    left_row_cnt,
                    right_row_cnt,
                    0,
                )
            } else {
                self.get_join_selectivity_core(
                    join_typ,
                    vec![],
                    Some(expr_tree),
                    schema,
                    column_refs,
                    input_correlation,
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
        column_refs: &BaseTableColumnRefs,
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

    /// Given a set of N columns involved in a multi-equality, find the total selectivity
    /// of the multi-equality.
    ///
    /// This is a generalization of get_join_selectivity_from_on_col_ref_pair().
    fn get_join_selectivity_from_most_selective_columns(
        &self,
        base_col_refs: HashSet<BaseTableColumnRef>,
    ) -> f64 {
        assert!(base_col_refs.len() > 1);
        let num_base_col_refs = base_col_refs.len();
        base_col_refs
            .into_iter()
            .map(|base_col_ref| {
                match self.get_column_comb_stats(&base_col_ref.table, &[base_col_ref.col_idx]) {
                    Some(per_col_stats) => per_col_stats.ndistinct,
                    None => DEFAULT_NUM_DISTINCT,
                }
            })
            .map(|ndistinct| 1.0 / ndistinct as f64)
            .sorted_by(|a, b| {
                a.partial_cmp(b)
                    .expect("No floats should be NaN since n-distinct is never 0")
            })
            .take(num_base_col_refs - 1)
            .product()
    }

    /// A predicate set defines a "multi-equality graph", which is an unweighted undirected graph. The
    /// nodes are columns while edges are predicates. The old graph is defined by `past_eq_columns`
    /// while the `predicate` is the new addition to this graph. This unweighted undirected graph
    /// consists of a number of connected components, where each connected component represents columns
    /// that are set to be equal to each other. Single nodes not connected to anything are considered
    /// standalone connected components.
    ///
    /// The selectivity of each connected component of N nodes is equal to the product of 1/ndistinct of
    /// the N-1 nodes with the highest ndistinct values. You can see this if you imagine that all columns
    /// being joined are unique columns and that they follow the inclusion principle (every element of the
    /// smaller tables is present in the larger tables). When these assumptions are not true, the selectivity
    /// may not be completely accurate. However, it is still fairly accurate.
    ///
    /// However, we cannot simply add `predicate` to the multi-equality graph and compute the selectivity of
    /// the entire connected component, because this would be "double counting" a lot of nodes. The join(s)
    /// before this join would already have a selectivity value. Thus, we compute the selectivity of the
    /// join(s) before this join (the first block of the function) and then the selectivity of the connected
    /// component after this join. The quotient is the "adjustment" factor.
    ///
    /// NOTE: This function modifies `past_eq_columns` by adding `predicate` to it.
    fn get_join_selectivity_adjustment_when_adding_to_multi_equality_graph(
        &self,
        predicate: &EqPredicate,
        past_eq_columns: &mut EqBaseTableColumnSets,
    ) -> f64 {
        if predicate.left == predicate.right {
            // self-join, TODO: is this correct?
            return 1.0;
        }
        // To find the adjustment, we need to know the selectivity of the graph before `predicate` is added.
        //
        // There are two cases: (1) adding `predicate` does not change the # of connected components, and
        // (2) adding `predicate` reduces the # of connected by 1. Note that columns not involved in any
        // predicates are considered a part of the graph and are a connected component on their own.
        let children_pred_sel = {
            if past_eq_columns.is_eq(&predicate.left, &predicate.right) {
                self.get_join_selectivity_from_most_selective_columns(
                    past_eq_columns.find_cols_for_eq_column_set(&predicate.left),
                )
            } else {
                let left_sel = if past_eq_columns.contains(&predicate.left) {
                    self.get_join_selectivity_from_most_selective_columns(
                        past_eq_columns.find_cols_for_eq_column_set(&predicate.left),
                    )
                } else {
                    1.0
                };
                let right_sel = if past_eq_columns.contains(&predicate.right) {
                    self.get_join_selectivity_from_most_selective_columns(
                        past_eq_columns.find_cols_for_eq_column_set(&predicate.right),
                    )
                } else {
                    1.0
                };
                left_sel * right_sel
            }
        };

        // Add predicate to past_eq_columns and compute the selectivity of the connected component it creates.
        past_eq_columns.add_predicate(predicate.clone());
        let new_pred_sel = {
            let cols = past_eq_columns.find_cols_for_eq_column_set(&predicate.left);
            self.get_join_selectivity_from_most_selective_columns(cols)
        };

        // Compute the adjustment factor.
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
        column_refs: &BaseTableColumnRefs,
        input_correlation: Option<SemanticCorrelation>,
        right_col_ref_offset: usize,
    ) -> f64 {
        let mut past_eq_columns = input_correlation
            .map(|c| EqBaseTableColumnSets::try_from(c).unwrap())
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
                    return self
                        .get_join_selectivity_adjustment_when_adding_to_multi_equality_graph(
                            &predicate,
                            &mut past_eq_columns,
                        );
                }

                self.get_join_selectivity_from_on_col_ref_pair(left_col_ref, right_col_ref)
            })
            .product()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use optd_core::rel_node::Value;

    use crate::{
        cost::base_cost::{tests::*, DEFAULT_EQ_SEL},
        plan_nodes::{BinOpType, JoinType, LogOpType, OptRelNodeRef},
        properties::{
            column_ref::{
                BaseTableColumnRef, BaseTableColumnRefs, ColumnRef, EqBaseTableColumnSets,
                EqPredicate, SemanticCorrelation,
            },
            schema::Schema,
        },
    };

    /// A wrapper around get_join_selectivity_from_expr_tree that extracts the
    /// table row counts from the cost model.
    fn test_get_join_selectivity(
        cost_model: &TestOptCostModel,
        reverse_tables: bool,
        join_typ: JoinType,
        expr_tree: OptRelNodeRef,
        schema: &Schema,
        column_refs: &BaseTableColumnRefs,
        input_correlation: Option<SemanticCorrelation>,
    ) -> f64 {
        let table1_row_cnt = cost_model.per_table_stats_map[TABLE1_NAME].row_cnt as f64;
        let table2_row_cnt = cost_model.per_table_stats_map[TABLE2_NAME].row_cnt as f64;
        if !reverse_tables {
            cost_model.get_join_selectivity_from_expr_tree(
                join_typ,
                expr_tree,
                schema,
                column_refs,
                input_correlation,
                table1_row_cnt,
                table2_row_cnt,
            )
        } else {
            cost_model.get_join_selectivity_from_expr_tree(
                join_typ,
                expr_tree,
                schema,
                column_refs,
                input_correlation,
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
                &Schema::new(vec![]),
                &vec![],
                None,
                f64::NAN,
                f64::NAN
            ),
            1.0
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_join_selectivity_from_expr_tree(
                JoinType::Inner,
                cnst(Value::Bool(false)),
                &Schema::new(vec![]),
                &vec![],
                None,
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
        let schema = Schema::new(vec![]);
        let column_refs = vec![
            ColumnRef::base_table_column_ref(String::from(TABLE1_NAME), 0),
            ColumnRef::base_table_column_ref(String::from(TABLE2_NAME), 0),
        ];
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                &cost_model,
                false,
                JoinType::Inner,
                expr_tree,
                &schema,
                &column_refs,
                None,
            ),
            0.2
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                &cost_model,
                false,
                JoinType::Inner,
                expr_tree_rev,
                &schema,
                &column_refs,
                None,
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
        let schema = Schema::new(vec![]);
        let column_refs = vec![
            ColumnRef::base_table_column_ref(String::from(TABLE1_NAME), 0),
            ColumnRef::base_table_column_ref(String::from(TABLE2_NAME), 0),
        ];
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                &cost_model,
                false,
                JoinType::Inner,
                expr_tree,
                &schema,
                &column_refs,
                None,
            ),
            0.2
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                &cost_model,
                false,
                JoinType::Inner,
                expr_tree_rev,
                &schema,
                &column_refs,
                None
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
        let schema = Schema::new(vec![]);
        let column_refs = vec![
            ColumnRef::base_table_column_ref(String::from(TABLE1_NAME), 0),
            ColumnRef::base_table_column_ref(String::from(TABLE2_NAME), 0),
        ];
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                &cost_model,
                false,
                JoinType::Inner,
                expr_tree,
                &schema,
                &column_refs,
                None
            ),
            0.05
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                &cost_model,
                false,
                JoinType::Inner,
                expr_tree_rev,
                &schema,
                &column_refs,
                None
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
        let schema = Schema::new(vec![]);
        let column_refs = vec![
            ColumnRef::base_table_column_ref(String::from(TABLE1_NAME), 0),
            ColumnRef::base_table_column_ref(String::from(TABLE2_NAME), 0),
        ];
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                &cost_model,
                false,
                JoinType::Inner,
                expr_tree,
                &schema,
                &column_refs,
                None,
            ),
            0.2
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                &cost_model,
                false,
                JoinType::Inner,
                expr_tree_rev,
                &schema,
                &column_refs,
                None
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
        let schema = Schema::new(vec![]);
        let column_refs = vec![
            ColumnRef::base_table_column_ref(String::from(TABLE1_NAME), 0),
            ColumnRef::base_table_column_ref(String::from(TABLE2_NAME), 0),
        ];
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                &cost_model,
                false,
                JoinType::Inner,
                expr_tree,
                &schema,
                &column_refs,
                None
            ),
            DEFAULT_EQ_SEL
        );
    }

    // We don't test joinsel or with oncond because if there is an oncond (on condition), the top-level operator must be an AND

    /// I made this helper function to avoid copying all eight lines over and over
    fn assert_outer_selectivities(
        cost_model: &TestOptCostModel,
        expr_tree: OptRelNodeRef,
        expr_tree_rev: OptRelNodeRef,
        schema: &Schema,
        column_refs: &BaseTableColumnRefs,
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
                schema,
                column_refs,
                None
            ),
            expected_table1_outer_sel
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                cost_model,
                false,
                JoinType::LeftOuter,
                expr_tree_rev.clone(),
                schema,
                column_refs,
                None
            ),
            expected_table1_outer_sel
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                cost_model,
                true,
                JoinType::RightOuter,
                expr_tree.clone(),
                schema,
                column_refs,
                None
            ),
            expected_table1_outer_sel
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                cost_model,
                true,
                JoinType::RightOuter,
                expr_tree_rev.clone(),
                schema,
                column_refs,
                None
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
                schema,
                column_refs,
                None
            ),
            expected_table2_outer_sel
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                cost_model,
                true,
                JoinType::LeftOuter,
                expr_tree_rev.clone(),
                schema,
                column_refs,
                None
            ),
            expected_table2_outer_sel
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                cost_model,
                false,
                JoinType::RightOuter,
                expr_tree.clone(),
                schema,
                column_refs,
                None
            ),
            expected_table2_outer_sel
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                cost_model,
                false,
                JoinType::RightOuter,
                expr_tree_rev.clone(),
                schema,
                column_refs,
                None
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
        let schema = Schema::new(vec![]);
        let column_refs = vec![
            ColumnRef::base_table_column_ref(String::from(TABLE1_NAME), 0),
            ColumnRef::base_table_column_ref(String::from(TABLE2_NAME), 0),
        ];
        // sanity check the expected inner sel
        let expected_inner_sel = 0.2;
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                &cost_model,
                false,
                JoinType::Inner,
                expr_tree.clone(),
                &schema,
                &column_refs,
                None
            ),
            expected_inner_sel
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                &cost_model,
                false,
                JoinType::Inner,
                expr_tree_rev.clone(),
                &schema,
                &column_refs,
                None
            ),
            expected_inner_sel
        );
        // check the outer sels
        assert_outer_selectivities(
            &cost_model,
            expr_tree,
            expr_tree_rev,
            &schema,
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
        let schema = Schema::new(vec![]);
        let column_refs = vec![
            ColumnRef::base_table_column_ref(String::from(TABLE1_NAME), 0),
            ColumnRef::base_table_column_ref(String::from(TABLE2_NAME), 0),
        ];
        // sanity check the expected inner sel
        let expected_inner_sel = 0.2;
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                &cost_model,
                false,
                JoinType::Inner,
                expr_tree.clone(),
                &schema,
                &column_refs,
                None
            ),
            expected_inner_sel
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                &cost_model,
                false,
                JoinType::Inner,
                expr_tree_rev.clone(),
                &schema,
                &column_refs,
                None
            ),
            expected_inner_sel
        );
        // check the outer sels
        assert_outer_selectivities(
            &cost_model,
            expr_tree,
            expr_tree_rev,
            &schema,
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
        let schema = Schema::new(vec![]);
        let column_refs = vec![
            ColumnRef::base_table_column_ref(String::from(TABLE1_NAME), 0),
            ColumnRef::base_table_column_ref(String::from(TABLE2_NAME), 0),
        ];
        // sanity check the expected inner sel
        let expected_inner_sel = 0.1;
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                &cost_model,
                false,
                JoinType::Inner,
                expr_tree.clone(),
                &schema,
                &column_refs,
                None
            ),
            expected_inner_sel
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                &cost_model,
                false,
                JoinType::Inner,
                expr_tree_rev.clone(),
                &schema,
                &column_refs,
                None
            ),
            expected_inner_sel
        );
        // check the outer sels
        assert_outer_selectivities(
            &cost_model,
            expr_tree,
            expr_tree_rev,
            &schema,
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
        let schema = Schema::new(vec![]);
        let column_refs = vec![
            ColumnRef::base_table_column_ref(String::from(TABLE1_NAME), 0),
            ColumnRef::base_table_column_ref(String::from(TABLE2_NAME), 0),
        ];
        // sanity check the expected inner sel
        let expected_inner_sel = 0.008;
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                &cost_model,
                false,
                JoinType::Inner,
                expr_tree.clone(),
                &schema,
                &column_refs,
                None
            ),
            expected_inner_sel
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                &cost_model,
                false,
                JoinType::Inner,
                expr_tree_inner_rev.clone(),
                &schema,
                &column_refs,
                None
            ),
            expected_inner_sel
        );
        // check the outer sels
        assert_outer_selectivities(
            &cost_model,
            expr_tree,
            expr_tree_inner_rev,
            &schema,
            &column_refs,
            0.25,
            0.02,
        );
    }

    /// Test all possible permutations of three-table joins.
    /// A three-table join consists of at least two joins. `join1_on_cond` is the condition of the first
    ///   join. There can only be one condition because only two tables are involved at the time of the
    ///   first join.
    #[test_case::test_case(&[(0, 1)])]
    #[test_case::test_case(&[(0, 2)])]
    #[test_case::test_case(&[(1, 2)])]
    #[test_case::test_case(&[(0, 1), (0, 2)])]
    #[test_case::test_case(&[(0, 1), (1, 2)])]
    #[test_case::test_case(&[(0, 2), (1, 2)])]
    #[test_case::test_case(&[(0, 1), (0, 2), (1, 2)])]
    fn test_three_table_join_for_initial_join_on_conds(initial_join_on_conds: &[(usize, usize)]) {
        assert!(
            !initial_join_on_conds.is_empty(),
            "initial_join_on_conds should be non-empty"
        );
        assert_eq!(
            initial_join_on_conds.len(),
            initial_join_on_conds.iter().collect::<HashSet<_>>().len(),
            "initial_join_on_conds shouldn't contain duplicates"
        );
        let cost_model = create_three_table_cost_model(
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                2,
                0.0,
                Some(TestDistribution::empty()),
            ),
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                3,
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
        let col_base_refs = vec![
            BaseTableColumnRef {
                table: String::from(TABLE1_NAME),
                col_idx: 0,
            },
            BaseTableColumnRef {
                table: String::from(TABLE2_NAME),
                col_idx: 0,
            },
            BaseTableColumnRef {
                table: String::from(TABLE3_NAME),
                col_idx: 0,
            },
        ];
        let col_refs: BaseTableColumnRefs = col_base_refs
            .clone()
            .into_iter()
            .map(|col_base_ref| col_base_ref.into())
            .collect();

        let mut eq_columns = EqBaseTableColumnSets::new();
        for initial_join_on_cond in initial_join_on_conds {
            eq_columns.add_predicate(EqPredicate::new(
                col_base_refs[initial_join_on_cond.0].clone(),
                col_base_refs[initial_join_on_cond.1].clone(),
            ));
        }
        let initial_selectivity = {
            if initial_join_on_conds.len() == 1 {
                let initial_join_on_cond = initial_join_on_conds.first().unwrap();
                if initial_join_on_cond == &(0, 1) {
                    1.0 / 3.0
                } else if initial_join_on_cond == &(0, 2) || initial_join_on_cond == &(1, 2) {
                    1.0 / 4.0
                } else {
                    panic!();
                }
            } else {
                1.0 / 12.0
            }
        };
        let semantic_correlation = SemanticCorrelation::new(eq_columns);
        let schema = Schema::new(vec![]);
        let column_refs = col_refs;
        let input_correlation = Some(semantic_correlation);

        // Try all join conditions of the final join which would lead to all three tables being joined.
        let eq0and1 = bin_op(BinOpType::Eq, col_ref(0), col_ref(1));
        let eq0and2 = bin_op(BinOpType::Eq, col_ref(0), col_ref(2));
        let eq1and2 = bin_op(BinOpType::Eq, col_ref(1), col_ref(2));
        let and_01_02 = log_op(LogOpType::And, vec![eq0and1.clone(), eq0and2.clone()]);
        let and_01_12 = log_op(LogOpType::And, vec![eq0and1.clone(), eq1and2.clone()]);
        let and_02_12 = log_op(LogOpType::And, vec![eq0and2.clone(), eq1and2.clone()]);
        let and_01_02_12 = log_op(
            LogOpType::And,
            vec![eq0and1.clone(), eq0and2.clone(), eq1and2.clone()],
        );
        let mut join2_expr_trees = vec![and_01_02, and_01_12, and_02_12, and_01_02_12];
        if initial_join_on_conds.len() == 1 {
            let initial_join_on_cond = initial_join_on_conds.first().unwrap();
            if initial_join_on_cond == &(0, 1) {
                join2_expr_trees.push(eq0and2);
                join2_expr_trees.push(eq1and2);
            } else if initial_join_on_cond == &(0, 2) {
                join2_expr_trees.push(eq0and1);
                join2_expr_trees.push(eq1and2);
            } else if initial_join_on_cond == &(1, 2) {
                join2_expr_trees.push(eq0and1);
                join2_expr_trees.push(eq0and2);
            } else {
                panic!();
            }
        }
        for expr_tree in join2_expr_trees {
            let overall_selectivity = initial_selectivity
                * test_get_join_selectivity(
                    &cost_model,
                    false,
                    JoinType::Inner,
                    expr_tree.clone(),
                    &schema,
                    &column_refs,
                    input_correlation.clone(),
                );
            assert_approx_eq::assert_approx_eq!(overall_selectivity, 1.0 / 12.0);
        }
    }

    #[test]
    fn test_join_which_connects_two_components_together() {
        let cost_model = create_four_table_cost_model(
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                2,
                0.0,
                Some(TestDistribution::empty()),
            ),
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                3,
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
        let col_base_refs = vec![
            BaseTableColumnRef {
                table: String::from(TABLE1_NAME),
                col_idx: 0,
            },
            BaseTableColumnRef {
                table: String::from(TABLE2_NAME),
                col_idx: 0,
            },
            BaseTableColumnRef {
                table: String::from(TABLE3_NAME),
                col_idx: 0,
            },
            BaseTableColumnRef {
                table: String::from(TABLE4_NAME),
                col_idx: 0,
            },
        ];
        let col_refs: BaseTableColumnRefs = col_base_refs
            .clone()
            .into_iter()
            .map(|col_base_ref| col_base_ref.into())
            .collect();

        let mut eq_columns = EqBaseTableColumnSets::new();
        eq_columns.add_predicate(EqPredicate::new(
            col_base_refs[0].clone(),
            col_base_refs[1].clone(),
        ));
        eq_columns.add_predicate(EqPredicate::new(
            col_base_refs[2].clone(),
            col_base_refs[3].clone(),
        ));
        let initial_selectivity = 1.0 / (3.0 * 5.0);
        let semantic_correlation = SemanticCorrelation::new(eq_columns);
        let schema = Schema::new(vec![]);
        let column_refs = col_refs;
        let input_correlation = Some(semantic_correlation);

        let eq1and2 = bin_op(BinOpType::Eq, col_ref(1), col_ref(2));
        let overall_selectivity = initial_selectivity
            * test_get_join_selectivity(
                &cost_model,
                false,
                JoinType::Inner,
                eq1and2.clone(),
                &schema,
                &column_refs,
                input_correlation,
            );
        assert_approx_eq::assert_approx_eq!(overall_selectivity, 1.0 / (3.0 * 4.0 * 5.0));
    }
}
