/// This module implements subquery decorrelation, based on the paper "Improving
/// Unnesting of Complex Queries (BTW 2025)" by Thomas Neumann.
///
/// Gaps to the paper's implementation / Future TODOs:
///
/// 1 - We currently only support full unnesting, without the "simple unnesting"
/// pass before that. This is logically correct, and anything that can be simply
/// unnested can be fully unnested. However, we can probably shave off a few
/// milliseconds by running a simple unnesting pass first if latency is essential in
/// the future (i.e. OLTP applications).
///
/// 2 - The 4 "advanced constructs" from the paper (CTEs, WITH RECURSIVE, FULL OUTER
/// joins, ORDER BY with LIMIT) are unsupported, since many of these constructs
/// don't even have a meaningful IR in optd. When these constructs are added, the
/// implementation rules for those operators can be added.
///
/// 3 - We compute outer-references and accessing sets on the fly, by checking
/// which columns of an operator are not bound by downstream columns being
/// produced. This is a much bigger latency concern than (1), since we do
/// potentially O(n) work per operator in the tree, i.e. potentially O(n^2)
/// total work. This should be done in a pass before decorrelation, where we
/// mark every dependent join with the operators that access outer columns, and
/// every operator with which of its columns are outer references or bound
/// references.
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::ir::IRContext;
use crate::ir::convert::{IntoOperator, IntoScalar};
use crate::ir::operator::join::JoinType;
use crate::ir::operator::{
    LogicalAggregate, LogicalDependentJoinBorrowed, LogicalJoin, LogicalProject, LogicalRemap,
    Operator,
};
use crate::ir::scalar::{BinaryOp, BinaryOpKind, ColumnAssign, ColumnRef, List};
use crate::ir::{Column, Scalar};

use super::UnnestingRule;
use super::helpers::{
    Unnesting, UnnestingInfo, compute_accessing_set, is_contained_in, remap_right_output_collisions,
};

impl UnnestingRule {
    // Creates the domain for the new unnesting struct
    fn create_domain(
        &self,
        outer_refs: Vec<Column>,
        outer: Arc<Operator>,
        ctx: &IRContext,
    ) -> (HashMap<Column, Column>, Arc<Operator>) {
        // Compute the domain representation
        let mut domain_repr: HashMap<Column, Column> = HashMap::new();
        for c in &outer_refs {
            let fresh = ctx.define_column(ctx.get_column_meta(c).data_type.clone(), None);
            domain_repr.insert(*c, fresh);
        }

        // Compute the domain operator
        let project_scalars: Vec<Arc<Scalar>> = outer_refs
            .iter()
            .map(|c| ColumnRef::new(*c).into_scalar())
            .collect();
        let project_list = List::new(project_scalars.into()).into_scalar();
        let domain_project = LogicalProject::new(outer.clone(), project_list).into_operator();
        let group_keys: Vec<Arc<Scalar>> = outer_refs
            .iter()
            .map(|c| ColumnAssign::new(*c, ColumnRef::new(*c).into_scalar()).into_scalar())
            .collect();
        let group_keys_list = List::new(group_keys.into()).into_scalar();
        let empty_exprs_list = List::new(vec![].into()).into_scalar();
        let domain_distinct =
            LogicalAggregate::new(domain_project, empty_exprs_list, group_keys_list)
                .into_operator();
        let remap_keys: Vec<Arc<Scalar>> = outer_refs
            .iter()
            .map(|c| {
                let target = *domain_repr.get(c).unwrap();
                let col_ref = ColumnRef::new(*c).into_scalar();
                ColumnAssign::new(target, col_ref).into_scalar()
            })
            .collect();
        let remap_list = List::new(remap_keys.into()).into_scalar();
        let domain_d = LogicalRemap::new(domain_distinct, remap_list).into_operator();

        (domain_repr, domain_d)
    }

    // This is the main dependent decorrelation join algorithm, following
    // the algorithm from page 11 of the referenced paper
    pub(super) fn d_join_elimination(
        &self,
        dep_join: LogicalDependentJoinBorrowed<'_>,
        mut parent_unnesting: Option<&mut Unnesting<'_>>,
        parent_accessing: Option<&HashSet<*const Operator>>,
        ctx: &IRContext,
    ) -> Arc<Operator> {
        // We currently only support inner dependent joins!
        // TODO: Support left outer, single, and mark joins
        let join_type = *dep_join.join_type();
        assert_eq!(join_type, JoinType::Inner);

        // In the nested case we have to unnest the left-hand side first
        let (new_outer, condition) = if let Some(ref mut pu) = parent_unnesting {
            let acc_left = parent_accessing
                .unwrap()
                .iter()
                .filter(|&&op_ptr| is_contained_in(op_ptr, dep_join.outer()))
                .copied()
                .collect();
            let op = self.unnest(dep_join.outer().clone(), pu, &acc_left, ctx);
            let cond = pu.rewrite_columns(dep_join.join_cond().clone());
            (op, cond)
        } else {
            (dep_join.outer().clone(), dep_join.join_cond().clone())
        };

        // Create a new unnesting struct
        let (accessing_operators, accessing_cols) = compute_accessing_set(dep_join.inner(), ctx);
        let mut outer_refs = HashSet::new();
        let outer_outputs = new_outer.output_columns(ctx);
        for c in &accessing_cols {
            if outer_outputs.contains(c) {
                outer_refs.insert(*c);
            } else if let Some(pu) = parent_unnesting.as_deref()
                && let Some(mapped) = pu.resolve_col(*c)
                && outer_outputs.contains(&mapped)
            {
                outer_refs.insert(mapped);
            }
        }
        let mut outer_refs_vec: Vec<Column> = outer_refs.iter().copied().collect();
        outer_refs_vec.sort_by_key(|c| c.0);
        let (domain_repr, domain_op) =
            self.create_domain(outer_refs_vec.clone(), new_outer.clone(), ctx);
        let mut unnesting = Unnesting::new(Arc::new(UnnestingInfo::new(
            outer_refs,
            (domain_repr, domain_op),
            parent_unnesting.as_deref(),
        )));

        // Unnest right-hand side
        unnesting.update_cclasses_equivalences(&condition);
        let new_inner = self.unnest(
            dep_join.inner().clone(),
            &mut unnesting,
            &accessing_operators,
            ctx,
        );
        let new_outer_cols = new_outer.output_columns(ctx);
        let (new_inner, remap) =
            remap_right_output_collisions(&new_outer_cols, new_inner, &mut unnesting, ctx);

        // Add equality to join condition
        let mut new_conds = Vec::new();
        for c in &outer_refs_vec {
            let left_ref = ColumnRef::new(*c).into_scalar();
            let right_ref = ColumnRef::new(unnesting.resolve_col(*c).unwrap_or(*c)).into_scalar();
            new_conds.push(
                BinaryOp::new(BinaryOpKind::IsNotDistinctFrom, left_ref, right_ref).into_scalar(),
            );
        }
        if !condition.is_true_scalar() {
            new_conds.push(condition.clone());
        }

        // If this nested elimination is inside an already-decorrelating scope,
        // reconcile parent-vs-inner representative choices
        let mut nested_repr_choices = Vec::new();
        let new_inner_cols = new_inner.output_columns(ctx);
        if let Some(parent) = parent_unnesting.as_deref() {
            for outer_col in parent.collect_outer_refs_recursive() {
                let parent_repr = parent
                    .resolve_col(outer_col)
                    .filter(|col| new_outer_cols.contains(col));
                let inner_repr = unnesting
                    .resolve_col(outer_col)
                    .map(|col: Column| remap.get(&col).copied().unwrap_or(col))
                    .filter(|col| new_inner_cols.contains(col));

                if let (Some(l), Some(r)) = (parent_repr, inner_repr) {
                    assert!(l != r);
                    let left_ref = ColumnRef::new(l).into_scalar();
                    let right_ref = ColumnRef::new(r).into_scalar();
                    new_conds.push(
                        BinaryOp::new(BinaryOpKind::IsNotDistinctFrom, left_ref, right_ref)
                            .into_scalar(),
                    );
                    nested_repr_choices.push((outer_col, l));
                } else if let Some(r) = inner_repr {
                    nested_repr_choices.push((outer_col, r));
                }
            }
        }

        // Propagate higher-scope representatives discovered in this nested
        // elimination back to the parent state.
        if let Some(parent) = parent_unnesting {
            for (outer_col, repr_col) in nested_repr_choices {
                parent.set_scoped_repr_of(outer_col, repr_col);
            }
        }

        let new_cond = Scalar::combine_conjuncts(new_conds);
        LogicalJoin::new(join_type, new_outer, new_inner, new_cond).into_operator()
    }
}
