/// This module contains the unnesting strategy for individual operators
use std::collections::HashSet;
use std::sync::Arc;

use crate::ir::convert::{IntoOperator, IntoScalar};
use crate::ir::operator::{
    LogicalAggregate, LogicalJoin, LogicalOrderBy, LogicalProject, LogicalRemap, LogicalSelect,
    Operator, OperatorKind, join::JoinType, LogicalDependentJoin
};
use crate::ir::rule::Rule;
use crate::ir::scalar::{
    BinaryOp, BinaryOpKind, ColumnAssign, ColumnRef, List, NaryOp, NaryOpKind,
};
use crate::ir::{Column, Scalar, ScalarKind, ColumnSet, IRContext};
use crate::rules::logical_select_simplify::LogicalSelectSimplifyRule;

use super::UnnestingRule;
use super::helpers::{Unnesting, is_contained_in, is_true_scalar};

impl UnnestingRule {
    fn add_repr_passthroughs_to_project(
        info: &Unnesting<'_>,
        projections: Arc<Scalar>,
        input_cols: &ColumnSet,
    ) -> Arc<Scalar> {
        let list = projections.try_borrow::<List>().unwrap();
        let mut members = list.members().to_vec();
        let mut output_cols: HashSet<Column> = members
            .iter()
            .filter_map(|expr| {
                if let Ok(assign) = expr.try_borrow::<ColumnAssign>() {
                    Some(*assign.column())
                } else if let Ok(col_ref) = expr.try_borrow::<ColumnRef>() {
                    Some(*col_ref.column())
                } else {
                    None
                }
            })
            .collect();
        let mut reps: Vec<Column> = info.get_all_repr_values();
        reps.sort_by_key(|c| c.0);
        reps.dedup_by_key(|c| c.0);
        for rep in reps {
            if input_cols.contains(&rep) && output_cols.insert(rep) {
                members.push(ColumnRef::new(rep).into_scalar());
            }
        }
        List::new(members.into()).into_scalar()
    }

    fn add_repr_passthroughs_to_remap(
        info: &Unnesting<'_>,
        mappings: Arc<Scalar>,
        input_cols: &ColumnSet,
    ) -> Arc<Scalar> {
        let list = mappings.try_borrow::<List>().unwrap();
        let mut members = list.members().to_vec();
        let mut output_cols: HashSet<Column> = members
            .iter()
            .filter_map(|expr| {
                expr.try_borrow::<ColumnAssign>()
                    .ok()
                    .map(|assign| *assign.column())
            })
            .collect();
        let mut reps: Vec<Column> = info.get_all_repr_values();
        reps.sort_by_key(|c| c.0);
        reps.dedup_by_key(|c| c.0);
        for rep in reps {
            if input_cols.contains(&rep) && output_cols.insert(rep) {
                members.push(ColumnAssign::new(rep, ColumnRef::new(rep).into_scalar()).into_scalar());
            }
        }
        List::new(members.into()).into_scalar()
    }

    // Helper function to rewrite columns, specifically for join conditions
    // (i.e. unnesting a non-dependent join), since it has different semantics.
    pub(super) fn rewrite_join_columns(
        left: &Unnesting<'_>,
        right: &Unnesting<'_>,
        scalar: Arc<Scalar>,
        left_cols: &ColumnSet,
        right_cols: &ColumnSet,
        join_type: JoinType,
    ) -> Arc<Scalar> {
        let rewritten = match &scalar.kind {
            ScalarKind::ColumnRef(cr) => {
                if left_cols.contains(&cr.column) ^ right_cols.contains(&cr.column) {
                    ColumnRef::new(cr.column).into_scalar()
                } else {
                    match (
                        left.resolve_mapped_col(cr.column),
                        right.resolve_mapped_col(cr.column),
                    ) {
                        (Some(l), Some(r)) => {
                            if matches!(join_type, JoinType::Inner)
                                && left.is_outer_ref_recursive(&l)
                                && !right.is_outer_ref_recursive(&r) {
                                ColumnRef::new(r).into_scalar()
                            } else {
                                ColumnRef::new(l).into_scalar()
                            }
                        }
                        (Some(l), None) => ColumnRef::new(l).into_scalar(),
                        (None, Some(r)) => ColumnRef::new(r).into_scalar(),
                        (None, None) => ColumnRef::new(cr.column).into_scalar(),
                    }
                }
            }
            _ => {
                let new_inputs: Vec<Arc<Scalar>> = scalar
                    .input_scalars()
                    .iter()
                    .map(|s| {
                        Self::rewrite_join_columns(
                            left,
                            right,
                            s.clone(),
                            left_cols,
                            right_cols,
                            join_type,
                        )
                    })
                    .collect();
                if new_inputs != scalar.input_scalars() {
                    Arc::new(scalar.clone_with_inputs(Some(Arc::from(new_inputs)), None))
                } else {
                    scalar
                }
            }
        };
        rewritten.simplify_nary_scalar()
    }

    pub(super) fn unnest(
        &self,
        op: Arc<Operator>,
        info: &mut Unnesting<'_>,
        accessing: &HashSet<*const Operator>,
        ctx: &IRContext,
    ) -> Arc<Operator> {
        self.unnest_inner(
            op,
            info,
            accessing,
            false,
            HashSet::new(),
            ctx,
        )
    }

    fn unnest_inner(
        &self,
        op: Arc<Operator>,
        info: &mut Unnesting<'_>,
        accessing: &HashSet<*const Operator>,
        may_need_domain: bool,
        mut seen_outer_refs: HashSet<Column>,
        ctx: &IRContext,
    ) -> Arc<Operator> {

        // Compute what outer refs this operator accesses
        let provided_by_inputs = op
            .input_operators()
            .into_iter()
            .fold(ColumnSet::default(), |acc, child| {
                acc | &child.output_columns(ctx)
            });
        let mut non_bound_outer_refs = HashSet::new();
        for scalar in op.input_scalars() {
            for col in scalar.used_columns().iter() {
                if !provided_by_inputs.contains(col) {
                    non_bound_outer_refs.insert(*col);
                }
            }
        }
        seen_outer_refs.extend(non_bound_outer_refs);

        // Terminal Condition: if no accessing operators remain in this subtree, stop here.
        if accessing.is_empty() {
            info.choose_repr_for_terminal(&seen_outer_refs, &op.output_columns(ctx));
            let mut required_domains = info.required_domain_ops(&seen_outer_refs);
            if may_need_domain && !required_domains.is_empty() {
                let mut domain_chain = required_domains.remove(0);
                for next_domain in required_domains {
                    domain_chain = LogicalJoin::new(
                        JoinType::Inner,
                        domain_chain,
                        next_domain,
                        crate::ir::scalar::Literal::boolean(true).into_scalar(),
                    )
                    .into_operator();
                }
                return LogicalJoin::new(
                    JoinType::Inner,
                    domain_chain,
                    op,
                    crate::ir::scalar::Literal::boolean(true).into_scalar(),
                )
                    .into_operator();
            }
            return op;
        }

        // Remove this node from the accessing set
        let mut child_accessing = accessing.clone();
        child_accessing.remove(&Arc::as_ptr(&op));
        let child_needs_domain = may_need_domain || accessing.contains(&Arc::as_ptr(&op));

        // Then unnest this operator
        match op.kind {
            OperatorKind::LogicalSelect(ref meta) => {
                let node = LogicalSelect::borrow_raw_parts(meta, &op.common);
                info.update_cclasses_equivalences(node.predicate());
                let new_input = self.unnest_inner(
                    node.input().clone(),
                    info,
                    &child_accessing,
                    child_needs_domain,
                    seen_outer_refs,
                    ctx,
                );
                let new_cond = info.rewrite_columns(node.predicate().clone());
                if is_true_scalar(&new_cond) {
                    new_input
                } else {
                    let mut rewritten_op = 
                        LogicalSelect::new(new_input, new_cond).into_operator();
                    let rule = LogicalSelectSimplifyRule::new();
                    let simplified = rule.transform(rewritten_op.as_ref(), ctx);
                    if let Ok(v) = simplified && !v.is_empty() {
                        rewritten_op = v[0].clone();
                    }
                    rewritten_op
                }
            }
            OperatorKind::LogicalProject(ref meta) => {
                let node = LogicalProject::borrow_raw_parts(meta, &op.common);
                let new_input = self.unnest_inner(
                    node.input().clone(),
                    info,
                    &child_accessing,
                    child_needs_domain,
                    seen_outer_refs,
                    ctx,
                );
                let new_exprs = info.rewrite_columns(node.projections().clone());
                let final_exprs = Self::add_repr_passthroughs_to_project(
                    info,
                    new_exprs,
                    &new_input.output_columns(ctx),
                );
                LogicalProject::new(new_input, final_exprs).into_operator()
            }
            OperatorKind::LogicalRemap(ref meta) => {
                let node = LogicalRemap::borrow_raw_parts(meta, &op.common);
                let new_input = self.unnest_inner(
                    node.input().clone(),
                    info,
                    &child_accessing,
                    child_needs_domain,
                    seen_outer_refs,
                    ctx,
                );
                let new_exprs = info.rewrite_columns(node.mappings().clone());
                let final_exprs = Self::add_repr_passthroughs_to_remap(
                    info,
                    new_exprs,
                    &new_input.output_columns(ctx),
                );
                LogicalRemap::new(new_input, final_exprs).into_operator()
            }
            OperatorKind::LogicalOrderBy(ref meta) => {
                let node = LogicalOrderBy::borrow_raw_parts(meta, &op.common);
                let new_input = self.unnest_inner(
                    node.input().clone(),
                    info,
                    &child_accessing,
                    child_needs_domain,
                    seen_outer_refs,
                    ctx,
                );
                let new_exprs = node
                    .exprs()
                    .iter()
                    .map(|e| info.rewrite_columns(e.clone()))
                    .collect::<Vec<_>>();
                Arc::new(op.clone_with_inputs(
                    Some(Arc::from(vec![new_input])),
                    Some(Arc::from(new_exprs)),
                ))
            }
            OperatorKind::LogicalAggregate(ref meta) => {
                let node = LogicalAggregate::borrow_raw_parts(meta, &op.common);
                let new_input = self.unnest_inner(
                    node.input().clone(),
                    info,
                    &child_accessing,
                    child_needs_domain,
                    seen_outer_refs,
                    ctx,
                );

                // The new expressions is based on a simple rewrite
                let new_exprs = info.rewrite_columns(node.exprs().clone());

                // The new keys is based on a rewrite, as well as the repr of
                // all outer columns
                let new_keys_scalar = info.rewrite_columns(node.keys().clone());
                let mut new_keys_vec = new_keys_scalar.input_scalars().to_vec();
                let mut key_cols: HashSet<Column> = new_keys_vec
                    .iter()
                    .filter_map(|s| s.try_borrow::<ColumnAssign>().ok().map(|a| *a.column()))
                    .collect();
                let mut outer_refs_vec: Vec<Column> = info.get_info().get_outer_refs().iter().copied().collect();
                outer_refs_vec.sort_by_key(|c| c.0);
                for c in &outer_refs_vec {
                    let rep = info.resolve_mapped_col(*c).unwrap_or(*c);
                    if key_cols.insert(rep) {
                        let key_expr = ColumnRef::new(rep).into_scalar();
                        new_keys_vec.push(ColumnAssign::new(rep, key_expr).into_scalar());
                    }
                }
                let final_keys = List::new(new_keys_vec.into()).into_scalar();

                let agg = LogicalAggregate::new(new_input, new_exprs, final_keys).into_operator();
                if node
                    .keys()
                    .try_borrow::<List>()
                    .unwrap()
                    .members()
                    .is_empty()
                {
                    // Special case for if there are no grouping columns
                    let mut conds = Vec::new();
                    for c in &outer_refs_vec {
                        let left_col = *info.get_info().get_domain_repr().get(c).unwrap_or(c);
                        let right_col = info.resolve_mapped_col(*c).unwrap_or(*c);
                        let left_ref = ColumnRef::new(left_col).into_scalar();
                        let right_ref = ColumnRef::new(right_col).into_scalar();
                        if left_ref == right_ref {
                            continue;
                        }
                        conds.push(
                            BinaryOp::new(BinaryOpKind::IsNotDistinctFrom, left_ref, right_ref)
                                .into_scalar(),
                        );
                    }
                    let join_cond = if conds.is_empty() {
                        crate::ir::scalar::Literal::boolean(true).into_scalar()
                    } else if conds.len() == 1 {
                        conds.pop().unwrap()
                    } else {
                        NaryOp::new(NaryOpKind::And, conds.into()).into_scalar()
                    };
                    LogicalJoin::new(JoinType::Left, info.get_info().get_domain_op().clone(), agg, join_cond)
                        .into_operator()
                } else {
                    agg
                }
            }
            OperatorKind::LogicalDependentJoin(ref meta) => {
                let dep = LogicalDependentJoin::borrow_raw_parts(meta, &op.common);
                self.d_join_elimination(dep, Some(info), Some(&child_accessing), ctx)
            }
            OperatorKind::LogicalJoin(ref meta) => {
                let node = LogicalJoin::borrow_raw_parts(meta, &op.common);
                let left = node.outer();
                let right = node.inner();

                // Split accessing into left and right subsets
                let left_accessing: HashSet<*const Operator> = child_accessing
                    .iter()
                    .filter(|&&op_ptr| is_contained_in(op_ptr, left))
                    .copied()
                    .collect();
                let right_accessing: HashSet<*const Operator> = child_accessing
                    .iter()
                    .filter(|&&op_ptr| is_contained_in(op_ptr, right))
                    .copied()
                    .collect();

                let join_type = *node.join_type();
                let outputs_unmatched_left = matches!(
                    join_type,
                    JoinType::Left | JoinType::Single | JoinType::Mark(_)
                );

                if right_accessing.is_empty() {
                    // No need to check for unmatched_right since we do not support
                    // JoinType::Right
                    let new_left =
                        self.unnest_inner(
                            left.clone(),
                            info,
                            &left_accessing,
                            child_needs_domain,
                            seen_outer_refs,
                            ctx,
                        );
                    let new_cond = info.rewrite_columns(node.join_cond().clone());
                    return LogicalJoin::new(join_type, new_left, right.clone(), new_cond)
                        .into_operator();
                }

                if left_accessing.is_empty() && !outputs_unmatched_left {
                    let new_right = self.unnest_inner(
                        right.clone(),
                        info,
                        &right_accessing,
                        child_needs_domain,
                        seen_outer_refs,
                        ctx,
                    );
                    let new_cond = info.rewrite_columns(node.join_cond().clone());
                    return LogicalJoin::new(join_type, left.clone(), new_right, new_cond)
                        .into_operator();
                }

                // Unnest both sides
                let mut left_info = Unnesting::new(info.get_info());
                let mut right_info = Unnesting::new(info.get_info());

                let new_left = self.unnest_inner(
                    left.clone(),
                    &mut left_info,
                    &left_accessing,
                    child_needs_domain,
                    seen_outer_refs.clone(),
                    ctx,
                );
                let new_right = self.unnest_inner(
                    right.clone(),
                    &mut right_info,
                    &right_accessing,
                    child_needs_domain,
                    seen_outer_refs,
                    ctx,
                );

                let new_cond = Self::rewrite_join_columns(
                    &left_info,
                    &right_info,
                    node.join_cond().clone(),
                    &new_left.output_columns(ctx),
                    &new_right.output_columns(ctx),
                    join_type,
                );

                let mut join_conds = Vec::new();
                if !is_true_scalar(&new_cond) {
                    join_conds.push(new_cond);
                }
                let mut outer_refs_vec: Vec<Column> = info.get_info().get_outer_refs().iter().copied().collect();
                outer_refs_vec.sort_by_key(|c| c.0);
                for c in &outer_refs_vec {
                    let left_repr = left_info.resolve_mapped_col(*c).unwrap_or(*c);
                    let right_repr = right_info.resolve_mapped_col(*c).unwrap_or(*c);
                    if left_repr != right_repr {
                        let left_ref = ColumnRef::new(left_repr).into_scalar();
                        let right_ref = ColumnRef::new(right_repr).into_scalar();
                        join_conds.push(
                            BinaryOp::new(BinaryOpKind::IsNotDistinctFrom, left_ref, right_ref)
                                .into_scalar(),
                        );
                    }
                }
                let final_cond = if join_conds.is_empty() {
                    crate::ir::scalar::Literal::boolean(true).into_scalar()
                } else if join_conds.len() == 1 {
                    join_conds.pop().unwrap()
                } else {
                    NaryOp::new(NaryOpKind::And, join_conds.into()).into_scalar()
                };

                info.merge_col_eq_from(&left_info);
                info.merge_col_eq_from(&right_info);

                for c in &outer_refs_vec {
                    let chosen = if outputs_unmatched_left {
                        left_info.get_repr_of(c)
                    } else {
                        left_info
                            .get_repr_of(c)
                            .or_else(|| right_info.get_repr_of(c))
                    };
                    if let Some(col) = chosen {
                        info.add_repr_of(*c, *col);
                    };
                }

                LogicalJoin::new(join_type, new_left, new_right, final_cond).into_operator()
            }
            _ => {
                panic!("UnnestingRule: Unsupported operator kind encountered");
            }
        }
    }
}
