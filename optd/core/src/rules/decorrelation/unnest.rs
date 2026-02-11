/// This module contains the unnesting strategy for individual operators
use std::collections::HashSet;
use std::sync::Arc;

use crate::ir::convert::{IntoOperator, IntoScalar};
use crate::ir::operator::LogicalDependentJoin;
use crate::ir::operator::{
    LogicalAggregate, LogicalJoin, LogicalOrderBy, LogicalProject, LogicalRemap, LogicalSelect,
    Operator, OperatorKind, join::JoinType,
};
use crate::ir::scalar::{
    BinaryOp, BinaryOpKind, ColumnAssign, ColumnRef, List, NaryOp, NaryOpKind,
};
use crate::ir::{Column, Scalar, ScalarKind};
use crate::ir::{ColumnSet, IRContext};

use super::UnnestingRule;
use super::helpers::{Unnesting, is_contained_in, is_true_scalar};

impl UnnestingRule {
    fn simplify_select(op: Arc<Operator>) -> Arc<Operator> {
        let OperatorKind::LogicalSelect(select_meta) = &op.kind else {
            return op;
        };
        let select = LogicalSelect::borrow_raw_parts(select_meta, &op.common);
        let predicate = select.predicate().clone();
        if is_true_scalar(&predicate) {
            return select.input().clone();
        }

        let OperatorKind::LogicalJoin(join_meta) = &select.input().kind else {
            return op;
        };
        let join = LogicalJoin::borrow_raw_parts(join_meta, &select.input().common);
        if !matches!(join.join_type(), &JoinType::Inner) {
            return op;
        }

        let join_cond = join.join_cond().clone();
        let merged_cond = if is_true_scalar(&join_cond) {
            predicate
        } else {
            NaryOp::new(NaryOpKind::And, vec![join_cond, predicate].into()).into_scalar()
        };
        LogicalJoin::new(
            JoinType::Inner,
            join.outer().clone(),
            join.inner().clone(),
            merged_cond,
        )
        .into_operator()
    }

    fn collect_non_bound_outer_refs(
        op: &Arc<Operator>,
        info: &Unnesting<'_>,
        ctx: &IRContext,
    ) -> HashSet<Column> {
        let provided_by_inputs = op
            .input_operators()
            .into_iter()
            .fold(ColumnSet::default(), |acc, child| {
                acc | &child.output_columns(ctx)
            });
        let mut refs = HashSet::new();
        for scalar in op.input_scalars() {
            for col in scalar.used_columns().iter() {
                if info.is_outer_ref_recursive(col) && !provided_by_inputs.contains(col) {
                    refs.insert(*col);
                }
            }
        }
        refs
    }

    // Returns the subset of domain operators that are still required based on
    // current column representatives.
    pub(super) fn required_domain_ops(un: &Unnesting, seen_outer_refs: &HashSet<Column>) -> Vec<Arc<Operator>> {
        fn collect_required_domain_ops_from_info<'a>(
            un: &Unnesting<'a>,
            seen_outer_refs: &HashSet<Column>,
            required_domain_ops: &mut Vec<Arc<Operator>>,
        ) {
            if un.info.domain.0.iter().any(|(outer_ref, domain_col)| {
                seen_outer_refs.contains(outer_ref)
                    && un.resolve_col(*outer_ref) == Some(*domain_col)
            }) {
                required_domain_ops.push(un.info.domain.1.clone());
            }
            if let Some(parent) = un.info.parent {
                collect_required_domain_ops_from_info(
                    &parent,
                    seen_outer_refs,
                    required_domain_ops,
                );
            }
        }

        let mut required_domain_ops = Vec::new();
        collect_required_domain_ops_from_info(
            un,
            seen_outer_refs,
            &mut required_domain_ops,
        );
        required_domain_ops
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
                let in_left = left_cols.contains(&cr.column);
                let in_right = right_cols.contains(&cr.column);

                if in_left ^ in_right {
                    ColumnRef::new(cr.column).into_scalar()
                } else {
                    let mapping = {
                        let left_mapped = left.resolve_col(cr.column);
                        let right_mapped = right.resolve_col(cr.column);

                        match (left_mapped, right_mapped) {
                            (Some(l), Some(r)) if l == r => Some(l),
                            (Some(l), Some(r)) => {
                                if matches!(join_type, JoinType::Inner)
                                    && left.is_outer_ref_recursive(&l)
                                    && !right.is_outer_ref_recursive(&r)
                                {
                                    Some(r)
                                } else {
                                    Some(l)
                                }
                            }
                            (Some(l), None) => Some(l),
                            (None, Some(r)) => Some(r),
                            (None, None) => None,
                        }
                    };

                    if let Some(mapped) = mapping {
                        ColumnRef::new(mapped).into_scalar()
                    } else {
                        ColumnRef::new(cr.column).into_scalar()
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
        needs_domain: bool,
        mut seen_outer_refs: HashSet<Column>,
        ctx: &IRContext,
    ) -> Arc<Operator> {
        seen_outer_refs.extend(Self::collect_non_bound_outer_refs(&op, info, ctx));

        // Terminal Condition: if no accessing operators remain in this subtree, stop here.
        if accessing.is_empty() {
            let input_available = op
                .input_operators()
                .into_iter()
                .fold(ColumnSet::default(), |acc, child| {
                    acc | &child.output_columns(ctx)
                });
            info.update_repr_with_available(&input_available);
            let new_scalars: Vec<Arc<Scalar>> = op
                .input_scalars()
                .iter()
                .map(|s| info.rewrite_columns(s.clone()))
                .collect();
            let rewritten_op = Self::simplify_select(Arc::new(
                op.clone_with_inputs(None, Some(Arc::from(new_scalars))),
            ));

            // Re-check availability at this node after rewriting; if this resolves all
            // domain representatives, we can skip injecting D here.
            info.update_repr_with_available(&rewritten_op.output_columns(ctx));
            let mut required_domains = Self::required_domain_ops(info, &seen_outer_refs);
            if needs_domain && !required_domains.is_empty() {
                let mut domain_op = required_domains.remove(0);
                for next_domain in required_domains {
                    domain_op = LogicalJoin::new(
                        JoinType::Inner,
                        domain_op,
                        next_domain,
                        crate::ir::scalar::Literal::boolean(true).into_scalar(),
                    )
                    .into_operator();
                }
                return LogicalJoin::new(
                    JoinType::Inner,
                    domain_op,
                    rewritten_op,
                    crate::ir::scalar::Literal::boolean(true).into_scalar(),
                )
                .into_operator();
            }
            return rewritten_op;
        }

        // Remove this node from the accessing set
        let mut child_accessing = accessing.clone();
        child_accessing.remove(&Arc::as_ptr(&op));
        let child_needs_domain = needs_domain || accessing.contains(&Arc::as_ptr(&op));

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
                info.update_repr_with_available(&new_input.output_columns(ctx));
                let new_cond = info.rewrite_columns(node.predicate().clone());
                if is_true_scalar(&new_cond) {
                    new_input
                } else {
                    Self::simplify_select(LogicalSelect::new(new_input, new_cond).into_operator())
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
                info.update_repr_with_available(&new_input.output_columns(ctx));
                let new_exprs = info.rewrite_columns(node.projections().clone());
                LogicalProject::new(new_input, new_exprs).into_operator()
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
                info.update_repr_with_available(&new_input.output_columns(ctx));
                let new_exprs = info.rewrite_columns(node.mappings().clone());
                LogicalRemap::new(new_input, new_exprs).into_operator()
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
                info.update_repr_with_available(&new_input.output_columns(ctx));
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
                info.update_repr_with_available(&new_input.output_columns(ctx));

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
                let mut outer_refs_vec: Vec<Column> = info.info.outer_refs.iter().copied().collect();
                outer_refs_vec.sort_by_key(|c| c.0);
                for c in &outer_refs_vec {
                    let rep = *info.repr.get(c).unwrap_or(c);
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
                        let left_col = *info.info.domain.0.get(c).unwrap_or(c);
                        let right_col = *info.repr.get(c).unwrap_or(c);
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
                    LogicalJoin::new(JoinType::Left, info.info.domain.1.clone(), agg, join_cond)
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
                    info.update_repr_with_available(&new_left.output_columns(ctx));
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
                    info.update_repr_with_available(&new_right.output_columns(ctx));
                    let new_cond = info.rewrite_columns(node.join_cond().clone());
                    return LogicalJoin::new(join_type, left.clone(), new_right, new_cond)
                        .into_operator();
                }

                // Unnest both sides
                let mut left_info = Unnesting::new(info.info.clone());
                let mut right_info = Unnesting::new(info.info.clone());

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

                left_info.update_repr_with_available(&new_left.output_columns(ctx));
                right_info.update_repr_with_available(&new_right.output_columns(ctx));
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
                let mut outer_refs_vec: Vec<Column> = info.info.outer_refs.iter().copied().collect();
                outer_refs_vec.sort_by_key(|c| c.0);
                for c in &outer_refs_vec {
                    let left_repr = left_info.repr.get(c).copied().unwrap_or(*c);
                    let right_repr = right_info.repr.get(c).copied().unwrap_or(*c);
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
                        left_info.repr.get(c).copied().unwrap_or(*c)
                    } else {
                        left_info
                            .resolve_col(*c)
                            .or_else(|| right_info.resolve_col(*c))
                            .unwrap_or(*c)
                    };
                    info.repr.insert(*c, chosen);
                }

                LogicalJoin::new(join_type, new_left, new_right, final_cond).into_operator()
            }
            _ => {
                panic!("UnnestingRule: Unsupported operator kind encountered");
            }
        }
    }
}
