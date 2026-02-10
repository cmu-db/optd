/// This module contains the unnesting strategy for individual operators

use std::collections::HashSet;
use std::sync::Arc;

use crate::ir::{ColumnSet, IRContext};
use crate::ir::convert::{IntoOperator, IntoScalar};
use crate::ir::operator::{
    LogicalAggregate, LogicalJoin, LogicalOrderBy, LogicalProject, LogicalRemap, LogicalSelect,
    Operator, OperatorKind, join::JoinType,
};
use crate::ir::scalar::{BinaryOp, BinaryOpKind, ColumnAssign, ColumnRef, List, NaryOp, NaryOpKind};
use crate::ir::{Column, Scalar, ScalarKind, ScalarValue};

use super::UnnestingRule;
use super::helpers::UnnestingInfo;

impl UnnestingRule {
    fn is_true_scalar(scalar: &Arc<Scalar>) -> bool {
        matches!(
            &scalar.kind,
            ScalarKind::Literal(meta) if matches!(meta.value, ScalarValue::Boolean(Some(true)))
        )
    }

    fn simplify_select(op: Arc<Operator>) -> Arc<Operator> {
        let OperatorKind::LogicalSelect(select_meta) = &op.kind else {
            return op;
        };
        let select = LogicalSelect::borrow_raw_parts(select_meta, &op.common);
        let predicate = select.predicate().clone();
        if Self::is_true_scalar(&predicate) {
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
        let merged_cond = if Self::is_true_scalar(&join_cond) {
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

    /// Check if an operator (by pointer) is contained within a subtree.
    pub(super) fn is_contained_in(op_ptr: *const Operator, subtree: &Arc<Operator>) -> bool {
        if Arc::as_ptr(subtree) == op_ptr {
            return true;
        }
        subtree.input_operators().iter().any(|c| Self::is_contained_in(op_ptr, c))
    }

    pub(super) fn unnest(
        &self,
        op: Arc<Operator>,
        info: &mut UnnestingInfo,
        accessing: &HashSet<*const Operator>,
        needs_domain: bool,
        ctx: &IRContext,
    ) -> Arc<Operator> {
        // If no accessing operators remain in this subtree, stop here.
        if accessing.is_empty() {
            let input_available = op
                .input_operators()
                .into_iter()
                .fold(ColumnSet::default(), |acc, child| acc | &child.output_columns(ctx));
            info.update_repr_with_available(&input_available);
            let new_scalars: Vec<Arc<Scalar>> = op
                .input_scalars()
                .iter()
                .map(|s| info.rewrite_columns(s.clone()))
                .collect();
            let rewritten_op =
                Self::simplify_select(Arc::new(op.clone_with_inputs(None, Some(Arc::from(new_scalars)))));

            // Re-check availability at this node after rewriting; if this resolves all
            // domain representatives, we can skip injecting D here.
            info.update_repr_with_available(&rewritten_op.output_columns(ctx));
            let needs_local_domain = info.needs_domain();
            let needs_parent_domain = info.needs_parent_domain();
            if needs_domain && (needs_local_domain || needs_parent_domain) {
                let domain_op = match (needs_local_domain, needs_parent_domain) {
                    (true, true) => {
                        let parent_domain = info
                            .parent_domain_op
                            .clone()
                            .expect("parent domain op missing despite required parent domain");
                        LogicalJoin::new(
                            JoinType::Inner,
                            info.domain_op.clone(),
                            parent_domain,
                            crate::ir::scalar::Literal::boolean(true).into_scalar(),
                        )
                        .into_operator()
                    }
                    (true, false) => info.domain_op.clone(),
                    (false, true) => info
                        .parent_domain_op
                        .clone()
                        .expect("parent domain op missing despite required parent domain"),
                    (false, false) => unreachable!(),
                };
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
                info.update_equivalences(node.predicate());
                let new_input = self.unnest(node.input().clone(), info, &child_accessing, child_needs_domain, ctx);
                info.update_repr_with_available(&new_input.output_columns(ctx));
                let new_cond = info.rewrite_columns(node.predicate().clone());
                if Self::is_true_scalar(&new_cond) {
                    new_input
                } else {
                    Self::simplify_select(LogicalSelect::new(new_input, new_cond).into_operator())
                }
            }
            OperatorKind::LogicalProject(ref meta) => {
                let node = LogicalProject::borrow_raw_parts(meta, &op.common);
                let new_input = self.unnest(node.input().clone(), info, &child_accessing, child_needs_domain, ctx);
                info.update_repr_with_available(&new_input.output_columns(ctx));
                let new_exprs = info.rewrite_columns(node.projections().clone());
                LogicalProject::new(new_input, new_exprs).into_operator()
            }
            OperatorKind::LogicalRemap(ref meta) => {
                let node = LogicalRemap::borrow_raw_parts(meta, &op.common);
                let new_input = self.unnest(node.input().clone(), info, &child_accessing, child_needs_domain, ctx);
                info.update_repr_with_available(&new_input.output_columns(ctx));
                let new_exprs = info.rewrite_columns(node.mappings().clone());
                LogicalRemap::new(new_input, new_exprs).into_operator()
            }
            OperatorKind::LogicalOrderBy(ref meta) => {
                let node = LogicalOrderBy::borrow_raw_parts(meta, &op.common);
                let new_input = self.unnest(node.input().clone(), info, &child_accessing, child_needs_domain, ctx);
                info.update_repr_with_available(&new_input.output_columns(ctx));
                let new_exprs = node.exprs()
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
                let new_input = self.unnest(node.input().clone(), info, &child_accessing, child_needs_domain, ctx);
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
                let mut outer_refs_vec: Vec<Column> = info.outer_refs.iter().copied().collect();
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
                if node.keys().try_borrow::<List>().unwrap().members().is_empty() {
                    // Special case for if there are no grouping columns
                    let mut conds = Vec::new();
                    for c in &outer_refs_vec {
                        let left_col = *info.domain_repr.get(c).unwrap_or(c);
                        let right_col = *info.repr.get(c).unwrap_or(c);
                        let left_ref = ColumnRef::new(left_col).into_scalar();
                        let right_ref = ColumnRef::new(right_col).into_scalar();
                        if left_ref == right_ref {
                            continue;
                        }
                        conds.push(BinaryOp::new(BinaryOpKind::IsNotDistinctFrom, left_ref, right_ref).into_scalar());
                    }
                    let join_cond = if conds.is_empty() {
                        crate::ir::scalar::Literal::boolean(true).into_scalar()
                    } else if conds.len() == 1 {
                        conds.pop().unwrap()
                    } else {
                        NaryOp::new(NaryOpKind::And, conds.into()).into_scalar()
                    };
                    LogicalJoin::new(JoinType::Left, info.domain_op.clone(), agg, join_cond)
                        .into_operator()
                } else {
                    agg
                }
            }
            OperatorKind::LogicalDependentJoin(_) => {
                self.d_join_elimination(op.clone(), Some(info), ctx)
            }
            OperatorKind::LogicalJoin(ref meta) => {
                let node = LogicalJoin::borrow_raw_parts(meta, &op.common);
                let left = node.outer();
                let right = node.inner();

                // Split accessing into left and right subsets
                let left_accessing: HashSet<*const Operator> = child_accessing
                    .iter()
                    .filter(|&&op_ptr| Self::is_contained_in(op_ptr, left))
                    .copied()
                    .collect();
                let right_accessing: HashSet<*const Operator> = child_accessing
                    .iter()
                    .filter(|&&op_ptr| Self::is_contained_in(op_ptr, right))
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
                        self.unnest(left.clone(), info, &left_accessing, child_needs_domain, ctx);
                    info.update_repr_with_available(&new_left.output_columns(ctx));
                    let new_cond = info.rewrite_columns(node.join_cond().clone());
                    return LogicalJoin::new(join_type, new_left, right.clone(), new_cond)
                        .into_operator();
                }

                if left_accessing.is_empty() && !outputs_unmatched_left {
                    let new_right = 
                        self.unnest(right.clone(), info, &right_accessing, child_needs_domain, ctx);
                    info.update_repr_with_available(&new_right.output_columns(ctx));
                    let new_cond = info.rewrite_columns(node.join_cond().clone());
                    return LogicalJoin::new(join_type, left.clone(), new_right, new_cond)
                        .into_operator();
                }

                // Unnest both sides
                let mut left_info = info.fork_for_join_branch();
                let mut right_info = info.fork_for_join_branch();

                let new_left = self.unnest(
                    left.clone(),
                    &mut left_info,
                    &left_accessing,
                    child_needs_domain,
                    ctx,
                );
                let new_right = self.unnest(
                    right.clone(),
                    &mut right_info,
                    &right_accessing,
                    child_needs_domain,
                    ctx,
                );

                left_info.update_repr_with_available(&new_left.output_columns(ctx));
                right_info.update_repr_with_available(&new_right.output_columns(ctx));
                let new_cond = left_info.rewrite_join_columns(
                    node.join_cond().clone(),
                    &right_info,
                    &new_left.output_columns(ctx),
                    &new_right.output_columns(ctx),
                    join_type,
                );

                let mut join_conds = Vec::new();
                if !Self::is_true_scalar(&new_cond) {
                    join_conds.push(new_cond);
                }
                let mut outer_refs_vec: Vec<Column> = info.outer_refs.iter().copied().collect();
                outer_refs_vec.sort_by_key(|c| c.0);
                for c in &outer_refs_vec {
                    let left_repr = left_info.resolve_mapped_column(*c);
                    let right_repr = right_info.resolve_mapped_column(*c);
                    if left_repr != right_repr {
                        let left_ref = ColumnRef::new(left_repr).into_scalar();
                        let right_ref = ColumnRef::new(right_repr).into_scalar();
                        join_conds.push(BinaryOp::new(BinaryOpKind::IsNotDistinctFrom, left_ref, right_ref).into_scalar());
                    }
                }
                let final_cond = if join_conds.is_empty() {
                    crate::ir::scalar::Literal::boolean(true).into_scalar()
                } else if join_conds.len() == 1 {
                    join_conds.pop().unwrap()
                } else {
                    NaryOp::new(NaryOpKind::And, join_conds.into()).into_scalar()
                };

                info.merge_from(&left_info);
                info.merge_from(&right_info);

                for c in &outer_refs_vec {
                    let chosen = if outputs_unmatched_left {
                        left_info.resolve_mapped_column(*c)
                    } else {
                        left_info
                            .repr
                            .get(c)
                            .copied()
                            .or_else(|| right_info.repr.get(c).copied())
                            .or_else(|| left_info.parent_repr.get(c).copied())
                            .or_else(|| right_info.parent_repr.get(c).copied())
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
