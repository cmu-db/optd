/// This module contains the unnesting strategy for individual operators
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::ir::convert::{IntoOperator, IntoScalar};
use crate::ir::operator::{
    LogicalAggregate, LogicalDependentJoin, LogicalJoin, LogicalOrderBy, LogicalProject,
    LogicalRemap, LogicalSelect, Operator, OperatorKind, join::JoinType,
};
use crate::ir::rule::Rule;
use crate::ir::scalar::{
    BinaryOp, BinaryOpKind, ColumnAssign, ColumnRef, List, NaryOp, NaryOpKind,
};
use crate::ir::{Column, ColumnSet, IRContext, Scalar, ScalarKind};
use crate::rules::decorrelation::helpers::UnnestingInfo;
use crate::rules::logical_select_simplify::LogicalSelectSimplifyRule;

use super::UnnestingRule;
use super::helpers::{Unnesting, is_contained_in};

impl UnnestingRule {
    fn get_required_domain_ops<'a>(
        info: &UnnestingInfo<'a>,
        unnesting: &Unnesting<'_>,
        seen_outer_refs: &HashSet<Column>,
        required_domain_ops: &mut Vec<Arc<Operator>>,
    ) {
        if info
            .get_domain_repr_set()
            .keys()
            .any(|v| seen_outer_refs.contains(v) && !unnesting.has_resolved_repr_for(v))
        {
            required_domain_ops.push(info.get_domain_op());
        }
        if let Some(parent) = info.get_parent() {
            Self::get_required_domain_ops(
                parent.get_info().as_ref(),
                unnesting,
                seen_outer_refs,
                required_domain_ops,
            );
        }
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
                    match (left.resolve_col(cr.column), right.resolve_col(cr.column)) {
                        (Some(l), Some(r)) => {
                            if matches!(join_type, JoinType::Inner)
                                && left.is_outer_ref_recursive(&l)
                                && !right.is_outer_ref_recursive(&r)
                            {
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
        let mut seen_refs = HashSet::new();
        self.unnest_inner(op, info, accessing, &mut seen_refs, ctx)
    }

    fn unnest_inner(
        &self,
        op: Arc<Operator>,
        info: &mut Unnesting<'_>,
        accessing: &HashSet<*const Operator>,
        seen_outer_refs: &mut HashSet<Column>,
        ctx: &IRContext,
    ) -> Arc<Operator> {
        // Terminal Condition: if no accessing operators remain in this subtree, stop here.
        if accessing.is_empty() {
            // Choose representative columns for each outer-ref
            // NOTE: The paper claims that we can use a heuristic optimisation
            // here to decide whether to use cclass equivalent columns, or
            // inject a domain join.
            // Current policy is all-or-nothing: if all seen refs can be
            // rewritten, use repr for all; otherwise do pure domain injection.
            let available: &ColumnSet = &op.output_columns(ctx);
            let mut available_cols: Vec<Column> = available.iter().copied().collect();
            available_cols.sort_by_key(|col| col.0);
            let mut seen_outer_refs_vec: Vec<Column> = seen_outer_refs.iter().copied().collect();
            seen_outer_refs_vec.sort_by_key(|col| col.0);
            let mut chosen_reprs: Vec<(Column, Column)> = Vec::new();
            let mut can_resolve_all = true;
            for c in &seen_outer_refs_vec {
                assert!(info.is_outer_ref_recursive(c));
                let target_class = info.get_cclass_of(c);
                if let Some(chosen) = available_cols
                    .iter()
                    .copied()
                    .find(|candidate| info.get_cclass_of(candidate) == target_class)
                {
                    chosen_reprs.push((*c, chosen));
                } else {
                    can_resolve_all = false;
                    break;
                }
            }
            if can_resolve_all {
                for (outer_col, repr_col) in chosen_reprs {
                    info.set_scoped_repr_of(outer_col, repr_col);
                }
            } else {
                for c in &seen_outer_refs_vec {
                    info.clear_scoped_repr_of(c);
                }
            }

            // Get the required domain operators
            let mut required_domains = Vec::new();
            Self::get_required_domain_ops(
                info.get_info().as_ref(),
                info,
                &seen_outer_refs,
                &mut required_domains,
            );
            if !required_domains.is_empty() {
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

        // Compute what outer refs this operator accesses
        let available: ColumnSet = op
            .input_operators()
            .into_iter()
            .fold(ColumnSet::default(), |acc, child| {
                acc | &child.output_columns(ctx)
            });
        for scalar in op.input_scalars() {
            seen_outer_refs.extend((scalar.used_columns() - &available).as_hash_set());
        }
        let mut child_accessing = accessing.clone();
        child_accessing.remove(&Arc::as_ptr(&op));

        // Then unnest this operator
        match op.kind {
            OperatorKind::LogicalSelect(ref meta) => {
                let node = LogicalSelect::borrow_raw_parts(meta, &op.common);
                info.update_cclasses_equivalences(node.predicate());
                let new_input = self.unnest_inner(
                    node.input().clone(),
                    info,
                    &child_accessing,
                    seen_outer_refs,
                    ctx,
                );
                let new_cond = info.rewrite_columns(node.predicate().clone());
                if new_cond.is_true_scalar() {
                    new_input
                } else {
                    let mut rewritten_op = LogicalSelect::new(new_input, new_cond).into_operator();
                    let rule = LogicalSelectSimplifyRule::new();
                    let simplified = rule.transform(rewritten_op.as_ref(), ctx);
                    if let Ok(v) = simplified
                        && !v.is_empty()
                    {
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
                    seen_outer_refs,
                    ctx,
                );
                let new_exprs = info.rewrite_columns(node.projections().clone());
                let mut members = new_exprs.try_borrow::<List>().unwrap().members().to_vec();
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
                let reps = info.get_resolved_repr_values();
                for rep in reps {
                    if new_input.output_columns(ctx).contains(&rep) && output_cols.insert(rep) {
                        members.push(ColumnRef::new(rep).into_scalar());
                    }
                }
                let final_exprs = List::new(members.into()).into_scalar();
                LogicalProject::new(new_input, final_exprs).into_operator()
            }
            OperatorKind::LogicalRemap(ref meta) => {
                let node = LogicalRemap::borrow_raw_parts(meta, &op.common);
                let new_input = self.unnest_inner(
                    node.input().clone(),
                    info,
                    &child_accessing,
                    seen_outer_refs,
                    ctx,
                );
                let new_exprs = info.rewrite_columns(node.mappings().clone());
                let mut members = new_exprs.try_borrow::<List>().unwrap().members().to_vec();
                let mut output_cols: HashSet<Column> = members
                    .iter()
                    .filter_map(|expr| {
                        expr.try_borrow::<ColumnAssign>()
                            .ok()
                            .map(|assign| *assign.column())
                    })
                    .collect();
                let reps = info.get_resolved_repr_values();
                for rep in reps {
                    if new_input.output_columns(ctx).contains(&rep) && output_cols.insert(rep) {
                        members.push(
                            ColumnAssign::new(rep, ColumnRef::new(rep).into_scalar()).into_scalar(),
                        );
                    }
                }
                let final_exprs = List::new(members.into()).into_scalar();
                LogicalRemap::new(new_input, final_exprs).into_operator()
            }
            OperatorKind::LogicalOrderBy(ref meta) => {
                let node = LogicalOrderBy::borrow_raw_parts(meta, &op.common);
                let new_input = self.unnest_inner(
                    node.input().clone(),
                    info,
                    &child_accessing,
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
                let mut outer_refs_vec: Vec<Column> =
                    info.get_info().get_outer_refs().iter().copied().collect();
                outer_refs_vec.sort_by_key(|c| c.0);
                for c in &outer_refs_vec {
                    let rep = info.resolve_col(*c).unwrap_or(*c);
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
                        let left_col = info.get_info().get_domain_repr_of(c).unwrap_or(*c);
                        let right_col = info.resolve_col(*c).unwrap_or(*c);
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
                    for c in &outer_refs_vec {
                        if let Some(_) = info.get_info().get_domain_repr_of(c) {
                            info.clear_scoped_repr_of(c);
                        }
                    }
                    LogicalJoin::new(
                        JoinType::Left,
                        info.get_info().get_domain_op(),
                        agg,
                        join_cond,
                    )
                    .into_operator()
                } else {
                    agg
                }
            }
            OperatorKind::LogicalDependentJoin(ref meta) => {
                let dep = LogicalDependentJoin::borrow_raw_parts(meta, &op.common);
                let mut unnested =
                    self.d_join_elimination(dep, Some(info), Some(&child_accessing), ctx);
                let dep_output_cols = unnested.output_columns(ctx);

                // Nested dependent-join elimination can choose representatives
                // for only a subset of seen outer refs. Any remaining refs must
                // still carry their domain joins at this boundary.
                let unresolved_outer_refs: HashSet<Column> = seen_outer_refs
                    .iter()
                    .copied()
                    .filter(|c| {
                        if !info.is_outer_ref_recursive(c) || dep_output_cols.contains(c) {
                            return false;
                        }
                        if let Some(repr) = info.get_resolved_repr_of(c)
                            && dep_output_cols.contains(repr)
                        {
                            return false;
                        }
                        if let Some(domain_repr) = info.resolve_domain_repr_recursive(*c)
                            && dep_output_cols.contains(&domain_repr)
                        {
                            info.set_scoped_repr_of(*c, domain_repr);
                            return false;
                        }
                        true
                    })
                    .collect();
                if !unresolved_outer_refs.is_empty() {
                    let mut required_domains = Vec::new();
                    Self::get_required_domain_ops(
                        info.get_info().as_ref(),
                        info,
                        &unresolved_outer_refs,
                        &mut required_domains,
                    );
                    if !required_domains.is_empty() {
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
                        unnested = LogicalJoin::new(
                            JoinType::Inner,
                            domain_chain,
                            unnested,
                            crate::ir::scalar::Literal::boolean(true).into_scalar(),
                        )
                        .into_operator();
                    }
                }

                unnested
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
                    let new_left = self.unnest_inner(
                        left.clone(),
                        info,
                        &left_accessing,
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
                let mut right_seen_refs = HashSet::new();
                let mut new_right = self.unnest_inner(
                    right.clone(),
                    &mut right_info,
                    &right_accessing,
                    &mut right_seen_refs,
                    ctx,
                );
                let mut left_needed_refs = if outputs_unmatched_left {
                    // There are two cases here: if the join is not an inner join,
                    // we will only propagate up the domain cols and repr cols
                    // from the left, thus the left side needs to inject all
                    // seen_outer_refs
                    seen_outer_refs.clone()
                } else {
                    // If the join is an inner join, we will propagate up domain
                    // cols and repr cols from both branches, so we only need to
                    // inject the domains the right branch has not seen
                    seen_outer_refs
                        .difference(&right_seen_refs)
                        .cloned()
                        .collect()
                };
                let new_left = self.unnest_inner(
                    left.clone(),
                    &mut left_info,
                    &left_accessing,
                    &mut left_needed_refs,
                    ctx,
                );

                // If both join inputs expose the same column id, right-side
                // columns would collapse in the join output. Remap right-side
                // collisions to fresh columns so we can add explicit equality.
                let left_output_cols = new_left.output_columns(ctx);
                let right_output_cols = new_right.output_columns(ctx);
                let mut right_remap: HashMap<Column, Column> = HashMap::new();
                let mut remap_members = Vec::new();
                let mut right_cols_vec: Vec<Column> = right_output_cols.iter().copied().collect();
                right_cols_vec.sort_by_key(|c| c.0);
                for col in right_cols_vec {
                    let out_col = if left_output_cols.contains(&col) {
                        let fresh =
                            ctx.define_column(ctx.get_column_meta(&col).data_type.clone(), None);
                        right_remap.insert(col, fresh);
                        fresh
                    } else {
                        col
                    };
                    remap_members.push(
                        ColumnAssign::new(out_col, ColumnRef::new(col).into_scalar()).into_scalar(),
                    );
                }
                if !right_remap.is_empty() {
                    let remap_list = List::new(remap_members.into()).into_scalar();
                    new_right = LogicalRemap::new(new_right, remap_list).into_operator();
                    right_info.remap_repr_values(&right_remap);
                }

                let new_cond = Self::rewrite_join_columns(
                    &left_info,
                    &right_info,
                    node.join_cond().clone(),
                    &left_output_cols,
                    &new_right.output_columns(ctx),
                    join_type,
                );

                let mut join_conds = Vec::new();
                if !new_cond.is_true_scalar() {
                    join_conds.push(new_cond);
                }
                let mut outer_refs_vec: Vec<Column> =
                    info.get_info().get_outer_refs().iter().copied().collect();
                outer_refs_vec.sort_by_key(|c| c.0);
                let mut nested_refs_vec: Vec<Column> = left_info
                    .get_nested_repr_set()
                    .keys()
                    .chain(right_info.get_nested_repr_set().keys())
                    .copied()
                    .collect();
                nested_refs_vec.sort_by_key(|c| c.0);
                nested_refs_vec.dedup_by_key(|c| c.0);
                let mut propagated_refs = outer_refs_vec.clone();
                propagated_refs.extend(nested_refs_vec);
                propagated_refs.sort_by_key(|c| c.0);
                propagated_refs.dedup_by_key(|c| c.0);
                for c in &propagated_refs {
                    let left_repr = left_info.resolve_col(*c).unwrap_or(*c);
                    let right_repr = right_info
                        .resolve_col(*c)
                        .map(|col: Column| right_remap.get(&col).copied().unwrap_or(col))
                        .unwrap_or(*c);
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
                if !outputs_unmatched_left {
                    info.merge_col_eq_from(&right_info);
                }

                for c in &propagated_refs {
                    let left_resolved = left_info.get_resolved_repr_of(c).copied();
                    let right_resolved = right_info
                        .get_resolved_repr_of(c)
                        .copied()
                        .map(|col| right_remap.get(&col).copied().unwrap_or(col));
                    let chosen = if outputs_unmatched_left {
                        left_resolved
                    } else {
                        left_resolved.or(right_resolved)
                    };
                    if let Some(col) = chosen {
                        info.set_scoped_repr_of(*c, col);
                    } else {
                        info.clear_scoped_repr_of(c);
                    }
                }

                LogicalJoin::new(join_type, new_left, new_right, final_cond).into_operator()
            }
            _ => {
                panic!("UnnestingRule: Unsupported operator kind encountered");
            }
        }
    }
}
