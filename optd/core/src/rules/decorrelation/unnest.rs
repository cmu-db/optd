/// This module contains the unnesting strategy for individual operators
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use snafu::whatever;

use crate::error::Result;
use crate::ir::convert::{IntoOperator, IntoScalar};
use crate::ir::operator::{
    Aggregate, DependentJoin, Join, Operator, OperatorKind, OrderBy, Project, Remap, Select,
    join::JoinType,
};
use crate::ir::rule::Rule;
use crate::ir::scalar::{BinaryOp, BinaryOpKind, ColumnRef, List};
use crate::ir::{Column, ColumnSet, IRContext, Scalar, ScalarKind};
use crate::rules::decorrelation::helpers::UnnestingInfo;
use crate::rules::logical_select_simplify::LogicalSelectSimplifyRule;

use super::UnnestingRule;
use super::helpers::{Unnesting, is_contained_in, remap_right_output_collisions};

impl UnnestingRule {
    fn build_domain_chain<'a>(
        info: &UnnestingInfo<'a>,
        unnesting: &Unnesting<'_>,
        seen_outer_refs: &HashSet<Column>,
        include_resolved_refs: bool,
    ) -> Option<Arc<Operator>> {
        fn get_required_domain_ops<'a>(
            info: &UnnestingInfo<'a>,
            unnesting: &Unnesting<'_>,
            seen_outer_refs: &HashSet<Column>,
            include_resolved_refs: bool,
            required_domain_ops: &mut Vec<Arc<Operator>>,
        ) {
            if info.get_domain_repr_set().keys().any(|v| {
                seen_outer_refs.contains(v)
                    && (include_resolved_refs || !unnesting.has_resolved_repr_for(v))
            }) {
                required_domain_ops.push(info.get_domain_op());
            }
            if let Some(parent) = info.get_parent() {
                get_required_domain_ops(
                    parent.get_info().as_ref(),
                    unnesting,
                    seen_outer_refs,
                    include_resolved_refs,
                    required_domain_ops,
                );
            }
        }

        let mut domains = Vec::new();
        get_required_domain_ops(
            info,
            unnesting,
            seen_outer_refs,
            include_resolved_refs,
            &mut domains,
        );
        if domains.is_empty() {
            return None;
        }
        let mut chain = domains.remove(0);
        for next_domain in domains {
            chain = Join::new(
                JoinType::Inner,
                chain,
                next_domain,
                crate::ir::scalar::Literal::boolean(true).into_scalar(),
                None,
            )
            .into_operator();
        }
        Some(chain)
    }

    fn build_domain<'a>(
        info: &UnnestingInfo<'a>,
        unnesting: &Unnesting<'_>,
        seen_outer_refs: &HashSet<Column>,
        op: Arc<Operator>,
    ) -> Arc<Operator> {
        let Some(chain) = Self::build_domain_chain(info, unnesting, seen_outer_refs, false) else {
            return op;
        };
        Join::new(
            JoinType::Inner,
            chain,
            op,
            crate::ir::scalar::Literal::boolean(true).into_scalar(),
            None,
        )
        .into_operator()
    }

    /// Collect all currently-resolved representative columns that should be
    /// preserved across projection/remap boundaries.
    fn collect_preserved_reprs(info: &Unnesting<'_>) -> Vec<Column> {
        let mut reps = Vec::new();
        for outer_col in info.collect_outer_refs_recursive() {
            if let Some(resolved) = info.resolve_col(outer_col) {
                reps.push(resolved);
            }
        }
        reps.sort();
        reps.dedup();
        reps
    }

    // Helper function to rewrite columns, specifically for join conditions
    // (i.e. unnesting a non-dependent join), since it has different semantics.
    pub(super) fn rewrite_join_columns(
        left: &Unnesting<'_>,
        right: &Unnesting<'_>,
        right_remap: &HashMap<Column, Column>,
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
                    let l_res = left.resolve_col(cr.column);
                    let r_res = right
                        .resolve_col(cr.column)
                        .map(|col| right_remap.get(&col).copied().unwrap_or(col));
                    match (l_res, r_res) {
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
                            right_remap,
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
    ) -> Result<Arc<Operator>> {
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
    ) -> Result<Arc<Operator>> {
        // Terminal Condition: if no accessing operators remain in this subtree,
        // stop here. Choose representative columns / domain-joins for each
        // outer-ref
        if accessing.is_empty() {
            let available = op.output_columns(ctx)?;
            let mut available_cols: Vec<Column> = available.iter().copied().collect();
            available_cols.sort();
            let mut seen_outer_refs_vec: Vec<Column> = seen_outer_refs.iter().copied().collect();
            seen_outer_refs_vec.retain(|c| info.is_outer_ref_recursive(c));
            seen_outer_refs_vec.sort();
            let mut chosen_reprs: Vec<(Column, Column)> = Vec::new();
            let mut can_resolve_all = true;
            for c in &seen_outer_refs_vec {
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

            // TODO: The paper claims that we can use a heuristic optimisation
            // here to decide whether to use cclass equivalent columns, or
            // inject a domain join. Current policy is all-or-nothing: if all
            // seen refs can be resolved, use repr for all; otherwise do pure
            // domain injection.
            if can_resolve_all {
                for (outer_col, repr_col) in chosen_reprs {
                    info.set_scoped_repr_of(outer_col, repr_col);
                }
            } else {
                for c in &seen_outer_refs_vec {
                    info.clear_scoped_repr_of(c);
                }
            }

            return Ok(Self::build_domain(
                info.get_info().as_ref(),
                info,
                seen_outer_refs,
                op,
            ));
        }

        // Compute what outer refs this operator accesses
        let available: ColumnSet = op
            .input_operators()
            .iter()
            .try_fold(ColumnSet::default(), |acc, child| {
                Ok(acc | child.output_columns(ctx)?.as_ref())
            })?;
        for scalar in op.input_scalars() {
            seen_outer_refs.extend((scalar.used_columns() - &available).as_hash_set());
        }
        let mut child_accessing = accessing.clone();
        child_accessing.remove(&Arc::as_ptr(&op));

        // Then unnest this operator
        match &op.kind {
            OperatorKind::Select(meta) => {
                let node = Select::borrow_raw_parts(meta, &op.common);
                info.update_cclasses_equivalences(node.predicate());
                let new_input = self.unnest_inner(
                    node.input().clone(),
                    info,
                    &child_accessing,
                    seen_outer_refs,
                    ctx,
                )?;
                let new_cond = info.rewrite_columns(node.predicate().clone());
                if new_cond.is_true_scalar() {
                    Ok(new_input)
                } else {
                    let mut rewritten_op = Select::new(new_input, new_cond).into_operator();
                    let rule = LogicalSelectSimplifyRule::new();
                    let simplified = rule.transform(rewritten_op.as_ref(), ctx);
                    if let Ok(v) = simplified
                        && !v.is_empty()
                    {
                        rewritten_op = v[0].clone();
                    }
                    Ok(rewritten_op)
                }
            }
            OperatorKind::Project(meta) => {
                let node = Project::borrow_raw_parts(meta, &op.common);
                let new_input = self.unnest_inner(
                    node.input().clone(),
                    info,
                    &child_accessing,
                    seen_outer_refs,
                    ctx,
                )?;
                let new_exprs = info.rewrite_columns(node.projections().clone());
                let mut members = new_exprs.try_borrow::<List>().unwrap().members().to_vec();
                let original_projection_len = members.len();
                let mut passthrough_cols: HashSet<Column> = members
                    .iter()
                    .filter_map(|expr| expr.try_borrow::<ColumnRef>().ok().map(|col| *col.column()))
                    .collect();
                let new_input_output_cols = new_input.output_columns(ctx)?;
                for rep in Self::collect_preserved_reprs(info) {
                    if new_input_output_cols.contains(&rep) && passthrough_cols.insert(rep) {
                        members.push(ColumnRef::new(rep).into_scalar());
                    }
                }
                let projected = ctx.project(new_input, members.clone())?.build();
                let project_table_index = *projected.borrow::<Project>().table_index();
                let output_rewrites = (0..original_projection_len)
                    .map(|idx| {
                        (
                            Column(*node.table_index(), idx),
                            Column(project_table_index, idx),
                        )
                    })
                    .collect::<HashMap<_, _>>();
                info.record_column_rewrites(&output_rewrites);
                let passthrough_mapping = members
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, expr)| {
                        expr.try_borrow::<ColumnRef>()
                            .ok()
                            .map(|col_ref| (*col_ref.column(), Column(project_table_index, idx)))
                    })
                    .collect::<HashMap<_, _>>();
                info.propagate_passthrough_mapping(&passthrough_mapping);
                Ok(projected)
            }
            OperatorKind::Remap(meta) => {
                let node = Remap::borrow_raw_parts(meta, &op.common);
                let new_input = self.unnest_inner(
                    node.input().clone(),
                    info,
                    &child_accessing,
                    seen_outer_refs,
                    ctx,
                )?;
                let ordered_input_cols = new_input.output_columns_in_order(ctx)?;
                let table_index =
                    ctx.add_binding(None, new_input.output_schema(ctx)?.inner().clone())?;
                let remapped = Remap::new(table_index, new_input).into_operator();
                let passthrough_mapping = ordered_input_cols
                    .into_iter()
                    .enumerate()
                    .map(|(idx, col)| (col, Column(table_index, idx)))
                    .collect::<HashMap<_, _>>();
                info.propagate_passthrough_mapping(&passthrough_mapping);
                Ok(remapped)
            }
            OperatorKind::OrderBy(meta) => {
                let node = OrderBy::borrow_raw_parts(meta, &op.common);
                let new_input = self.unnest_inner(
                    node.input().clone(),
                    info,
                    &child_accessing,
                    seen_outer_refs,
                    ctx,
                )?;
                let new_exprs = node
                    .exprs()
                    .iter()
                    .map(|e| info.rewrite_columns(e.clone()))
                    .collect::<Vec<_>>();
                Ok(Arc::new(op.clone_with_inputs(
                    Some(Arc::from(vec![new_input])),
                    Some(Arc::from(new_exprs)),
                )))
            }
            OperatorKind::Aggregate(meta) => {
                let node = Aggregate::borrow_raw_parts(meta, &op.common);
                let new_input = self.unnest_inner(
                    node.input().clone(),
                    info,
                    &child_accessing,
                    seen_outer_refs,
                    ctx,
                )?;

                let new_exprs = info.rewrite_columns(node.exprs().clone());
                let new_exprs_vec = new_exprs.borrow::<List>().members().to_vec();
                let new_keys_scalar = info.rewrite_columns(node.keys().clone());
                let mut new_keys_vec = new_keys_scalar.borrow::<List>().members().to_vec();
                let mut key_cols: HashSet<Column> = new_keys_vec
                    .iter()
                    .filter_map(|s| s.try_borrow::<ColumnRef>().ok().map(|a| *a.column()))
                    .collect();
                let input_cols = new_input.output_columns(ctx)?;
                let mut outer_refs_vec = info.collect_outer_refs_recursive();
                outer_refs_vec.retain(|c| {
                    info.resolve_col(*c)
                        .map(|rep| input_cols.contains(&rep))
                        .unwrap_or(false)
                });
                for c in &outer_refs_vec {
                    let rep = info.resolve_col(*c).unwrap();
                    if key_cols.insert(rep) {
                        new_keys_vec.push(ColumnRef::new(rep).into_scalar());
                    }
                }
                let agg = new_input
                    .with_ctx(ctx)
                    .logical_aggregate(new_exprs_vec.clone(), new_keys_vec.clone())?
                    .build();
                let (key_table_index, aggregate_table_index) = {
                    let new_agg = agg.borrow::<Aggregate>();
                    (*new_agg.key_table_index(), *new_agg.aggregate_table_index())
                };
                let output_rewrites = (0..new_exprs_vec.len())
                    .map(|idx| {
                        (
                            Column(*node.aggregate_table_index(), idx),
                            Column(aggregate_table_index, idx),
                        )
                    })
                    .collect::<HashMap<_, _>>();
                info.record_column_rewrites(&output_rewrites);
                let passthrough_mapping = new_keys_vec
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, expr)| {
                        expr.try_borrow::<ColumnRef>()
                            .ok()
                            .map(|col_ref| (*col_ref.column(), Column(key_table_index, idx)))
                    })
                    .collect::<HashMap<_, _>>();
                info.propagate_passthrough_mapping(&passthrough_mapping);

                if node
                    .keys()
                    .try_borrow::<List>()
                    .unwrap()
                    .members()
                    .is_empty()
                {
                    // Special case for if there are no grouping columns
                    let outer_refs_set: HashSet<Column> = outer_refs_vec.iter().copied().collect();
                    let domain_input = Self::build_domain_chain(
                        info.get_info().as_ref(),
                        info,
                        &outer_refs_set,
                        true,
                    )
                    .unwrap_or_else(|| info.get_info().get_domain_op());
                    let domain_output_cols = domain_input.output_columns(ctx)?;
                    let (agg, _agg_remap) =
                        remap_right_output_collisions(domain_output_cols.as_ref(), agg, info, ctx)?;

                    let mut conds = Vec::new();
                    for c in &outer_refs_vec {
                        let left_col = info.resolve_domain_repr_recursive(*c).unwrap_or(*c);
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
                    let join_cond = Scalar::combine_conjuncts(conds);
                    for c in &outer_refs_vec {
                        if info.resolve_domain_repr_recursive(*c).is_some() {
                            info.clear_scoped_repr_of(c);
                        }
                    }
                    Ok(
                        Join::new(JoinType::LeftOuter, domain_input, agg, join_cond, None)
                            .into_operator(),
                    )
                } else {
                    Ok(agg)
                }
            }
            OperatorKind::DependentJoin(meta) => {
                let dep = DependentJoin::borrow_raw_parts(meta, &op.common);
                let unnested =
                    self.d_join_elimination(dep, Some(info), Some(&child_accessing), ctx)?;
                let dep_output_cols = unnested.output_columns(ctx)?;

                // Nested dependent-join elimination may choose representatives
                // for only a subset of seen outer refs. Any remaining refs must
                // still carry their domain joins at this boundary.
                let unresolved_outer_refs: HashSet<Column> = seen_outer_refs
                    .iter()
                    .copied()
                    .filter(|c| {
                        if let Some(repr) = info.get_resolved_repr_of(c)
                            && dep_output_cols.contains(repr)
                        {
                            return false;
                        }
                        if let Some(domain_repr) = info.resolve_domain_repr_recursive(*c)
                            && dep_output_cols.contains(&domain_repr)
                        {
                            info.clear_scoped_repr_of(c);
                            return false;
                        }
                        true
                    })
                    .collect();
                if !unresolved_outer_refs.is_empty() {
                    Ok(Self::build_domain(
                        info.get_info().as_ref(),
                        info,
                        &unresolved_outer_refs,
                        unnested,
                    ))
                } else {
                    Ok(unnested)
                }
            }
            OperatorKind::Join(meta) => {
                let node = Join::borrow_raw_parts(meta, &op.common);
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
                    JoinType::LeftOuter | JoinType::Single | JoinType::Mark(_)
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
                    )?;
                    let new_cond = info.rewrite_columns(node.join_cond().clone());
                    return Ok(
                        Join::new(join_type, new_left, right.clone(), new_cond, None)
                            .into_operator(),
                    );
                }

                if left_accessing.is_empty() && !outputs_unmatched_left {
                    let new_right = self.unnest_inner(
                        right.clone(),
                        info,
                        &right_accessing,
                        seen_outer_refs,
                        ctx,
                    )?;
                    let new_cond = info.rewrite_columns(node.join_cond().clone());
                    return Ok(
                        Join::new(join_type, left.clone(), new_right, new_cond, None)
                            .into_operator(),
                    );
                }

                // Unnest both sides
                let mut left_info = Unnesting::new(info.get_info());
                let mut right_info = Unnesting::new(info.get_info());
                let mut right_seen_refs = HashSet::new();
                let new_right = self.unnest_inner(
                    right.clone(),
                    &mut right_info,
                    &right_accessing,
                    &mut right_seen_refs,
                    ctx,
                )?;
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
                )?;

                // If both join inputs expose the same column id, right-side
                // columns would collapse in the join output. Remap right-side
                // collisions to fresh columns so we can add explicit equality.
                let left_output_cols = new_left.output_columns(ctx)?;
                let (new_right, right_remap) = remap_right_output_collisions(
                    left_output_cols.as_ref(),
                    new_right,
                    &mut right_info,
                    ctx,
                )?;

                let right_output_cols = new_right.output_columns(ctx)?;
                let new_cond = Self::rewrite_join_columns(
                    &left_info,
                    &right_info,
                    &right_remap,
                    node.join_cond().clone(),
                    left_output_cols.as_ref(),
                    right_output_cols.as_ref(),
                    join_type,
                );

                let mut join_conds = Vec::new();
                if !new_cond.is_true_scalar() {
                    join_conds.push(new_cond);
                }
                let mut propagated_refs = info.collect_outer_refs_recursive().clone();
                propagated_refs.sort();
                propagated_refs.dedup();
                propagated_refs.retain(|c| {
                    let left_repr = left_info
                        .resolve_col(*c)
                        .filter(|col| left_output_cols.contains(col));
                    let right_repr = right_info
                        .resolve_col(*c)
                        .map(|col: Column| right_remap.get(&col).copied().unwrap_or(col))
                        .filter(|col| right_output_cols.contains(col));
                    left_repr.is_some() && right_repr.is_some()
                });
                for c in &propagated_refs {
                    let left_repr = left_info.resolve_col(*c).unwrap();
                    let right_repr = right_info.resolve_col(*c).unwrap();
                    assert!(left_repr != right_repr);
                    let left_ref = ColumnRef::new(left_repr).into_scalar();
                    let right_ref = ColumnRef::new(right_repr).into_scalar();
                    join_conds.push(
                        BinaryOp::new(BinaryOpKind::IsNotDistinctFrom, left_ref, right_ref)
                            .into_scalar(),
                    );
                }

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

                Ok(Join::new(
                    join_type,
                    new_left,
                    new_right,
                    Scalar::combine_conjuncts(join_conds),
                    None,
                )
                .into_operator())
            }
            k => {
                whatever!(
                    "UnnestingRule: Unsupported operator kind {:?} encountered",
                    k
                );
            }
        }
    }
}
