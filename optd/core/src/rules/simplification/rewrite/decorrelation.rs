//! Simplification rules for plans produced by decorrelating static scalar
//! aggregates.
//!
//! Neumann-style static aggregate decorrelation introduces a distinct domain
//! relation for the correlated outer keys, then left-joins that domain to the
//! aggregate value side so unmatched outer keys can still produce a NULL
//! aggregate result.
//!
//! The cleanup here is intentionally ordered:
//!
//! 1. `EliminateNullRejectedLeftOuterJoinRulePass` walks null-rejection facts
//!    down the plan. A predicate above the decorrelated aggregate may reject the
//!    NULL-extended aggregate result, which makes the decorrelation-introduced
//!    `LeftOuter` join equivalent to an `Inner` join. Those facts are propagated
//!    through null-propagating projections, so a rejection on `Project(x + 1)`
//!    rejects `x`, while a rejection on `coalesce(x, 0)` does not.
//! 2. `RemoveRedundantDomainJoinRulePass` then looks for the resulting
//!    `Project(Domain INNER JOIN AggregateValue)` shape. Once the join is inner,
//!    the domain no longer needs to preserve rows for missing aggregate groups.
//!    If every domain key is equated to a value-side aggregate key and the
//!    parent join already binds those projected domain keys back to the original
//!    outer columns, the domain side is redundant and can be removed.
//!
//! `RemoveRedundantDomainJoinRulePass` relies on
//! `EliminateNullRejectedLeftOuterJoinRulePass` having run first; otherwise it
//! could remove the domain side while it is still preserving scalar aggregate
//! NULL-extension semantics.

use super::super::{
    rule::RulePass,
    scalar::{combine_conjuncts_simplified, split_conjuncts, substitute_columns},
};
use crate::{
    error::Result,
    ir::{
        Column, ColumnSet, IRContext, Operator, Scalar, ScalarKind,
        convert::{IntoOperator, IntoScalar},
        operator::{Aggregate, Join, Project, Select, join::JoinType},
        scalar::{BinaryOp, BinaryOpKind, Cast, ColumnRef, InList, IsNotNull, Like, List, NaryOp},
    },
};
use std::{collections::HashMap, sync::Arc};

/// Converts decorrelation-introduced `LeftOuter` joins to `Inner` joins when an
/// ancestor predicate rejects the null-extended side.
pub struct EliminateNullRejectedLeftOuterJoinRulePass;

/// Removes the distinct-domain side introduced for static aggregate unnesting
/// once a parent inner join already binds the domain keys to the original
/// outer-side columns.
pub struct RemoveRedundantDomainJoinRulePass;

impl RulePass for EliminateNullRejectedLeftOuterJoinRulePass {
    fn apply(&self, root: Arc<Operator>, ctx: &IRContext) -> Result<Arc<Operator>> {
        eliminate_null_rejected_left_outer_joins(root, ctx, &ColumnSet::default())
    }
}

impl RulePass for RemoveRedundantDomainJoinRulePass {
    fn apply(&self, root: Arc<Operator>, ctx: &IRContext) -> Result<Arc<Operator>> {
        rewrite_redundant_domain_joins(root, ctx)
    }
}

fn eliminate_null_rejected_left_outer_joins(
    op: Arc<Operator>,
    ctx: &IRContext,
    rejected_outputs: &ColumnSet,
) -> Result<Arc<Operator>> {
    if let Ok(select) = op.try_borrow::<Select>() {
        let input_rejections =
            rejected_outputs | &predicate_null_rejecting_columns(select.predicate());
        let new_input = eliminate_null_rejected_left_outer_joins(
            select.input().clone(),
            ctx,
            &input_rejections,
        )?;
        return Ok(if new_input == *select.input() {
            op
        } else {
            Select::new(new_input, select.predicate().clone()).into_operator()
        });
    }

    if let Ok(project) = op.try_borrow::<Project>() {
        let input_rejections = rejected_project_input_columns(&project, rejected_outputs);
        let new_input = eliminate_null_rejected_left_outer_joins(
            project.input().clone(),
            ctx,
            &input_rejections,
        )?;
        return Ok(if new_input == *project.input() {
            op
        } else {
            Project::new(
                *project.table_index(),
                new_input,
                project.projections().clone(),
            )
            .into_operator()
        });
    }

    if let Ok(join) = op.try_borrow::<Join>() {
        let outer_cols = join.outer().output_columns(ctx)?;
        let inner_cols = join.inner().output_columns(ctx)?;
        let mut join_type = *join.join_type();
        let mut outer_rejections = rejected_outputs & outer_cols.as_ref();
        let mut inner_rejections = rejected_outputs & inner_cols.as_ref();

        if join_type == JoinType::LeftOuter && !inner_rejections.is_empty() {
            join_type = JoinType::Inner;
        }

        if join_type == JoinType::Inner {
            let join_rejections = predicate_null_rejecting_columns(join.join_cond());
            outer_rejections |= &(join_rejections.clone() & outer_cols.as_ref());
            inner_rejections |= &(join_rejections & inner_cols.as_ref());
        }

        let new_outer =
            eliminate_null_rejected_left_outer_joins(join.outer().clone(), ctx, &outer_rejections)?;
        let new_inner =
            eliminate_null_rejected_left_outer_joins(join.inner().clone(), ctx, &inner_rejections)?;

        return Ok(
            if new_outer == *join.outer()
                && new_inner == *join.inner()
                && join_type == *join.join_type()
            {
                op
            } else {
                Join::new(
                    join_type,
                    new_outer,
                    new_inner,
                    join.join_cond().clone(),
                    join.implementation().clone(),
                )
                .into_operator()
            },
        );
    }

    let rewritten_inputs = op
        .input_operators()
        .iter()
        .map(|input| {
            eliminate_null_rejected_left_outer_joins(input.clone(), ctx, &ColumnSet::default())
        })
        .collect::<Result<Vec<_>>>()?;

    if rewritten_inputs.as_slice() == op.input_operators() {
        Ok(op)
    } else {
        Ok(Arc::new(op.clone_with_inputs(
            Some(Arc::from(rewritten_inputs)),
            None,
        )))
    }
}

fn rejected_project_input_columns(
    project: &crate::ir::operator::ProjectBorrowed<'_>,
    rejected_outputs: &ColumnSet,
) -> ColumnSet {
    let Ok(projections) = project.projections().try_borrow::<List>() else {
        return ColumnSet::default();
    };

    let mut rejected_inputs = ColumnSet::default();
    for (idx, projection) in projections.members().iter().enumerate() {
        if rejected_outputs.contains(&Column(*project.table_index(), idx)) {
            rejected_inputs |= &null_propagating_columns(projection);
        }
    }
    rejected_inputs
}

fn predicate_null_rejecting_columns(predicate: &Arc<Scalar>) -> ColumnSet {
    match &predicate.kind {
        ScalarKind::BinaryOp(meta) => {
            let binary = BinaryOp::borrow_raw_parts(meta, &predicate.common);
            match binary.op_kind() {
                BinaryOpKind::Eq
                | BinaryOpKind::Ne
                | BinaryOpKind::Lt
                | BinaryOpKind::Le
                | BinaryOpKind::Gt
                | BinaryOpKind::Ge
                | BinaryOpKind::RegexMatch
                | BinaryOpKind::RegexIMatch
                | BinaryOpKind::RegexNotMatch
                | BinaryOpKind::RegexNotIMatch
                | BinaryOpKind::LikeMatch
                | BinaryOpKind::ILikeMatch
                | BinaryOpKind::NotLikeMatch
                | BinaryOpKind::NotILikeMatch => {
                    null_propagating_columns(binary.lhs()) | &null_propagating_columns(binary.rhs())
                }
                BinaryOpKind::IsDistinctFrom | BinaryOpKind::IsNotDistinctFrom => {
                    ColumnSet::default()
                }
                _ => ColumnSet::default(),
            }
        }
        ScalarKind::NaryOp(meta) => {
            let nary = NaryOp::borrow_raw_parts(meta, &predicate.common);
            if nary.is_and() {
                nary.terms()
                    .iter()
                    .fold(ColumnSet::default(), |mut acc, term| {
                        acc |= &predicate_null_rejecting_columns(term);
                        acc
                    })
            } else {
                let mut terms = nary.terms().iter();
                let Some(first) = terms.next() else {
                    return ColumnSet::default();
                };
                terms.fold(predicate_null_rejecting_columns(first), |acc, term| {
                    acc & &predicate_null_rejecting_columns(term)
                })
            }
        }
        ScalarKind::IsNotNull(meta) => {
            let is_not_null = IsNotNull::borrow_raw_parts(meta, &predicate.common);
            null_propagating_columns(is_not_null.expr())
        }
        ScalarKind::Like(meta) => {
            let like = Like::borrow_raw_parts(meta, &predicate.common);
            null_propagating_columns(like.expr()) | &null_propagating_columns(like.pattern())
        }
        ScalarKind::InList(meta) => {
            let in_list = InList::borrow_raw_parts(meta, &predicate.common);
            null_propagating_columns(in_list.expr()) | &null_propagating_columns(in_list.list())
        }
        ScalarKind::Literal(_)
        | ScalarKind::ColumnRef(_)
        | ScalarKind::List(_)
        | ScalarKind::Function(_)
        | ScalarKind::Cast(_)
        | ScalarKind::IsNull(_)
        | ScalarKind::Case(_) => ColumnSet::default(),
    }
}

fn null_propagating_columns(scalar: &Arc<Scalar>) -> ColumnSet {
    match &scalar.kind {
        ScalarKind::ColumnRef(meta) => {
            let column_ref = ColumnRef::borrow_raw_parts(meta, &scalar.common);
            std::iter::once(*column_ref.column()).collect()
        }
        ScalarKind::Cast(meta) => {
            let cast = Cast::borrow_raw_parts(meta, &scalar.common);
            null_propagating_columns(cast.expr())
        }
        ScalarKind::BinaryOp(meta) => {
            let binary = BinaryOp::borrow_raw_parts(meta, &scalar.common);
            match binary.op_kind() {
                BinaryOpKind::Plus
                | BinaryOpKind::Minus
                | BinaryOpKind::Multiply
                | BinaryOpKind::Divide
                | BinaryOpKind::Modulo
                | BinaryOpKind::Eq
                | BinaryOpKind::Ne
                | BinaryOpKind::Lt
                | BinaryOpKind::Le
                | BinaryOpKind::Gt
                | BinaryOpKind::Ge
                | BinaryOpKind::RegexMatch
                | BinaryOpKind::RegexIMatch
                | BinaryOpKind::RegexNotMatch
                | BinaryOpKind::RegexNotIMatch
                | BinaryOpKind::LikeMatch
                | BinaryOpKind::ILikeMatch
                | BinaryOpKind::NotLikeMatch
                | BinaryOpKind::NotILikeMatch
                | BinaryOpKind::BitwiseAnd
                | BinaryOpKind::BitwiseOr
                | BinaryOpKind::BitwiseXor
                | BinaryOpKind::BitwiseShiftRight
                | BinaryOpKind::BitwiseShiftLeft
                | BinaryOpKind::StringConcat
                | BinaryOpKind::AtArrow
                | BinaryOpKind::ArrowAt
                | BinaryOpKind::Arrow
                | BinaryOpKind::LongArrow
                | BinaryOpKind::HashArrow
                | BinaryOpKind::HashLongArrow
                | BinaryOpKind::AtAt
                | BinaryOpKind::IntegerDivide
                | BinaryOpKind::HashMinus
                | BinaryOpKind::AtQuestion
                | BinaryOpKind::Question
                | BinaryOpKind::QuestionAnd
                | BinaryOpKind::QuestionPipe => {
                    null_propagating_columns(binary.lhs()) | &null_propagating_columns(binary.rhs())
                }
                BinaryOpKind::IsDistinctFrom | BinaryOpKind::IsNotDistinctFrom => {
                    ColumnSet::default()
                }
            }
        }
        ScalarKind::Like(meta) => {
            let like = Like::borrow_raw_parts(meta, &scalar.common);
            null_propagating_columns(like.expr()) | &null_propagating_columns(like.pattern())
        }
        ScalarKind::InList(meta) => {
            let in_list = InList::borrow_raw_parts(meta, &scalar.common);
            null_propagating_columns(in_list.expr()) | &null_propagating_columns(in_list.list())
        }
        ScalarKind::Literal(_)
        | ScalarKind::NaryOp(_)
        | ScalarKind::List(_)
        | ScalarKind::Function(_)
        | ScalarKind::IsNull(_)
        | ScalarKind::IsNotNull(_)
        | ScalarKind::Case(_) => ColumnSet::default(),
    }
}

fn rewrite_redundant_domain_joins(op: Arc<Operator>, ctx: &IRContext) -> Result<Arc<Operator>> {
    let rewritten_inputs = op
        .input_operators()
        .iter()
        .map(|input| rewrite_redundant_domain_joins(input.clone(), ctx))
        .collect::<Result<Vec<_>>>()?;

    let op = if rewritten_inputs.as_slice() == op.input_operators() {
        op
    } else {
        Arc::new(op.clone_with_inputs(Some(Arc::from(rewritten_inputs)), None))
    };

    let Ok(join) = op.try_borrow::<Join>() else {
        return Ok(op);
    };
    if join.join_type() != &JoinType::Inner {
        return Ok(op);
    }

    let mut new_outer = join.outer().clone();
    let mut new_inner = join.inner().clone();
    let mut parent_cond = join.join_cond().clone();
    let mut changed = false;

    if let Some(rewrite) =
        try_remove_domain_from_project(join.outer(), join.inner(), join.join_cond(), ctx)?
    {
        new_outer = rewrite.input;
        parent_cond = rewrite_parent_domain_key_conds(parent_cond, &rewrite.parent_key_bindings);
        changed = true;
    }

    if let Some(rewrite) =
        try_remove_domain_from_project(join.inner(), join.outer(), &parent_cond, ctx)?
    {
        new_inner = rewrite.input;
        parent_cond = rewrite_parent_domain_key_conds(parent_cond, &rewrite.parent_key_bindings);
        changed = true;
    }

    Ok(if changed {
        Join::new(
            JoinType::Inner,
            new_outer,
            new_inner,
            parent_cond,
            join.implementation().clone(),
        )
        .into_operator()
    } else {
        op
    })
}

struct DomainProjectRewrite {
    input: Arc<Operator>,
    parent_key_bindings: Vec<(Column, Column)>,
}

fn try_remove_domain_from_project(
    candidate: &Arc<Operator>,
    parent_other: &Arc<Operator>,
    parent_cond: &Arc<Scalar>,
    ctx: &IRContext,
) -> Result<Option<DomainProjectRewrite>> {
    let Ok(project) = candidate.try_borrow::<Project>() else {
        return Ok(None);
    };
    let Ok(domain_join) = project.input().try_borrow::<Join>() else {
        return Ok(None);
    };
    if domain_join.join_type() != &JoinType::Inner {
        return Ok(None);
    }

    let outer_domain_rewrite = try_remove_domain_from_project_join_side(
        &project,
        domain_join.outer(),
        domain_join.inner(),
        domain_join.join_cond(),
        parent_other,
        parent_cond,
        ctx,
    )?;
    if outer_domain_rewrite.is_some() {
        return Ok(outer_domain_rewrite);
    }

    try_remove_domain_from_project_join_side(
        &project,
        domain_join.inner(),
        domain_join.outer(),
        domain_join.join_cond(),
        parent_other,
        parent_cond,
        ctx,
    )
}

fn try_remove_domain_from_project_join_side(
    project: &crate::ir::operator::ProjectBorrowed<'_>,
    domain: &Arc<Operator>,
    value: &Arc<Operator>,
    domain_join_cond: &Arc<Scalar>,
    parent_other: &Arc<Operator>,
    parent_cond: &Arc<Scalar>,
    ctx: &IRContext,
) -> Result<Option<DomainProjectRewrite>> {
    let Some(domain_key_sources) = domain_key_sources(domain)? else {
        return Ok(None);
    };
    // This is the post-unnesting shape for static scalar aggregates:
    // Project(D ⋈ Agg), joined above to the original outer input. Once the
    // left outer join has become inner, D is redundant if every D key is both
    // equated to an aggregate key and bound by the parent join to its original
    // outer column.
    let domain_cols = domain.output_columns(ctx)?;
    let value_cols = value.output_columns(ctx)?;
    let Some(domain_to_value) = domain_to_value_key_substitutions(
        domain_join_cond,
        domain_cols.as_ref(),
        value_cols.as_ref(),
    ) else {
        return Ok(None);
    };
    if !domain_cols
        .iter()
        .all(|domain_col| domain_to_value.contains_key(domain_col))
    {
        return Ok(None);
    }

    let Ok(projections) = project.projections().try_borrow::<List>() else {
        return Ok(None);
    };
    if projections.members().iter().any(|projection| {
        !(projection.used_columns() & domain_cols.as_ref()).is_empty()
            && !projection
                .used_columns()
                .iter()
                .all(|column| !domain_cols.contains(column) || domain_to_value.contains_key(column))
    }) {
        return Ok(None);
    }

    let project_domain_keys =
        projected_domain_key_bindings(project, projections.members(), &domain_key_sources);
    if project_domain_keys.len() != domain_key_sources.len() {
        return Ok(None);
    }

    let parent_other_cols = parent_other.output_columns(ctx)?;
    let mut parent_key_bindings = Vec::with_capacity(project_domain_keys.len());
    for (project_col, source_col) in project_domain_keys {
        if !parent_cond_binds_project_key(
            parent_cond,
            project_col,
            source_col,
            parent_other_cols.as_ref(),
        ) {
            return Ok(None);
        }
        parent_key_bindings.push((project_col, source_col));
    }

    let substitutions = domain_to_value
        .into_iter()
        .map(|(domain_col, value_col)| (domain_col, ColumnRef::new(value_col).into_scalar()))
        .collect::<HashMap<_, _>>();
    let new_projections = substitute_columns(project.projections().clone(), &substitutions);
    Ok(Some(DomainProjectRewrite {
        input: Project::new(*project.table_index(), value.clone(), new_projections).into_operator(),
        parent_key_bindings,
    }))
}

fn domain_key_sources(domain: &Arc<Operator>) -> Result<Option<HashMap<Column, Column>>> {
    let Ok(aggregate) = domain.try_borrow::<Aggregate>() else {
        return Ok(None);
    };
    let Ok(exprs) = aggregate.exprs().try_borrow::<List>() else {
        return Ok(None);
    };
    if !exprs.members().is_empty() {
        return Ok(None);
    }

    let Ok(keys) = aggregate.keys().try_borrow::<List>() else {
        return Ok(None);
    };
    let mut sources = HashMap::with_capacity(keys.members().len());
    for (idx, key) in keys.members().iter().enumerate() {
        let Ok(column_ref) = key.try_borrow::<ColumnRef>() else {
            return Ok(None);
        };
        sources.insert(
            Column(*aggregate.key_table_index(), idx),
            *column_ref.column(),
        );
    }
    Ok(Some(sources))
}

fn domain_to_value_key_substitutions(
    join_cond: &Arc<Scalar>,
    domain_cols: &ColumnSet,
    value_cols: &ColumnSet,
) -> Option<HashMap<Column, Column>> {
    let mut substitutions = HashMap::new();
    for conjunct in split_conjuncts(join_cond.clone()) {
        let Some((lhs, rhs, _)) = binary_column_comparison(&conjunct) else {
            continue;
        };
        let mapping = match (
            domain_cols.contains(&lhs),
            value_cols.contains(&lhs),
            domain_cols.contains(&rhs),
            value_cols.contains(&rhs),
        ) {
            (true, false, false, true) => Some((lhs, rhs)),
            (false, true, true, false) => Some((rhs, lhs)),
            _ => None,
        };
        if let Some((domain_col, value_col)) = mapping {
            match substitutions.insert(domain_col, value_col) {
                Some(existing) if existing != value_col => return None,
                _ => {}
            }
        }
    }
    Some(substitutions)
}

fn projected_domain_key_bindings(
    project: &crate::ir::operator::ProjectBorrowed<'_>,
    projections: &[Arc<Scalar>],
    domain_key_sources: &HashMap<Column, Column>,
) -> Vec<(Column, Column)> {
    projections
        .iter()
        .enumerate()
        .filter_map(|(idx, projection)| {
            let column_ref = projection.try_borrow::<ColumnRef>().ok()?;
            let source_col = domain_key_sources.get(column_ref.column())?;
            Some((Column(*project.table_index(), idx), *source_col))
        })
        .collect()
}

fn parent_cond_binds_project_key(
    parent_cond: &Arc<Scalar>,
    project_col: Column,
    source_col: Column,
    parent_other_cols: &ColumnSet,
) -> bool {
    parent_other_cols.contains(&source_col)
        && split_conjuncts(parent_cond.clone())
            .into_iter()
            .any(|conjunct| {
                let Some((lhs, rhs, _)) = binary_column_comparison(&conjunct) else {
                    return false;
                };
                (lhs == project_col && rhs == source_col)
                    || (lhs == source_col && rhs == project_col)
            })
}

fn rewrite_parent_domain_key_conds(
    parent_cond: Arc<Scalar>,
    parent_key_bindings: &[(Column, Column)],
) -> Arc<Scalar> {
    let rewritten = split_conjuncts(parent_cond)
        .into_iter()
        .map(|conjunct| {
            let Some((lhs, rhs, op_kind)) = binary_column_comparison(&conjunct) else {
                return conjunct;
            };
            if op_kind == BinaryOpKind::IsNotDistinctFrom
                && parent_key_bindings.iter().any(|(project_col, source_col)| {
                    (lhs == *project_col && rhs == *source_col)
                        || (lhs == *source_col && rhs == *project_col)
                })
            {
                BinaryOp::new(
                    BinaryOpKind::Eq,
                    ColumnRef::new(lhs).into_scalar(),
                    ColumnRef::new(rhs).into_scalar(),
                )
                .into_scalar()
            } else {
                conjunct
            }
        })
        .collect();
    combine_conjuncts_simplified(rewritten)
}

fn binary_column_comparison(scalar: &Arc<Scalar>) -> Option<(Column, Column, BinaryOpKind)> {
    let binary = scalar.try_borrow::<BinaryOp>().ok()?;
    if !matches!(
        binary.op_kind(),
        BinaryOpKind::Eq | BinaryOpKind::IsNotDistinctFrom
    ) {
        return None;
    }
    let lhs = binary.lhs().try_borrow::<ColumnRef>().ok()?;
    let rhs = binary.rhs().try_borrow::<ColumnRef>().ok()?;
    Some((*lhs.column(), *rhs.column(), *binary.op_kind()))
}
