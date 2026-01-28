use std::sync::Arc;

use itertools::{Either, Itertools};

use crate::ir::{
    Column, IRContext, OperatorKind, Scalar,
    builder::boolean,
    convert::IntoScalar,
    operator::{LogicalJoin, LogicalJoinBorrowed, join::JoinType},
    rule::{OperatorPattern, Rule},
    scalar::{BinaryOp, BinaryOpBorrowed, ColumnRef, NaryOp, NaryOpKind},
};

pub struct LogicalJoinAsPhysicalHashJoinRule {
    pattern: OperatorPattern,
}

impl Default for LogicalJoinAsPhysicalHashJoinRule {
    fn default() -> Self {
        Self::new()
    }
}

type SplittedJoinConds = (Vec<(Column, Column)>, Vec<Arc<Scalar>>);

impl LogicalJoinAsPhysicalHashJoinRule {
    pub fn new() -> Self {
        let pattern = OperatorPattern::with_top_matches(|kind| {
            matches!(
                kind,
                OperatorKind::LogicalJoin(meta)
                    if matches!(meta.join_type, JoinType::Inner | JoinType::Left)
            )
        });
        Self { pattern }
    }

    /// Best effort splitting equi join conditions from non equi join conditions.
    fn split_equi_and_non_equi_conditions<'ir>(
        join: &LogicalJoinBorrowed<'ir>,
        ctx: &IRContext,
    ) -> crate::error::Result<SplittedJoinConds> {
        let outer_columns = join.outer().output_columns(ctx);
        let inner_columns = join.inner().output_columns(ctx);

        let maybe_get_join_keys = |eq: BinaryOpBorrowed<'_>| {
            let lhs = eq.lhs().try_borrow::<ColumnRef>().ok()?;
            let rhs = eq.rhs().try_borrow::<ColumnRef>().ok()?;
            match (
                outer_columns.contains(lhs.column()),
                outer_columns.contains(rhs.column()),
                inner_columns.contains(lhs.column()),
                inner_columns.contains(rhs.column()),
            ) {
                (true, false, false, true) => Some((*lhs.column(), *rhs.column())),
                (false, true, true, false) => Some((*rhs.column(), *lhs.column())),
                _ => None,
            }
        };
        if let Ok(binary_op) = join.join_cond().try_borrow::<BinaryOp>()
            && binary_op.is_eq()
        {
            match maybe_get_join_keys(binary_op) {
                // Singleton equi-conditions.
                Some(keys) => Ok((vec![keys], vec![])),
                None => Ok((vec![], vec![join.join_cond().clone()])),
            }
        } else if let Ok(nary_op) = join.join_cond().try_borrow::<NaryOp>()
            && nary_op.is_and()
        {
            Ok(nary_op.terms().iter().partition_map(|term| {
                if let Ok(binary_op) = term.try_borrow::<BinaryOp>()
                    && binary_op.is_eq()
                {
                    match maybe_get_join_keys(binary_op) {
                        Some(keys) => Either::Left(keys),
                        None => Either::Right(term.clone()),
                    }
                } else {
                    Either::Right(term.clone())
                }
            }))
        } else {
            // Default case, put the entire join cond into the non-equi condition.
            Ok((vec![], vec![join.join_cond().clone()]))
        }
    }
}

impl Rule for LogicalJoinAsPhysicalHashJoinRule {
    fn name(&self) -> &'static str {
        "logical_join_as_physical_hash_join"
    }

    fn pattern(&self) -> &OperatorPattern {
        &self.pattern
    }

    fn transform(
        &self,
        operator: &crate::ir::Operator,
        ctx: &crate::ir::IRContext,
    ) -> crate::error::Result<Vec<std::sync::Arc<crate::ir::Operator>>> {
        let join = operator.try_borrow::<LogicalJoin>().unwrap();

        let (equi_conds, non_equi_conds) = Self::split_equi_and_non_equi_conditions(&join, ctx)?;

        if equi_conds.is_empty() {
            return Ok(vec![]);
        }

        let build_side = join.outer().clone();
        let probe_side = join.inner().clone();

        let non_equi_conds = match &non_equi_conds[..] {
            [] => boolean(true),
            [singleton] => singleton.clone(),
            terms => NaryOp::new(NaryOpKind::And, terms.into()).into_scalar(),
        };

        Ok(vec![build_side.hash_join(
            probe_side,
            equi_conds.into(),
            non_equi_conds,
            *join.join_type(),
        )])
    }
}
