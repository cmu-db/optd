//! The logical join operator joins two input relations based on a join
//! condition.

use crate::ir::{
    Column, IRCommon, IRContext, Operator, Scalar,
    explain::Explain,
    macros::{define_node, impl_operator_conversion},
    properties::OperatorProperties,
    scalar::{BinaryOp, BinaryOpBorrowed, ColumnRef, NaryOp},
};
use itertools::{Either, Itertools};
use pretty_xmlish::Pretty;
use std::sync::Arc;

define_node!(
    /// Metadata:
    /// - join_type: The type of join (e.g., Inner, Left, Mark, Single).
    /// Scalars:
    /// - join_cond: The join conditions to join on
    LogicalJoin, LogicalJoinBorrowed {
        properties: OperatorProperties,
        metadata: LogicalJoinMetadata {
            join_type: JoinType,
        },
        inputs: {
            operators: [outer, inner],
            scalars: [join_cond],
        }
    }
);
impl_operator_conversion!(LogicalJoin, LogicalJoinBorrowed);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JoinType {
    Inner,
    Left,
    Single,
    Mark(Column),
}

impl LogicalJoin {
    pub fn new(
        join_type: JoinType,
        outer: Arc<Operator>,
        inner: Arc<Operator>,
        join_cond: Arc<Scalar>,
    ) -> Self {
        Self {
            meta: LogicalJoinMetadata { join_type },
            common: IRCommon::new(Arc::new([outer, inner]), Arc::new([join_cond])),
        }
    }
}

type SplittedJoinConds = (Vec<(Column, Column)>, Vec<Arc<Scalar>>);

/// Best effort splitting equi join conditions from non equi join conditions.
pub fn split_equi_and_non_equi_conditions<'ir>(
    join: &LogicalJoinBorrowed<'ir>,
    ctx: &IRContext,
) -> crate::error::Result<SplittedJoinConds> {
    let outer_columns = join.outer().output_columns(ctx)?;
    let inner_columns = join.inner().output_columns(ctx)?;

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

impl Explain for LogicalJoinBorrowed<'_> {
    fn explain<'a>(
        &self,
        ctx: &crate::ir::IRContext,
        option: &crate::ir::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        let mut fields = Vec::with_capacity(3);
        fields.push((".join_type", Pretty::debug(self.join_type())));
        fields.push((".join_cond", self.join_cond().explain(ctx, option)));
        fields.extend(self.common.explain_operator_properties(ctx, option));
        let children = self.common.explain_input_operators(ctx, option);
        Pretty::simple_record("LogicalJoin", fields, children)
    }
}
