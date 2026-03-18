use crate::ir::{
    OperatorKind,
    convert::IntoOperator,
    operator::{
        Join, JoinImplementation, JoinSide, join::JoinType, split_equi_and_non_equi_conditions,
    },
    rule::{OperatorPattern, Rule},
};

pub struct LogicalJoinAsPhysicalHashJoinRule {
    pattern: OperatorPattern,
}

impl Default for LogicalJoinAsPhysicalHashJoinRule {
    fn default() -> Self {
        Self::new()
    }
}

impl LogicalJoinAsPhysicalHashJoinRule {
    pub fn new() -> Self {
        let pattern = OperatorPattern::with_top_matches(|kind| {
            matches!(
                kind,
                OperatorKind::Join(meta)
                    if meta.implementation.is_none()
                        && matches!(meta.join_type, JoinType::Inner | JoinType::LeftOuter)
            )
        });
        Self { pattern }
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
        let join = operator.try_borrow::<Join>().unwrap();

        let (equi_conds, non_equi_conds) = split_equi_and_non_equi_conditions(&join, ctx)?;

        if equi_conds.is_empty() {
            return Ok(vec![]);
        }

        debug_assert!(!non_equi_conds.is_empty() || !equi_conds.is_empty());

        Ok(vec![
            Join::new(
                *join.join_type(),
                join.outer().clone(),
                join.inner().clone(),
                join.join_cond().clone(),
                Some(JoinImplementation::hash(JoinSide::Outer, equi_conds.into())),
            )
            .into_operator(),
        ])
    }
}
