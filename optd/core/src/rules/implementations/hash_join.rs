use crate::ir::{
    OperatorKind,
    builder::boolean,
    convert::IntoScalar,
    operator::{LogicalJoin, join::JoinType, split_equi_and_non_equi_conditions},
    rule::{OperatorPattern, Rule},
    scalar::{NaryOp, NaryOpKind},
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
                OperatorKind::LogicalJoin(meta)
                    if matches!(meta.join_type, JoinType::Inner | JoinType::Left)
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
        let join = operator.try_borrow::<LogicalJoin>().unwrap();

        let (equi_conds, non_equi_conds) = split_equi_and_non_equi_conditions(&join, ctx)?;

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
