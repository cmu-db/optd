use crate::ir::{
    OperatorKind,
    convert::IntoOperator,
    operator::{LogicalJoin, PhysicalNLJoin},
    rule::{OperatorPattern, Rule},
};

pub struct LogicalJoinAsPhysicalNLJoinRule {
    pattern: OperatorPattern,
}

impl Default for LogicalJoinAsPhysicalNLJoinRule {
    fn default() -> Self {
        Self::new()
    }
}

impl LogicalJoinAsPhysicalNLJoinRule {
    pub fn new() -> Self {
        let pattern =
            OperatorPattern::with_top_matches(|kind| matches!(kind, OperatorKind::LogicalJoin(_)));
        Self { pattern }
    }
}

impl Rule for LogicalJoinAsPhysicalNLJoinRule {
    fn name(&self) -> &'static str {
        "logical_join_as_physical_nl_join"
    }

    fn pattern(&self) -> &OperatorPattern {
        &self.pattern
    }

    fn transform(
        &self,
        operator: &crate::ir::Operator,
        _ctx: &crate::ir::IRContext,
    ) -> Result<Vec<std::sync::Arc<crate::ir::Operator>>, ()> {
        let join = operator.try_bind_ref::<LogicalJoin>().unwrap();
        let nl_join = PhysicalNLJoin::new(
            *join.join_type(),
            join.outer().clone(),
            join.inner().clone(),
            join.join_cond().clone(),
        );
        Ok(vec![nl_join.into_operator()])
    }
}

#[cfg(test)]
mod tests {
    use crate::ir::{
        IRContext, ScalarValue,
        convert::IntoScalar,
        operator::{MockScan, MockSpec, join::JoinType},
        scalar::Literal,
    };

    use super::*;

    #[test]
    fn logical_join_as_physical_nl_join_behavior() {
        let ctx = IRContext::with_empty_magic();
        let m_outer = MockScan::with_mock_spec(1, MockSpec::default()).into_operator();
        let m_inner = MockScan::with_mock_spec(2, MockSpec::default()).into_operator();
        let join_cond = Literal::boolean(true).into_scalar();
        let inner_join = LogicalJoin::new(
            JoinType::Inner,
            m_outer.clone(),
            m_inner.clone(),
            join_cond.clone(),
        )
        .into_operator();

        let rule = LogicalJoinAsPhysicalNLJoinRule::new();
        assert!(rule.pattern.matches_without_expand(&inner_join));
        let nl_join = rule
            .transform(&inner_join, &ctx)
            .unwrap()
            .pop()
            .unwrap()
            .try_bind_ref::<PhysicalNLJoin>()
            .unwrap();

        assert_eq!(
            &1,
            nl_join
                .outer()
                .try_bind_ref::<MockScan>()
                .unwrap()
                .mock_id()
        );
        assert_eq!(
            &2,
            nl_join
                .inner()
                .try_bind_ref::<MockScan>()
                .unwrap()
                .mock_id()
        );
        assert_eq!(&JoinType::Inner, nl_join.join_type());
        assert_eq!(
            &ScalarValue::Boolean(Some(true)),
            nl_join
                .join_cond()
                .try_bind_ref::<Literal>()
                .unwrap()
                .value()
        );
    }
}
