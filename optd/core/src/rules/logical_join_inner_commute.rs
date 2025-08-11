use crate::ir::{
    OperatorKind,
    convert::IntoOperator,
    operator::{LogicalJoin, join::JoinType},
    rule::{OperatorPattern, Rule},
};

/// Commutes the two inputs of a logical inner join.
///
///
/// * LogicalJoin (JoinType::Inner, **a**, **b**, cond)
/// * => LogicalJoin (JoinType::Inner, **b**, **a**, cond)
pub struct LogicalJoinInnerCommuteRule {
    pattern: OperatorPattern,
}

impl Default for LogicalJoinInnerCommuteRule {
    fn default() -> Self {
        Self::new()
    }
}

impl LogicalJoinInnerCommuteRule {
    pub fn new() -> Self {
        let pattern = OperatorPattern::with_top_matches(
            |kind| matches!(kind, OperatorKind::LogicalJoin(meta) if meta.join_type == JoinType::Inner),
        );
        Self { pattern }
    }
}

impl Rule for LogicalJoinInnerCommuteRule {
    fn name(&self) -> &'static str {
        "logical_join_inner_commute"
    }

    fn pattern(&self) -> &OperatorPattern {
        &self.pattern
    }

    fn transform(
        &self,
        operator: &crate::ir::Operator,
        _ctx: &crate::ir::IRContext,
    ) -> Result<Vec<std::sync::Arc<crate::ir::Operator>>, ()> {
        let join = operator.try_borrow::<LogicalJoin>().unwrap();
        assert_eq!(join.join_type(), &JoinType::Inner);

        let new_outer = join.inner().clone();
        let new_inner = join.outer().clone();
        let join_commuted = LogicalJoin::new(
            JoinType::Inner,
            new_outer,
            new_inner,
            join.join_cond().clone(),
        );
        Ok(vec![join_commuted.into_operator()])
    }
}

#[cfg(test)]
mod tests {
    use crate::ir::{
        IRContext,
        convert::IntoScalar,
        operator::{MockScan, MockSpec},
        scalar::Literal,
    };

    use super::*;

    #[test]
    fn logical_join_inner_commute_behavior() {
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

        let rule = LogicalJoinInnerCommuteRule::new();
        assert!(rule.pattern.matches_without_expand(&inner_join));
        let ctx = IRContext::with_empty_magic();
        let res = rule.transform(&inner_join, &ctx).unwrap().pop().unwrap();
        let commuted = res.try_borrow::<LogicalJoin>().unwrap();

        let new_outer = commuted.outer().try_borrow::<MockScan>().unwrap();
        let new_inner = commuted.inner().try_borrow::<MockScan>().unwrap();

        assert_eq!(new_outer.mock_id(), &2);
        assert_eq!(new_inner.mock_id(), &1);

        let left_outer_join =
            LogicalJoin::new(JoinType::Left, m_outer, m_inner, join_cond).into_operator();
        assert!(!rule.pattern.matches_without_expand(&left_outer_join));
    }
}
