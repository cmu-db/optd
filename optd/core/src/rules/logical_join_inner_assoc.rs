use crate::ir::{
    IRContext, OperatorKind,
    operator::{LogicalJoin, join::JoinType},
    rule::{OperatorPattern, Rule},
};

/// Applies right associativity to two inner join operators.
/// ((a JOIN b) JOIN c) → (a JOIN (b JOIN c))
pub struct LogicalJoinInnerAssocRule {
    pattern: OperatorPattern,
}

impl Default for LogicalJoinInnerAssocRule {
    fn default() -> Self {
        Self::new()
    }
}

impl LogicalJoinInnerAssocRule {
    pub fn new() -> Self {
        const OUTER: usize = 0;
        let is_inner_join = |kind: &OperatorKind| matches!(kind, OperatorKind::LogicalJoin(meta) if meta.join_type == JoinType::Inner);
        let mut pattern = OperatorPattern::with_top_matches(is_inner_join);
        pattern.add_input_operator_pattern(OUTER, OperatorPattern::with_top_matches(is_inner_join));
        Self { pattern }
    }
}

impl Rule for LogicalJoinInnerAssocRule {
    fn name(&self) -> &'static str {
        "logical_join_inner_assoc"
    }

    fn pattern(&self) -> &OperatorPattern {
        &self.pattern
    }

    fn transform(
        &self,
        operator: &crate::ir::Operator,
        ctx: &IRContext,
    ) -> Result<Vec<std::sync::Arc<crate::ir::Operator>>, ()> {
        // ((a JOIN b, cond_low) JOIN c, cond_up) → (a JOIN (b JOIN c, cond_up), cond_low)
        let join_upper = operator.try_borrow::<LogicalJoin>().unwrap();
        assert_eq!(join_upper.join_type(), &JoinType::Inner);
        let join_lower = join_upper.outer().try_borrow::<LogicalJoin>().unwrap();
        assert_eq!(join_lower.join_type(), &JoinType::Inner);

        let a = join_lower.outer().clone();
        let b = join_lower.inner().clone();
        let c = join_upper.inner().clone();

        if !join_upper
            .join_cond()
            .used_columns()
            .is_subset(&(&*b.output_columns(ctx) & &*c.output_columns(ctx)))
        {
            return Ok(vec![]);
        }

        let new_join_upper = a.logical_join(
            b.logical_join(c, join_upper.join_cond().clone(), JoinType::Inner),
            join_lower.join_cond().clone(),
            JoinType::Inner,
        );
        Ok(vec![new_join_upper])
    }
}

#[cfg(test)]
mod tests {
    use crate::ir::{
        ScalarValue,
        convert::IntoScalar,
        operator::{MockScan, MockSpec},
        scalar::Literal,
    };

    use super::*;

    #[test]
    fn logical_join_inner_assoc_behavior() {
        let a = MockScan::with_mock_spec(1, MockSpec::default()).into_operator();
        let b = MockScan::with_mock_spec(2, MockSpec::default()).into_operator();
        let c = MockScan::with_mock_spec(3, MockSpec::default()).into_operator();
        let cond_upper = Literal::boolean(true).into_scalar();
        let cond_lower = Literal::boolean(false).into_scalar();
        let join_ab = LogicalJoin::new(JoinType::Inner, a.clone(), b.clone(), cond_lower.clone())
            .into_operator();
        let inner_joins = LogicalJoin::new(
            JoinType::Inner,
            join_ab.clone(),
            c.clone(),
            cond_upper.clone(),
        )
        .into_operator();

        let ctx = IRContext::with_empty_magic();
        let rule = LogicalJoinInnerAssocRule::new();
        assert!(rule.pattern.matches_without_expand(&inner_joins));
        let res = rule.transform(&inner_joins, &ctx).unwrap().pop().unwrap();
        let new_upper = res.try_borrow::<LogicalJoin>().unwrap();
        let a_ref = new_upper.outer().try_borrow::<MockScan>().unwrap();

        let new_lower = new_upper.inner().try_borrow::<LogicalJoin>().unwrap();
        let b_ref = new_lower.outer().try_borrow::<MockScan>().unwrap();
        let c_ref = new_lower.inner().try_borrow::<MockScan>().unwrap();

        assert_eq!(&1, a_ref.mock_id());
        assert_eq!(&2, b_ref.mock_id());
        assert_eq!(&3, c_ref.mock_id(),);
        assert_eq!(
            &ScalarValue::Boolean(Some(false)),
            new_upper
                .join_cond()
                .try_borrow::<Literal>()
                .unwrap()
                .value()
        );

        assert_eq!(
            &ScalarValue::Boolean(Some(true)),
            new_lower
                .join_cond()
                .try_borrow::<Literal>()
                .unwrap()
                .value()
        );

        // This rule does not apply to left outer joins.
        let left_outer_joins = LogicalJoin::new(
            JoinType::Left,
            {
                LogicalJoin::new(JoinType::Left, a.clone(), b.clone(), cond_lower.clone())
                    .into_operator()
            },
            c.clone(),
            cond_upper.clone(),
        )
        .into_operator();
        assert!(!rule.pattern.matches_without_expand(&left_outer_joins));

        // Join between two non join operator does not qualify.
        assert!(!rule.pattern.matches_without_expand(&join_ab));
    }
}
