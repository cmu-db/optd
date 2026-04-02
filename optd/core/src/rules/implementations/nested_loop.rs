use crate::ir::{
    OperatorKind,
    convert::IntoOperator,
    operator::{Join, JoinImplementation},
    rule::{OperatorPattern, Rule},
};

pub struct LogicalJoinAsPhysicalNestedLoopRule {
    pattern: OperatorPattern,
}

impl Default for LogicalJoinAsPhysicalNestedLoopRule {
    fn default() -> Self {
        Self::new()
    }
}

impl LogicalJoinAsPhysicalNestedLoopRule {
    pub fn new() -> Self {
        let pattern = OperatorPattern::with_top_matches(|kind| {
            matches!(
                kind,
                OperatorKind::Join(meta)
                    if meta.implementation.is_none()
            )
        });
        Self { pattern }
    }
}

impl Rule for LogicalJoinAsPhysicalNestedLoopRule {
    fn name(&self) -> &'static str {
        "logical_join_as_physical_nested_loop"
    }

    fn pattern(&self) -> &OperatorPattern {
        &self.pattern
    }

    fn transform(
        &self,
        operator: &crate::ir::Operator,
        _ctx: &crate::ir::IRContext,
    ) -> crate::error::Result<Vec<std::sync::Arc<crate::ir::Operator>>> {
        let join = operator.try_borrow::<Join>().unwrap();
        let nl_join = Join::new(
            *join.join_type(),
            join.outer().clone(),
            join.inner().clone(),
            join.join_cond().clone(),
            Some(JoinImplementation::nested_loop()),
        );
        Ok(vec![nl_join.into_operator()])
    }
}

#[cfg(test)]
mod tests {
    use crate::ir::{
        ScalarValue,
        convert::IntoScalar,
        operator::{JoinImplementation, join::JoinType},
        scalar::Literal,
        table_ref::TableRef,
        test_utils::test_ctx_with_tables,
    };

    use super::*;

    #[test]
    fn logical_join_as_physical_nl_join_behavior() -> crate::error::Result<()> {
        let ctx = test_ctx_with_tables(&[("t1", 1), ("t2", 1)])?;
        let m_outer = ctx.table_scan(TableRef::bare("t1"), None)?.build();
        let m_inner = ctx.table_scan(TableRef::bare("t2"), None)?.build();
        let join_cond = Literal::boolean(true).into_scalar();
        let inner_join = Join::new(
            JoinType::Inner,
            m_outer.clone(),
            m_inner.clone(),
            join_cond.clone(),
            None,
        )
        .into_operator();

        let rule = LogicalJoinAsPhysicalNestedLoopRule::new();
        assert!(rule.pattern.matches_without_expand(&inner_join));
        let after = rule.transform(&inner_join, &ctx).unwrap().pop().unwrap();

        let nl_join = after.try_borrow::<Join>().unwrap();

        assert_eq!(nl_join.outer(), &m_outer);
        assert_eq!(nl_join.inner(), &m_inner);
        assert_eq!(&JoinType::Inner, nl_join.join_type());
        assert_eq!(
            nl_join.implementation(),
            &Some(JoinImplementation::nested_loop())
        );
        assert_eq!(
            &ScalarValue::Boolean(Some(true)),
            nl_join.join_cond().try_borrow::<Literal>().unwrap().value()
        );
        Ok(())
    }
}
