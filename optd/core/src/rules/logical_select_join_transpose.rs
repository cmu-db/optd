use crate::ir::{
    IRContext, OperatorKind,
    operator::{Join, Select, join::JoinType},
    rule::{OperatorPattern, Rule},
};

pub struct LogicalSelectJoinTransposeRule {
    pattern: OperatorPattern,
}

impl Default for LogicalSelectJoinTransposeRule {
    fn default() -> Self {
        Self::new()
    }
}

impl LogicalSelectJoinTransposeRule {
    pub fn new() -> Self {
        const INPUT: usize = 0;
        let mut pattern =
            OperatorPattern::with_top_matches(|kind| matches!(kind, OperatorKind::Select(_)));
        pattern.add_input_operator_pattern(
            INPUT,
            OperatorPattern::with_top_matches(|kind| {
                matches!(
                    kind,
                    OperatorKind::Join(meta)
                        if meta.implementation.is_none()
                            && matches!(meta.join_type, JoinType::Inner | JoinType::Left)
                )
            }),
        );
        Self { pattern }
    }
}

impl Rule for LogicalSelectJoinTransposeRule {
    fn name(&self) -> &'static str {
        "logical_select_join_transpose"
    }

    fn pattern(&self) -> &OperatorPattern {
        &self.pattern
    }

    fn transform(
        &self,
        operator: &crate::ir::Operator,
        ctx: &IRContext,
    ) -> crate::error::Result<Vec<std::sync::Arc<crate::ir::Operator>>> {
        let select = operator.try_borrow::<Select>().unwrap();
        let join = select.input().try_borrow::<Join>().unwrap();

        let outer = join.outer().clone();
        let inner = join.inner().clone();

        let used_columns = select.predicate().used_columns();
        let outer_output_columns = outer.output_columns(ctx)?;
        let inner_output_columns = inner.output_columns(ctx)?;
        let is_bound_by_outer = used_columns.is_subset(outer_output_columns.as_ref());
        let is_bound_by_inner = used_columns.is_subset(inner_output_columns.as_ref());

        let maybe_transformed = match (is_bound_by_outer, is_bound_by_inner) {
            (false, false) => None,
            (true, false) => Some(
                outer
                    .logical_select(select.predicate().clone())
                    .logical_join(inner, join.join_cond().clone(), *join.join_type()),
            ),
            (false, true) => Some(outer.logical_join(
                inner.logical_select(select.predicate().clone()),
                join.join_cond().clone(),
                *join.join_type(),
            )),
            // Wrong: false,
            (true, true) => None,
        };

        Ok(maybe_transformed.into_iter().collect())
    }
}
