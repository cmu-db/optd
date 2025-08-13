use tracing::info;

use crate::ir::{
    IRContext, OperatorKind,
    operator::{LogicalJoin, LogicalSelect},
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
        let mut pattern = OperatorPattern::with_top_matches(|kind| {
            matches!(kind, OperatorKind::LogicalSelect(_))
        });
        pattern.add_input_operator_pattern(
            INPUT,
            OperatorPattern::with_top_matches(|kind| matches!(kind, OperatorKind::LogicalJoin(_))),
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
    ) -> Result<Vec<std::sync::Arc<crate::ir::Operator>>, ()> {
        let select = operator.try_borrow::<LogicalSelect>().unwrap();
        let join = select.input().try_borrow::<LogicalJoin>().unwrap();

        let outer = join.outer().clone();
        let inner = join.inner().clone();

        let used_columns = select.predicate().used_columns();
        let is_bound_by_outer = used_columns.is_subset(&*outer.output_columns(ctx));
        let is_bound_by_inner = used_columns.is_subset(&*inner.output_columns(ctx));

        let maybe_transformed = match (is_bound_by_outer, is_bound_by_inner) {
            (false, false) => None,
            (true, false) => Some(
                outer
                    .logical_select(select.predicate().clone())
                    .logical_join(inner, join.join_cond().clone(), join.join_type().clone()),
            ),
            (false, true) => Some(outer.logical_join(
                inner.logical_select(select.predicate().clone()),
                join.join_cond().clone(),
                join.join_type().clone(),
            )),
            // Wrong: false,
            (true, true) => Some(select.input().clone()),
        };

        Ok(maybe_transformed.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn logical_select_join_transpose_behavior() {
        todo!()
    }
}
