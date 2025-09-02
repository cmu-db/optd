use crate::ir::{
    OperatorKind,
    convert::IntoOperator,
    operator::{LogicalProject, PhysicalProject},
    rule::{OperatorPattern, Rule},
};

pub struct LogicalProjectAsPhysicalProjectRule {
    pattern: OperatorPattern,
}

impl Default for LogicalProjectAsPhysicalProjectRule {
    fn default() -> Self {
        Self::new()
    }
}

impl LogicalProjectAsPhysicalProjectRule {
    pub fn new() -> Self {
        let pattern = OperatorPattern::with_top_matches(|kind| {
            matches!(kind, OperatorKind::LogicalProject(_))
        });
        Self { pattern }
    }
}

impl Rule for LogicalProjectAsPhysicalProjectRule {
    fn name(&self) -> &'static str {
        "logical_project_as_physical_project"
    }

    fn pattern(&self) -> &OperatorPattern {
        &self.pattern
    }

    fn transform(
        &self,
        operator: &crate::ir::Operator,
        _ctx: &crate::ir::IRContext,
    ) -> crate::error::Result<Vec<std::sync::Arc<crate::ir::Operator>>> {
        let project = operator.try_borrow::<LogicalProject>().unwrap();
        Ok(vec![
            PhysicalProject::new(project.input().clone(), project.projections().clone())
                .into_operator(),
        ])
    }
}
