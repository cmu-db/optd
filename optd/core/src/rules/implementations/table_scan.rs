use crate::ir::{
    OperatorKind,
    convert::IntoOperator,
    operator::{LogicalGet, PhysicalTableScan},
    rule::{OperatorPattern, Rule},
};

pub struct LogicalGetAsPhysicalTableScanRule {
    pattern: OperatorPattern,
}

impl Default for LogicalGetAsPhysicalTableScanRule {
    fn default() -> Self {
        Self::new()
    }
}

impl LogicalGetAsPhysicalTableScanRule {
    pub fn new() -> Self {
        let pattern =
            OperatorPattern::with_top_matches(|kind| matches!(kind, OperatorKind::LogicalGet(_)));
        Self { pattern }
    }
}

impl Rule for LogicalGetAsPhysicalTableScanRule {
    fn name(&self) -> &'static str {
        "logical_get_as_physical_table_scan"
    }

    fn pattern(&self) -> &OperatorPattern {
        &self.pattern
    }

    fn transform(
        &self,
        operator: &crate::ir::Operator,
        _ctx: &crate::ir::IRContext,
    ) -> crate::error::Result<Vec<std::sync::Arc<crate::ir::Operator>>> {
        let get = operator.try_borrow::<LogicalGet>().unwrap();
        let table_scan = PhysicalTableScan::new(
            *get.source(),
            *get.first_column(),
            get.projections().clone(),
        );
        Ok(vec![table_scan.into_operator()])
    }
}
