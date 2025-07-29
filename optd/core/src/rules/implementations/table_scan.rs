use crate::ir::{
    OperatorKind,
    convert::IntoOperator,
    operator::{LogicalGet, PhysicalTableScan},
    rule::{OperatorPattern, Rule},
};

pub struct LogicalGetAsPhysicalTableScan {
    pattern: OperatorPattern,
}

impl Default for LogicalGetAsPhysicalTableScan {
    fn default() -> Self {
        Self::new()
    }
}

impl LogicalGetAsPhysicalTableScan {
    pub fn new() -> Self {
        let pattern =
            OperatorPattern::with_top_matches(|kind| matches!(kind, OperatorKind::LogicalGet(_)));
        Self { pattern }
    }
}

impl Rule for LogicalGetAsPhysicalTableScan {
    fn name(&self) -> &'static str {
        "logical_get_as_physical_table_scan"
    }

    fn pattern(&self) -> &OperatorPattern {
        &self.pattern
    }

    fn transform(
        &self,
        operator: &crate::ir::Operator,
    ) -> Result<Vec<std::sync::Arc<crate::ir::Operator>>, ()> {
        let get = operator.try_bind_ref::<LogicalGet>().unwrap();
        let table_scan = PhysicalTableScan::new(*get.table_id(), get.projection_list().clone());
        Ok(vec![table_scan.into_operator()])
    }
}
