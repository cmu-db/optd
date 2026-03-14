use crate::ir::{
    OperatorKind,
    convert::IntoOperator,
    operator::{Get, GetImplementation},
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
        let pattern = OperatorPattern::with_top_matches(
            |kind| matches!(kind, OperatorKind::Get(meta) if meta.implementation == GetImplementation::Logical),
        );
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
        let get = operator.try_borrow::<Get>().unwrap();
        let table_scan = Get::table_scan(
            *get.data_source_id(),
            *get.table_index(),
            get.projections().clone(),
        );
        Ok(vec![table_scan.into_operator()])
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::ir::{
        catalog::DataSourceId,
        convert::IntoOperator,
        operator::{Get, GetImplementation},
    };

    use super::*;

    #[test]
    fn logical_get_as_physical_table_scan_behavior() {
        let logical_get = Get::logical(DataSourceId(1), 1, Arc::new([])).into_operator();

        let rule = LogicalGetAsPhysicalTableScanRule::new();
        assert!(rule.pattern.matches_without_expand(&logical_get));

        let after = rule
            .transform(&logical_get, &crate::ir::IRContext::with_empty_magic())
            .unwrap()
            .pop()
            .unwrap();
        let get = after.try_borrow::<Get>().unwrap();

        assert_eq!(get.data_source_id(), &DataSourceId(1));
        assert_eq!(get.table_index(), &1);
        assert!(get.projections().is_empty());
        assert_eq!(get.implementation(), &GetImplementation::TableScan);

        let table_scan = Get::table_scan(DataSourceId(1), 1, Arc::new([])).into_operator();
        assert_eq!(after.kind, table_scan.kind);
    }
}
