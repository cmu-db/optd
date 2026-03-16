use crate::ir::{
    OperatorKind,
    convert::IntoOperator,
    operator::Get,
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
            |kind| matches!(kind, OperatorKind::Get(meta) if meta.implementation.is_none()),
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
        operator::{Get, GetImplementation},
        table_ref::TableRef,
        test_utils::test_ctx_with_tables,
    };

    use super::*;

    #[test]
    fn logical_get_as_physical_table_scan_behavior() -> crate::error::Result<()> {
        let ctx = test_ctx_with_tables(&[("t1", 1)])?;
        let logical_get = ctx
            .logical_get(TableRef::bare("t1"), Some(Arc::new([])))?
            .build();
        let logical_get_borrowed = logical_get.try_borrow::<Get>().unwrap();

        let rule = LogicalGetAsPhysicalTableScanRule::new();
        assert!(rule.pattern.matches_without_expand(&logical_get));

        let after = rule.transform(&logical_get, &ctx).unwrap().pop().unwrap();
        let get = after.try_borrow::<Get>().unwrap();

        assert_eq!(get.data_source_id(), logical_get_borrowed.data_source_id());
        assert_eq!(get.table_index(), logical_get_borrowed.table_index());
        assert!(get.projections().is_empty());
        assert_eq!(get.implementation(), &Some(GetImplementation::TableScan));
        assert_eq!(get.projections(), logical_get_borrowed.projections());
        Ok(())
    }
}
