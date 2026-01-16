//! The table scan operator is a scan on some table data - as one implementation
//! of the logical get operator.

use std::sync::Arc;
use pretty_xmlish::Pretty;
use crate::ir::{
    Column, IRCommon,
    catalog::DataSourceId,
    explain::Explain,
    macros::{define_node, impl_operator_conversion},
    properties::{OperatorProperties},
};

define_node!(
    PhysicalTableScan, PhysicalTableScanBorrowed {
        properties: OperatorProperties,
        metadata: PhysicalTableScanMetadata {
            source: DataSourceId,
            first_column: Column,
            projections: Arc<[usize]>,
        },
        inputs: {
            operators: [],
            scalars: [],
        }
    }
);
impl_operator_conversion!(PhysicalTableScan, PhysicalTableScanBorrowed);

/// Metadata: 
/// - source: The data source to scan.
/// - first_column: The columns of the data source have monotonic indices
///                 starting from this column.
/// - projections: The list of column indices to project from this table.
/// Scalars: (none)
impl PhysicalTableScan {
    pub fn new(source: DataSourceId, first_column: Column, projections: Arc<[usize]>) -> Self {
        Self {
            meta: PhysicalTableScanMetadata {
                source,
                first_column,
                projections,
            },
            common: IRCommon::empty(),
        }
    }
}

impl Explain for PhysicalTableScanBorrowed<'_> {
    fn explain<'a>(
        &self,
        ctx: &crate::ir::IRContext,
        option: &crate::ir::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        let mut fields = Vec::with_capacity(2);
        fields.push((".source", Pretty::display(&self.source().0)));
        fields.extend(self.common.explain_operator_properties(ctx, option));
        Pretty::childless_record("PhysicalTableScan", fields)
    }
}
