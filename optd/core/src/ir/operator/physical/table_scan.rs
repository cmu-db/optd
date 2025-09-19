use std::sync::Arc;

use pretty_xmlish::Pretty;

use crate::ir::{
    Column, IRCommon,
    catalog::DataSourceId,
    explain::Explain,
    macros::{define_node, impl_operator_conversion},
    properties::{Derive, OperatorProperties, OutputColumns},
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

impl Derive<OutputColumns> for PhysicalTableScanBorrowed<'_> {
    fn derive_by_compute(
        &self,
        _ctx: &crate::ir::IRContext,
    ) -> <OutputColumns as crate::ir::properties::PropertyMarker>::Output {
        Arc::new(
            self.projections()
                .iter()
                .map(|x| Column(self.first_column().0 + (*x)))
                .collect(),
        )
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
