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
    LogicalGet, LogicalGetBorrowed {
        properties: OperatorProperties,
        metadata: LogicalGetMetadata {
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
impl_operator_conversion!(LogicalGet, LogicalGetBorrowed);

impl LogicalGet {
    pub fn new(source: DataSourceId, first_column: Column, projections: Arc<[usize]>) -> Self {
        Self {
            meta: LogicalGetMetadata {
                source,
                first_column,
                projections,
            },
            common: IRCommon::empty(),
        }
    }
}

impl Derive<OutputColumns> for LogicalGetBorrowed<'_> {
    fn derive_by_compute(
        &self,
        _ctx: &crate::ir::IRContext,
    ) -> <OutputColumns as crate::ir::properties::PropertyMarker>::Output {
        Arc::new(
            self.projections()
                .iter()
                .map(|x| Column(self.first_column().0 + usize::from(*x)))
                .collect(),
        )
    }
}

impl Explain for LogicalGetBorrowed<'_> {
    fn explain<'a>(
        &self,
        ctx: &crate::ir::IRContext,
        option: &crate::ir::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        let mut fields = Vec::with_capacity(2);
        fields.push((".source", Pretty::display(&self.source().0)));
        fields.extend(self.common.explain_operator_properties(ctx, option));
        Pretty::childless_record("LogicalGet", fields)
    }
}
