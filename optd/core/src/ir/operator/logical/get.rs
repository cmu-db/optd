//! The logical get operator is a scan on some data source.

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

/// Metadata: 
/// - source: The data source to scan.
/// - first_column: The columns of the data source have monotonic indices
///                 starting from this column.
/// - projections: The list of column indices to project from this table.
/// Scalars: (none)
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
