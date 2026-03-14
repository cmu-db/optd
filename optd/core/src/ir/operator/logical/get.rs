//! The logical get operator is a scan on some data source.

use crate::ir::{
    IRCommon,
    catalog::DataSourceId,
    explain::Explain,
    macros::{define_node, impl_operator_conversion},
    properties::OperatorProperties,
};
use pretty_xmlish::Pretty;
use std::sync::Arc;

define_node!(
    /// Metadata:
    /// - source: The data source to scan.
    /// - first_column: The columns of the data source have monotonic indices
    ///                 starting from this column.
    /// - projections: The list of column indices to project from this table.
    /// Scalars: (none)
    LogicalGet, LogicalGetBorrowed {
        properties: OperatorProperties,
        metadata: LogicalGetMetadata {
            data_source_id: DataSourceId,
            table_index: i64,
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
    pub fn new(data_source_id: DataSourceId, table_index: i64, projections: Arc<[usize]>) -> Self {
        Self {
            meta: LogicalGetMetadata {
                data_source_id,
                table_index,
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
        let mut fields = Vec::with_capacity(3);
        fields.push((".data_source_id", Pretty::display(&self.data_source_id().0)));
        fields.push((".table_index", Pretty::display(&self.table_index())));
        fields.extend(self.common.explain_operator_properties(ctx, option));
        Pretty::childless_record("LogicalGet", fields)
    }
}
