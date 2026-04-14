//! The get operator is a scan on some data source. Its metadata tracks whether
//! the operator is still logical or has been lowered to a physical scan.

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
    Get, GetBorrowed {
        properties: OperatorProperties,
        metadata: GetMetadata {
            data_source_id: DataSourceId,
            table_index: i64,
            projections: Arc<[usize]>,
            implementation: Option<GetImplementation>,
        },
        inputs: {
            operators: [],
            scalars: [],
        }
    }
);
impl_operator_conversion!(Get, GetBorrowed);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum GetImplementation {
    #[default]
    TableScan,
}

impl Get {
    pub fn new(
        data_source_id: DataSourceId,
        table_index: i64,
        projections: Arc<[usize]>,
        implementation: Option<GetImplementation>,
    ) -> Self {
        Self {
            meta: GetMetadata {
                data_source_id,
                table_index,
                projections,
                implementation,
            },
            common: IRCommon::empty(),
        }
    }

    pub fn logical(
        data_source_id: DataSourceId,
        table_index: i64,
        projections: Arc<[usize]>,
    ) -> Self {
        Self::new(data_source_id, table_index, projections, None)
    }

    pub fn table_scan(
        data_source_id: DataSourceId,
        table_index: i64,
        projections: Arc<[usize]>,
    ) -> Self {
        Self::new(
            data_source_id,
            table_index,
            projections,
            Some(GetImplementation::TableScan),
        )
    }
}

impl Explain for GetBorrowed<'_> {
    fn explain<'a>(
        &self,
        ctx: &crate::ir::IRContext,
        option: &crate::ir::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        let mut fields = Vec::with_capacity(5);
        fields.push((".data_source_id", Pretty::display(&self.data_source_id().0)));
        fields.push((".table_index", Pretty::display(&self.table_index())));
        fields.push((".implementation", Pretty::debug(self.implementation())));
        fields.extend(self.common.explain_operator_properties(ctx, option));
        Pretty::childless_record("Get", fields)
    }
}
